const Driver = require('../models/Driver');

function shuffle(items) {
  const copy = [...items];
  for (let i = copy.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [copy[i], copy[j]] = [copy[j], copy[i]];
  }
  return copy;
}

// Reserves one AVAILABLE driver for a ride, skipping drivers already offered this ride.
//
// Each reservation is a single atomic Mongo update guarded by `status: 'AVAILABLE'`.
// If two workers pick the same driver at the same moment, only one update matches;
// the other gets null and moves on to the next candidate. A driver can therefore
// never be reserved by two rides at once.
//
// Returns the claimed driver, or null if no untried driver is currently AVAILABLE.
async function claimDriver(rideId, excludeDriverIds = []) {
  const candidates = await Driver.find({
    status: 'AVAILABLE',
    driverId: { $nin: excludeDriverIds },
  })
    .select('driverId')
    .lean();

  for (const candidate of shuffle(candidates)) {
    const claimed = await Driver.findOneAndUpdate(
      { driverId: candidate.driverId, status: 'AVAILABLE' },
      { $set: { status: 'BUSY', currentRideId: rideId } },
      { new: true }
    ).lean();
    if (claimed) return claimed;
  }
  return null;
}

// Frees a driver, but only if they are still held by this ride. A late or duplicate
// release job can therefore never free a driver who has since moved on to another ride.
async function releaseDriver(driverId, rideId) {
  const result = await Driver.updateOne(
    { driverId, currentRideId: rideId },
    { $set: { status: 'AVAILABLE', currentRideId: null } }
  );
  return result.modifiedCount === 1;
}

// Frees any driver reserved by this ride. Used to clean up after a crashed attempt.
async function releaseDriversHeldBy(rideId) {
  const result = await Driver.updateMany(
    { currentRideId: rideId },
    { $set: { status: 'AVAILABLE', currentRideId: null } }
  );
  return result.modifiedCount;
}

module.exports = { claimDriver, releaseDriver, releaseDriversHeldBy };
