const Ride = require('../models/Ride');
const { publishRideEvent } = require('../events');

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Publishes a status event, retrying a few times if the broker is briefly unavailable.
async function publishStatusEvent(payload, attempts = 3) {
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await publishRideEvent(payload);
    } catch (err) {
      console.error(
        `[events] publish failed for ride ${payload.rideId} (attempt ${attempt}/${attempts}): ${err.message}`
      );
      if (attempt === attempts) throw err;
      await sleep(200 * attempt);
    }
  }
  return null;
}

// Moves a ride out of REQUESTED into a final status, and publishes the matching event.
//
// The update is one atomic Mongo operation with the filter `status: 'REQUESTED'`.
// If two workers (or a redelivered job) both try to finish the same ride, exactly one
// update matches and the other gets null. That is what makes double assignment impossible.
//
// Returns the updated ride, or null if the ride was no longer REQUESTED.
async function transitionRide(rideId, toStatus, extraSet = {}) {
  const ride = await Ride.findOneAndUpdate(
    { rideId, status: 'REQUESTED' },
    { $set: { status: toStatus, ...extraSet } },
    { new: true }
  ).lean();

  if (!ride) return null;

  await publishStatusEvent({
    rideId,
    status: toStatus,
    driverId: ride.assignedDriverId || null,
  });

  return ride;
}

module.exports = { publishStatusEvent, transitionRide };
