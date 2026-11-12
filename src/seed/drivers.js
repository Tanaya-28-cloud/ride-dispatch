const Driver = require('../models/Driver');

const NAMES = [
  'Aarav', 'Bhavna', 'Chetan', 'Divya', 'Eshan',
  'Farhan', 'Gauri', 'Harsh', 'Isha', 'Jayesh',
];

// Hardcoded list of 10 fake drivers: D01 .. D10
const DRIVERS = NAMES.map((name, i) => ({
  driverId: `D${String(i + 1).padStart(2, '0')}`,
  name,
}));

// Inserts any missing drivers. Existing drivers are left untouched, so restarting
// the API never frees a driver who is mid-trip.
async function seedDrivers() {
  await Driver.bulkWrite(
    DRIVERS.map((driver) => ({
      updateOne: {
        filter: { driverId: driver.driverId },
        update: { $setOnInsert: { ...driver, status: 'AVAILABLE', currentRideId: null } },
        upsert: true,
      },
    }))
  );
}

// Puts every driver back to AVAILABLE. Used by the load test and the Jest tests.
async function resetDrivers() {
  await Driver.updateMany({}, { $set: { status: 'AVAILABLE', currentRideId: null } });
}

module.exports = { DRIVERS, seedDrivers, resetDrivers };
