const mongoose = require('mongoose');

const RIDE_STATUSES = ['REQUESTED', 'ASSIGNED', 'NO_DRIVER_FOUND'];

const rideSchema = new mongoose.Schema(
  {
    rideId: { type: String, required: true, unique: true, index: true },
    riderId: { type: String, required: true },
    pickup: { type: String, required: true },
    dropoff: { type: String, required: true },

    status: { type: String, enum: RIDE_STATUSES, default: 'REQUESTED', index: true },
    assignedDriverId: { type: String, default: null },

    // Drivers already offered this ride, so the same driver is never offered it twice.
    triedDrivers: { type: [String], default: [] },
    // Drivers who said no. Reaching the limit (3) ends in NO_DRIVER_FOUND.
    rejectionCount: { type: Number, default: 0 },
    // Times the worker found every untried driver BUSY and re-queued the ride.
    waitAttempts: { type: Number, default: 0 },
  },
  { timestamps: true }
);

module.exports = mongoose.model('Ride', rideSchema);
module.exports.RIDE_STATUSES = RIDE_STATUSES;
