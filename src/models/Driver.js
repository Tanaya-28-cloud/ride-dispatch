const mongoose = require('mongoose');

const DRIVER_STATUSES = ['AVAILABLE', 'BUSY'];

const driverSchema = new mongoose.Schema(
  {
    driverId: { type: String, required: true, unique: true, index: true },
    name: { type: String, required: true },
    status: { type: String, enum: DRIVER_STATUSES, default: 'AVAILABLE', index: true },
    // The ride this driver is currently serving (null when AVAILABLE).
    currentRideId: { type: String, default: null },
  },
  { timestamps: true }
);

module.exports = mongoose.model('Driver', driverSchema);
module.exports.DRIVER_STATUSES = DRIVER_STATUSES;
