const express = require('express');
const { randomUUID } = require('crypto');
const Ride = require('./models/Ride');
const { enqueueRide } = require('./queue');
const { publishStatusEvent, transitionRide } = require('./services/rideService');

function isNonEmptyString(value) {
  return typeof value === 'string' && value.trim().length > 0;
}

function createApp() {
  const app = express();
  app.use(express.json());

  app.get('/health', (req, res) => res.json({ ok: true }));

  // Book a ride. Returns a rideId immediately; the worker assigns a driver later.
  app.post('/rides', async (req, res, next) => {
    try {
      const body = req.body || {};
      const invalid = ['riderId', 'pickup', 'dropoff'].filter((field) => !isNonEmptyString(body[field]));
      if (invalid.length > 0) {
        return res.status(400).json({ error: 'Missing or invalid fields', fields: invalid });
      }

      const rideId = randomUUID();
      await Ride.create({
        rideId,
        riderId: body.riderId.trim(),
        pickup: body.pickup.trim(),
        dropoff: body.dropoff.trim(),
        status: 'REQUESTED',
      });

      try {
        // The REQUESTED event goes out before the job is queued, so no later event
        // (ASSIGNED / NO_DRIVER_FOUND) can ever overtake it.
        await publishStatusEvent({ rideId, status: 'REQUESTED' });
        await enqueueRide(rideId);
      } catch (err) {
        console.error(`[api] could not queue ride ${rideId}: ${err.message}`);
        // Never leave a ride without a final status.
        await transitionRide(rideId, 'NO_DRIVER_FOUND').catch(() => {});
        return res.status(503).json({ error: 'Could not queue ride, please retry', rideId });
      }

      return res.status(202).json({ rideId, status: 'REQUESTED' });
    } catch (err) {
      return next(err);
    }
  });

  // Check the current state of a ride.
  app.get('/rides/:id', async (req, res, next) => {
    try {
      const ride = await Ride.findOne({ rideId: req.params.id }).lean();
      if (!ride) return res.status(404).json({ error: 'Ride not found' });

      return res.json({
        rideId: ride.rideId,
        riderId: ride.riderId,
        pickup: ride.pickup,
        dropoff: ride.dropoff,
        status: ride.status,
        assignedDriverId: ride.assignedDriverId,
        rejectionCount: ride.rejectionCount,
        waitAttempts: ride.waitAttempts,
        createdAt: ride.createdAt,
        updatedAt: ride.updatedAt,
      });
    } catch (err) {
      return next(err);
    }
  });

  // eslint-disable-next-line no-unused-vars
  app.use((err, req, res, next) => {
    console.error('[api] unhandled error:', err);
    res.status(500).json({ error: 'Internal server error' });
  });

  return app;
}

module.exports = { createApp };
