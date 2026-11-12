const { Worker, DelayedError } = require('bullmq');
const config = require('./config');
const { connectDb, disconnectDb } = require('./db');
const Ride = require('./models/Ride');
const { seedDrivers } = require('./seed/drivers');
const { connection, enqueueDriverRelease, closeQueues } = require('./queue');
const { getChannel, closeEvents } = require('./events');
const { transitionRide } = require('./services/rideService');
const {
  claimDriver,
  releaseDriver,
  releaseDriversHeldBy,
} = require('./services/driverService');

const randomBetween = (min, max) => min + Math.floor(Math.random() * (max - min + 1));

// Frees the driver again after a short random "trip" time (the driver stays BUSY meanwhile).
function scheduleRelease(driverId, rideId) {
  const { busyMinMs, busyMaxMs } = config.dispatch;
  return enqueueDriverRelease(driverId, rideId, randomBetween(busyMinMs, busyMaxMs));
}

// Handles one ride: offers it to drivers one at a time until someone accepts,
// three drivers have rejected it, or no driver frees up in time.
//
// Every path ends in ASSIGNED or NO_DRIVER_FOUND, except the "all drivers busy" path,
// which re-queues the same job with a delay and is capped by MAX_WAIT_ATTEMPTS.
async function processRide(job, token) {
  const { rideId } = job.data;
  const {
    maxDriverRejections,
    acceptProbability,
    waitRetryDelayMs,
    maxWaitAttempts,
  } = config.dispatch;

  let ride = await Ride.findOne({ rideId }).lean();
  if (!ride) {
    console.warn(`[worker] ride ${rideId} not found, dropping job`);
    return { outcome: 'MISSING' };
  }

  if (ride.status === 'ASSIGNED') {
    // An earlier attempt assigned the ride and then crashed. Make sure the driver is
    // still freed later (a duplicate release job is harmless).
    await scheduleRelease(ride.assignedDriverId, rideId);
    return { outcome: 'ASSIGNED', driverId: ride.assignedDriverId };
  }
  if (ride.status !== 'REQUESTED') {
    return { outcome: ride.status };
  }

  // Drop any driver reservation left behind by a crashed earlier attempt of this ride.
  await releaseDriversHeldBy(rideId);

  for (;;) {
    // Rule: three rejections and the ride is NO_DRIVER_FOUND.
    if (ride.rejectionCount >= maxDriverRejections) {
      await transitionRide(rideId, 'NO_DRIVER_FOUND');
      return { outcome: 'NO_DRIVER_FOUND', reason: 'rejections' };
    }

    // Reserve a driver we haven't offered this ride to yet. The driver is marked BUSY
    // while the offer is pending, so no other ride can be offered to them meanwhile.
    const driver = await claimDriver(rideId, ride.triedDrivers);

    if (!driver) {
      // Every untried driver is busy right now. Wait and retry, up to a cap.
      if (ride.waitAttempts >= maxWaitAttempts) {
        await transitionRide(rideId, 'NO_DRIVER_FOUND');
        return { outcome: 'NO_DRIVER_FOUND', reason: 'no-available-driver' };
      }
      await Ride.updateOne({ rideId, status: 'REQUESTED' }, { $inc: { waitAttempts: 1 } });
      // Moves this same job to the delayed set; it does not count as a failed attempt.
      await job.moveToDelayed(Date.now() + waitRetryDelayMs, token);
      throw new DelayedError();
    }

    // Fake driver response: accept with probability ACCEPT_PROBABILITY (~50%).
    const accepted = Math.random() < acceptProbability;

    if (accepted) {
      const assigned = await transitionRide(rideId, 'ASSIGNED', {
        assignedDriverId: driver.driverId,
      });
      if (!assigned) {
        // The ride was already finished by someone else; give the driver back.
        await releaseDriver(driver.driverId, rideId);
        return { outcome: 'SKIPPED' };
      }
      await scheduleRelease(driver.driverId, rideId);
      return { outcome: 'ASSIGNED', driverId: driver.driverId };
    }

    // Rejected: the driver is free again straight away (a rejection is not a trip).
    await releaseDriver(driver.driverId, rideId);
    ride = await Ride.findOneAndUpdate(
      { rideId, status: 'REQUESTED' },
      { $addToSet: { triedDrivers: driver.driverId }, $inc: { rejectionCount: 1 } },
      { new: true }
    ).lean();
    if (!ride) return { outcome: 'SKIPPED' };
  }
}

// Handles the delayed "trip finished" job: the driver becomes AVAILABLE again.
// The ride itself stays ASSIGNED.
async function processRelease(job) {
  const { driverId, rideId } = job.data;
  const freed = await releaseDriver(driverId, rideId);
  return { freed };
}

async function startWorkers() {
  await connectDb();
  await seedDrivers();
  await getChannel(); // fail fast if RabbitMQ is not reachable

  const dispatchWorker = new Worker(config.rideQueueName, processRide, {
    connection,
    concurrency: config.dispatch.workerConcurrency,
  });
  const releaseWorker = new Worker(config.driverReleaseQueueName, processRelease, {
    connection,
    concurrency: 20,
  });

  dispatchWorker.on('error', (err) => console.error('[worker] dispatch worker error:', err.message));
  releaseWorker.on('error', (err) => console.error('[worker] release worker error:', err.message));

  dispatchWorker.on('completed', (job, result) => {
    if (result && result.outcome) {
      console.log(`[worker] ride ${job.data.rideId} -> ${result.outcome}${result.driverId ? ` (${result.driverId})` : ''}`);
    }
  });

  // Safety net: if a ride's job fails for good (retries used up, or stalled too often),
  // still give the ride a final status so nothing is ever left in REQUESTED.
  dispatchWorker.on('failed', async (job, err) => {
    console.error(`[worker] job for ride ${job && job.data.rideId} failed: ${err.message}`);
    if (!job) return;
    try {
      if ((await job.getState()) !== 'failed') return; // BullMQ will retry it
      await releaseDriversHeldBy(job.data.rideId);
      await transitionRide(job.data.rideId, 'NO_DRIVER_FOUND');
    } catch (e) {
      console.error('[worker] safety net failed:', e.message);
    }
  });

  // Same idea for a release job that fails for good: never leave a driver BUSY forever.
  releaseWorker.on('failed', async (job, err) => {
    console.error(`[worker] release job failed: ${err.message}`);
    if (!job) return;
    try {
      if ((await job.getState()) !== 'failed') return;
      await releaseDriver(job.data.driverId, job.data.rideId);
    } catch (e) {
      console.error('[worker] release safety net failed:', e.message);
    }
  });

  console.log(
    `[worker] started (concurrency ${config.dispatch.workerConcurrency}, ` +
      `accept ${config.dispatch.acceptProbability}, busy ${config.dispatch.busyMinMs}-${config.dispatch.busyMaxMs}ms)`
  );

  let shuttingDown = false;
  const shutdown = async (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`[worker] ${signal} received, shutting down`);
    await Promise.all([dispatchWorker.close(), releaseWorker.close()]).catch(() => {});
    await closeQueues().catch(() => {});
    await closeEvents().catch(() => {});
    await disconnectDb().catch(() => {});
    process.exit(0);
  };
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  return { dispatchWorker, releaseWorker };
}

if (require.main === module) {
  startWorkers().catch((err) => {
    console.error('[worker] failed to start:', err);
    process.exit(1);
  });
}

module.exports = { processRide, processRelease, startWorkers };
