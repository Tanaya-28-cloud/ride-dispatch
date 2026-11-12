const { Queue } = require('bullmq');
const config = require('./config');

const connection = { host: config.redis.host, port: config.redis.port };

// Queue 1: rides waiting to be offered to a driver. The worker consumes this.
const rideQueue = new Queue(config.rideQueueName, {
  connection,
  defaultJobOptions: {
    // If the worker crashes or throws unexpectedly, BullMQ retries the job.
    attempts: 5,
    backoff: { type: 'fixed', delay: 500 },
    removeOnComplete: { age: 3600, count: 5000 },
    removeOnFail: { age: 24 * 3600 },
  },
});

// Queue 2: delayed jobs that free a driver once their simulated trip is over.
const driverReleaseQueue = new Queue(config.driverReleaseQueueName, {
  connection,
  defaultJobOptions: {
    attempts: 5,
    backoff: { type: 'fixed', delay: 500 },
    removeOnComplete: { age: 3600, count: 5000 },
    removeOnFail: { age: 24 * 3600 },
  },
});

// jobId = rideId, so enqueueing the same ride twice can never create two jobs.
function enqueueRide(rideId) {
  return rideQueue.add('dispatch', { rideId }, { jobId: rideId });
}

function enqueueDriverRelease(driverId, rideId, delayMs) {
  return driverReleaseQueue.add(
    'release',
    { driverId, rideId },
    { delay: delayMs, jobId: `release-${rideId}` }
  );
}

async function closeQueues() {
  await Promise.all([rideQueue.close(), driverReleaseQueue.close()]);
}

module.exports = {
  connection,
  rideQueue,
  driverReleaseQueue,
  enqueueRide,
  enqueueDriverRelease,
  closeQueues,
};
