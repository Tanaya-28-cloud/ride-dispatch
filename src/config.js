require('dotenv').config();

const int = (value, fallback) => {
  const parsed = parseInt(value, 10);
  return Number.isNaN(parsed) ? fallback : parsed;
};

const float = (value, fallback) => {
  const parsed = parseFloat(value);
  return Number.isNaN(parsed) ? fallback : parsed;
};

module.exports = {
  port: int(process.env.PORT, 3000),
  mongoUri: process.env.MONGO_URI || 'mongodb://localhost:27017/ride_dispatch',

  redis: {
    host: process.env.REDIS_HOST || 'localhost',
    port: int(process.env.REDIS_PORT, 6379),
  },

  rabbitUrl: process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672',
  eventsExchange: 'ride.events', // fanout exchange: every bound queue gets every event

  // One durable queue per subscriber. The publisher also declares and binds these on
  // startup, so no event is lost even if a consumer has never been started yet.
  subscriberQueues: {
    billing: 'billing.events',
    ops: 'ops.events',
  },

  rideQueueName: 'ride-dispatch',
  driverReleaseQueueName: 'driver-release',

  dispatch: {
    maxDriverRejections: int(process.env.MAX_DRIVER_REJECTIONS, 3),
    acceptProbability: float(process.env.ACCEPT_PROBABILITY, 0.5),
    busyMinMs: int(process.env.BUSY_MIN_MS, 3000),
    busyMaxMs: int(process.env.BUSY_MAX_MS, 5000),
    waitRetryDelayMs: int(process.env.WAIT_RETRY_DELAY_MS, 1000),
    maxWaitAttempts: int(process.env.MAX_WAIT_ATTEMPTS, 8),
    workerConcurrency: int(process.env.WORKER_CONCURRENCY, 10),
  },
};
