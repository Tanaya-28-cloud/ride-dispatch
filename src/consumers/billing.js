// Program A: billing.
// Receives every ride event, but only an ASSIGNED ride is billable.
// REQUESTED is ignored, and NO_DRIVER_FOUND charges nothing.
const config = require('../config');
const { startConsumer } = require('./common');

const charged = new Set();
let eventsSeen = 0;

function handleEvent(event) {
  eventsSeen += 1;

  if (event.status !== 'ASSIGNED') return;

  // RabbitMQ delivers at least once, so the same event can arrive twice.
  // Remembering charged rides makes sure a rider is never charged twice.
  if (charged.has(event.rideId)) return;
  charged.add(event.rideId);

  console.log(`charging rider for ride ${event.rideId}`);
}

function summary() {
  return `events seen: ${eventsSeen}, riders charged: ${charged.size}`;
}

if (require.main === module) {
  startConsumer({
    name: 'billing',
    queueName: config.subscriberQueues.billing,
    handler: handleEvent,
    summary,
  }).catch((err) => {
    console.error('[billing] failed to start:', err);
    process.exit(1);
  });
}

module.exports = { handleEvent, summary };
