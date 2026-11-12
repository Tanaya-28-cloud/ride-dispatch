// Program B: ops.
// Receives every ride event and prints the ride's new status
// along with the assigned driver ID.

const config = require('../config');
const { startConsumer } = require('./common');

const countsByStatus = {};
let eventsSeen = 0;

function handleEvent(event) {
  eventsSeen += 1;
  countsByStatus[event.status] = (countsByStatus[event.status] || 0) + 1;

  const driverInfo =
    event.status === 'ASSIGNED' && event.driverId
      ? ` | Driver: ${event.driverId}`
      : '';

  console.log(
    `[OPS] Ride ${event.rideId} → ${event.status}${driverInfo}`
  );
}

function summary() {
  const breakdown = Object.entries(countsByStatus)
    .map(([status, count]) => `${status}=${count}`)
    .join(', ');

  return `events seen: ${eventsSeen}${breakdown ? ` (${breakdown})` : ''}`;
}

if (require.main === module) {
  startConsumer({
    name: 'ops',
    queueName: config.subscriberQueues.ops,
    handler: handleEvent,
    summary,
  }).catch((err) => {
    console.error('[ops] failed to start:', err);
    process.exit(1);
  });
}

module.exports = { handleEvent, summary };