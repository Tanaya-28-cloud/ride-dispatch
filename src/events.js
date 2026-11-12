const amqp = require('amqplib');
const { randomUUID } = require('crypto');
const config = require('./config');

let connection = null;
let channelPromise = null;

// Lazily opens one connection plus one confirm channel and reuses them.
// If either closes, the next publish transparently reconnects.
function getChannel() {
  if (!channelPromise) {
    channelPromise = (async () => {
      const conn = await amqp.connect(config.rabbitUrl);
      conn.on('error', (err) => console.error('[events] connection error:', err.message));
      conn.on('close', () => {
        connection = null;
        channelPromise = null;
      });

      // A confirm channel lets us wait until the broker has accepted each message.
      const channel = await conn.createConfirmChannel();
      channel.on('error', (err) => console.error('[events] channel error:', err.message));
      channel.on('close', () => {
        channelPromise = null;
      });

      // Fanout: the exchange copies every message into every queue bound to it.
      // Billing and ops each bind their own queue, so each receives every event.
      await channel.assertExchange(config.eventsExchange, 'fanout', { durable: true });

      // Declare and bind every subscriber queue up front. A fanout exchange drops a
      // message if no queue is bound when it arrives, so doing this here guarantees
      // billing and ops miss nothing, even if they start after the first rides.
      // (The consumers declare the same queues themselves; declaring twice is harmless.)
      for (const queueName of Object.values(config.subscriberQueues)) {
        await channel.assertQueue(queueName, { durable: true });
        await channel.bindQueue(queueName, config.eventsExchange, '');
      }

      connection = conn;
      return channel;
    })().catch((err) => {
      channelPromise = null;
      throw err;
    });
  }
  return channelPromise;
}

// Publishes one ride status event and resolves once RabbitMQ confirms it.
async function publishRideEvent({ rideId, status, driverId = null }) {
  const channel = await getChannel();
  const event = {
    eventId: randomUUID(),
    type: 'RIDE_STATUS_CHANGED',
    rideId,
    status,
    driverId,
    timestamp: new Date().toISOString(),
  };

  await new Promise((resolve, reject) => {
    channel.publish(
      config.eventsExchange,
      '', // routing key is ignored by a fanout exchange
      Buffer.from(JSON.stringify(event)),
      { persistent: true, contentType: 'application/json', messageId: event.eventId },
      (err) => (err ? reject(err) : resolve())
    );
  });

  return event;
}

async function closeEvents() {
  const pending = channelPromise;
  channelPromise = null;
  if (pending) {
    try {
      const channel = await pending;
      await channel.close();
    } catch (_) {
      // channel already closed
    }
  }
  if (connection) {
    try {
      await connection.close();
    } catch (_) {
      // connection already closed
    }
    connection = null;
  }
}

module.exports = { getChannel, publishRideEvent, closeEvents };
