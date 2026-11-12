const amqp = require('amqplib');
const config = require('../config');

// Starts a RabbitMQ consumer that reads every event from its own queue.
//
// Each subscriber (billing, ops) owns a separate queue bound to the same fanout
// exchange. The exchange copies every event into every bound queue, so both
// programs receive all events instead of splitting them between each other.
//
//   name       label used in log lines
//   queueName  this subscriber's own durable queue
//   handler    async (event) => void; the message is acknowledged after it returns
//   summary    optional () => string printed when the program stops
async function startConsumer({ name, queueName, handler, summary }) {
  const connection = await amqp.connect(config.rabbitUrl);
  const channel = await connection.createChannel();

  await channel.assertExchange(config.eventsExchange, 'fanout', { durable: true });
  await channel.assertQueue(queueName, { durable: true });
  await channel.bindQueue(queueName, config.eventsExchange, '');
  await channel.prefetch(10);

  let closing = false;

  connection.on('error', (err) => console.error(`[${name}] connection error: ${err.message}`));
  connection.on('close', () => {
    if (closing) return;
    console.error(`[${name}] connection to RabbitMQ lost, exiting`);
    process.exit(1);
  });

  await channel.consume(queueName, async (message) => {
    if (!message) return; // the broker cancelled this consumer

    let event;
    try {
      event = JSON.parse(message.content.toString());
    } catch (err) {
      console.error(`[${name}] dropping malformed message: ${err.message}`);
      channel.nack(message, false, false); // never retry a message that cannot be parsed
      return;
    }

    try {
      await handler(event);
      channel.ack(message); // acknowledge only after the event was handled
    } catch (err) {
      console.error(`[${name}] failed to handle event ${event.eventId}: ${err.message}`);
      channel.nack(message, false, true); // put it back so it is delivered again
    }
  });

  console.log(`[${name}] waiting for events on queue "${queueName}" (Ctrl+C to stop)`);

  const shutdown = async (signal) => {
    if (closing) return;
    closing = true;
    console.log(`\n[${name}] ${signal} received, shutting down`);
    if (summary) console.log(`[${name}] ${summary()}`);
    try {
      await channel.close();
      await connection.close();
    } catch (_) {
      // already closed
    }
    process.exit(0);
  };
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  return { connection, channel };
}

module.exports = { startConsumer };
