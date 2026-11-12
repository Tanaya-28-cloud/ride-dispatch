const config = require('./config');
const { connectDb, disconnectDb } = require('./db');
const { seedDrivers } = require('./seed/drivers');
const { getChannel, closeEvents } = require('./events');
const { closeQueues } = require('./queue');
const { createApp } = require('./app');

async function main() {
  await connectDb();
  await seedDrivers();
  await getChannel(); // fail fast if RabbitMQ is not reachable

  const app = createApp();
  const server = app.listen(config.port, () => {
    console.log(`[api] listening on http://localhost:${config.port}`);
  });

  let shuttingDown = false;
  const shutdown = async (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`[api] ${signal} received, shutting down`);
    server.close();
    await closeQueues().catch(() => {});
    await closeEvents().catch(() => {});
    await disconnectDb().catch(() => {});
    process.exit(0);
  };
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

main().catch((err) => {
  console.error('[api] failed to start:', err);
  process.exit(1);
});
