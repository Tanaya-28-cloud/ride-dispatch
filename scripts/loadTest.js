// Task 3: books many rides at once, waits for every ride to finish, then prints a report.
//
// Run it while the API, the worker, billing and ops are all running:
//   npm run load-test
//
// Options (environment variables): RIDES (default 100), API_URL, TIMEOUT_MS.
//
// How the checks work:
//   - Ride states come straight from MongoDB.
//   - "Assigned to 2 drivers" is checked independently of the ride document, which can only
//     hold one driver. The script listens to the same event exchange as billing and ops and
//     counts how many ASSIGNED events each ride produced and which drivers they named.
//
// Note: the script puts every driver back to AVAILABLE before it starts, so each run
// begins from the same state.
const amqp = require('amqplib');
const config = require('../src/config');
const { connectDb, disconnectDb } = require('../src/db');
const Ride = require('../src/models/Ride');
const { seedDrivers, resetDrivers } = require('../src/seed/drivers');

const API_URL = process.env.API_URL || `http://localhost:${config.port}`;
const TOTAL = parseInt(process.env.RIDES || '100', 10);
const TIMEOUT_MS = parseInt(process.env.TIMEOUT_MS || '120000', 10);
const FINAL_STATUSES = ['ASSIGNED', 'NO_DRIVER_FOUND'];

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Listens to every ride event on a private queue that exists only while the script runs.
// It is one more subscriber on the fanout exchange, so billing and ops are not affected.
async function startEventCollector() {
  const connection = await amqp.connect(config.rabbitUrl);
  const channel = await connection.createChannel();
  await channel.assertExchange(config.eventsExchange, 'fanout', { durable: true });
  const { queue } = await channel.assertQueue('', { exclusive: true, autoDelete: true });
  await channel.bindQueue(queue, config.eventsExchange, '');

  const events = [];
  await channel.consume(
    queue,
    (message) => {
      if (message) events.push(JSON.parse(message.content.toString()));
    },
    { noAck: true }
  );

  return {
    events,
    close: async () => {
      try {
        await channel.close();
        await connection.close();
      } catch (_) {
        // already closed
      }
    },
  };
}

async function bookRide(index) {
  try {
    const response = await fetch(`${API_URL}/rides`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        riderId: `load-rider-${index}`,
        pickup: `Pickup ${index}`,
        dropoff: `Dropoff ${index}`,
      }),
    });
    if (response.status !== 202) return { ok: false, error: `HTTP ${response.status}` };
    const body = await response.json();
    return { ok: true, rideId: body.rideId };
  } catch (err) {
    return { ok: false, error: err.message };
  }
}

async function main() {
  console.log(`Load test: booking ${TOTAL} rides at once against ${API_URL}`);

  await connectDb();
  await seedDrivers();
  await resetDrivers();
  const collector = await startEventCollector();

  // 1. Book everything at the same moment.
  const startedAt = Date.now();
  const bookings = await Promise.all(Array.from({ length: TOTAL }, (_, i) => bookRide(i)));
  const created = bookings.filter((b) => b.ok);
  const failed = bookings.filter((b) => !b.ok);
  const rideIds = created.map((b) => b.rideId);
  const idSet = new Set(rideIds);
  console.log(
    `Booked ${created.length}/${TOTAL} rides in ${Date.now() - startedAt} ms` +
      (failed.length ? ` (${failed.length} failed, e.g. ${failed[0].error})` : '')
  );
  console.log('Waiting for every ride to reach a final status...');

  // 2. Wait until every ride is final, or give up at the timeout.
  let rides = [];
  const deadline = Date.now() + TIMEOUT_MS;
  for (;;) {
    rides = await Ride.find({ rideId: { $in: rideIds } })
      .select('rideId status assignedDriverId')
      .lean();
    const pending = rideIds.length - rides.filter((r) => FINAL_STATUSES.includes(r.status)).length;
    if (pending === 0 || Date.now() > deadline) break;
    process.stdout.write(`\r  ${pending} ride(s) still waiting...   `);
    await sleep(500);
  }
  process.stdout.write(`\r${' '.repeat(40)}\r`);

  // 3. Give the last events a moment to arrive (they are published just after the DB update).
  const finalEventsFor = () =>
    collector.events.filter((e) => idSet.has(e.rideId) && FINAL_STATUSES.includes(e.status));
  const finishedCount = rides.filter((r) => FINAL_STATUSES.includes(r.status)).length;
  const graceUntil = Date.now() + 5000;
  while (finalEventsFor().length < finishedCount && Date.now() < graceUntil) await sleep(100);
  const elapsedMs = Date.now() - startedAt;

  // 4. Work out the numbers.
  const assigned = rides.filter((r) => r.status === 'ASSIGNED');
  const noDriver = rides.filter((r) => r.status === 'NO_DRIVER_FOUND');
  const stuck = rideIds.length - assigned.length - noDriver.length;

  const assignedEventsByRide = new Map();
  for (const event of collector.events) {
    if (!idSet.has(event.rideId) || event.status !== 'ASSIGNED') continue;
    if (!assignedEventsByRide.has(event.rideId)) assignedEventsByRide.set(event.rideId, []);
    assignedEventsByRide.get(event.rideId).push(event.driverId);
  }
  const rideById = new Map(rides.map((r) => [r.rideId, r]));
  let doubleAssigned = 0;
  for (const [rideId, driverIds] of assignedEventsByRide) {
    const stored = rideById.get(rideId);
    const moreThanOnce = driverIds.length > 1;
    const disagrees = stored && stored.assignedDriverId !== driverIds[0];
    if (moreThanOnce || disagrees) doubleAssigned += 1;
  }

  const finalEventCount = finalEventsFor().length;
  await collector.close();

  // 5. Report.
  const verdict = (ok) => (ok ? 'PASS' : 'FAIL');
  const rows = {
    'Total rides created': {
      Expected: TOTAL,
      Actual: idSet.size,
      Result: verdict(idSet.size === TOTAL),
    },
    'ASSIGNED + NO_DRIVER_FOUND': {
      Expected: TOTAL,
      Actual: `${assigned.length} + ${noDriver.length} = ${assigned.length + noDriver.length}`,
      Result: verdict(assigned.length + noDriver.length === TOTAL),
    },
    'Any ride assigned to 2 drivers?': {
      Expected: 0,
      Actual: doubleAssigned,
      Result: verdict(doubleAssigned === 0),
    },
    'Any ride stuck with no final status?': {
      Expected: 0,
      Actual: stuck,
      Result: verdict(stuck === 0),
    },
    'Final-status events published': {
      Expected: TOTAL,
      Actual: finalEventCount,
      Result: verdict(finalEventCount === TOTAL),
    },
  };

  console.log('');
  console.table(rows);

  const perDriver = {};
  for (const ride of assigned) {
    perDriver[ride.assignedDriverId] = (perDriver[ride.assignedDriverId] || 0) + 1;
  }
  const spread = Object.keys(perDriver)
    .sort()
    .map((id) => `${id}=${perDriver[id]}`)
    .join('  ');
  console.log(`Finished in ${(elapsedMs / 1000).toFixed(1)} s`);
  console.log(`Rides per driver: ${spread || '(none assigned)'}`);

  const allPassed = Object.values(rows).every((row) => row.Result === 'PASS');
  console.log(allPassed ? '\nALL CHECKS PASSED' : '\nSOME CHECKS FAILED');

  await disconnectDb();
  process.exit(allPassed ? 0 : 1);
}

main().catch(async (err) => {
  console.error('Load test failed to run:', err);
  await disconnectDb().catch(() => {});
  process.exit(1);
});
