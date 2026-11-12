// API tests: validation, booking, concurrent booking, lookup, and the failure path.
//
// Uses an in-memory MongoDB, so no Docker services are needed. The queue and the event
// publisher are replaced with mocks, so Redis and RabbitMQ are not needed either.
//
// To run against your Docker MongoDB instead, point TEST_MONGO_URI at a separate database
// (its name must contain "test", because the tests delete everything in it):
//   TEST_MONGO_URI=mongodb://localhost:27017/ride_dispatch_test npm test
const http = require('http');
const request = require('supertest');
const mongoose = require('mongoose');
const { MongoMemoryServer } = require('mongodb-memory-server');

jest.mock('../src/queue', () => ({
  enqueueRide: jest.fn().mockResolvedValue(undefined),
}));
jest.mock('../src/events', () => ({
  publishRideEvent: jest.fn().mockResolvedValue({}),
}));

const Ride = require('../src/models/Ride');
const { enqueueRide } = require('../src/queue');
const { publishRideEvent } = require('../src/events');
const { createApp } = require('../src/app');

let mongod;
let server;

const validBody = { riderId: 'rider-1', pickup: 'Bandra', dropoff: 'Andheri' };
const publishedEvents = () => publishRideEvent.mock.calls.map((call) => call[0]);

beforeAll(async () => {
  let uri = process.env.TEST_MONGO_URI;
  if (uri) {
    if (!uri.includes('test')) {
      throw new Error('TEST_MONGO_URI must point at a database whose name contains "test"');
    }
  } else {
    mongod = await MongoMemoryServer.create();
    uri = mongod.getUri();
  }
  await mongoose.connect(uri);
  await Ride.init(); // make sure the unique index exists before any test runs

  server = http.createServer(createApp()).listen(0);
}, 180000);

afterAll(async () => {
  if (server) await new Promise((resolve) => server.close(resolve));
  await mongoose.disconnect();
  if (mongod) await mongod.stop();
});

beforeEach(async () => {
  await Ride.deleteMany({});
  jest.clearAllMocks();
});

describe('POST /rides', () => {
  test('books a ride, returns a rideId straight away and queues the ride', async () => {
    const res = await request(server).post('/rides').send(validBody);

    expect(res.status).toBe(202);
    expect(typeof res.body.rideId).toBe('string');
    expect(res.body.status).toBe('REQUESTED');

    const stored = await Ride.findOne({ rideId: res.body.rideId }).lean();
    expect(stored.status).toBe('REQUESTED');
    expect(stored.riderId).toBe('rider-1');

    expect(enqueueRide).toHaveBeenCalledTimes(1);
    expect(enqueueRide).toHaveBeenCalledWith(res.body.rideId);

    const events = publishedEvents();
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({ rideId: res.body.rideId, status: 'REQUESTED' });
  });

  test('publishes the REQUESTED event before the ride is queued', async () => {
    await request(server).post('/rides').send(validBody);

    const publishedAt = publishRideEvent.mock.invocationCallOrder[0];
    const queuedAt = enqueueRide.mock.invocationCallOrder[0];
    expect(publishedAt).toBeLessThan(queuedAt);
  });

  test.each([
    ['an empty body', {}],
    ['a missing pickup and dropoff', { riderId: 'rider-1' }],
    ['a missing dropoff', { riderId: 'rider-1', pickup: 'Bandra' }],
    ['a blank riderId', { riderId: '   ', pickup: 'Bandra', dropoff: 'Andheri' }],
    ['a riderId that is not a string', { riderId: 42, pickup: 'Bandra', dropoff: 'Andheri' }],
  ])('rejects %s with 400 and creates nothing', async (_label, body) => {
    const res = await request(server).post('/rides').send(body);

    expect(res.status).toBe(400);
    expect(res.body.fields.length).toBeGreaterThan(0);
    expect(await Ride.countDocuments()).toBe(0);
    expect(enqueueRide).not.toHaveBeenCalled();
    expect(publishRideEvent).not.toHaveBeenCalled();
  });

  test('100 simultaneous bookings create 100 distinct rides and 100 queue jobs', async () => {
    const responses = await Promise.all(
      Array.from({ length: 100 }, (_, i) =>
        request(server)
          .post('/rides')
          .send({ riderId: `rider-${i}`, pickup: 'A', dropoff: 'B' })
      )
    );

    expect(responses.every((res) => res.status === 202)).toBe(true);

    const rideIds = responses.map((res) => res.body.rideId);
    expect(new Set(rideIds).size).toBe(100);
    expect(await Ride.countDocuments({ status: 'REQUESTED' })).toBe(100);

    expect(enqueueRide).toHaveBeenCalledTimes(100);
    expect(new Set(enqueueRide.mock.calls.map((call) => call[0])).size).toBe(100);
    expect(publishedEvents().filter((e) => e.status === 'REQUESTED')).toHaveLength(100);
  });

  test('if the ride cannot be queued, it still gets a final status and the caller gets 503', async () => {
    const consoleError = jest.spyOn(console, 'error').mockImplementation(() => {});
    enqueueRide.mockRejectedValueOnce(new Error('redis is down'));

    const res = await request(server).post('/rides').send(validBody);

    expect(res.status).toBe(503);
    expect(typeof res.body.rideId).toBe('string');

    const stored = await Ride.findOne({ rideId: res.body.rideId }).lean();
    expect(stored.status).toBe('NO_DRIVER_FOUND');

    const statuses = publishedEvents().map((e) => e.status);
    expect(statuses).toEqual(['REQUESTED', 'NO_DRIVER_FOUND']);

    consoleError.mockRestore();
  });
});

describe('GET /rides/:id', () => {
  test('returns the ride', async () => {
    const created = await request(server).post('/rides').send(validBody);
    const res = await request(server).get(`/rides/${created.body.rideId}`);

    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({
      rideId: created.body.rideId,
      riderId: 'rider-1',
      pickup: 'Bandra',
      dropoff: 'Andheri',
      status: 'REQUESTED',
      assignedDriverId: null,
      rejectionCount: 0,
    });
  });

  test('returns 404 for an unknown ride', async () => {
    const res = await request(server).get('/rides/does-not-exist');
    expect(res.status).toBe(404);
  });
});
