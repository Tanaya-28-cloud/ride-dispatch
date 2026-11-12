// Dispatch tests: driver claiming, the 3-rejection rule, busy drivers,
// waiting, and guards that prevent double assignment.
//
// Uses an in-memory MongoDB. Redis and RabbitMQ are mocked.

// Driver ID mapping:
// D01 = Driver 1
// D02 = Driver 2
// D03 = Driver 3
// D04 = Driver 4
// D05 = Driver 5
// D06 = Driver 6
// D07 = Driver 7
// D08 = Driver 8
// D09 = Driver 9
// D10 = Driver 10

const DRIVERS = {
  DRIVER_01: 'D01',
  DRIVER_02: 'D02',
  DRIVER_03: 'D03',
  DRIVER_04: 'D04',
  DRIVER_05: 'D05',
  DRIVER_06: 'D06',
  DRIVER_07: 'D07',
  DRIVER_08: 'D08',
  DRIVER_09: 'D09',
  DRIVER_10: 'D10',
};

const mongoose = require('mongoose');
const { DelayedError } = require('bullmq');
const { MongoMemoryServer } = require('mongodb-memory-server');

jest.mock('../src/queue', () => ({
  connection: {},
  enqueueDriverRelease: jest.fn().mockResolvedValue(undefined),
  closeQueues: jest.fn().mockResolvedValue(undefined),
}));

jest.mock('../src/events', () => ({
  publishRideEvent: jest.fn().mockResolvedValue({}),
  getChannel: jest.fn().mockResolvedValue({}),
  closeEvents: jest.fn().mockResolvedValue(undefined),
}));

const config = require('../src/config');
const Ride = require('../src/models/Ride');
const Driver = require('../src/models/Driver');
const { seedDrivers } = require('../src/seed/drivers');
const { enqueueDriverRelease } = require('../src/queue');
const { publishRideEvent } = require('../src/events');
const { transitionRide } = require('../src/services/rideService');
const { processRide, processRelease } = require('../src/worker');

let mongod;

const eventsWithStatus = (status) =>
  publishRideEvent.mock.calls
    .map((call) => call[0])
    .filter((event) => event.status === status);

function makeRide(rideId, overrides = {}) {
  return Ride.create({
    rideId,
    riderId: 'rider-1',
    pickup: 'A',
    dropoff: 'B',
    status: 'REQUESTED',
    ...overrides,
  });
}

// A stand-in for the BullMQ job that processRide receives.
function makeJob(rideId) {
  return {
    data: { rideId },
    moveToDelayed: jest.fn().mockResolvedValue(undefined),
  };
}

const markAllDriversBusy = (filter = {}) =>
  Driver.updateMany(filter, {
    $set: {
      status: 'BUSY',
      currentRideId: 'some-other-ride',
    },
  });

beforeAll(async () => {
  let uri = process.env.TEST_MONGO_URI;

  if (uri) {
    if (!uri.includes('test')) {
      throw new Error(
        'TEST_MONGO_URI must point at a database whose name contains "test"'
      );
    }
  } else {
    mongod = await MongoMemoryServer.create();
    uri = mongod.getUri();
  }

  await mongoose.connect(uri);
  await Promise.all([Ride.init(), Driver.init()]);
}, 180000);

afterAll(async () => {
  await mongoose.disconnect();

  if (mongod) {
    await mongod.stop();
  }
});

beforeEach(async () => {
  await Promise.all([
    Ride.deleteMany({}),
    Driver.deleteMany({}),
  ]);

  await seedDrivers();

  jest.clearAllMocks();

  config.dispatch.acceptProbability = 1;
  config.dispatch.maxDriverRejections = 3;
  config.dispatch.maxWaitAttempts = 8;
  config.dispatch.waitRetryDelayMs = 1000;
  config.dispatch.busyMinMs = 3000;
  config.dispatch.busyMaxMs = 5000;
});

describe('accepting a ride', () => {
  test('assigns an available driver, marks them BUSY and schedules their release', async () => {
    await makeRide('ride-1');

    const result = await processRide(makeJob('ride-1'), 'token');

    expect(result.outcome).toBe('ASSIGNED');

    const ride = await Ride.findOne({ rideId: 'ride-1' }).lean();

    expect(ride.status).toBe('ASSIGNED');
    expect(ride.assignedDriverId).toBe(result.driverId);

    const driver = await Driver.findOne({
      driverId: result.driverId,
    }).lean();

    expect(driver.status).toBe('BUSY');
    expect(driver.currentRideId).toBe('ride-1');

    expect(enqueueDriverRelease).toHaveBeenCalledTimes(1);

    const [driverId, rideId, delayMs] =
      enqueueDriverRelease.mock.calls[0];

    expect(driverId).toBe(result.driverId);
    expect(rideId).toBe('ride-1');

    expect(delayMs).toBeGreaterThanOrEqual(
      config.dispatch.busyMinMs
    );

    expect(delayMs).toBeLessThanOrEqual(
      config.dispatch.busyMaxMs
    );

    const assignedEvents = eventsWithStatus('ASSIGNED');

    expect(assignedEvents).toHaveLength(1);

    expect(assignedEvents[0]).toMatchObject({
      rideId: 'ride-1',
      driverId: result.driverId,
    });
  });

  test('skips BUSY drivers and picks the only available one', async () => {
    await makeRide('ride-1');

    // Only Driver 7 (D07) is available.
    await markAllDriversBusy({
      driverId: { $ne: DRIVERS.DRIVER_07 },
    });

    const result = await processRide(makeJob('ride-1'), 'token');

    expect(result).toMatchObject({
      outcome: 'ASSIGNED',
      driverId: DRIVERS.DRIVER_07,
    });
  });

  test('never offers the ride to a driver who already rejected it', async () => {
    await makeRide('ride-1', {
      rejectionCount: 2,
      triedDrivers: [
        DRIVERS.DRIVER_01,
        DRIVERS.DRIVER_02,
      ],
    });

    for (let attempt = 0; attempt < 5; attempt += 1) {
      await Ride.updateOne(
        { rideId: 'ride-1' },
        {
          $set: {
            status: 'REQUESTED',
            assignedDriverId: null,
          },
        }
      );

      await Driver.updateMany(
        {},
        {
          $set: {
            status: 'AVAILABLE',
            currentRideId: null,
          },
        }
      );

      const result = await processRide(
        makeJob('ride-1'),
        'token'
      );

      expect(result.outcome).toBe('ASSIGNED');

      expect([
        DRIVERS.DRIVER_01,
        DRIVERS.DRIVER_02,
      ]).not.toContain(result.driverId);
    }
  });
});

describe('rejections', () => {
  test('three rejections end in NO_DRIVER_FOUND, with three different drivers offered', async () => {
    config.dispatch.acceptProbability = 0;

    await makeRide('ride-1');

    const result = await processRide(makeJob('ride-1'), 'token');

    expect(result).toMatchObject({
      outcome: 'NO_DRIVER_FOUND',
      reason: 'rejections',
    });

    const ride = await Ride.findOne({ rideId: 'ride-1' }).lean();

    expect(ride.status).toBe('NO_DRIVER_FOUND');
    expect(ride.rejectionCount).toBe(3);
    expect(ride.triedDrivers).toHaveLength(3);

    expect(new Set(ride.triedDrivers).size).toBe(3);

    // Rejected drivers become available again.
    expect(
      await Driver.countDocuments({ status: 'AVAILABLE' })
    ).toBe(10);

    expect(enqueueDriverRelease).not.toHaveBeenCalled();

    expect(eventsWithStatus('NO_DRIVER_FOUND')).toHaveLength(1);
    expect(eventsWithStatus('ASSIGNED')).toHaveLength(0);
  });

  test('a ride rejected twice can still be accepted by the third driver', async () => {
    await makeRide('ride-1', {
      rejectionCount: 2,
      triedDrivers: [
        DRIVERS.DRIVER_01,
        DRIVERS.DRIVER_02,
      ],
    });

    const result = await processRide(makeJob('ride-1'), 'token');

    expect(result.outcome).toBe('ASSIGNED');
  });

  test('a ride that already has three rejections finishes without offering to anyone', async () => {
    await makeRide('ride-1', {
      rejectionCount: 3,
      triedDrivers: [
        DRIVERS.DRIVER_01,
        DRIVERS.DRIVER_02,
        DRIVERS.DRIVER_03,
      ],
    });

    const result = await processRide(makeJob('ride-1'), 'token');

    expect(result.outcome).toBe('NO_DRIVER_FOUND');

    expect(
      await Driver.countDocuments({ status: 'BUSY' })
    ).toBe(0);
  });
});

describe('when every driver is busy', () => {
  test('waits: the job is pushed back with a delay and the ride stays REQUESTED', async () => {
    await makeRide('ride-1');

    await markAllDriversBusy();

    const job = makeJob('ride-1');

    await expect(
      processRide(job, 'token')
    ).rejects.toBeInstanceOf(DelayedError);

    expect(job.moveToDelayed).toHaveBeenCalledTimes(1);

    const [runAt, token] = job.moveToDelayed.mock.calls[0];

    expect(token).toBe('token');
    expect(runAt).toBeGreaterThan(Date.now());

    expect(runAt).toBeLessThanOrEqual(
      Date.now() + config.dispatch.waitRetryDelayMs
    );

    const ride = await Ride.findOne({ rideId: 'ride-1' }).lean();

    expect(ride.status).toBe('REQUESTED');
    expect(ride.waitAttempts).toBe(1);

    expect(publishRideEvent).not.toHaveBeenCalled();
  });

  test('gives up with NO_DRIVER_FOUND once the wait limit is used up', async () => {
    await makeRide('ride-1', {
      waitAttempts: config.dispatch.maxWaitAttempts,
    });

    await markAllDriversBusy();

    const result = await processRide(makeJob('ride-1'), 'token');

    expect(result).toMatchObject({
      outcome: 'NO_DRIVER_FOUND',
      reason: 'no-available-driver',
    });

    const ride = await Ride.findOne({ rideId: 'ride-1' }).lean();

    expect(ride.status).toBe('NO_DRIVER_FOUND');

    expect(eventsWithStatus('NO_DRIVER_FOUND')).toHaveLength(1);
  });
});

describe('no driver is ever used twice', () => {
  test('30 rides at once with 10 drivers: exactly 10 are assigned, each to a different driver', async () => {
    const rideIds = Array.from(
      { length: 30 },
      (_, i) => `ride-${i}`
    );

    await Ride.insertMany(
      rideIds.map((rideId) => ({
        rideId,
        riderId: 'rider-1',
        pickup: 'A',
        dropoff: 'B',
      }))
    );

    const settled = await Promise.allSettled(
      rideIds.map((rideId) =>
        processRide(makeJob(rideId), 'token')
      )
    );

    const assigned = settled.filter(
      (s) =>
        s.status === 'fulfilled' &&
        s.value.outcome === 'ASSIGNED'
    );

    const waiting = settled.filter(
      (s) =>
        s.status === 'rejected' &&
        s.reason instanceof DelayedError
    );

    expect(assigned).toHaveLength(10);
    expect(waiting).toHaveLength(20);

    const assignedRides = await Ride.find({
      status: 'ASSIGNED',
    }).lean();

    expect(assignedRides).toHaveLength(10);

    expect(
      new Set(assignedRides.map((r) => r.assignedDriverId)).size
    ).toBe(10);

    const busyDrivers = await Driver.find({
      status: 'BUSY',
    }).lean();

    expect(busyDrivers).toHaveLength(10);

    for (const driver of busyDrivers) {
      const ride = assignedRides.find(
        (r) => r.rideId === driver.currentRideId
      );

      expect(ride.assignedDriverId).toBe(driver.driverId);
    }

    expect(eventsWithStatus('ASSIGNED')).toHaveLength(10);
  });

  test('when many workers finish the same ride at once, exactly one wins and one event is published', async () => {
    await makeRide('ride-1');

    const driverIds = [
      DRIVERS.DRIVER_01,
      DRIVERS.DRIVER_02,
      DRIVERS.DRIVER_03,
      DRIVERS.DRIVER_04,
      DRIVERS.DRIVER_05,
      DRIVERS.DRIVER_06,
      DRIVERS.DRIVER_07,
      DRIVERS.DRIVER_08,
      DRIVERS.DRIVER_09,
      DRIVERS.DRIVER_10,
    ];

    const attempts = Array.from({ length: 20 }, (_, i) =>
      i % 2 === 0
        ? transitionRide('ride-1', 'ASSIGNED', {
          assignedDriverId: driverIds[(i / 2) % 10],
        })
        : transitionRide('ride-1', 'NO_DRIVER_FOUND')
    );

    const results = await Promise.all(attempts);

    const winners = results.filter(Boolean);

    expect(winners).toHaveLength(1);

    const ride = await Ride.findOne({ rideId: 'ride-1' }).lean();

    expect(ride.status).toBe(winners[0].status);

    expect(publishRideEvent).toHaveBeenCalledTimes(1);
  });
});

describe('redelivered and crashed jobs', () => {
  test('a job redelivered for an already ASSIGNED ride changes nothing and publishes nothing', async () => {
    await makeRide('ride-1', {
      status: 'ASSIGNED',
      assignedDriverId: DRIVERS.DRIVER_02,
    });

    await Driver.updateOne(
      { driverId: DRIVERS.DRIVER_02 },
      {
        $set: {
          status: 'BUSY',
          currentRideId: 'ride-1',
        },
      }
    );

    const result = await processRide(makeJob('ride-1'), 'token');

    expect(result).toMatchObject({
      outcome: 'ASSIGNED',
      driverId: DRIVERS.DRIVER_02,
    });

    const ride = await Ride.findOne({ rideId: 'ride-1' }).lean();

    expect(ride.assignedDriverId).toBe(DRIVERS.DRIVER_02);

    expect(publishRideEvent).not.toHaveBeenCalled();

    // Schedule release again in case the first attempt crashed.
    expect(enqueueDriverRelease).toHaveBeenCalledTimes(1);

    expect(
      enqueueDriverRelease.mock.calls[0].slice(0, 2)
    ).toEqual([DRIVERS.DRIVER_02, 'ride-1']);
  });

  test('a job for a ride that is already NO_DRIVER_FOUND is ignored', async () => {
    await makeRide('ride-1', {
      status: 'NO_DRIVER_FOUND',
    });

    const result = await processRide(makeJob('ride-1'), 'token');

    expect(result.outcome).toBe('NO_DRIVER_FOUND');

    expect(publishRideEvent).not.toHaveBeenCalled();

    expect(
      await Driver.countDocuments({ status: 'BUSY' })
    ).toBe(0);
  });

  test('a driver left reserved by a crashed attempt is freed and can be used again', async () => {
    await makeRide('ride-1');

    // Driver 3 was reserved for this ride by an attempt that crashed.
    // Everyone else is busy elsewhere.
    await markAllDriversBusy({
      driverId: { $ne: DRIVERS.DRIVER_03 },
    });

    await Driver.updateOne(
      { driverId: DRIVERS.DRIVER_03 },
      {
        $set: {
          status: 'BUSY',
          currentRideId: 'ride-1',
        },
      }
    );

    const result = await processRide(makeJob('ride-1'), 'token');

    expect(result).toMatchObject({
      outcome: 'ASSIGNED',
      driverId: DRIVERS.DRIVER_03,
    });
  });
});

describe('releasing a driver after the trip', () => {
  test('frees the driver but leaves the ride ASSIGNED', async () => {
    await makeRide('ride-1');

    const { driverId } = await processRide(
      makeJob('ride-1'),
      'token'
    );

    await processRelease({
      data: {
        driverId,
        rideId: 'ride-1',
      },
    });

    const driver = await Driver.findOne({ driverId }).lean();

    expect(driver.status).toBe('AVAILABLE');
    expect(driver.currentRideId).toBeNull();

    const ride = await Ride.findOne({ rideId: 'ride-1' }).lean();

    expect(ride.status).toBe('ASSIGNED');
  });

  test('a late release for an old ride does not free a driver who has moved on to a new ride', async () => {
    await Driver.updateOne(
      { driverId: DRIVERS.DRIVER_01 },
      {
        $set: {
          status: 'BUSY',
          currentRideId: 'ride-new',
        },
      }
    );

    await processRelease({
      data: {
        driverId: DRIVERS.DRIVER_01,
        rideId: 'ride-old',
      },
    });

    let driver = await Driver.findOne({
      driverId: DRIVERS.DRIVER_01,
    }).lean();

    expect(driver.status).toBe('BUSY');
    expect(driver.currentRideId).toBe('ride-new');

    await processRelease({
      data: {
        driverId: DRIVERS.DRIVER_01,
        rideId: 'ride-new',
      },
    });

    driver = await Driver.findOne({
      driverId: DRIVERS.DRIVER_01,
    }).lean();

    expect(driver.status).toBe('AVAILABLE');
  });
});