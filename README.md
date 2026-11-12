# Ride Booking System

A simplified version of how Ola or Uber assigns a ride to a driver. A rider books a ride and gets an id back immediately. The ride is then dispatched asynchronously through a job queue, offered to fake drivers one at a time, and every status change is published as an event that independent programs (billing and ops) consume. A load-test script books 100 rides at once and verifies that nothing is lost or assigned twice.

## Contents

1. [Tech stack](#tech-stack)
2. [Architecture](#architecture)
3. [How a ride flows](#how-a-ride-flows)
4. [Ride and driver lifecycles](#ride-and-driver-lifecycles)
5. [Additions beyond the assignment](#additions-beyond-the-assignment)
6. [Data model](#data-model)
7. [Events](#events)
8. [API reference](#api-reference)
9. [Correctness guarantees](#correctness-guarantees)
10. [Configuration](#configuration)
11. [Getting started](#getting-started)
12. [Load test (Task 3)](#load-test-task-3)
13. [Automated tests](#automated-tests)
14. [Project structure](#project-structure)
15. [How this maps to the assignment](#how-this-maps-to-the-assignment)
16. [Design decisions and assumptions](#design-decisions-and-assumptions)
17. [Known limitations and next steps](#known-limitations-and-next-steps)
18. [Troubleshooting](#troubleshooting)

## Tech stack

| Layer | Technology | Purpose |
| --- | --- | --- |
| API | Node.js 18+ and Express 4 | Accept ride bookings and answer status lookups |
| Database | MongoDB with Mongoose 8 | Store rides and drivers |
| Job queue | Redis with BullMQ 5 | Process ride requests asynchronously, retries, delayed jobs |
| Event broker | RabbitMQ (amqplib) | Publish ride status events to every subscriber |
| Consumers | Node.js | Billing (Program A) and operations (Program B) |
| Testing | Jest, Supertest, mongodb-memory-server | API and dispatch tests, including concurrency |
| Load testing | Custom Node.js script | Book 100 rides at once and verify the outcome |
| Local infrastructure | Docker Compose | Runs MongoDB, Redis and RabbitMQ with one command |

Two different brokers are used on purpose, because the assignment asks for two different messaging patterns:

- **Work queue (Redis and BullMQ).** Each ride must be handled by exactly one worker, even when several workers run. BullMQ also provides delayed jobs (used for driver busy periods and for waiting when all drivers are busy), retries with backoff, and crash recovery for stalled jobs.
- **Publish and subscribe (RabbitMQ fanout exchange).** Billing and ops must each receive every event, not split them. A fanout exchange copies every message into every queue bound to it, and each subscriber owns its own queue.

## Architecture

```mermaid
flowchart LR
    Client["Rider / load test"] -->|"POST /rides, GET /rides/:id"| API["API - Express"]
    API -->|"1. save ride as REQUESTED"| Mongo[("MongoDB<br/>rides + drivers")]
    API -->|"2. publish REQUESTED"| Exchange{{"RabbitMQ fanout exchange<br/>ride.events"}}
    API -->|"3. enqueue job"| Dispatch[("Redis + BullMQ<br/>ride-dispatch queue")]
    Dispatch -->|"job"| Worker["Dispatch worker"]
    Worker <-->|"atomic driver claim and ride transitions"| Mongo
    Worker -->|"publish ASSIGNED or NO_DRIVER_FOUND"| Exchange
    Worker -->|"delayed job after an accept"| Release[("Redis + BullMQ<br/>driver-release queue")]
    Release -->|"after 3-5 s"| ReleaseWorker["Release worker"]
    ReleaseWorker -->|"driver back to AVAILABLE"| Mongo
    Exchange -->|"queue billing.events"| Billing["Billing - Program A"]
    Exchange -->|"queue ops.events"| Ops["Ops - Program B"]
```

The same picture as plain text:

```
                       +---------------------+
 Rider / load test --->|     API (Express)   |
 POST /rides           |  POST /rides        |
 GET  /rides/:id       |  GET  /rides/:id    |
                       +---+------+------+---+
              1. save ride |      |      | 3. enqueue job
                 REQUESTED |      |      |
                           v      |      v
                  +------------+  |   +--------------------------+
                  |  MongoDB   |  |   | Redis + BullMQ           |
                  |  rides     |  |   | queue: ride-dispatch     |
                  |  drivers   |  |   +------------+-------------+
                  +-----^------+  |                |
        atomic driver     |       | 2. publish     v
        claim + ride      |       |    REQUESTED  +---------------------+
        transitions       +-------|---------------+  Dispatch worker    |
                                  |               +----+-----------+----+
                                  v                    |           | delayed job
                    +---------------------------+      |           v
                    | RabbitMQ fanout exchange  |<-----+   +----------------+
                    | ride.events               | ASSIGNED | driver-release |
                    +------+-------------+------+ or NO_   | queue + worker |
                           |             |        DRIVER_  +-------+--------+
              billing.events        ops.events    FOUND            |
                           |             |                         v
                           v             v              driver set back to
                    +-----------+   +-----------+       AVAILABLE in MongoDB
                    |  Billing  |   |    Ops    |
                    | Program A |   | Program B |
                    +-----------+   +-----------+
```

### Processes

The system is five small programs that share code in `src/`. Each one is started separately.

| Command | File | Role |
| --- | --- | --- |
| `npm run start:api` | `src/server.js` | HTTP API. Creates the ride, publishes `REQUESTED`, queues the job, returns the ride id. |
| `npm run start:worker` | `src/worker.js` | Runs two BullMQ workers: the dispatch worker (offers rides to drivers) and the release worker (frees drivers after their trip). |
| `npm run start:billing` | `src/consumers/billing.js` | Program A. Reads every event, charges only for `ASSIGNED`. |
| `npm run start:ops` | `src/consumers/ops.js` | Program B. Reads every event and prints the new status. |
| `npm run load-test` | `scripts/loadTest.js` | Books 100 rides at once and prints the report. |

You can run more than one worker process. Each ride is still handled by exactly one of them.

## How a ride flows

```mermaid
sequenceDiagram
    autonumber
    participant R as Rider
    participant API as API
    participant DB as MongoDB
    participant MQ as RabbitMQ
    participant Q as BullMQ queue
    participant W as Dispatch worker
    participant B as Billing
    participant O as Ops

    R->>API: POST /rides
    API->>DB: create ride as REQUESTED
    API->>MQ: publish REQUESTED
    API->>Q: enqueue job with jobId equal to rideId
    API-->>R: 202 with rideId
    Q->>W: deliver job
    loop until accepted, 3 rejections, or no driver frees up in time
        W->>DB: atomically claim an AVAILABLE driver, now BUSY
        alt driver rejects
            W->>DB: free the driver, record the rejection
        else driver accepts
            W->>DB: atomically move ride from REQUESTED to ASSIGNED
        end
    end
    W->>MQ: publish ASSIGNED or NO_DRIVER_FOUND
    MQ-->>B: copy of every event
    MQ-->>O: copy of every event
    W->>Q: if assigned, schedule driver release in 3 to 5 s
```

In words:

1. `POST /rides` validates the body, creates the ride as `REQUESTED` in MongoDB, publishes the `REQUESTED` event, queues a BullMQ job and returns `202` with the `rideId` straight away. The event is published before the job is queued, so no later event can ever overtake it.
2. A worker picks the job up. It reserves one available driver that has not already been offered this ride. Reserving is a single atomic update, and it marks the driver `BUSY` while the offer is pending.
3. The driver's answer is faked: accept with probability `ACCEPT_PROBABILITY` (default 50%).
4. **Accept:** the ride moves to `ASSIGNED` with that driver, the `ASSIGNED` event is published, and a delayed job frees the driver after a random 3 to 5 seconds.
5. **Reject:** the driver is freed immediately, the rejection is recorded, and the ride is offered to the next driver. After 3 rejections the ride becomes `NO_DRIVER_FOUND`.
6. **No driver free right now:** if every untried driver is busy, the same job is pushed back with a 1 second delay and tried again, up to 8 times. After that the ride becomes `NO_DRIVER_FOUND`.
7. Billing and ops each receive a copy of every event through their own queue.

## Ride and driver lifecycles

### Ride status

```mermaid
stateDiagram-v2
    [*] --> REQUESTED: POST /rides
    REQUESTED --> ASSIGNED: a driver accepts
    REQUESTED --> NO_DRIVER_FOUND: 3 rejections, or no driver frees up in time
    ASSIGNED --> [*]
    NO_DRIVER_FOUND --> [*]
```

A ride has exactly the three statuses from the assignment. Both `ASSIGNED` and `NO_DRIVER_FOUND` are final, and a ride can leave `REQUESTED` only once.

### Driver status

```mermaid
stateDiagram-v2
    [*] --> AVAILABLE
    AVAILABLE --> BUSY: reserved for a ride (offer pending)
    BUSY --> AVAILABLE: driver rejects, freed at once
    BUSY --> AVAILABLE: trip time over, release job after 3 to 5 s
```

| Driver status | Meaning | Offered new rides? |
| --- | --- | --- |
| `AVAILABLE` | Free to receive an offer | Yes |
| `BUSY` | Either an offer is pending, or the driver accepted a ride and the simulated trip is still running | No, the worker skips them |

A driver's `currentRideId` records which ride is holding them. A release only takes effect if the driver is still held by the ride named in the release job.

## Additions beyond the assignment

The assignment only asks for a hardcoded list of about 10 fake drivers. The following were added on top, and each one is configurable.

**Driver availability and a busy state.** A driver who has been offered a ride, or who has accepted one, is `BUSY` and is skipped by the worker until they are free again. This makes the simulation behave like a real dispatch system, where one driver cannot serve two rides at once. Reserving a driver is an atomic update that only succeeds if the driver is still `AVAILABLE`, so two workers can never grab the same driver.

**Simulated trip completion.** A driver who accepts stays `BUSY` for a short random time (`BUSY_MIN_MS` to `BUSY_MAX_MS`, 3 to 5 seconds by default) and is then released by a delayed BullMQ job. Without this, only 10 of the 100 rides in the load test could ever be assigned. The ride itself stays `ASSIGNED` when the trip ends. A separate `COMPLETED` ride status was deliberately not added, because the assignment defines three statuses and its Task 3 check expects `ASSIGNED + NO_DRIVER_FOUND` to equal the total.

**Waiting when every driver is busy.** With only 10 drivers, a burst of rides will often find every driver busy. Instead of failing immediately, the ride waits and retries (`WAIT_RETRY_DELAY_MS`, up to `MAX_WAIT_ATTEMPTS`) in case a driver frees up. Rejections and waits are counted separately: only driver rejections count towards the 3-rejection rule.

**A driver is never offered the same ride twice.** Drivers who already rejected a ride are excluded from later offers for it.

**Other additions.** `GET /rides/:id` to check a ride, `GET /health`, an independent event-based double-assignment check in the load test, and a safety net that guarantees every ride ends with a final status even if processing fails.

## Data model

**Ride** (`rides` collection)

| Field | Description |
| --- | --- |
| `rideId` | UUID returned to the rider. Unique. Also used as the BullMQ `jobId`. |
| `riderId`, `pickup`, `dropoff` | Booking details supplied by the rider |
| `status` | `REQUESTED`, `ASSIGNED` or `NO_DRIVER_FOUND` |
| `assignedDriverId` | The driver who accepted, or `null` |
| `triedDrivers` | Drivers already offered this ride, so none is offered twice |
| `rejectionCount` | Number of rejections so far (limit 3) |
| `waitAttempts` | Number of times the ride waited because every driver was busy (limit 8) |
| `createdAt`, `updatedAt` | Timestamps |

**Driver** (`drivers` collection)

| Field | Description |
| --- | --- |
| `driverId` | `D01` to `D10`. Unique. |
| `name` | Display name (the ten fake drivers are seeded on startup) |
| `status` | `AVAILABLE` or `BUSY` |
| `currentRideId` | The ride currently holding this driver, or `null` |

Seeding inserts only missing drivers, so restarting the API never frees a driver who is mid-trip.

## Events

Every status change is published to a durable **fanout exchange** named `ride.events`.

```json
{
  "eventId": "5b2f0c1e-6a52-4d83-9c4e-0f1d2a3b4c5d",
  "type": "RIDE_STATUS_CHANGED",
  "rideId": "9d3a7f10-2b44-4c6e-8a11-7e5f0b9c1d22",
  "status": "ASSIGNED",
  "driverId": "D04",
  "timestamp": "2026-11-12T08:30:00.000Z"
}
```

`driverId` is `null` for `REQUESTED` and `NO_DRIVER_FOUND`.

| Subscriber | Queue | Behaviour |
| --- | --- | --- |
| Billing (Program A) | `billing.events` | Receives every event. Prints `charging rider for ride X` only for `ASSIGNED`. `REQUESTED` is ignored and `NO_DRIVER_FOUND` charges nothing. Remembers charged rides so a redelivered event never charges twice. |
| Ops (Program B) | `ops.events` | Receives every event. Prints `ride X is now in status Y`. |

**Why both programs see everything.** Each subscriber has its own queue bound to the same fanout exchange, and the exchange puts a copy of every event into each queue. They do not compete for messages. If they shared one queue, they would split the events between them, which is exactly what the assignment says must not happen.

**Delivery details.**

- The publisher uses a confirm channel, so it waits until RabbitMQ has accepted each event, and retries a few times if the broker is briefly unavailable.
- Messages and queues are durable, so events survive a broker restart.
- The publisher also declares and binds both subscriber queues on startup. A fanout exchange silently drops a message if no queue is bound, so this guarantees that nothing is lost even if a consumer has never been started yet.
- Consumers acknowledge a message only after handling it. A handling error puts the message back for redelivery, and a message that cannot be parsed is dropped.
- Press Ctrl+C in a consumer to see how many events it received. Billing and ops should report the same number.

## API reference

Base URL: `http://localhost:3000`

### `POST /rides`

Books a ride. Returns immediately. The ride is dispatched in the background.

Request body (all three fields are required, non-empty strings):

```json
{ "riderId": "rider-1", "pickup": "Bandra", "dropoff": "Andheri" }
```

| Status | Meaning | Body |
| --- | --- | --- |
| `202` | Ride accepted and queued | `{ "rideId": "<uuid>", "status": "REQUESTED" }` |
| `400` | A field is missing or not a non-empty string | `{ "error": "Missing or invalid fields", "fields": ["pickup"] }` |
| `503` | The ride could not be queued. It is moved to `NO_DRIVER_FOUND` so it is never left without a final status. | `{ "error": "Could not queue ride, please retry", "rideId": "<uuid>" }` |

### `GET /rides/:id`

Returns the current state of a ride.

```json
{
  "rideId": "9d3a7f10-2b44-4c6e-8a11-7e5f0b9c1d22",
  "riderId": "rider-1",
  "pickup": "Bandra",
  "dropoff": "Andheri",
  "status": "ASSIGNED",
  "assignedDriverId": "D04",
  "rejectionCount": 1,
  "waitAttempts": 0,
  "createdAt": "2026-11-12T08:30:00.000Z",
  "updatedAt": "2026-11-12T08:30:01.000Z"
}
```

Returns `404` with `{ "error": "Ride not found" }` for an unknown id.

### `GET /health`

Returns `{ "ok": true }`.

## Correctness guarantees

The two checks that matter most are that no ride is assigned to two drivers and that no ride is left without a final status. This is how the code protects them.

| Risk | How it is prevented | Where |
| --- | --- | --- |
| A ride gets two drivers, for example when two workers or a redelivered job finish it at once | A ride leaves `REQUESTED` through one atomic update that only matches while the ride is still `REQUESTED`. Exactly one caller wins, the others get nothing and publish nothing. | `rideService.transitionRide` |
| Two rides are offered the same driver | Reserving a driver is one atomic update that only matches while the driver is `AVAILABLE`. If it matches nothing, the worker tries another driver. | `driverService.claimDriver` |
| The same ride is queued twice | The BullMQ `jobId` is the `rideId`, so a duplicate add is ignored. | `queue.enqueueRide` |
| A late or duplicate release frees a driver who has moved on | The release only matches if the driver is still held by the ride in the job. | `driverService.releaseDriver` |
| A worker crashes mid-ride | BullMQ retries the job (5 attempts, 500 ms apart) and re-runs stalled jobs. A restarted job first frees any driver left reserved by the crashed attempt, and an already-assigned ride just has its release re-scheduled. | `worker.processRide` |
| A job fails for good | A safety net moves the ride to `NO_DRIVER_FOUND` and releases any driver it held, so no ride stays in `REQUESTED`. | `worker.startWorkers` |
| A ride waits forever because all drivers stay busy | Waiting is capped by `MAX_WAIT_ATTEMPTS`. Waiting uses BullMQ's delayed state and does not use up the crash-retry attempts. | `worker.processRide` |
| A later event overtakes an earlier one | `REQUESTED` is published before the job is queued. | `app.js` |
| The queue is unavailable when a ride is booked | The ride is moved to `NO_DRIVER_FOUND`, an event is published and the caller gets `503`. | `app.js` |
| A consumer processes an event twice | Billing remembers which rides it has charged. Consumers acknowledge only after handling. | `consumers/billing.js`, `common.js` |

Single-document atomic updates are enough here, so no Mongo transactions and no replica set are needed. That also means the project works on a plain local MongoDB or a free Atlas cluster.

## Configuration

Copy `.env.example` to `.env`. Every value has a default, so the file is optional for a local run.

| Variable | Default | Description |
| --- | --- | --- |
| `PORT` | `3000` | API port |
| `MONGO_URI` | `mongodb://localhost:27017/ride_dispatch` | MongoDB connection string (a MongoDB Atlas `mongodb+srv://` string works too) |
| `REDIS_HOST` / `REDIS_PORT` | `localhost` / `6379` | Redis for BullMQ |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672` | RabbitMQ connection |
| `MAX_DRIVER_REJECTIONS` | `3` | Rejections before a ride becomes `NO_DRIVER_FOUND` |
| `ACCEPT_PROBABILITY` | `0.5` | Chance a driver accepts an offer |
| `BUSY_MIN_MS` / `BUSY_MAX_MS` | `3000` / `5000` | A driver who accepts stays `BUSY` for a random time in this range |
| `WAIT_RETRY_DELAY_MS` | `1000` | Delay before retrying when every untried driver is busy |
| `MAX_WAIT_ATTEMPTS` | `8` | Wait retries before giving up with `NO_DRIVER_FOUND` |
| `WORKER_CONCURRENCY` | `10` | Rides one worker process handles at the same time |

For the load test only: `RIDES` (default `100`), `API_URL` (default `http://localhost:3000`) and `TIMEOUT_MS` (default `120000`). For the tests only: `TEST_MONGO_URI`.

Restart the worker after changing any dispatch setting.

## Getting started

### Prerequisites

- Node.js 18 or newer (the load test uses the built-in `fetch`)
- Docker, for MongoDB, Redis and RabbitMQ (or MongoDB Atlas for the database, see below)

### 1. Start the infrastructure

```bash
docker compose up -d
docker compose ps
```

Wait until `mongo`, `redis` and `rabbitmq` all show as healthy. The RabbitMQ management UI is at http://localhost:15672 (login `guest` / `guest`).

**Using MongoDB Atlas instead of the local Mongo container.** Create a free cluster, add a database user, allow your IP under Network Access, and copy the connection string with the database name `ride_dispatch` added before the `?`. Put it in `.env` as `MONGO_URI`, and start only the other two services:

```bash
docker compose up -d redis rabbitmq
```

### 2. Install and configure

```bash
cp .env.example .env
npm install
```

On Windows PowerShell use `copy .env.example .env`.

`npm install` also downloads a MongoDB binary for the in-memory test database, which can be slow. To skip that download, set `MONGOMS_DISABLE_POSTINSTALL=1` first (bash: `MONGOMS_DISABLE_POSTINSTALL=1 npm install`, PowerShell: `$env:MONGOMS_DISABLE_POSTINSTALL=1` then `npm install`). To install only what the app needs and skip the test tooling, use `npm install --omit=dev`.

### 3. Start the four programs

Run each in its own terminal, from the project folder. Billing and ops go first so you can watch events from the start.

```bash
npm run start:billing
npm run start:ops
npm run start:worker
npm run start:api
```

### 4. Book a ride

```bash
curl -X POST http://localhost:3000/rides \
  -H "Content-Type: application/json" \
  -d '{"riderId":"rider-1","pickup":"Bandra","dropoff":"Andheri"}'
```

On Windows PowerShell use `curl.exe` and put the JSON on one line with escaped quotes: `-d "{\"riderId\":\"rider-1\",\"pickup\":\"Bandra\",\"dropoff\":\"Andheri\"}"`.

You get a `rideId` back immediately. The ops terminal prints `ride <id> is now in status REQUESTED` followed by the final status. If the ride was assigned, billing prints `charging rider for ride <id>`. Check the ride at any time with `GET /rides/<rideId>`.

## Load test (Task 3)

With the API, worker, billing and ops running:

```bash
npm run load-test
```

The script resets every driver to `AVAILABLE`, books 100 rides at the same moment, waits until every ride has a final status (or the timeout passes), and prints a report.

| Check | Expected |
| --- | --- |
| Total rides created | 100 |
| Rides that ended `ASSIGNED` + `NO_DRIVER_FOUND` | Should add up to 100 |
| Any ride assigned to 2 drivers? | 0 |
| Any ride stuck with no final status? | 0 |
| Final-status events published (extra check) | 100, one per ride |

The report is a terminal table with a `PASS` or `FAIL` for each check, followed by how long the run took, how many rides each driver received, and `ALL CHECKS PASSED` or `SOME CHECKS FAILED`. The process exits with code 0 when everything passes and 1 otherwise.

**How "assigned to 2 drivers" is verified.** A ride document can only hold one driver, so checking it alone would prove nothing. The script also subscribes to the event exchange on a private temporary queue (one more subscriber that does not disturb billing or ops) and counts how many `ASSIGNED` events each ride produced, and which driver each one named. A ride counts as a violation if it has more than one `ASSIGNED` event, or if the event's driver disagrees with the one stored in MongoDB.

**Why most rides can end as `NO_DRIVER_FOUND`.** Only 10 drivers exist and each one who accepts is busy for 3 to 5 seconds, while 100 rides arrive at once. Rides wait up to `WAIT_RETRY_DELAY_MS` × `MAX_WAIT_ATTEMPTS` (8 seconds by default) for a driver to free up. A ride that cannot get a driver in that window ends as `NO_DRIVER_FOUND`, which is a valid outcome and still satisfies the "adds up to 100" check. To see more assignments, give rides longer to wait, for example `MAX_WAIT_ATTEMPTS=20`, and restart the worker. The split between the two statuses varies from run to run because acceptance is random.

## Automated tests

```bash
npm test
```

The tests use an in-memory MongoDB, so no Docker services are needed. The queue and the event publisher are replaced with mocks. To run them against a real MongoDB instead, point `TEST_MONGO_URI` at a **separate** database whose name contains `test`, because the tests delete everything in it:

```bash
TEST_MONGO_URI=mongodb://localhost:27017/ride_dispatch_test npm test
```

| File | What it covers |
| --- | --- |
| `tests/rides.api.test.js` | Valid booking returns `202` and queues the ride. The `REQUESTED` event is published before queueing. Invalid bodies return `400` and create nothing. 100 simultaneous bookings create 100 distinct rides and jobs. A queueing failure gives `503` and a final status. `GET /rides/:id` returns the ride, or `404`. |
| `tests/dispatch.test.js` | An accepted ride becomes `ASSIGNED`, the driver becomes `BUSY` and a release is scheduled. Busy drivers are skipped. A driver is never re-offered a ride they rejected. Three rejections end in `NO_DRIVER_FOUND`. When all drivers are busy the job is delayed, and giving up happens only at the wait cap. 30 rides at once against 10 drivers give exactly 10 assignments to 10 different drivers. 20 racing transitions on one ride have exactly one winner. Redelivered jobs and stale reservations are handled. A late release does not free a driver who has moved on. |

## Project structure

```
ride-dispatch/
├── docker-compose.yml          MongoDB, Redis and RabbitMQ for local development
├── package.json                dependencies and npm scripts
├── .env.example                configuration template (copy to .env)
├── README.md
├── scripts/
│   └── loadTest.js             books 100 rides at once and prints the report
├── tests/
│   ├── rides.api.test.js       API tests
│   └── dispatch.test.js        dispatch and concurrency tests
└── src/
    ├── config.js               environment variables and dispatch settings
    ├── db.js                   MongoDB connect and disconnect
    ├── queue.js                BullMQ queues: ride dispatch and driver release
    ├── events.js               RabbitMQ fanout publisher
    ├── app.js                  Express app: POST /rides, GET /rides/:id, GET /health
    ├── server.js               API entry point
    ├── worker.js               dispatch worker and release worker
    ├── models/
    │   ├── Ride.js
    │   └── Driver.js
    ├── seed/
    │   └── drivers.js          the 10 hardcoded fake drivers
    ├── services/
    │   ├── rideService.js      atomic ride transition and event publishing
    │   └── driverService.js    atomic driver claim and release
    └── consumers/
        ├── common.js           shared RabbitMQ consumer setup
        ├── billing.js          Program A
        └── ops.js              Program B
```

## How this maps to the assignment

| Requirement | Where it is implemented |
| --- | --- |
| Task 1: `POST /rides` returns a `rideId` immediately | `src/app.js` |
| Task 1: put the ride in a queue, a separate worker picks it up | `src/queue.js`, `src/worker.js` (BullMQ on Redis) |
| Task 1: hardcoded list of about 10 fake drivers | `src/seed/drivers.js` (`D01` to `D10`) |
| Task 1: random accept or reject at about 50% | `ACCEPT_PROBABILITY` in `src/worker.js` |
| Task 1: accept gives `ASSIGNED`, reject offers the next driver, 3 rejections give `NO_DRIVER_FOUND` | `src/worker.js` (`processRide`) |
| Task 2: publish an event on every status change | `src/events.js`, `src/services/rideService.js`, `src/app.js` |
| Task 2: billing program prints "charging rider for ride X" | `src/consumers/billing.js` |
| Task 2: ops program prints "ride X is now in status Y" | `src/consumers/ops.js` |
| Task 2: both programs must see every event | one queue per subscriber on a fanout exchange |
| Task 3: book 100 rides at once and report the four checks | `scripts/loadTest.js` |

## Design decisions and assumptions

**Redis queue plus RabbitMQ fanout.** The two requirements are different problems. Dispatch needs competing consumers with delays and retries, which BullMQ provides. Event distribution needs every subscriber to see everything, which a fanout exchange with one queue per subscriber provides.

**Atomic single-document updates instead of transactions.** Every critical step (claiming a driver, finishing a ride) is one conditional update, which MongoDB performs atomically. That is simpler than transactions, works on a standalone MongoDB, and makes redelivered or duplicate work harmless.

**The ride status stays `ASSIGNED` after the trip.** Only the driver goes back to `AVAILABLE`. This keeps to the three statuses in the assignment and keeps the Task 3 sum check valid.

**Drivers are fully simulated.** There is no real driver app or trip completion API. A driver's answer is a random draw, and a trip is just a short delay.

**Billing charges only for `ASSIGNED`.** It still receives every event, as required, and simply ignores the others. A ride with no driver is not charged.

**Acceptance and busy timings are configurable.** All timings and limits are environment variables so behaviour can be tuned without code changes.

## Known limitations and next steps

- **State change and event are two separate writes.** If RabbitMQ is down for longer than the short publish retries, an event can be lost even though the ride's state changed. The production fix is a transactional outbox: write the event into MongoDB together with the state change, and have a relay publish it.
- **Billing's duplicate protection lives in memory.** It is lost when billing restarts. A real system would store charges in a database with a unique key on the ride id.
- **Crash cleanup assumes one active job per ride.** When a job restarts, it frees any driver still reserved for that ride. This is safe because BullMQ runs one job per ride at a time, but an unusual lock takeover while the original is still running could break that assumption.
- **No authentication, rate limiting or request throttling** on the API.
- **Delivery is at least once.** Consumers must tolerate duplicates, and billing does.
- **No real trip lifecycle.** There is no `COMPLETED` ride status and no driver or trip service.
- **A single Redis, MongoDB and RabbitMQ instance** is used. There is no replication or failover.

## Troubleshooting

| Symptom | Likely cause and fix |
| --- | --- |
| The API or worker exits at startup with a RabbitMQ error | RabbitMQ is not running or not healthy yet. Run `docker compose ps` and wait for it to become healthy. |
| MongoDB connection or server-selection timeout | The Mongo container is not up, or (for Atlas) your IP is not in Network Access or the password in `MONGO_URI` has special characters that need URL-encoding. |
| A `querySrv` DNS error with Atlas | Your network is blocking the SRV lookup. Use Atlas's standard (non-SRV) connection string, or try another network. |
| A ride stays in `REQUESTED` | The worker is not running. Start it with `npm run start:worker`. |
| Billing and ops show different event counts | They should match. Check that both were started against the same RabbitMQ and that neither crashed. |
| The load test reports mostly `NO_DRIVER_FOUND` | Expected with 10 drivers and short wait windows. Raise `MAX_WAIT_ATTEMPTS` and restart the worker. |
| The load test fails with a connection error | The API is not running, or `API_URL` is wrong. |
| `npm install` is very slow | The MongoDB test binary is downloading. See the install notes under Getting started. |
| A port is already in use | Stop whatever uses 27017, 6379, 5672, 15672 or 3000, or change the port mapping in `docker-compose.yml` and the matching `.env` value. |

## License

MIT
