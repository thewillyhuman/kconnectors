# kafka-connector-source-amq

A Kafka Connect source connector that consumes messages from ActiveMQ over **AMQP 1.0**
and produces them to Kafka. It is a small, focused replacement for
`CamelJmsamqp10sourceSourceConnector`-based setups whose main design goal is
**not losing messages**: the Camel-based setup acknowledges messages when they are read,
so anything buffered inside the connector when it crashes or its internal queues fill up
is lost.

- **At-least-once delivery** — a message is acknowledged on the broker *only after* Kafka
  has confirmed the corresponding record. If the connector crashes, is rebalanced, or the
  connection drops, the broker redelivers the unacknowledged messages.
- **One connector per queue, not per broker** — `amq.url` takes a list of brokers and the
  connector consumes from *all of them concurrently*, distributing them across tasks. Each
  broker connection is independent (own reconnect backoff, own redelivery scope), so one
  unreachable broker never blocks the others.
- **Backpressure instead of buffering** — consumption pauses when too many messages are
  awaiting Kafka confirmation, so a slow or unavailable Kafka pushes back on the broker
  instead of accumulating unbounded internal state.
- **Drop-in for existing SMT chains** — record values are the raw message payload
  (bytes by default), so existing transformation chains that expect raw bytes keep
  working unchanged.
- **Operable** — per-task JMX metrics, a periodic status log line, structured lifecycle
  logging, automatic reconnection with exponential backoff.
- **Maintained stack** — the only runtime dependency is
  [Apache Qpid JMS](https://qpid.apache.org/components/jms/index.html), the actively
  maintained AMQP 1.0 JMS client, using the same broker URIs and URI options as before
  (including `jms.prefetchPolicy.all=...` and `failover:(...)`).

## Delivery semantics

```
 broker                 task (poll loop)                Kafka Connect            Kafka
   │                          │                               │                    │
   │── message (unacked) ────>│  session is in               │                    │
   │                          │  INDIVIDUAL_ACKNOWLEDGE mode  │                    │
   │                          │── SourceRecord ──────────────>│── produce ────────>│
   │                          │   (tracked in-flight)         │                    │
   │                          │                               │<─── ack ───────────│
   │                          │<── commitRecord() ────────────│                    │
   │                          │    (queued for ack)           │                    │
   │<── acknowledge ──────────│  next poll drains acks        │                    │
```

1. `poll()` receives messages on a JMS session in *individual acknowledge* mode: nothing is
   acknowledged at read time. Each message is tracked in an in-flight registry, keyed by an
   acknowledge id embedded in the record's source offset.
2. Kafka Connect applies the configured transformation chain and produces the record.
3. Once the Kafka producer confirms the write (or an SMT intentionally filters the record
   out), the framework calls `commitRecord()`. The message is then queued for
   acknowledgement, which the task thread sends on its next poll (JMS sessions are
   single-threaded by contract).
4. Anything that dies between steps 1 and 4 — task crash, worker restart, rebalance,
   connection reset — leaves the messages unacknowledged, and the broker redelivers them.

Consequences to be aware of:

- Delivery is **at-least-once**: after a failure the same message can be produced to Kafka
  twice. Records carry a `jms.message.id` header (and optionally use it as record key) for
  downstream deduplication.
- With `errors.tolerance: all`, a record that fails conversion/production is skipped by the
  framework and the source message **is acknowledged** — identical to the previous
  behaviour, and the price of tolerating poison messages. Run with the default
  `errors.tolerance: none` if a poison message should stop the task instead.
- Exactly-once source semantics are not possible for JMS (no replayable offsets); the
  connector honestly reports `ExactlyOnceSupport.UNSUPPORTED`.

## Brokers and scaling

`amq.url` is a comma-separated broker list. When producers spread messages across
all brokers (no consuming load balancer in front of them), consuming a queue means
consuming it on *every* broker — the connector owns that fan-in so you deploy **one
connector per queue** instead of one per broker:

```
amq.url: amqp://broker-1.example.com:61112?jms.prefetchPolicy.all=100,amqp://broker-2.example.com:61112?jms.prefetchPolicy.all=100,...
```

Brokers are distributed across tasks:

- `tasksMax >= brokers` — one broker per task; surplus tasks become additional competing
  consumers (rotating over the broker list). `tasksMax: 6` with 6 brokers reproduces the
  old one-connector-per-broker topology exactly, as a single resource.
- `tasksMax < brokers` — each task consumes from several brokers concurrently, sweeping
  them fairly (rotating start, poll timeout split across brokers).

### Example: `tasksMax: 1` with 6 brokers

A single task opens **6 concurrent connections**, one per broker, each with its own
individual-acknowledge session and consumer, and every `poll()` services all of them:

1. **Non-blocking sweep** — the task first drains whatever each of the 6 clients has
   already prefetched (`receiveNoWait`), stopping when the batch is full. The broker the
   sweep *starts* from rotates on every poll, so under load no broker is systematically
   favoured and none can starve the others.
2. **Blocking wait only when idle** — if the sweep found nothing buffered anywhere, the
   task waits for a message by splitting `poll.timeout.ms` across the brokers, in the same
   rotating order (with 6 brokers and the default 1000 ms: `max(1000/6, 50)` ≈ 166 ms
   each). A quiet broker therefore delays a poll by at most its slice, and the rotation
   guarantees every broker regularly gets the first waiting slot.

Acknowledgements are always routed back to the exact broker each message came from, and a
connection reset on one broker only redelivers *that* broker's messages. Reconnection is
per broker and never sleeps while others are healthy: if broker-3 is down, the other five keep
flowing and `BrokersConnected` reports 5/6.

What is shared across the 6 brokers: the `max.unacked.messages` cap (2048 total, not per
broker), `batch.max.size` per poll, and the single task thread with its one Kafka producer
pipeline. That last point is the practical limit — correctness is unaffected, but one
thread carries the combined throughput of all brokers, so `tasksMax: 1` suits low/medium
volume queues; for hot queues raise `tasksMax` towards the broker count.

### Example: `tasksMax: 60` with 6 brokers

The opposite extreme: 60 tasks are created and task *i* connects to broker `i % 6`, so
every broker ends up with **10 competing consumers** (60 connections in total). Each task
handles exactly one broker, so the multi-broker sweep and rotation logic play no role: a
task's poll is a plain blocking receive with the full `poll.timeout.ms`, and the broker
itself load-balances the queue's messages among its 10 consumers (how many each holds at a
time is governed by the prefetch, e.g. `jms.prefetchPolicy.all=100`).

This scales consumption both across brokers *and* across the Connect cluster — the 60
tasks are spread over the Connect workers, each with its own Kafka producer pipeline.
Things to keep in mind at this scale:

- `max.unacked.messages` is **per task**, so the worst-case in-flight (and duplicate window
  after a crash) is `60 × 2048` messages; lower the per-task cap if that matters.
- Message order within the queue is not preserved across competing consumers — already
  true today with `tasksMax > 1`, but 10 consumers per broker interleave more aggressively.
- More tasks than the aggregate message rate needs just adds idle connections, rebalance
  weight and metrics noise. Size `tasksMax` as `brokers × consumers-per-broker` you
  actually want; multiples of the broker count keep the load even (e.g. 12 → 2 per broker,
  60 → 10 per broker), while non-multiples leave some brokers with one consumer more than
  others.

Note the distinction from `failover:(a,b)`, which connects to **one** broker at a time
(HA): list entries are consumed concurrently, and each entry may itself be a failover URI
for an HA pair.

Every record carries a `jms.broker` header and a `broker` source-partition field, so the
origin broker stays visible downstream — previously that provenance only existed in the
connector name. A connection reset on one broker only drops (for redelivery) the messages
of that broker; in-flight messages from the others are unaffected.

For **queues** (including ActiveMQ virtual-topic `Consumer.*` queues),
tasks are competing consumers. For **topics**, a durable subscription
(`amq.subscription.name` + `amq.client.id`) is supported but limited to a single task.

## Configuration

| Key | Default | Description |
|---|---|---|
| `amq.url` | — (required) | Comma-separated list of AMQP 1.0 URIs, e.g. `amqp://broker-1.example.com:61112?jms.prefetchPolicy.all=100,amqp://broker-2.example.com:61112`. All brokers are consumed concurrently. All [Qpid JMS URI options](https://qpid.apache.org/releases/qpid-jms-2.7.0/docs/index.html) work per entry, including `amqps://` for TLS and `failover:(amqp://a,amqp://b)` for HA pairs. |
| `amq.username` | `null` | SASL username. |
| `amq.password` | `null` | SASL password; use a config provider, never plain text. |
| `amq.destination.name` | — (required) | Queue/topic to consume, e.g. `Consumer.sample.events`. |
| `amq.destination.type` | `queue` | `queue` or `topic`. |
| `amq.message.selector` | `null` | Optional JMS selector. |
| `amq.client.id` | `null` | JMS client id (needed for durable subscriptions). |
| `amq.subscription.name` | `null` | Durable topic subscription name (single task only). |
| `kafka.topic` | — (required) | Target Kafka topic. |
| `max.unacked.messages` | `2048` | Backpressure limit: max messages consumed but not yet confirmed by Kafka (replaces `camel.source.maxNotCommittedRecords`). |
| `batch.max.size` | `1024` | Max records per poll. |
| `poll.timeout.ms` | `1000` | Wait for the first message of a batch. |
| `conversion.error.policy` | `fail` | `fail` (task fails, message stays on the broker) or `discard` (log + count + acknowledge). |
| `record.value.format` | `bytes` | `bytes` (Text and Bytes messages become `byte[]`, matching byte-oriented SMT pipelines) or `string`. |
| `record.key.source` | `none` | `none`, `message-id` or `correlation-id`. |
| `record.headers.enabled` | `true` | Copy JMS metadata/properties to record headers (`jms.message.id`, `jms.timestamp`, `jms.redelivered`, `jms.property.<name>`, ...). |
| `connection.retry.backoff.ms` | `1000` | Initial reconnect backoff (doubles up to the max). |
| `connection.retry.backoff.max.ms` | `60000` | Max reconnect backoff. |

TLS and advanced transport settings (truststores, SASL mechanisms, idle timeout, prefetch)
are configured through the standard Qpid JMS URI options on `amq.url`, so existing
`remoteURI` values keep working.

## Migrating from the Camel connector

| Camel (`CamelJmsamqp10sourceSourceConnector`) | This connector |
|---|---|
| `connector.class: ...CamelJmsamqp10sourceSourceConnector` | `connector.class: io.github.thewillyhuman.kafka.connect.source.amq.AmqSourceConnector` |
| `camel.kamelet.jms-amqp-10-source.remoteURI` | `amq.url` (same value works; N per-broker connectors can be merged into one by listing all their URIs) |
| `camel.kamelet.jms-amqp-10-source.destinationType` | `amq.destination.type` |
| `camel.kamelet.jms-amqp-10-source.destinationName` | `amq.destination.name` |
| `camel.component.jms.username` | `amq.username` |
| `camel.component.jms.password` | `amq.password` |
| `topics` | `kafka.topic` |
| `camel.source.maxNotCommittedRecords` | `max.unacked.messages` |
| `transforms.*`, `value.converter`, `errors.tolerance` | unchanged — framework features |

See [`examples/multi-broker.yaml`](examples/multi-broker.yaml) for a full Strimzi
`KafkaConnector` resource consuming one queue from six brokers with a single connector.

## Metrics

Each task registers an MBean:

```
io.github.thewillyhuman.kafka.connect.source.amq:type=source-task,connector="<name>",task="<id>"
```

| Attribute | Meaning |
|---|---|
| `MessagesReceived` | Messages received from the broker (includes redeliveries). |
| `MessagesAcknowledged` | Messages acknowledged after Kafka confirmation. |
| `InFlightMessages` | Gauge: delivered to Kafka Connect, not yet confirmed. |
| `PendingAcknowledgements` | Gauge: confirmed by Kafka, acknowledgement not yet sent. |
| `RedeliveredMessagesReceived` | Received with the JMS redelivered flag (duplicate indicator). |
| `MessagesDiscarded` / `ConversionErrors` | Poison-message counters. |
| `AcknowledgeFailures` / `StaleAcknowledgements` | Acks that failed / arrived after a connection reset. |
| `ConnectionResets` | Broker connection resets. |
| `BrokersConfigured` / `BrokersConnected` | Brokers assigned to this task / currently connected. |
| `Connected` | Whether the task is connected to **every** configured broker. |
| `LastMessageEpochMillis` / `LastAcknowledgeEpochMillis` | Liveness timestamps for alerting. |

`MessagesReceived - MessagesAcknowledged - InFlight - Pending` steadily growing, or
`BrokersConnected < BrokersConfigured`, are the two alert conditions to watch. The standard Kafka Connect
source-task metrics (`source-record-poll-rate`, `source-record-write-rate`, ...) apply as
well, and every task logs a compact status line at each offset flush interval:

```
Status: received=15234, acknowledged=15230, inFlight=4, pendingAcks=0, redelivered=2, ...
```

## Security

- `amqps://` URIs enable TLS; truststore/keystore via URI options
  (`transport.trustStoreLocation`, ...).
- Credentials are `Password`-typed configs: masked in logs and REST API responses. Broker
  URLs are sanitized before logging in case credentials are embedded in the URI.
- Use Kafka Connect config providers for secrets, e.g.
  `amq.password: ${file:/opt/kafka/external-configuration/amq-credentials.properties:password}`.

## Build, quality and releases

This connector is built with Bazel as part of the
[kconnectors](../../README.md) monorepo:

```bash
bazelisk test //connectors/kafka-connector-source-amq/...     # unit + integration tests
bazelisk build //connectors/kafka-connector-source-amq:plugin # Kafka Connect plugin zip
tools/lint/pmd.sh kafka-connector-source-amq                  # PMD (blocking in CI)
```

The integration tests start embedded ActiveMQ Artemis brokers with AMQP acceptors and
verify the delivery semantics end-to-end: acknowledge-after-commit, redelivery of
uncommitted messages after a restart, backpressure at `max.unacked.messages`,
multi-broker consumption and partial broker outages.

**Releasing**: bump `VERSION` in `version.bzl`, merge, then tag — the tag is the release:

```bash
git tag kafka-connector-source-amq-v1.0.0
git push origin kafka-connector-source-amq-v1.0.0
```

CI publishes `io.github.thewillyhuman:kafka-connector-source-amq:<version>` to the
repository's GitHub Packages Maven registry and attaches the plugin zip to a GitHub
release. See the [repository README](../../README.md) for the full release contract.

## Design notes

| Class | Responsibility |
|---|---|
| `AmqSourceConnector` | Config validation, broker-to-task assignment, task fan-out. |
| `AmqSourceTask` | Poll loop over its brokers, ack draining, per-broker reconnect/backoff, backpressure. |
| `JmsClient` | One broker connection: Qpid connection/session/consumer lifecycle, individual-ack session mode, connection epoch. |
| `AckRegistry` | In-flight and pending-acknowledgement tracking (lock-free), scoped per broker connection. |
| `MessageRecordBuilder` | JMS message → `SourceRecord` (payload, key, headers, timestamp, broker provenance). |
| `BrokerUrls` | Broker-list parsing (failover-URI aware). |
| `TaskMetrics` | JMX MBean, counters/gauges. |

Threading: all JMS calls happen on the Connect task thread. `commitRecord()` runs on Kafka
producer callback threads and only moves messages into a concurrent queue; the task thread
drains it at the start of every poll. Every tracked message remembers the broker connection
(and its epoch) it arrived on: when one connection is reset, only its messages are dropped
for broker redelivery, and a Kafka confirmation racing with the reset is detected by the
epoch check instead of being acknowledged through the wrong (or a dead) session.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
