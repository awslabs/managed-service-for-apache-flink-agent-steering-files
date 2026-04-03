<!-- Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
     SPDX-License-Identifier: Apache-2.0 -->

# Kinesis Connector Guide

This guide covers Kinesis connector configuration for Amazon Managed Service for Apache Flink applications.

## Maven Dependencies

**CRITICAL:** Use the correct Kinesis connector dependency from the official Apache Flink project:

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-aws-kinesis-streams</artifactId>
    <version>${kinesis-streams.version}</version>
</dependency>
```

Where `kinesis-streams.version` should match your Flink version (e.g., `5.1.0-1.20` for Flink 1.20, `6.0.0-2.0` for Flink 2.2). See `dependency-management.md` for the full version mapping.

## Kinesis Source Configuration

When creating a Kinesis source, use the `KinesisStreamsSource` builder pattern. This API is the same for both Flink 1.20 and 2.2.

### Correct API Pattern

```java
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions;
import org.apache.flink.shaded.guava31.com.google.common.collect.Maps;

Map<String, Properties> applicationProperties = loadApplicationProperties(env);
Properties inputConfig = applicationProperties.get("input.kinesis.config");
Configuration sourceConfig = Configuration.fromMap(Maps.fromProperties(inputConfig));
String inputStreamArn = inputConfig.getProperty("stream.arn");

KinesisStreamsSource<String> source = KinesisStreamsSource.<String>builder()
    .setStreamArn(inputStreamArn)
    .setDeserializationSchema(new SimpleStringSchema())
    .setSourceConfig(sourceConfig)
    .build();

DataStream<Event> events = env
        .fromSource(source, 
                    WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                            .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                            .withIdleness(Duration.ofSeconds(10)),  // CRITICAL for low-throughput streams
                    "kinesis-source",
                    org.apache.flink.api.common.typeinfo.TypeInformation.of(Event.class))
        .name("kinesis-source")
        .uid("kinesis-source-uid")
        .filter(event -> event != null)
        .name("filter-null-events")
        .uid("filter-null-events-uid");
```

### Key Configuration Methods

- `.setStreamArn(String)` - Set the stream ARN (preferred over stream name)
- `.setDeserializationSchema(DeserializationSchema<T>)` - Set how to deserialize records
- `.setSourceConfig(Configuration)` - Set Kinesis client configuration
- `.build()` - Build the source

## Kinesis Sink Configuration

For Kinesis sinks, use the `KinesisStreamsSink` builder:

```java
Properties outputConfig = applicationProperties.get("output.kinesis.config");
String outputStreamArn = outputConfig.getProperty("stream.arn");

KinesisStreamsSink<String> sink = KinesisStreamsSink.<String>builder()
    .setStreamArn(outputStreamArn)
    .setSerializationSchema(new SimpleStringSchema())
    .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
    .setKinesisClientProperties(outputConfig)
    .build();

events.sinkTo(sink)
        .name("kinesis-sink")
        .uid("kinesis-sink-uid");
```

### Key Sink Methods

- `.setStreamArn(String)` - Set the destination stream ARN
- `.setSerializationSchema(SerializationSchema<T>)` - Set how to serialize records
- `.setPartitionKeyGenerator(PartitionKeyGenerator<T>)` - Set partition key logic
- `.build()` - Build the sink

## Enhanced Fan-Out (EFO) Configuration

### When to Use EFO vs Standard Polling

Use Enhanced Fan-Out when:
- Multiple consumers read from the same Kinesis stream (EFO gives each consumer dedicated 2 MB/s per shard, vs shared 2 MB/s with polling)
- You need sub-200ms read latency (EFO uses HTTP/2 push, polling has up to 200ms delay per `GetRecords` call)
- Your application cannot tolerate `ReadProvisionedThroughputExceeded` throttling from competing consumers

Standard polling is sufficient when:
- Your Flink application is the only consumer on the stream
- Latency requirements are relaxed (200ms+ acceptable)
- You want to minimize cost (EFO incurs additional per-consumer, per-shard-hour charges)

### EFO Source Configuration

Enable EFO by setting `READER_TYPE` and `EFO_CONSUMER_NAME` in the source configuration:

```java
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions;
import org.apache.flink.configuration.Configuration;

Configuration sourceConfig = new Configuration();
sourceConfig.set(KinesisSourceConfigOptions.READER_TYPE, KinesisSourceConfigOptions.ReaderType.EFO);
sourceConfig.set(KinesisSourceConfigOptions.EFO_CONSUMER_NAME, "my-flink-efo-consumer");

KinesisStreamsSource<String> source = KinesisStreamsSource.<String>builder()
    .setStreamArn("arn:aws:kinesis:us-east-1:123456789012:stream/my-stream")
    .setDeserializationSchema(new SimpleStringSchema())
    .setSourceConfig(sourceConfig)
    .build();

// CRITICAL: set parallelism >= shard count to avoid unassigned shards
env.fromSource(source,
        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withIdleness(Duration.ofSeconds(10)),
        "kinesis-efo-source")
    .setParallelism(shardCount)  // match or exceed shard count
    .uid("kinesis-efo-source-uid");
```

The consumer name must be unique per stream but can be reused across different streams. Reusing an existing consumer name on the same stream will terminate the previous subscription.

### Consumer Lifecycle Management

By default (`JOB_MANAGED`), the `KinesisStreamsSource` automatically registers the stream consumer on job start and deregisters on graceful stop. For environments where you want external control:

- `JOB_MANAGED` (default): Flink registers/deregisters the consumer automatically. Preferred for most Managed Service for Apache Flink applications.
- `SELF_MANAGED`: You register the consumer externally via AWS CLI (`aws kinesis register-stream-consumer`) or SDK, then provide the consumer ARN to the job. Use this when multiple jobs share a consumer or when you need to control the consumer lifecycle independently.

### Source Parallelism and Shard Count

Source parallelism must match or exceed the Kinesis shard count when using EFO. If parallelism is less than the shard count, some subtasks handle multiple shards — this still works but reduces the throughput benefit of EFO. If parallelism exceeds the shard count, idle subtasks will block watermark generation unless `withIdleness()` is configured on the `WatermarkStrategy`.

| Scenario | Parallelism vs Shards | Effect |
|---|---|---|
| Parallelism = shard count | 1:1 mapping | Optimal — each subtask gets dedicated 2 MB/s |
| Parallelism < shard count | Some subtasks handle multiple shards | Works but reduces per-shard throughput isolation |
| Parallelism > shard count | Idle subtasks | Requires `withIdleness()` or watermarks stall |

### EFO Interaction with Managed Service for Apache Flink Auto-Scaling and KPU Allocation

When Managed Service for Apache Flink auto-scaling adjusts KPU count, the total parallelism changes. This affects EFO consumers:
- Scaling up increases parallelism, potentially creating idle subtasks if parallelism exceeds shard count — ensure `withIdleness()` is set
- Scaling down reduces parallelism, causing subtasks to handle more shards — EFO still provides dedicated throughput per shard but each subtask processes more data
- During scaling events, the EFO consumer subscription is re-established automatically; expect brief transient errors in logs (this is normal)
- Set Managed Service for Apache Flink auto-scaling min KPU to ensure parallelism never drops below shard count for optimal EFO throughput

### Troubleshooting EFO

**Consumer registration errors (`LimitExceededException`)**:
- Kinesis limits concurrent consumer registrations to 5 in `CREATING` state per account. If you see this during job startup, the consumer will retry automatically. For persistent failures, check if other applications are registering consumers simultaneously.
- Maximum consumers per stream: 20 (Provisioned/On-demand Standard) or 50 (On-demand Advantage). Verify you haven't hit the limit with `aws kinesis list-stream-consumers`.

**Throughput exceptions (`SubscribeToShard` failures)**:
- Each EFO consumer gets dedicated 2 MB/s per shard. If you see throughput errors, verify the consumer is properly registered (`ACTIVE` status) using `aws kinesis describe-stream-consumer`.
- Transient `SubscribeToShard` errors are expected — subscriptions last 5 minutes and are automatically re-acquired.

**IAM permissions for EFO**:
The Managed Service for Apache Flink application's IAM execution role needs these additional permissions beyond standard Kinesis read access:

```json
{
    "Effect": "Allow",
    "Action": [
        "kinesis:RegisterStreamConsumer",
        "kinesis:DeregisterStreamConsumer",
        "kinesis:DescribeStreamConsumer",
        "kinesis:SubscribeToShard"
    ],
    "Resource": [
        "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream",
        "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream/consumer/*"
    ]
}
```

Note the consumer resource ARN (`stream/*/consumer/*`) — `SubscribeToShard` and `DescribeStreamConsumer` require permissions on the consumer resource, not just the stream.

**Retry strategy tuning**:
If `DescribeStreamConsumer` calls fail during startup (common when the consumer was just registered and is still in `CREATING` state), tune the dedicated retry strategy:

```java
sourceConfig.set(KinesisSourceConfigOptions.EFO_DESCRIBE_CONSUMER_RETRY_STRATEGY_MAX_ATTEMPTS, 30);
sourceConfig.set(KinesisSourceConfigOptions.EFO_DESCRIBE_CONSUMER_RETRY_STRATEGY_MIN_DELAY, Duration.ofSeconds(2));
sourceConfig.set(KinesisSourceConfigOptions.EFO_DESCRIBE_CONSUMER_RETRY_STRATEGY_MAX_DELAY, Duration.ofSeconds(10));
```

## Migrating from Legacy Kinesis Consumer to KinesisStreamsSource

**CRITICAL for Flink 2.x upgrades:** KDS connector versions below 5.0 have state that is incompatible with the Flink 2.2 connector (v6.0.0-2.0). You must migrate to connector v5.0+ on Flink 1.x before upgrading to Flink 2.x. See the [AWS blog post on the new Kinesis source connector](https://aws.amazon.com/blogs/big-data/introducing-the-new-amazon-kinesis-source-connector-for-apache-flink/) for full details.

The legacy `FlinkKinesisConsumer` uses the removed `SourceFunction` interface and will not work with Flink 2.x. The new `KinesisStreamsSource` uses the FLIP-27 Source API.

### Migration Paths

**DataStream API with operator UIDs defined:**
1. Update dependencies: replace `flink-connector-kinesis` with `flink-connector-aws-kinesis-streams` v5.0.0+
2. Replace `FlinkKinesisConsumer` with `KinesisStreamsSource` builder pattern
3. Change the UID of the source operator to a new string (this selectively resets source state while preserving all other operator state)
4. Configure starting position with `AT_TIMESTAMP` set to just before deployment time
5. Deploy with `allowNonRestoredState = true`

**Table API/SQL or DataStream without operator UIDs:**
1. Update dependencies and code as above
2. Deploy with `SKIP_RESTORE_FROM_SNAPSHOT` since Flink cannot map old operator state to new operators
3. After the application is running, switch back to `RESTORE_FROM_LATEST_SNAPSHOT` for future restarts

### Key Differences from Legacy Connector

| Feature | Legacy `FlinkKinesisConsumer` | New `KinesisStreamsSource` |
|---------|-------------------------------|---------------------------|
| Interface | `SourceFunction` (removed in 2.x) | FLIP-27 Source API |
| Stream identifier | Stream name | Stream ARN (cross-region/account support) |
| Watermarks | Implicit defaults | Explicit `WatermarkStrategy` required |
| Ordering on reshard | Not guaranteed | Guaranteed via parent-child shard lineage |
| Shard assigner | Even shard distribution | Uniform partition-key distribution |
| AWS SDK | v1 | v2 (non-blocking I/O) |
| JAR size | ~60 MB | ~200 KB |
| KCL/KPL dependency | Included | Removed (no built-in KPL de-aggregation) |

### State Compatibility Warning

The saved state from `FlinkKinesisConsumer` is not compatible with `KinesisStreamsSource`. You cannot restore source position from a snapshot taken with the legacy connector. Plan for either selective state reset (with UIDs) or full state reset (without UIDs) as described above.

## Authentication

In Managed Service for Apache Flink, authentication to Kinesis is handled automatically via the application's IAM execution role. No explicit credentials configuration is needed in the code.

