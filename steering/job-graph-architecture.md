<!-- Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
     SPDX-License-Identifier: Apache-2.0 -->

# Job Graph Architecture Guide

## Overview

This guide covers Flink job graph design for Managed Service for Apache Flink applications: operator chaining, data skew detection, operator-to-task-slot mapping, and common anti-patterns (monolith jobs, high fan-out). Use it when designing a new job graph, diagnosing performance problems in an existing one, or deciding whether to split a large application.

## Operator Chaining and Job Graph Basics

### How Operator Chaining Works

Flink automatically chains operators that meet all of these conditions into a single task:

- Same parallelism
- Connected by a forward (non-shuffle) data exchange
- Same slot sharing group
- Neither operator has chaining explicitly disabled

Chained operators run in the same thread, eliminating serialization/deserialization overhead and thread context switching between them. In the Flink Web UI, chained operators appear as a single box in the job graph with names joined by arrows (e.g., `Source → Map → Filter`).

**Verifying chaining in the Flink Web UI:**
1. Open the Flink Web UI from the Managed Service for Apache Flink console
2. Navigate to the running job's "Overview" tab
3. Each box in the job graph represents one task (a chain of operators)
4. Click a task box to see which operators are chained inside it
5. If operators you expected to be chained appear as separate boxes, check that parallelism matches and the data exchange is forward

### Operator-to-Task-Slot Mapping

Each task slot runs one parallel pipeline of chained operators. The number of operators per task slot depends on how many operators chain together and how many slot sharing groups exist.

**Rule of thumb: 20–40 operators per task slot.** This range balances resource utilization against overhead:

| Operators per Task Slot | Behavior |
|------------------------|----------|
| < 20 | Underutilized slots; consider consolidating operators or reducing KPUs |
| 20–40 | Healthy range for most workloads |
| 40–100 | Monitor GC pressure and checkpoint duration closely |
| 100–200 | Likely experiencing performance degradation; consider restructuring |
| > 200 | Split the job or restructure the graph (see Operator-to-Task-Slot Overload) |

### Using `disableChaining()` and `startNewChain()` Strategically

Break chains only when you have a specific reason:

```java
// Break the chain before a CPU-intensive operator to isolate its metrics
DataStream<Result> results = events
    .keyBy(Event::getKey)
    .process(new ExpensiveProcessor())
    .startNewChain()  // This operator starts a new chain
    .uid("expensive-processor-uid");

// Completely disable chaining for a specific operator (rarely needed)
DataStream<Enriched> enriched = events
    .map(new ExternalServiceLookup())
    .disableChaining()  // Runs in its own task, not chained with anything
    .uid("external-lookup-uid");
```

**When to break chains:**
- To isolate a CPU-intensive operator so its metrics (busyTime, backpressure) are visible independently in the Flink Web UI
- To separate an operator that makes external calls (HTTP, database) from the rest of the pipeline so its latency does not mask upstream/downstream metrics
- To control parallelism boundaries — operators with different parallelism cannot chain anyway, but `startNewChain()` makes the intent explicit

**When NOT to break chains:**
- For debugging only — use the Web UI's subtask metrics instead
- To "improve parallelism" — breaking chains does not change parallelism; it only adds serialization overhead
- Preemptively on every operator — this defeats the purpose of chaining and increases resource consumption

### Viewing the Physical Execution Plan

The Flink Web UI shows two views of the job:

1. **Job Graph** (Overview tab): Shows the logical plan with chained operators grouped into task boxes. Use this to verify chaining decisions.
2. **Task Managers** tab: Shows which task slots are allocated on each TaskManager and what tasks run in each slot. Use this to verify operator-to-task-slot distribution.

To check operator-to-task-slot assignments:
1. Open the Flink Web UI → select the running job
2. Click on a task box in the job graph to expand its details
3. The "Subtasks" tab shows each parallel instance (subtask) and which TaskManager hosts it
4. Cross-reference with the TaskManagers tab to see total slot utilization per TaskManager


## Data Skew Detection and Mitigation

### Identifying Skew via Flink Web UI

Data skew occurs when some subtasks of a keyed operator receive significantly more data than others. This causes the overloaded subtasks to become bottlenecks while other subtasks sit idle.

**How to detect skew in the Flink Web UI:**
1. Open the running job → click on a keyed operator (any operator after a `keyBy`)
2. Select the "Subtasks" tab
3. Compare these columns across subtasks:
   - `Bytes Received`: should be roughly equal across subtasks
   - `Records Received`: should be roughly equal across subtasks
   - `Busy Time (ms/s)`: skewed subtasks show much higher values
4. If the max value is more than 3× the median, the operator has significant skew

A subtask processing 10× more records than its peers will become the throughput bottleneck for the entire operator, regardless of how many other subtasks are idle.

### Diagnostic Checklist for Uneven Processing

When you observe uneven processing across subtasks, work through this checklist:

1. **Key distribution**: Are the keys evenly distributed in the source data? A small number of "hot" keys (e.g., a default customer ID, a catch-all category) can concentrate traffic on a few subtasks. Query your source data to check key cardinality and frequency distribution.

2. **Partition assignment**: For Kinesis/Kafka sources, are partitions evenly sized? If one Kinesis shard receives 80% of the traffic (due to partition key hashing), the downstream `keyBy` may inherit that imbalance. Check shard-level `IncomingBytes` metrics in CloudWatch.

3. **Hash collisions**: Flink uses `Object.hashCode()` (or the key serializer's hash) to assign keys to subtasks. Poor hash functions can map many distinct keys to the same subtask. If using custom key types, verify the `hashCode()` implementation distributes evenly.

4. **Key cardinality vs parallelism**: If the number of distinct keys is close to or less than the parallelism, some subtasks may get zero keys. Ensure key cardinality is at least 10× the operator parallelism for reasonable distribution.

5. **Temporal skew**: Some keys may be hot only during certain time windows (e.g., flash sales, batch uploads). Check if skew is sustained or periodic by monitoring subtask metrics over time.

### Warning: High Core Counts + Kryo Serialization + Data Skew

Running Managed Service for Apache Flink applications with high KPU counts (e.g., 64+ KPUs) combined with Kryo serialization and data skew creates a compounding performance problem:

- **Kryo serialization** is slower and produces larger serialized objects than POJO or Avro serialization. This increases network transfer time during shuffles (`keyBy`, `rebalance`).
- **Data skew** concentrates traffic on a few subtasks, which must deserialize a disproportionate share of Kryo-encoded records.
- **High parallelism** amplifies the shuffle: with 64 subtasks, each record after a `keyBy` is serialized, sent over the network, and deserialized. The skewed subtasks become CPU-bound on Kryo deserialization.

**Remediation:**
- Switch from Kryo to POJO serialization by ensuring your data classes follow Flink's POJO rules (public class, public no-arg constructor, public fields or getters/setters). Check logs for `"Class ... cannot be used as a POJO type"` to find Kryo fallbacks.
- If POJO is not feasible, use Avro serialization with a defined schema.
- Address the skew itself: add a salt/prefix to hot keys to spread them across subtasks, then aggregate in a second pass.
- Reduce parallelism for the skewed operator if most subtasks are idle — fewer subtasks with more even load is better than many subtasks with extreme imbalance.


## Operator-to-Task-Slot Overload

### Recommended Ratios and Performance Implications

Each task slot runs a parallel pipeline of chained operators within a single thread. The more operators packed into a slot, the more work that thread must perform — including state access, serialization, timer management, and checkpoint barrier handling.

**Recommended ratio: 20–40 operators per task slot.**

| Ratio Range | Impact |
|-------------|--------|
| 20–40 | Optimal. Checkpoint barriers propagate quickly, GC pressure is manageable, per-operator metrics remain meaningful. |
| 40–100 | Elevated GC pressure from increased object allocation. Checkpoint duration starts to grow as more state must be snapshotted per slot. Latency percentiles widen. |
| 100–200 | Noticeable degradation. GC pauses become frequent, checkpoint durations may approach the checkpoint interval, and tail latency increases significantly. |
| > 200 | Critical. Split the job or restructure the graph. At this density, GC overhead dominates CPU time, checkpoints risk timing out, and individual operator metrics become unreliable. |

### Symptoms of Task Slot Overload

Watch for these indicators that task slots are overloaded:

- **High GC pressure**: `heapMemoryUtilization` in CloudWatch sustained above 80%, frequent full GC pauses visible in TaskManager logs. Each operator in the slot maintains objects on the heap — more operators means more live objects for the GC to traverse.
- **Slow checkpoints**: `lastCheckpointDuration` increasing over time or approaching the checkpoint interval. Each operator in the slot must complete its checkpoint barrier processing sequentially.
- **Increased latency**: End-to-end latency grows because a single thread cycles through more operators per record. The `busyTimeMsPerSecond` metric for the task approaches 1000 (fully saturated).
- **Unresponsive heartbeats**: In extreme cases, TaskManagers miss heartbeat deadlines because GC pauses or checkpoint processing block the heartbeat thread, causing TaskManager restarts.

### Threshold: >200 Operators → Split or Restructure

When a task slot contains more than 200 operators, the overhead from managing that many operators in a single thread outweighs the benefits of co-location. At this point:

1. **Split the job** into two or more independent Flink applications (see Monolith Job Anti-Pattern below). Identify independent processing paths that do not share state and separate them.
2. **Restructure the graph** to reduce operator count:
   - Combine multiple sequential map/filter operations into a single `ProcessFunction` that performs all transformations in one pass
   - Replace chains of simple operators with a `RichFlatMapFunction` that handles multiple transformation steps
   - Remove redundant operators (e.g., a `filter` followed by a `map` that also filters can be merged)
3. **Adjust slot sharing groups** to distribute operators across more slots. By default, all operators share the same slot sharing group. Assigning different groups forces operators into separate slots:

```java
// Place the heavy processing chain in its own slot sharing group
DataStream<Result> results = events
    .keyBy(Event::getKey)
    .process(new HeavyProcessor())
    .slotSharingGroup("heavy-processing")
    .uid("heavy-processor-uid");
```

### Operator Group Scheduling for Efficient Placement

Flink's slot sharing allows operators from different parts of the pipeline to share the same task slot. This is efficient when operators have complementary resource profiles (e.g., a CPU-light source and a CPU-heavy processor in the same slot).

However, when all operators are in the default slot sharing group and the job has many operators, every task slot ends up running one subtask of every operator — leading to overload.

**Strategies for efficient placement:**
- Group operators by resource profile: put CPU-intensive operators in one slot sharing group and I/O-bound operators in another
- Keep source operators in their own group if they have different parallelism than downstream operators (Flink handles this automatically when parallelism differs)
- Use the Flink Web UI's TaskManagers tab to verify that slot utilization is balanced — no single slot should have dramatically more operators than others


## Monolith Job Anti-Pattern

### Indicators of a Monolith Job

A monolith Flink job tries to do too much in a single application. Watch for these indicators:

- **Excessive operator count**: More than 100–150 distinct operators in the job graph. Each operator adds state management, checkpoint overhead, and metric cardinality.
- **Unrelated business logic paths**: The job reads from multiple sources and processes them through independent pipelines that never join or share state. These are separate applications forced into one deployment.
- **Parallelism exceeding 100 per KPU**: When the application needs very high parallelism to keep up, it often means multiple workloads with different throughput requirements are bundled together.
- **Mixed SLA requirements**: Part of the job needs sub-second latency while another part is a batch-like hourly aggregation. These cannot be optimally tuned in a single application.
- **Frequent full-job restarts for partial changes**: A code change to one processing path requires restarting the entire application, including unrelated paths that lose their state or must restore from checkpoint.

### Criteria for Splitting

Split a monolith job when any of these conditions are met:

1. **Independent processing paths**: Two or more pipelines in the job share no state and no data exchange. They are logically separate applications.
2. **Different scaling requirements**: One path needs 32 KPUs to handle peak traffic while another needs only 4. Bundling them wastes resources on the smaller path or under-provisions the larger one.
3. **Different SLA requirements**: One path has a strict latency SLA (< 1 second end-to-end) while another is latency-tolerant (minutes). Checkpoint intervals, parallelism, and resource allocation cannot be optimized for both simultaneously.
4. **Blast radius reduction**: A failure or bug in one processing path should not take down unrelated paths. Separate applications isolate failures.
5. **Independent deployment cycles**: Different teams own different processing paths and need to deploy changes independently.

### Splitting Patterns

**Pattern: Shared Kinesis stream as intermediate topic**

Split the monolith into a producer job and one or more consumer jobs, connected by a Kinesis Data Stream:

```
[Source → Transform → Kinesis Sink]  →  Kinesis Stream  →  [Kinesis Source → Aggregate → Sink]
         Job A (producer)                                          Job B (consumer)
```

- The intermediate Kinesis stream provides durability and decoupling
- Each job scales independently based on its own throughput requirements
- If Job B fails, Job A continues producing; Job B replays from the stream on recovery

**Pattern: Idempotent sinks for exactly-once across jobs**

When splitting a job that previously had end-to-end exactly-once guarantees within a single checkpoint:

- Each job maintains its own checkpoint independently
- Use idempotent sinks (upsert to DynamoDB, deduplication keys in the target) to handle potential duplicates at the boundary between jobs
- The intermediate Kinesis stream provides at-least-once delivery; the consumer job's idempotent sink handles deduplication

### Trade-Offs of Job Splitting

| Factor | Monolith | Split Jobs |
|--------|----------|------------|
| Infrastructure cost | Single Managed Service for Apache Flink application | Multiple applications; additional Kinesis stream costs for intermediate topics |
| Blast radius | One failure affects everything | Failures isolated to individual jobs |
| End-to-end latency | Lower (no intermediate serialization) | Higher (additional serialization hop through intermediate stream) |
| Operational complexity | One deployment, one set of metrics | Multiple deployments, multiple metric dashboards |
| Scaling flexibility | One-size-fits-all KPU allocation | Each job sized independently |
| Debugging | Single job graph to inspect | Must trace records across multiple applications |
| Deployment independence | Full restart for any change | Change and restart only the affected job |

**General guidance**: Accept the additional infrastructure cost and latency of splitting when the monolith causes operational pain (frequent restarts, resource waste, mixed SLAs). For small applications with < 50 operators and uniform requirements, a single job is simpler and appropriate.


## High Fan-Out Anti-Pattern

### The One-Sink-Per-Shard/Partition Pattern

A common mistake is creating a separate sink operator for each output shard or partition. For example, a developer routing events to 64 Kinesis shards might create 64 individual `KinesisStreamsSink` instances, each writing to a specific shard:

```java
// AVOID: Creating one sink per shard
for (int i = 0; i < 64; i++) {
    final int shardIndex = i;
    events.filter(e -> e.getShardKey() % 64 == shardIndex)
          .sinkTo(createKinesisSink("output-stream"))
          .uid("sink-shard-" + shardIndex);
}
```

This creates 64 sink operators in the job graph, each with its own parallelism, state, checkpoint overhead, and connection pool. The job graph balloons in complexity.

### Resource and Performance Consequences

Excessive sink operators cause cascading resource problems:

- **Thread exhaustion**: Each sink operator maintains its own thread pool for async writes. With 64 sinks × default thread pool size, the TaskManager can exhaust available threads, causing write timeouts and backpressure.
- **Connection pool saturation**: Each sink opens its own connections to the target service (Kinesis, Kafka, S3). Hundreds of concurrent connections from a single TaskManager can hit service-side connection limits or client-side socket exhaustion.
- **Checkpoint size inflation**: Every sink operator maintains its own state (pending writes, transaction state for exactly-once). More sinks means more state to checkpoint, increasing checkpoint duration and size proportionally.
- **Metric cardinality explosion**: Each sink produces its own set of CloudWatch metrics. With 64 sinks, the metric count multiplies by 64, increasing CloudWatch costs and making dashboards unusable.
- **Operator-to-task-slot overload**: The 64 sink operators consume a large share of the operator budget per task slot, pushing toward the >200 operator threshold discussed above.

### Alternative Patterns

**Use a single partitioned sink with a partition key generator:**

```java
// RECOMMENDED: Single sink with built-in partitioning
KinesisStreamsSink<Event> sink = KinesisStreamsSink.<Event>builder()
    .setStreamName("output-stream")
    .setSerializationSchema(new EventSerializationSchema())
    .setPartitionKeyGenerator(event -> event.getPartitionKey())
    .build();

events.sinkTo(sink)
    .setParallelism(16)  // Match KPU count, not shard count
    .uid("kinesis-sink-uid");
```

The Kinesis sink handles partitioning internally using the partition key generator. One sink operator distributes records across all shards without creating separate operators per shard.

The same principle applies to Kafka sinks — use a single `KafkaSink` with a `KafkaRecordSerializationSchema` that sets the partition, rather than creating one sink per partition.

### Maximum Recommended Sink Count

**Rule of thumb: no more than 2–4 sink operators per KPU.**

| KPU Count | Max Recommended Sinks | Reasoning |
|-----------|----------------------|-----------|
| 4 | 8–16 | Each sink adds connection overhead and checkpoint state |
| 16 | 32–64 | Beyond this, connection pool and thread pressure becomes significant |
| 32 | 64–128 | At this scale, prefer fewer sinks with built-in partitioning |

If the application genuinely needs to write to many distinct destinations (different streams, different tables), consider:
- Using a single sink with a routing function that directs records to different targets based on record content
- Splitting into multiple jobs, each responsible for a subset of destinations (see Monolith Job Anti-Pattern above)
- Using side outputs to route records to a small number of categorized sinks rather than one per destination

## References

- See [Resource Optimization](resource-optimization.md) for KPU sizing, operator parallelism tuning, and checkpoint resource impact
- See [Best Practices](best-practices.md) for state management and serialization guidance
- See [Monitoring and Metrics](monitoring-and-metrics.md) for CloudWatch metric details and alarm configuration
- See [Managed Service for Apache Flink Overview](msf-overview.md) for KPU resource model and service constraints
