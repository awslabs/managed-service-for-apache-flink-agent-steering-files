<!-- Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
     SPDX-License-Identifier: Apache-2.0 -->

# Resource Optimization Guide

## Overview

This guide covers KPU sizing, operator parallelism tuning, Amazon Managed Service for Apache Flink configuration overrides, and checkpoint resource impact. Use it when right-sizing a new Managed Service for Apache Flink application or optimizing an existing one based on CloudWatch metrics.

## KPU Sizing Methodology

### Understanding the KPU Resource Model

Each Kinesis Processing Unit (KPU) provides exactly:
- 1 vCPU
- 4 GB memory (3 GiB JVM heap + 1 GiB reserved for native code allocations including RocksDB, network buffers, and framework overhead)
- 50 GB running application storage

Managed Service for Apache Flink allocates KPUs based on the parallelism settings. The relationship is:

**Total Parallelism (task slots) = KPU Count × Parallelism per KPU**

| KPU Count | Parallelism per KPU | Total Parallelism | Task Slots |
|-----------|---------------------|-------------------|------------|
| 4         | 1                   | 4                 | 4          |
| 8         | 1                   | 8                 | 8          |
| 8         | 2                   | 16                | 16         |
| 16        | 1                   | 16                | 16         |
| 16        | 2                   | 32                | 32         |
| 32        | 1                   | 32                | 32         |

Use parallelism per KPU = 1 for most workloads. The maximum is 8. Increase only when the application is CPU-light and memory-light per task slot (e.g., simple filtering or routing jobs), or for applications with blocking operations (e.g., I/O) where higher values lead to fuller utilization of KPU resources. Higher values pack more task slots per KPU, reducing memory/CPU/storage available per slot.

### Estimating Initial KPU Count

Start with the highest of these three estimates, then add headroom:

**1. Throughput-based estimate:**
```
base_kpus = (input_record_rate × avg_record_size_bytes × processing_amplification) / throughput_per_kpu
```
- `processing_amplification`: ratio of total bytes processed (including intermediate shuffles) to input bytes. Typically 2–4× for jobs with `keyBy` and windowing.
- `throughput_per_kpu`: start with 5–10 MB/s per KPU for typical ETL workloads. CPU-intensive transformations (regex, JSON parsing, ML inference) reduce this to 1–3 MB/s.

**2. State-size-based estimate:**
```
base_kpus = total_state_size_gb / usable_memory_per_kpu_gb
```
- `usable_memory_per_kpu_gb`: approximately 2–2.5 GB per KPU after JVM overhead and network buffers (out of 3 GiB heap). The 1 GiB native memory is used by RocksDB and framework overhead. With parallelism per KPU = 2, usable memory per slot drops to ~1–1.2 GB.

**3. Source-parallelism-based estimate:**
```
base_kpus = max(kinesis_shard_count, kafka_partition_count)
```
Source parallelism should match the partition/shard count. If the source has 16 shards, you generally need at least 16 task slots.

**Final KPU count with headroom:**
```
recommended_kpus = max(throughput_estimate, state_estimate, source_estimate) × 1.3
```
The 1.3× multiplier provides ~30% headroom for checkpoint overhead, traffic spikes, and GC pauses. Round up to the nearest even number for balanced TaskManager allocation.

### Auto-Scaling Behavior

Managed Service for Apache Flink's built-in auto-scaling uses fixed rules based on `containerCPUUtilization` — there are no configurable min/max KPU bounds in the native auto-scaler:

- **Scale up**: When `containerCPUUtilization` exceeds 75% for 15 consecutive 1-minute datapoints, Managed Service for Apache Flink doubles the application's `CurrentParallelism` (and consequently doubles KPUs). `ParallelismPerKPU` is not modified.
- **Scale down**: When `containerCPUUtilization` stays below 10% for 360 consecutive 1-minute datapoints (6 hours), Managed Service for Apache Flink halves the parallelism (rounded up). Managed Service for Apache Flink will never reduce `CurrentParallelism` below the application's configured `Parallelism` setting.
- **During scaling**: The application enters `AUTOSCALING` status and experiences downtime. The only valid API action during this state is `StopApplication` with `Force=true`.
- Auto-scaling does not react to `heapMemoryUtilization` or backpressure — only CPU. If you experience spikes in other metrics, scale manually.
- The default KPU limit is 64 per application. Request a quota increase via Service Quotas if needed.

**For finer-grained control** (custom thresholds, non-CPU metrics, or min/max bounds), disable built-in auto-scaling and implement custom auto-scaling using CloudWatch alarms and the `UpdateApplication` API. AWS provides examples for both [metric-based](https://aws.amazon.com/blogs/big-data/enable-metric-based-and-scheduled-scaling-for-amazon-managed-service-for-apache-flink/) and scheduled scaling patterns.

### Using CloudWatch Metrics to Determine Scaling Direction

After deployment, use these CloudWatch metrics to validate sizing and adjust:

| Metric | Scale Up Signal | Scale Down Signal |
|--------|----------------|-------------------|
| `containerCPUUtilization` | Sustained > 75% over 15 min (triggers auto-scale-up) | Sustained < 10% over 6 hours (triggers auto-scale-down) |
| `heapMemoryUtilization` | Sustained > 80% | Sustained < 40% |
| `backPressuredTimeMsPerSecond` | > 100 ms/s sustained | Consistently 0 |
| `lastCheckpointDuration` | Increasing trend, approaching interval | Stable and well below interval |
| `millisBehindLatest` (Kinesis) | Increasing over time | Stable near 0 |

**Scaling decision process:**
1. If `containerCPUUtilization` is high but `heapMemoryUtilization` is low → add more KPUs (CPU-bound)
2. If `heapMemoryUtilization` is high but `containerCPUUtilization` is low → increase KPUs or request memory override via AWS support (memory-bound). Note: built-in auto-scaling will not trigger on heap metrics alone — scale manually.
3. If `backPressuredTimeMsPerSecond` is high → identify the bottleneck operator in Flink Web UI, then scale or optimize that operator
4. If `millisBehindLatest` is growing → the application cannot keep up with input rate; add KPUs or optimize processing logic

## Operator Parallelism Tuning

### When to Override Default Parallelism

Managed Service for Apache Flink sets application-level parallelism through the service console (KPU count × parallelism per KPU). In most cases, all operators inherit this default. Override per-operator parallelism with `setParallelism()` only when:

- A source operator needs parallelism matched to its partition/shard count
- A CPU-intensive operator needs higher parallelism than the rest of the pipeline
- A lightweight operator (simple filter, static enrichment) can run at lower parallelism to free task slots

**Do not** set application-level parallelism in code for Managed Service for Apache Flink deployments. Managed Service for Apache Flink service-level settings take precedence.

### Source Parallelism Recommendations

**Kinesis source**: Set source parallelism equal to the number of Kinesis shards.

```java
// If the Kinesis stream has 16 shards:
DataStream<Event> events = env
    .fromSource(kinesisSource, watermarkStrategy, "kinesis-source")
    .setParallelism(16)  // Match shard count
    .uid("kinesis-source-uid");
```

Each source subtask is assigned one or more shards. If parallelism < shard count, some subtasks handle multiple shards, which can create uneven load. If parallelism > shard count, excess subtasks sit idle and waste task slots.

**Kafka source**: Set source parallelism equal to the number of Kafka partitions.

```java
// If the Kafka topic has 24 partitions:
DataStream<Event> events = env
    .fromSource(kafkaSource, watermarkStrategy, "kafka-source")
    .setParallelism(24)  // Match partition count
    .uid("kafka-source-uid");
```

The same logic applies: Kafka partitions map 1:1 to source subtasks for optimal throughput.

### Per-Operator Parallelism and KPU Interaction

When you set per-operator parallelism, Managed Service for Apache Flink still allocates task slots based on the maximum parallelism across all operators. The total task slot count equals the application's total parallelism (KPU count × parallelism per KPU).

Operators with lower parallelism than the max use fewer task slots, leaving the remaining slots available for other operators in the same pipeline. Operators with higher parallelism than the default require enough total task slots to accommodate them.

**Example**: Application has 16 KPUs × 1 parallelism per KPU = 16 task slots.

```java
// Source: 8 shards → parallelism 8 (uses 8 of 16 slots)
DataStream<Event> events = env
    .fromSource(kinesisSource, watermarkStrategy, "kinesis-source")
    .setParallelism(8)
    .uid("kinesis-source-uid");

// CPU-heavy processing: use full parallelism (16 slots)
DataStream<Result> results = events
    .keyBy(Event::getKey)
    .process(new HeavyProcessor())
    .uid("heavy-processor-uid");
    // Inherits default parallelism of 16

// Lightweight sink: default parallelism is fine
results.sinkTo(sink)
    .uid("sink-uid");
```


## Managed Service for Apache Flink Configuration Overrides

### Parameters Overridable via AWS Support

Managed Service for Apache Flink manages most infrastructure configuration automatically. However, certain parameters can be overridden by opening an AWS support case. These include:

| Parameter | Default | Override Range | Use Case |
|-----------|---------|---------------|----------|
| JVM heap size | 3 GiB (~75% of KPU memory) | Custom | Applications with large in-memory caches or high object churn |
| TaskManager native memory | 1 GiB (~25% of KPU memory) | Custom | Adjusting RocksDB vs heap balance |
| RocksDB block cache size | Auto-configured | Custom size | Large state with frequent random reads |
| RocksDB write buffer count | Default | 2–6 | High write-throughput state workloads |
| Network buffer memory | Auto-configured | Custom size | Jobs with high fan-out or many network channels |
| State backend type (RocksDB vs. HashMap) | RocksDB | | Jobs with lightweight state that can stay in-memory and benefit from faster in-memory performance |

### Process for Requesting Overrides

1. Gather diagnostic evidence: CloudWatch metrics showing the resource constraint (heap utilization, GC time, checkpoint duration trends)
2. Open an AWS support case under "Managed Service for Apache Flink"
3. Include: application ARN, current KPU count, the specific parameter to override, the requested value, and the diagnostic evidence
4. AWS support applies the override at the service level — no application code changes needed
5. After the override is applied, Managed Service for Apache Flink restarts the application to pick up the new configuration


### OOM and GC Diagnostic Steps

If the application throws `OutOfMemoryError` or shows sustained high GC activity:

1. **Check `heapMemoryUtilization` in CloudWatch.** If consistently > 85%, the application is memory-constrained.
2. **Check `lastCheckpointSize` and `lastCheckpointDuration`.** Large checkpoints consume significant heap during snapshot creation.
3. **Review state TTL configuration.** Missing or overly long TTL causes state to accumulate indefinitely.
4. **Check for Kryo serialization fallbacks.** Kryo uses more memory than POJO serialization. Look for log messages: `"Class ... cannot be used as a POJO type"`.
5. **Review operator state usage.** Use Flink Web UI to check state size per operator. Identify operators with disproportionately large state.

**If the root cause is legitimate memory pressure after optimization:**
- Request a JVM heap size increase via AWS support
- Consider increasing KPU count to spread state across more TaskManagers
- Request RocksDB block cache increase if state reads are the bottleneck (high cache miss rate)

## Checkpoint Impact on Resources

### Checkpoint Size and KPU Memory Consumption

During a checkpoint, Flink snapshots operator state and uploads it to S3 (Managed Service for Apache Flink-managed bucket). This process consumes memory and CPU on each TaskManager:

- **RocksDB state backend** (default on Managed Service for Apache Flink): For incremental checkpoints, Flink uploads only new SST files created since the last checkpoint to S3. The async phase (uploading to S3) does not block record processing but does consume network bandwidth and some memory for upload buffers. The sync phase (snapshotting RocksDB) briefly blocks the subtask.
- Larger state per TaskManager means more data to read and upload, increasing memory and network pressure during the checkpoint window. Skewed state distribution can cause individual TaskManagers to become bottlenecks even when aggregate resources are sufficient.
- There is no official AWS or Flink guidance specifying a fixed percentage of KPU memory to reserve for checkpoint overhead. The actual impact depends on state size, checkpoint type (incremental vs full), and upload concurrency. Monitor `lastCheckpointDuration` and `heapMemoryUtilization` during checkpoint windows to assess whether checkpoint overhead is causing memory pressure.

### Checkpoint Frequency vs CPU and Network Bandwidth

Checkpoint frequency is configured via the Managed Service for Apache Flink `CheckpointInterval` setting (default: 60000ms / 60 seconds). The `MinPauseBetweenCheckpoints` (default: 5000ms) prevents continuous checkpointing when a checkpoint takes longer than the interval. You must set `ConfigurationType` to `CUSTOM` to modify these values. More frequent checkpoints:

- **Increase CPU usage**: each checkpoint triggers RocksDB file reads and S3 uploads across all TaskManagers. With incremental checkpoints (Managed Service for Apache Flink default), the CPU impact is proportional to state changes, not total state size.
- **Increase network bandwidth**: checkpoint data flows from TaskManagers to S3. With large state and frequent checkpoints, this can compete with data processing traffic.
- **Reduce recovery time**: more frequent checkpoints mean less data to replay from sources after a failure.

**Trade-off guidance per Flink docs:** When checkpoints frequently take longer than the base interval, the system ends up constantly taking checkpoints, tying up resources and reducing operator progress. Use `MinPauseBetweenCheckpoints` to prevent this. The Flink documentation does not prescribe specific interval ranges for state sizes — tune based on observed `lastCheckpointDuration` relative to your interval, and ensure checkpoints complete well within the interval with room to spare.

### Checkpoint Duration Exceeding Interval

**Symptoms:**
- `lastCheckpointDuration` in CloudWatch approaches or exceeds the configured checkpoint interval
- `numberOfInProgressCheckpoints` stays > 0 for extended periods
- Increasing `backPressuredTimeMsPerSecond` during checkpoint windows
- `millisBehindLatest` (Kinesis) or consumer lag (Kafka) grows during checkpoints

**Consequences:**
- By default, the next checkpoint is triggered immediately once the ongoing one completes. With `MinPauseBetweenCheckpoints` (default 5s on Managed Service for Apache Flink), there's a minimum gap, but the system can still end up constantly checkpointing.
- For aligned checkpoints (the default mode), checkpoint barriers can cause channels to block while waiting for alignment, contributing to backpressure. Unaligned checkpoints (available from Flink 1.15+, requestable via AWS support for Managed Service for Apache Flink) avoid this alignment delay but have other trade-offs.
- In extreme cases, checkpoint timeouts trigger checkpoint failures, and repeated failures can cause application restarts.

**Remediation:**
1. **Verify incremental checkpoints are active** (they are enabled by default on Managed Service for Apache Flink). If for some reason they were overridden, re-enable them — incremental checkpoints only upload state changes since the last checkpoint, dramatically reducing upload size for large state.
2. **Increase checkpoint interval** via the `UpdateApplication` API with `ConfigurationType: CUSTOM` to give more time for completion. Also consider increasing `MinPauseBetweenCheckpoints`.
3. **Add KPUs** to spread state across more TaskManagers, reducing per-TaskManager checkpoint size.
4. **Reduce state size**: add or tighten TTL on keyed state, reduce key cardinality, or use more compact serialization (POJO over Kryo).
5. **Request RocksDB tuning overrides** via AWS support if compaction or read amplification is the bottleneck.
6. **Consider buffer debloating** — request enablement via AWS support case. This can help applications with backpressure-related checkpoint issues.
