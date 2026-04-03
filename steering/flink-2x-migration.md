<!-- Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
     SPDX-License-Identifier: Apache-2.0 -->

# Flink 2.x Migration Guide

## Overview

This guide covers migrating Flink applications from 1.x to 2.x. Key changes: Java 17 minimum (Java 8 no longer supported), major API removals, and serialization breaking changes affecting state compatibility.

**CRITICAL**: State migration from 1.x to 2.x fails for applications using Kryo or POJOs with collection fields. See [State Compatibility](#state-compatibility) before planning migrations.

## Major Breaking Changes Summary

| Category | Change | Impact |
|----------|--------|--------|
| Java | Java 8 removed, Java 17 default, Java 21 supported | Must upgrade runtime |
| Source API | `SourceFunction` removed | Must migrate to new Source API |
| Sink API | `SinkFunction`, `SinkV1` removed | Must migrate to Sink V2 API |
| Config | `flink-conf.yaml` removed | Must use `config.yaml` (standard YAML) |
| Time API | `Time` class deprecated | Use `java.time.Duration` |
| Serialization | Kryo 2.x → 5.x, new collection serializers | State incompatibility (see below) |
| DataStream | `IterativeStream`, `TimeCharacteristic` removed | Refactor required |

## Dependency Changes

### Version Updates Required

```xml
<properties>
    <!-- Core versions -->
    <flink.version>2.2.0</flink.version>
    <target.java.version>17</target.java.version>
    <maven.compiler.source>17</maven.compiler.source>
    <maven.compiler.target>17</maven.compiler.target>
    
    <!-- Connector versions (use 2.x compatible) -->
    <kafka.version>4.0.1-2.0</kafka.version>
    <kinesis-streams.version>5.1.0-2.0</kinesis-streams.version>
    
    <!-- Updated dependencies -->
    <msk-iam-auth.version>2.3.5</msk-iam-auth.version>
    <aws.sdk.version>1.12.677</aws.sdk.version>
    <jackson.version>2.15.2</jackson.version>
    <lombok.version>1.18.36</lombok.version>
    <log4j.version>2.23.1</log4j.version>
    <maven.compiler.plugin.version>3.11.0</maven.compiler.plugin.version>
</properties>
```

### Logging Changes

Flink 2.x uses `log4j-slf4j-impl` instead of `slf4j-log4j12`:

```xml
<!-- Remove these -->
<!-- <artifactId>slf4j-log4j12</artifactId> -->

<!-- Use these -->
<dependency>
    <groupId>org.apache.logging.log4j</groupId>
    <artifactId>log4j-slf4j-impl</artifactId>
    <version>${log4j.version}</version>
</dependency>
<dependency>
    <groupId>org.apache.logging.log4j</groupId>
    <artifactId>log4j-api</artifactId>
    <version>${log4j.version}</version>
</dependency>
<dependency>
    <groupId>org.apache.logging.log4j</groupId>
    <artifactId>log4j-core</artifactId>
    <version>${log4j.version}</version>
</dependency>
```

### Glue Schema Registry Conflict

Exclude old flink-avro from Glue Schema Registry until it is updated for Flink 2.x:

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-avro</artifactId>
    <version>${flink.version}</version>
</dependency>
<dependency>
    <groupId>software.amazon.glue</groupId>
    <artifactId>schema-registry-flink-serde</artifactId>
    <version>1.1.15</version>
    <exclusions>
        <exclusion>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-avro</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

### Scope Changes for Standalone Deployment

For non-Managed Service for Apache Flink deployment, change scope from `provided` to `compile`:

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-java</artifactId>
    <version>${flink.version}</version>
    <scope>compile</scope> <!-- Required for CEP serialization in 2.x -->
</dependency>
```

## Code Changes

### Time API Migration

Replace `org.apache.flink.streaming.api.windowing.time.Time` with `java.time.Duration`:

```java
// Before (1.x)
import org.apache.flink.streaming.api.windowing.time.Time;
.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
.window(SlidingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(1)))
.window(EventTimeSessionWindows.withGap(Time.seconds(30)))
.within(Time.seconds(10))

// After (2.x)
import java.time.Duration;
.window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
.window(SlidingProcessingTimeWindows.of(Duration.ofMinutes(1), Duration.ofSeconds(1)))
.window(EventTimeSessionWindows.withGap(Duration.ofSeconds(30)))
.within(Duration.ofSeconds(10))
```

### Configuration API Migration

Replace string-based config with type-safe ConfigOptions:

```java
// Before (1.x)
config.setInteger("rest.port", 8081);
config.setBoolean("web.submit.enable", true);

// After (2.x)
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.WebOptions;
config.set(RestOptions.PORT, 8081);
config.set(WebOptions.SUBMIT_ENABLE, true);
```

### Function Lifecycle Changes

`open()` method signature changed from `Configuration` to `OpenContext`:

```java
// Before (1.x)
@Override
public void open(Configuration parameters) {
    // initialization
}

// After (2.x)
@Override
public void open(org.apache.flink.api.common.functions.OpenContext openContext) throws Exception {
    // initialization
}
```

### CEP Pattern Type Information

CEP requires explicit TypeInformation for pattern output:

```java
// Before (1.x)
CEP.pattern(stream, pattern)
    .inEventTime()
    .select(this::extractResult);

// After (2.x)
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

CEP.pattern(stream, pattern)
    .inEventTime()
    .select(
        this::extractResult,
        TypeInformation.of(new TypeHint<ResultType>() {})
    );
```

### POJO Requirements

POJOs must implement Serializable with no-args constructor:

```java
// Before (1.x) - might work without these
@Data
@Builder
public class Event {
    private String id;
    private long timestamp;
}

// After (2.x) - required for proper serialization
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Event implements Serializable {
    private static final long serialVersionUID = 1L;
    private String id;
    private long timestamp;
}
```

### Removed Classes

- `org.apache.flink.streaming.api.TimeCharacteristic` - removed, event time is default
- `org.apache.flink.api.java.typeutils.runtime.kryo.Serializers` - Kryo serializer classes removed
- `flink-java` module removed entirely

## State Compatibility

### Critical Context

### Breaking Changes Summary

Three serialization incompatibilities prevent state migration from 1.x to 2.x:

| Issue | Root Cause | Affected Patterns | Error Signature |
|-------|-----------|-------------------|-----------------|
| Kryo reference tracking | Kryo 2.x → 5.x upgrade | `registerTypeWithKryoSerializer()` | `IndexOutOfBoundsException: Index 116 out of bounds for length 1` |
| Kryo CollectionSerializer | Kryo's internal collection format changed | Generic collections: `List<T>`, `Map<K,V>`, `Set<T>` in state | `ClassNotFoundException: value2` (data misinterpreted as class names) |
| PojoSerializer collection handling | TypeExtractor selects different serializers (FLINK-34037) | POJO fields: List, Map, Set, Collection, Queue, Deque | `StateMigrationException: PojoSerializer@8bf85b5d incompatible with @3282ee3` |

### Compatible Patterns (State Migrates Successfully)

**Serialization Methods:**
- **Avro serialization** with explicit `AvroTypeInfo` - schema-based, independent of Flink's type system
- **Protobuf serialization** - schema-based, bypasses TypeExtractor
- **Custom TypeSerializer** implementations - user-controlled serialization
- **Simple POJOs** without collection fields - no TypeExtractor collection handling
- **Flink Tuples** - direct field access, no reflection
- **Primitive types** - fastest, no serialization changes

**State Types (all compatible with above serializers):**
- ValueState, MapState, ListState, ReducingState, AggregatingState
- BroadcastState with control streams
- Operator state (even-split and union redistribution)
- Window state (tumbling, sliding, session windows)
- Timer state (event-time and processing-time)

**Connectors:**
- Kinesis connector state (default polling and Enhanced Fan-Out)
- Kafka connector state (offsets, partition tracking)

**Table API/SQL:**
- All tested patterns compatible: GROUP BY, window aggregations (TUMBLE, HOP, SESSION)
- Stream joins (INNER, LEFT OUTER), Top-N, deduplication
- DISTINCT aggregations, OVER windows
- Table API provides alternative migration path avoiding DataStream serialization issues

### Incompatible Patterns (State Migration Fails)

**Direct Kryo Usage:**
```java
// INCOMPATIBLE - Kryo 2.x → 5.x reference tracking changed
env.getConfig().registerTypeWithKryoSerializer(MyType.class, MyKryoSerializer.class);
```

**Generic Collections in State:**
```java
// INCOMPATIBLE - Kryo CollectionSerializer format changed
ValueState<List<String>> listState;
ValueState<Map<String, Integer>> mapState;
ValueState<Set<Long>> setState;
```

**POJOs with Collection Fields:**
```java
// INCOMPATIBLE - TypeExtractor selects different collection serializers
public class UserSession {
    public String userId;
    public List<String> eventTypes;        // BREAKS
    public Map<String, Integer> counts;    // BREAKS
    public Set<String> visitedPages;       // BREAKS
    public Queue<Event> eventQueue;        // BREAKS
}
```

### Failure Behavior and Detection

**Failure Path:**
1. Managed Service for Apache Flink application update completes successfully (no errors during deployment)
2. Job transitions to RUNNING state
3. State restoration begins during operator initialization
4. Deserialization fails with serializer mismatch
5. Job transitions to FAILED state
6. Restart strategy triggers automatic restart
7. **Infinite restart loop** - same failure repeats until manual intervention

**Critical: No Sync API Feedback**
- Update API returns success before state restoration
- Failures occur during state restoration path (outside async workflow scope)
- State restoration duration varies (seconds to tens of minutes based on state size)
- **Only visible through metrics**: downtime, restart count, uptime

**Error Signatures to Watch For:**

*Kryo Reference Tracking:*
```
com.esotericsoftware.kryo.KryoException: Unable to resolve reference for String with id: 116
Caused by: java.lang.IndexOutOfBoundsException: Index 116 out of bounds for length 1
```

*Kryo CollectionSerializer:*
```
com.esotericsoftware.kryo.KryoException: Unable to find class: value2
Caused by: java.lang.ClassNotFoundException: value2
```

*PojoSerializer Collection Handling:*
```
org.apache.flink.util.StateMigrationException: The new state serializer 
(org.apache.flink.api.java.typeutils.runtime.PojoSerializer@8bf85b5d) must not be 
incompatible with the old state serializer 
(org.apache.flink.api.java.typeutils.runtime.PojoSerializer@3282ee3)
```

### Pre-Migration Assessment

**Identify Incompatible Patterns:**

1. **Search for Kryo registration:**
```java
grep -r "registerTypeWithKryoSerializer" src/
grep -r "registerKryoType" src/
```

2. **Search for collection fields in POJOs:**
```java
// Look for POJOs with List, Map, Set, Collection, Queue, Deque fields
// that are used in state (ValueState, MapState, etc.)
```

3. **Search for generic collection state:**
```java
grep -r "ValueState<List" src/
grep -r "ValueState<Map" src/
grep -r "ValueState<Set" src/
```

4. **Check for Kryo fallback warnings in logs:**
```
"Class ... cannot be used as a POJO type because not all fields are valid POJO fields"
```

### Migration Strategies for Incompatible Apps

**Strategy 1: Parallel Deployment (Zero Downtime)**
- Deploy Flink 2.x application alongside 1.x
- Let 2.x rebuild state from scratch while 1.x continues processing
- Switch traffic once 2.x state is fully built
- Best for: Applications that can tolerate dual processing temporarily

**Strategy 2: State Processor API (State Transformation)**
- Use State Processor API to read 1.x savepoint
- Transform state serialization to 2.x-compatible format (Avro/Protobuf)
- Write new savepoint for 2.x restoration
- Best for: Large state that cannot be rebuilt quickly

**Strategy 3: Reprocess from Source (Clean Start)**
- Take final savepoint from 1.x for audit/rollback
- Deploy 2.x with refactored serialization (Avro/Protobuf)
- Reprocess historical data from Kinesis/Kafka/S3
- Best for: Applications with replayable sources and acceptable rebuild time

**Strategy 4: Refactor Before Migration (Recommended)**
- Refactor 1.x application to use compatible serialization (Avro/Protobuf)
- Deploy refactored 1.x, let it checkpoint with new serialization
- Then migrate to 2.x with state preservation
- Best for: Minimizing risk and ensuring future compatibility

### Best Practices for Migration-Safe Applications

**Use Schema-Based Serialization:**
```java
// RECOMMENDED: Avro with explicit TypeInformation
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;

ValueStateDescriptor<UserEvent> descriptor = new ValueStateDescriptor<>(
    "user-events",
    AvroTypeInfo.of(UserEvent.class)
);
```

**Avoid Collection Fields in POJOs:**
```java
// INSTEAD OF:
public class UserSession {
    public List<String> events; // BREAKS on migration
}

// USE:
public class UserSession {
    public String eventsJson; // Serialize collections as strings
    // Or use Avro schema with array types
}
```

**Use MapState Instead of Collections:**
```java
// INSTEAD OF:
ValueState<Map<String, Integer>> mapState; // BREAKS

// USE:
MapState<String, Integer> mapState; // Compatible - Flink's built-in state type
```

**Detect Kryo Fallbacks During Development:**
```java
// Add during development to catch serialization issues early
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.getConfig().disableGenericTypes(); // Throws exception if Kryo would be used
```

## Build Process

Set Java 17 before building:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17) && mvn clean package -DskipTests
```

## Removed APIs and Migration Paths

### Source API Migration

`SourceFunction` is completely removed. Migrate to the new Source API:

```java
// REMOVED in 2.x
env.addSource(new MySourceFunction<>());

// USE instead - new Source API
env.fromSource(
    mySource,
    WatermarkStrategy.forMonotonousTimestamps(),
    "source-name"
);
```

### Sink API Migration

`SinkFunction` and `SinkV1` are removed. Migrate to Sink V2:

```java
// REMOVED in 2.x
stream.addSink(new MySinkFunction<>());

// USE instead - Sink V2 API
stream.sinkTo(mySinkV2);
```

### DataStream API Removals

```java
// REMOVED - IterativeStream (feedback loops)
stream.iterate();
stream.iterate(5000);

// REMOVED - TimeCharacteristic (event time is now default)
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

// REMOVED - keyBy with field positions/names
stream.keyBy(0);           // Use KeySelector instead
stream.keyBy("fieldName"); // Use KeySelector instead

// REMOVED - partitionCustom with field positions
stream.partitionCustom(partitioner, 0);

// REMOVED - legacy file methods
env.readTextFile("path");
stream.writeAsText("path");
stream.writeAsCsv("path");

// REMOVED - legacy window methods
stream.timeWindowAll(Time.seconds(10));
```

### Configuration API Removals

```java
// REMOVED - string-based configuration methods
config.setInteger("key", value);
config.getInteger("key", default);
config.setBoolean("key", value);
// ... all primitive setters/getters with string keys

// REMOVED - Kryo registration methods from StreamExecutionEnvironment
env.registerTypeWithKryoSerializer(MyClass.class, MySerializer.class);
env.addDefaultKryoSerializer(MyClass.class, MySerializer.class);
env.registerType(MyClass.class);

// REMOVED - legacy state backend setter
env.setStateBackend(stateBackend);

// REMOVED - restart strategy methods
env.setRestartStrategy(restartStrategy);
env.getRestartStrategy();
env.setNumberOfExecutionRetries(3);
```

### Configuration File Migration

Legacy `flink-conf.yaml` is no longer supported. Use `config.yaml` with standard YAML format:

```yaml
# config.yaml (new format)
jobmanager:
  rpc:
    address: localhost
    port: 6123
  memory:
    process:
      size: 1600m

taskmanager:
  memory:
    process:
      size: 1728m
  numberOfTaskSlots: 4

parallelism:
  default: 4
```

A migration tool is provided: see Flink documentation for "Migrate from flink-conf.yaml to config.yaml".

### Window Assigner Changes

Window assigners now use `Duration` instead of `Time`:

```java
// REMOVED - Time-based factory methods
TumblingEventTimeWindows.of(Time.seconds(10));
SlidingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(10));
EventTimeSessionWindows.withGap(Time.minutes(5));

// USE instead
TumblingEventTimeWindows.of(Duration.ofSeconds(10));
SlidingProcessingTimeWindows.of(Duration.ofMinutes(1), Duration.ofSeconds(10));
EventTimeSessionWindows.withGap(Duration.ofMinutes(5));
```

### Connector Compatibility

First-party connectors require 2.x compatible versions. Check connector documentation for migration status:

| Connector | Status |
|-----------|--------|
| Kafka | 2.x compatible versions available |
| Kinesis | 2.x compatible versions available |
| File System | Updated API |
| JDBC | Check release notes |

## New Features in 2.x

### Disaggregated State Management

Flink 2.x introduces ForSt (disaggregated state backend) for cloud-native deployments:
- Leverages distributed file systems as primary storage
- Asynchronous execution model for better performance
- Fast rescaling for large state (hundreds of TB)
- Reduced local disk requirements

### DataStream V2 API (Experimental)

New DataStream API with improved design (not yet production-ready):
- Cleaner separation of concerns
- Better state management primitives
- Improved type safety

### SQL Enhancements

- C-style escape strings supported
- New `QUALIFY` clause for filtering window function outputs
- Table functions usable without `TABLE()` wrapper in `FROM`

## Quick Reference: Breaking Changes

| Category | 1.x | 2.x |
|----------|-----|-----|
| Java version | 8, 11+ | 17+ (21 supported) |
| Config file | `flink-conf.yaml` | `config.yaml` (standard YAML) |
| Source API | `SourceFunction` | New Source API only |
| Sink API | `SinkFunction`, `SinkV1` | Sink V2 only |
| Time API | `Time.seconds(n)` | `Duration.ofSeconds(n)` |
| Config API | `config.setInteger("key", val)` | `config.set(ConfigOption, val)` |
| Function open() | `open(Configuration)` | `open(OpenContext)` |
| Logging bridge | `slf4j-log4j12` | `log4j-slf4j-impl` |
| CEP select | Implicit type inference | Explicit `TypeInformation` required |
| Kryo version | 2.x | 5.x (incompatible state format) |
| Collection serializers | Kryo-based | Dedicated serializers (FLINK-34123) |
| TimeCharacteristic | Configurable | Removed (event time default) |
| IterativeStream | Supported | Removed |
| keyBy(int) | Supported | Removed (use KeySelector) |

## Not Supported Features in Managed Service for Apache Flink

The following Flink 2.2 features are not currently supported in Managed Service for Apache Flink as they are still considered experimental in Apache Flink: 

* Materialized Tables
* ForSt State Backend (disaggregated state storage)
* Java 21
* Custom metric reporters/telemetry configurations

For details on which features are supported in Managed Service for Apache Flink, refer to  [Apache Flink 2.2 features supported](https://docs.aws.amazon.com/managed-flink/latest/java/flink-2-2.html#flink-2-2-supported-features). 

## References

- [Flink 2.0 Release Announcement](https://flink.apache.org/2025/03/24/apache-flink-2.0.0-a-new-era-of-real-time-data-processing/)
- [FLINK-34037](https://issues.apache.org/jira/browse/FLINK-34037) - Serialization configuration changes
- [FLINK-34123](https://issues.apache.org/jira/browse/FLINK-34123) - Collection serializer changes
- [FLIP-398](https://cwiki.apache.org/confluence/display/FLINK/FLIP-398) - Serialization improvements
