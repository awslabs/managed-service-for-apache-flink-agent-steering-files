<!-- Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
     SPDX-License-Identifier: Apache-2.0 -->

# Managed Service for Apache Flink Development Best Practices

## Overview

This guide provides Managed Service for Apache Flink-optimized development patterns, anti-patterns, and best practices for building robust, performant, and secure Flink applications on Amazon Managed Service for Apache Flink. For existing applications, use the current user's Flink version. For new applications, assume Flink 2.2 and ask if the user has a preference. 

Code examples in this guide use Flink 1.20 APIs by default. Key API differences for Flink 2.2 are noted inline. See `flink-2x-migration.md` for the complete migration reference.

## Development Patterns

### Best Practices for Managed Service for Apache Flink

**Application Design**:
- Design for KPU-based automatic scaling with service-level parallelism configuration
- Use appropriate parallelism levels as suggestions (Managed Service for Apache Flink service-level settings take precedence)
- Implement proper backpressure handling for Managed Service for Apache Flink's automatic scaling algorithms
- Design stateful operations with Managed Service for Apache Flink-managed checkpoint intervals in mind

**Resource Management**:
- Configure application for KPU-based resource allocation (1 vCPU, 4GB per KPU)
- Let Managed Service for Apache Flink manage checkpoint intervals and retention through service-level configuration
- Monitor resource utilization patterns through CloudWatch metrics
- Implement proper error handling that works with Managed Service for Apache Flink's automatic recovery

**Monitoring and Alerting**:
- Leverage integrated CloudWatch dashboards and metrics
- Configure Managed Service for Apache Flink-specific alarms for KPU utilization and throughput
- Monitor key performance metrics through Managed Service for Apache Flink console and CloudWatch
- Implement application health checks that integrate with Managed Service for Apache Flink monitoring

### Managed Service for Apache Flink-Optimized Application Structure

**Best Practice: Clean Application Architecture**

Flink 1.20:
```java
// Recommended Managed Service for Apache Flink application structure for Flink 1.20
public class MSFStreamingApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // NO checkpoint configuration in application code - managed by Managed Service for Apache Flink service
        // Managed Service for Apache Flink handles: checkpoint intervals, storage, retention, parallelism
        
        // Source configuration with Managed Service for Apache Flink optimizations
        DataStream<Event> events = env
            .addSource(createKinesisSource())
            .name("kinesis-source")
            .uid("kinesis-source-uid");
        
        // Processing logic with proper operator chaining
        DataStream<ProcessedEvent> processed = events
            .keyBy(Event::getKey)
            .process(new EventProcessor())
            .name("event-processor")
            .uid("event-processor-uid");
        
        // Sink with Managed Service for Apache Flink-optimized configuration
        processed.addSink(createS3Sink())
            .name("s3-sink")
            .uid("s3-sink-uid");
        
        env.execute("Managed Service for Apache Flink Streaming Application");
    }
}
```

Flink 2.2 (note: `addSource`/`addSink` are removed — use `fromSource`/`sinkTo`):
```java
// Recommended Managed Service for Apache Flink application structure for Flink 2.2
public class MSFStreamingApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Source configuration using new Source API (required in 2.x)
        DataStream<Event> events = env
            .fromSource(createKinesisSource(), WatermarkStrategy.forMonotonousTimestamps(), "kinesis-source")
            .uid("kinesis-source-uid");
        
        // Processing logic with proper operator chaining
        DataStream<ProcessedEvent> processed = events
            .keyBy(Event::getKey)
            .process(new EventProcessor())
            .name("event-processor")
            .uid("event-processor-uid");
        
        // Sink using Sink V2 API (required in 2.x)
        processed.sinkTo(createS3Sink())
            .name("s3-sink")
            .uid("s3-sink-uid");
        
        env.execute("Managed Service for Apache Flink Streaming Application");
    }
}
```

**Local Docker Development Configuration (docker-compose.yml)**
See `environment-setup.md` for docker-compose.yml setup example.

**Anti-Pattern: Monolithic Processing**
```java
// AVOID: Single large operator doing everything
events.map(event -> {
    // Complex transformation logic
    // Multiple business rules
    // Data enrichment
    // Validation
    // Formatting
    return processedEvent;
}); // Hard to debug, scale, and maintain
```

### State Management Best Practices

**Best Practice: Efficient State Usage with Managed Service for Apache Flink**
- Estimate state size ahead of time to ensure state will remain bounded over time.
- Enable state TTL to ensure state gets cleaned up automatically if not cleaned up manually.
- Use the corret state type for each use case, and perform updates to state in performant way (e.g. make updates to a map key, rather than replacing the entire map).
- AVOID: Storing large objects or unbounded collections in state

```java
public class OptimizedKeyedProcessor extends KeyedProcessFunction<String, Event, Result> {
    
    // Use appropriate state types
    private transient ValueState<EventAggregate> aggregateState;
    
    @Override
    public void open(Configuration parameters) {
        // Flink 2.2: use open(OpenContext openContext) instead of open(Configuration parameters)
        ValueStateDescriptor<EventAggregate> aggregateDescriptor = 
            new ValueStateDescriptor<>("aggregate", EventAggregate.class);
        
        // TTL configuration - application-level concern
        aggregateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofHours(24))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build());
        
        aggregateState = getRuntimeContext().getState(aggregateDescriptor);
    }
    
    @Override
    public void processElement(Event event, Context ctx, Collector<Result> out) throws Exception {
        // Efficient state access patterns
        EventAggregate current = aggregateState.value();
        if (current == null) {
            current = new EventAggregate();
        }
        
        // Update state efficiently
        current.update(event);
        aggregateState.update(current);
        
        // Use timers for handling events that occur after the input event - e.g. in this case we want to trigger the output an hour after the input occurs
        ctx.timerService().registerEventTimeTimer(event.getTimestamp() + 3600000); // 1 hour
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Result> out) throws Exception {
        // Handle timer firing - cleanup or emit final results
        EventAggregate current = aggregateState.value();
        if (current != null) {
            // Emit final result or perform cleanup
            out.collect(new Result(ctx.getCurrentKey(), current.getFinalValue()));
            // Clear state after processing to re-initialize if needed
            aggregateState.clear();
        }
    }
}
```

**Managed Service for Apache Flink State Management**
Managed Service for Apache Flink service handles:
- State backend configuration (RocksDB with S3 for checkpoints/savepoints)
- Checkpoint storage and retention
- Savepoint management through console
- State size monitoring and alerting
Application code should NOT configure these aspects

### Serialization Best Practices

**Performance Hierarchy: Choose the Right Serializer**

Flink serialization performance (fastest to slowest):
1. **Flink Tuples/Rows** - Fastest (direct field access, no reflection)
2. **POJOs** - Fast (~30% slower than tuples, supports schema evolution)
3. **Protobuf** - Good performance (~30% slower than POJOs)
4. **Avro Specific** - Moderate (~50% slower than POJOs)
5. **Avro Generic/Thrift** - Slower (~70% slower than POJOs)
6. **Kryo** - Avoid (50%+ performance penalty). Note: Kryo registration APIs are removed from `StreamExecutionEnvironment` in Flink 2.x; use `env.getConfig()` methods instead, or better yet, avoid Kryo entirely.

**Best Practice: POJO Serialization for Managed Service for Apache Flink**
```java
// Recommended: Flink POJO for optimal performance with schema evolution
public class OptimizedEvent {
    // All fields must be public or have public getters/setters
    public String eventId;
    public long timestamp;
    public String userId;
    public EventType type;
    
    // Required: public no-argument constructor
    public OptimizedEvent() {}
    
    public OptimizedEvent(String eventId, long timestamp, String userId, EventType type) {
        this.eventId = eventId;
        this.timestamp = timestamp;
        this.userId = userId;
        this.type = type;
    }
    
    // Getters and setters for private fields (if any)
}

// Enum types work well with POJO serialization
public enum EventType {
    USER_ACTION, SYSTEM_EVENT, ERROR_EVENT
}
```

**Best Practice: Tuple Types for Maximum Performance**
```java
// Use when performance is critical and schema evolution is not needed
DataStream<Tuple4<String, Long, String, Integer>> events = source
    .map(event -> Tuple4.of(event.getId(), event.getTimestamp(), 
                           event.getUserId(), event.getCount()));

// Access fields by position (f0, f1, f2, f3)
events.keyBy(tuple -> tuple.f2) // Key by userId (f2)
      .process(new TupleProcessor());
```

**Best Practice: Avro for External Integration**
```java
// Use Avro when integrating with external systems or when advanced schema evolution is needed
public class AvroEventProcessor extends ProcessFunction<SpecificRecordBase, ProcessedEvent> {
    
    @Override
    public void processElement(SpecificRecordBase avroEvent, Context ctx, 
                              Collector<ProcessedEvent> out) {
        // Avro specific records provide type safety and good performance
        if (avroEvent instanceof UserEvent) {
            UserEvent userEvent = (UserEvent) avroEvent;
            ProcessedEvent result = new ProcessedEvent();
            result.setUserId(userEvent.getUserId().toString());
            result.setTimestamp(userEvent.getTimestamp());
            out.collect(result);
        }
    }
}

// Configure Avro serialization (Flink 1.20 only; in 2.x, use AvroTypeInfo explicitly instead)
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.getConfig().enableForceAvro(); // Use Avro for POJOs instead of Kryo
```

**Best Practice: Protobuf Integration via Kryo**
```java
// When using Protobuf, register with Kryo for better performance
// Note: In Flink 2.x, Kryo registration methods are removed from StreamExecutionEnvironment.
// Use env.getConfig() methods, or preferably use Avro/POJO serialization instead.
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// Flink 1.20: Register Protobuf serializer
env.getConfig().registerTypeWithKryoSerializer(
    MyProtobufMessage.class, 
    ProtobufSerializer.class
);

// Ensure Protobuf classes are registered
env.getConfig().registerKryoType(MyProtobufMessage.class);
```

**Critical: Avoid Kryo Fallbacks**
```java
// Monitor for Kryo fallbacks in logs - these indicate performance issues
// Log message: "Class ... cannot be used as a POJO type because not all fields are valid POJO fields"

// To detect Kryo usage, disable it temporarily during development
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.getConfig().disableGenericTypes(); // Throws exception if Kryo would be used

// This will fail with: "Generic types have been disabled in the ExecutionConfig"
// Use this to identify where Kryo fallbacks occur, then fix the data types
```

**Best Practice: Type Registration for Performance**
```java
// Register frequently used types to avoid class name serialization overhead IF you use Kryo - but otherwise can avoid Kryo overall
// Note: In Flink 2.x, these env-level registration methods are removed. Use env.getConfig() methods instead.
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// Register custom types with Kryo (when Kryo usage is unavoidable)
env.getConfig().registerKryoType(CustomEvent.class);
env.getConfig().registerKryoType(ProcessingResult.class);

// Register with custom serializers for better performance
env.getConfig().registerTypeWithKryoSerializer(
    ComplexObject.class, 
    CustomKryoSerializer.class
);
```

**State Serialization Considerations**
```java
// For state objects, prioritize schema evolution support
public class StatefulProcessor extends KeyedProcessFunction<String, Event, Result> {
    
    // Use POJO or Avro for state that needs to evolve
    private transient ValueState<EventAggregate> aggregateState; // POJO - good performance + evolution
    
    // Use primitive types for simple state
    private transient ValueState<Long> counterState; // Primitive - fastest
    
    @Override
    public void open(Configuration parameters) {
        // POJO state descriptor - supports schema evolution
        ValueStateDescriptor<EventAggregate> aggregateDescriptor = 
            new ValueStateDescriptor<>("aggregate", EventAggregate.class);
        aggregateState = getRuntimeContext().getState(aggregateDescriptor);
        
        // Primitive state descriptor - fastest serialization
        ValueStateDescriptor<Long> counterDescriptor = 
            new ValueStateDescriptor<>("counter", Long.class);
        counterState = getRuntimeContext().getState(counterDescriptor);
    }
}
```

**Anti-Patterns: Serialization Performance Killers**
```java
// AVOID: Default Java Serialization (extremely slow)
public class SlowEvent implements Serializable {
    // Java serialization is 10x+ slower than POJO serialization
}

// AVOID: Complex nested objects without proper POJO structure
public class BadEvent {
    private Map<String, Object> data; // Generic Object causes Kryo fallback
    private List<SomeInterface> items; // Interface types cause Kryo fallback
}

// AVOID: Missing no-argument constructor (causes Kryo fallback)
public class InvalidPOJO {
    public String field;
    
    // Missing no-arg constructor - will use Kryo instead of POJO serializer
    public InvalidPOJO(String field) {
        this.field = field;
    }
}

// AVOID: Private fields without getters/setters (causes Kryo fallback)
public class AlmostPOJO {
    private String secretField; // No getter/setter - not a valid POJO field
    public String publicField;   // This is fine
    
    public AlmostPOJO() {} // Constructor is correct
}
```

## Performance Best Practices

### KPU-Based Resource Configuration

**Best Practice: Managed Service for Apache Flink KPU-Optimized Applications**
```java
// Application code should be KPU-agnostic
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// DO NOT set parallelism in application code for Managed Service for Apache Flink deployment
// Managed Service for Apache Flink manages parallelism through KPU configuration

// For local Docker development only:
if (isLocalDevelopment()) {
    int localParallelism = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
    env.setParallelism(localParallelism);
}

// Set operator-specific parallelism only when business logic or infrastructure requires it
// E.g. you may set parallelism on a Kafka source operator to be equal to number of partitions, of the overall app parallelism is higher than the number of partitions (lower parallelism for source, but enable high parallelism for processing operators with keyBy or similar operators that spread load)
dataStream
    .keyBy(Event::getPartitionKey)
    .process(new HeavyProcessor())
    // Only set if this operator specifically needs different parallelism
    .setParallelism(5);
```


### Error Handling and Recovery in Managed Service for Apache Flink

**Best Practice: Use Side Outputs for dead letter queues for bad data handling and dependency failures**
```java
public class RobustProcessor extends ProcessFunction<Event, ProcessedEvent> {
    private transient Counter errorCounter;
    private transient OutputTag<Event> deadLetterTag;
    
    @Override
    public void open(Configuration parameters) {
        errorCounter = getRuntimeContext().getMetricGroup().counter("processing_errors");
        deadLetterTag = new OutputTag<Event>("dead-letter") {};
    }
    
    @Override
    public void processElement(Event event, Context ctx, Collector<ProcessedEvent> out) {
        try {
            // Validate input
            if (!isValidEvent(event)) {
                LOG.warn("Invalid event received: {}", event);
                ctx.output(deadLetterTag, event);
                return;
            }
            
            ProcessedEvent result = processEvent(event);
            out.collect(result);
            
        } catch (TransientException e) {
            // Let Managed Service for Apache Flink handle transient errors through restart strategy
            LOG.warn("Transient error processing event {}, Managed Service for Apache Flink will retry", event.getId(), e);
            throw e; // Managed Service for Apache Flink restart strategy handles this
            
        } catch (Exception e) {
            // Handle permanent errors gracefully
            LOG.error("Permanent error processing event {}", event.getId(), e);
            errorCounter.inc();
            ctx.output(deadLetterTag, event);
            // Don't throw - continue processing other events
        }
    }
}
```

## Configuration Optimization

**Critical Principle**: Managed Service for Apache Flink applications must clearly separate local development configuration from Managed Service for Apache Flink service-level configuration.

- **Local Docker Configuration**: Used only for Kiro-based development with Docker containers
- **Managed Service for Apache Flink Service Configuration**: Managed through Managed Service for Apache Flink console and service APIs, not in application code
- **Application Code**: Should be environment-agnostic and avoid hardcoded infrastructure settings

### Environment-Specific Configuration Management

**Best Practice: Clean Configuration Separation**
```java
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

public class FlinkStreamingJob {
    private static final String LOCAL_APPLICATION_PROPERTIES_RESOURCE = "flink-application-properties-dev.json";
    
    private static boolean isLocal(StreamExecutionEnvironment env) {
        // Check for LocalStreamEnvironment OR explicit RUNTIME_ENVIRONMENT=local (when run in Docker)
        String runtime = System.getenv("RUNTIME_ENVIRONMENT");
        return env instanceof LocalStreamEnvironment || "local".equalsIgnoreCase(runtime);
    }

    private static Map<String, Properties> loadApplicationProperties(StreamExecutionEnvironment env) throws IOException {
        if (isLocal(env)) {
            LOG.info("Loading application properties from '{}'", LOCAL_APPLICATION_PROPERTIES_RESOURCE);
            
            // Extract resource to temp file so KinesisAnalyticsRuntime can read it in both docker and local Maven
            InputStream input = KinesisAggregatorJob.class.getClassLoader()
                    .getResourceAsStream(LOCAL_APPLICATION_PROPERTIES_RESOURCE);
            if (input == null) {
                throw new IOException("Unable to find " + LOCAL_APPLICATION_PROPERTIES_RESOURCE);
            }
            
            java.nio.file.Path tempFile = java.nio.file.Files.createTempFile("flink-app-props", ".json");
            java.nio.file.Files.copy(input, tempFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            input.close();
            tempFile.toFile().deleteOnExit();
            
            return KinesisAnalyticsRuntime.getApplicationProperties(tempFile.toString());
        } else {
            LOG.info("Loading application properties from Amazon Managed Service for Apache Flink");
            return KinesisAnalyticsRuntime.getApplicationProperties();
        }
    }

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        if( isLocal(env)) {
            env.enableCheckpointing(10_000);
            env.setParallelism(3);
        }

        // Load the application properties
        final Map<String, Properties> applicationProperties = loadApplicationProperties(env);

        LOG.info("Application properties: {}", applicationProperties);
    }
}
```

Local configuration files for the app-level config should look like:

```json
[
  {
    "PropertyGroupId": "kafka.config",
    "PropertyMap": {
      "bootstrap.servers": "localhost:9092",
      "input.topic": "input-events",
      "output.topic": "aggregated-results",
      "consumer.group.id": "stream-processor-local-group"
    }
  },
  {
    "PropertyGroupId": "window.config",
    "PropertyMap": {
      "size.minutes": "5",
      "allowed.lateness.seconds": "30"
    }
  }
]
```

In Managed Service for Apache Flink, these are configured at the Managed Service for Apache Flink Application level with the same structure (property groups with sub-properties configured).

## Anti-Patterns to Avoid

### Configuration Anti-Patterns

1. **Hardcoded Infrastructure Configuration in Application Code**
2. **App-Level Savepoint Management**

### Performance Anti-Patterns

1. **KPU-Unaware Parallelism Configuration**
   ```java
   // AVOID: Fixed parallelism that doesn't align with KPU model
   env.setParallelism(7); // Doesn't align with KPU scaling
   dataStream.setParallelism(13); // Arbitrary parallelism, only set when operator requires custom parallelism
   ```
2. **Excessive Rebalancing**
   ```java
   // AVOID: Unnecessary rebalance operations
   stream.rebalance().map(...).rebalance().filter(...);
   // Breaks Managed Service for Apache Flink's automatic load balancing
   ```

3. **Blocking Operations in Processing Functions**
   ```java
   // AVOID: Synchronous external calls that block KPU resources, use Async functions instead
   public void processElement(Event event, Context ctx, Collector<Result> out) {
       Result result = externalService.blockingCall(event); // Blocks KPU
       out.collect(result);
   }
   ```

4. **Large State Objects Without TTL**
   ```java
   // AVOID: Unbounded state growth
   private transient ListState<Event> allEvents; // Can exhaust KPU memory
   private transient MapState<String, LargeObject> cache; // No TTL configured
   ```
