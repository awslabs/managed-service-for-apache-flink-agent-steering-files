<!-- Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
     SPDX-License-Identifier: Apache-2.0 -->

# Managed Service for Apache Flink Overview and Concepts Guide

## Introduction

Amazon Managed Service for Apache Flink is a fully managed service that makes it easy to develop, deploy, and operate Apache Flink applications on AWS using Kiro IDE for development. Managed Service for Apache Flink supports Flink 1.20 and Flink 2.2. For new applications, default to Flink 2.2. For existing applications, use the user's current Flink version. This guide provides a comprehensive overview of Managed Service for Apache Flink architecture, KPU-based scaling, service-level configuration, and key differences from self-managed Flink deployments.

Managed Service for Apache Flink abstracts infrastructure complexity while providing the full power of Apache Flink for stream processing, with a clear separation between service-level configuration managed by Managed Service for Apache Flink and application-level configuration controlled by developers through Kiro IDE.

## Managed Service for Apache Flink Architecture Overview

### Service Architecture

Managed Service for Apache Flink abstracts away the complexity of managing Flink clusters while providing the full power of Apache Flink for stream processing. The service architecture consists of:

**Control Plane**:
- Application lifecycle management through Managed Service for Apache Flink console and APIs
- KPU-based automatic scaling and resource management
- Service-level configuration management (checkpoints, savepoints, parallelism)
- Integrated CloudWatch monitoring and logging
- IAM-based security and access control

**Data Plane**:
- Flink JobManager and TaskManager processes managed by Managed Service for Apache Flink
- Managed Service for Apache Flink-controlled checkpointing and savepoint storage in managed S3 buckets
- Optimized network and storage configuration for AWS environment
- Automatic fault tolerance and recovery with service-level checkpoint management

### Key Components

1. **Flink Applications**: Your stream processing logic packaged as JAR files, developed using Kiro IDE
2. **KPU Configuration**: Kinesis Processing Units providing standardized resource allocation (1 vCPU, 4GB memory per KPU)
3. **Service-Level Configuration**: Managed Service for Apache Flink-managed settings for checkpoints, savepoints, parallelism, and infrastructure
4. **Application Configuration**: User-controlled runtime parameters, business logic settings, and connector configurations
5. **CloudWatch Integration**: Automatic metrics collection, logs aggregation, and monitoring dashboards

### KPU-Based Resource Model

**Kinesis Processing Units (KPUs)**:
- Each KPU provides exactly 1 vCPU and 4 GB of memory (standardized resource allocation)
- Managed Service for Apache Flink automatically scales KPUs based on application throughput and backpressure metrics
- Parallelism is configured at the Managed Service for Apache Flink service level through console/API, not in application code
- KPU allocation determines the maximum parallelism and resource capacity available to your application
- No need to manage individual TaskManager instances or select EC2 instance types

**Resource Scaling**:
- Automatic horizontal scaling based on real-time throughput and backpressure analysis
- Service-level parallelism configuration through Managed Service for Apache Flink console overrides application defaults
- Managed Service for Apache Flink manages TaskManager allocation and distribution across KPUs automatically
- Pay-per-use pricing model - only pay for the KPUs your application actively uses
- Scaling decisions are made by Managed Service for Apache Flink based on performance metrics, not manual configuration

## Managed Service for Apache Flink Capabilities

### Core Streaming Capabilities

**Stream Processing APIs**:
- DataStream API for low-level stream processing
- Table API for relational stream processing with improved performance
- Flink SQL for declarative stream analytics with expanded function library
- Complex Event Processing (CEP) for pattern detection (note: Flink 2.x requires explicit TypeInformation for CEP pattern output)

**State Management**:
- Managed keyed state with Managed Service for Apache Flink-controlled automatic checkpointing
- Broadcast state for configuration distribution
- RocksDB state backend optimized for Managed Service for Apache Flink environment
- Automatic state cleanup and TTL management
- Service-level checkpoint configuration (interval, retention, storage)

**Time Processing**:
- Event time processing with watermarks
- Processing time for low-latency scenarios
- Ingestion time for simplified time handling
- Custom timestamp extractors and watermark strategies

### AWS Service Integration

**Data Sources**:
- Amazon Kinesis Data Streams
- Amazon Data Firehose
- Amazon MSK (Managed Streaming for Apache Kafka)
- Amazon S3 for batch processing
- Amazon DynamoDB Streams

**Data Sinks**:
- Apache Iceberg
- Amazon S3 with various formats (Parquet, JSON, CSV)
- Amazon DynamoDB for real-time updates
- Amazon OpenSearch Service
- Custom sinks via AWS SDK

## Managed Service for Apache Flink vs Self-Managed Flink

### Key Differences

| Aspect | Managed Service for Apache Flink | Self-Managed Flink |
|--------|-----|-------------------|
| **Flink Version** | Flink 1.20 and 2.2 (managed updates) | Any version (manual upgrades) |
| **Infrastructure Management** | Fully managed by AWS | Manual cluster setup and maintenance |
| **Resource Model** | KPU-based scaling (1 vCPU, 4GB per KPU) | Manual instance type selection and scaling |
| **Parallelism Configuration** | Service-level through Managed Service for Apache Flink console | Application-level in code |
| **Checkpoint Configuration** | Service-level managed by Managed Service for Apache Flink | Application-level configuration required |
| **Savepoint Management** | Managed Service for Apache Flink console and API-based | Manual CLI or API operations |
| **Monitoring** | Integrated CloudWatch | Custom monitoring setup required |
| **Security** | Built-in IAM integration | Manual security configuration |
| **Cost Model** | Pay for KPUs used | Pay for entire cluster resources |

### Configuration Separation: Managed Service for Apache Flink-Managed vs User-Controlled

**Managed Service for Apache Flink-Managed Configuration (Service-Level)**:
- Checkpoint intervals, retention policies, and storage location (managed S3 buckets)
- Savepoint storage, lifecycle management, and automated cleanup policies
- Parallelism allocation and KPU-based scaling parameters
- Network configuration, VPC settings, and security group management
- Flink cluster configuration (JobManager heap size, TaskManager allocation, slot distribution)
- State backend configuration (RocksDB tuning, memory allocation, disk usage)
- Fault tolerance settings (restart strategies, failure recovery policies)

**User-Controlled Configuration (Application-Level)**:
- Business logic implementation and data processing algorithms
- Custom serializers, data formats, and schema definitions
- Application-specific properties, parameters, and runtime configurations
- Source and sink connector configurations (Kinesis, Kafka, S3, DynamoDB connection settings)
- Custom metrics definitions and application-level logging configurations
- Watermark strategies, time characteristic settings, and windowing logic
- User-defined functions (UDFs) and custom operators

### Advantages of Managed Service for Apache Flink

**Operational Simplicity**:
- No cluster management overhead
- Automatic failure recovery with service-level checkpoint management
- Built-in monitoring and alerting through CloudWatch
- Simplified deployment process through Managed Service for Apache Flink console

**AWS Integration**:
- Native connectivity to AWS services with optimized connectors
- Optimized performance for AWS data sources and sinks
- Integrated security and compliance with IAM
- Cost optimization through KPU-based resource allocation

**Scalability**:
- Automatic KPU-based scaling based on throughput
- Service-level parallelism optimization without code changes
- Elastic resource allocation with pay-per-use pricing
- No manual TaskManager or cluster sizing decisions

### Considerations for Migration

**From Self-Managed Flink to Managed Service for Apache Flink**:
- Application code remains largely unchanged (ensure it targets a supported Flink version: 1.20 or 2.2)
- Remove any application-level checkpoint and savepoint configuration (now Managed Service for Apache Flink-managed)
- Update parallelism settings to work with KPU-based scaling model
- Replace custom monitoring with CloudWatch integration
- Migrate from instance-type based scaling to KPU-based resource allocation
- Update development workflow to use Kiro IDE with Docker-based local development

**Configuration Migration**:
- Move checkpoint configuration from application code to Managed Service for Apache Flink service-level settings
- Replace TaskManager memory settings with KPU allocation through Managed Service for Apache Flink console
- Update connector configurations for Managed Service for Apache Flink-optimized AWS service integration
- Remove cluster-level configurations (now managed by Managed Service for Apache Flink)

**Performance Characteristics**:
- Optimized for AWS network and storage with KPU-based resource allocation
- Different latency characteristics due to managed infrastructure
- Automatic resource scaling based on Managed Service for Apache Flink performance metrics
- Built-in performance monitoring through CloudWatch integration

## Managed Service for Apache Flink-Specific Constraints and Optimizations

### Service Constraints

**Resource Limits**:
- Maximum parallelism per application: 1000 (managed through Managed Service for Apache Flink service-level configuration)
- Maximum memory per TaskManager: 8 GB (automatically allocated based on KPU count)
- Maximum number of applications per account: 50 (adjustable through AWS support)
- Checkpoint interval minimum: 1 second (configured via Managed Service for Apache Flink console, not application code)
- Maximum KPU count per application: 250 (providing up to 250 vCPUs and 1TB memory)

**Network Constraints**:
- VPC-only deployment (no direct public internet access for security)
- Specific subnet requirements for high availability across multiple AZs
- Managed Service for Apache Flink-managed security group configuration for service communication
- NAT Gateway or VPC endpoints required for external AWS service connectivity
- Cross-region data transfer limitations for compliance and performance

**Storage Constraints**:
- Checkpoints automatically stored in Managed Service for Apache Flink-managed S3 buckets (user cannot configure location)
- Savepoints require user-specified S3 bucket in same region as Managed Service for Apache Flink application
- State backend limited to RocksDB (optimized and managed by Managed Service for Apache Flink)
- Maximum state size per key: 2 MB (enforced by Managed Service for Apache Flink for performance)
- Checkpoint retention managed by Managed Service for Apache Flink service-level policies, not application configuration

## Common Managed Service for Apache Flink Patterns

### Streaming ETL Pattern
```java
KinesisStreamsSource<String> kdsSource =
        KinesisStreamsSource.<String>builder()
                .setStreamArn("arn:aws:kinesis:us-east-1:123456789012:stream/test-stream")
                .setSourceConfig(sourceConfig)
                .setDeserializationSchema(new SimpleStringSchema())
                .setKinesisShardAssigner(ShardAssignerFactory.uniformShardAssigner()) // This is optional, by default uniformShardAssigner will be used.
                .build();
                
DataStream<ProcessedRecord> processed = source
    .map(new ProcessingFunction())
    .keyBy(ProcessedRecord::getKey)
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
    .aggregate(new AggregationFunction());


KinesisStreamsSink<String> kdsSink =
    KinesisStreamsSink.<String>builder()
        .setKinesisClientProperties(sinkProperties)                               // Required
        .setSerializationSchema(new SimpleStringSchema())                         // Required
        .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))  // Required
        .setStreamArn("arn:aws:kinesis:us-east-1:123456789012:stream/sink-stream")                                        // Required
        .setFailOnError(false)                                                    // Optional
        .setMaxBatchSize(500)                                                     // Optional
        .setMaxInFlightRequests(50)                                               // Optional
        .setMaxBufferedRequests(10_000)                                           // Optional
        .setMaxBatchSizeInBytes(5 * 1024 * 1024)                                  // Optional
        .setMaxTimeInBufferMS(5000)                                               // Optional
        .setMaxRecordSizeInBytes(1 * 1024 * 1024)                                 // Optional
        .build();

processed.sinkTo(kdsSink);
```

### Real-time Analytics Pattern
```sql
-- Managed Service for Apache Flink-optimized Flink SQL for real-time analytics (developed in Kiro IDE)
CREATE TABLE kinesis_source (
    user_id STRING,
    event_type STRING,
    timestamp_col TIMESTAMP(3),
    WATERMARK FOR timestamp_col AS timestamp_col - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kinesis',
    'stream' = 'user-events',
    'aws.region' = 'us-east-1',
    'format' = 'json'
);

CREATE TABLE s3_sink (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    user_count BIGINT,
    event_count BIGINT
) WITH (
    'connector' = 's3',
    'path' = 's3://analytics-bucket/results/',
    'format' = 'parquet'
);

INSERT INTO s3_sink
SELECT 
    TUMBLE_START(timestamp_col, INTERVAL '1' HOUR) as window_start,
    TUMBLE_END(timestamp_col, INTERVAL '1' HOUR) as window_end,
    COUNT(DISTINCT user_id) as user_count,
    COUNT(*) as event_count
FROM kinesis_source
GROUP BY TUMBLE(timestamp_col, INTERVAL '1' HOUR);
```

## Next Steps

After understanding Managed Service for Apache Flink architecture and capabilities:

1. **Environment Setup**: Configure your Kiro IDE development environment for Managed Service for Apache Flink development with Docker containerization
2. **Development Patterns**: Learn Managed Service for Apache Flink-optimized application patterns and templates
3. **Local Development**: Set up Docker-based local testing workflows in Kiro before Managed Service for Apache Flink deployment
4. **Deployment**: Understand Managed Service for Apache Flink deployment procedures and service-level configuration best practices

For detailed guidance on each of these areas, refer to the corresponding guides in the steering directory. All development workflows are optimized for Kiro IDE with Docker-based local development targeting Managed Service for Apache Flink deployment.


### Local Docker Configuration vs Managed Service for Apache Flink Service Configuration

**IMPORTANT DISTINCTION**: Local Docker configuration is for development only and differs significantly from Managed Service for Apache Flink service configuration.

#### Managed Service for Apache Flink Service Configuration (Production Deployment)

**Managed Service for Apache Flink KPU Configuration (Service Level)**
Configured through Managed Service for Apache Flink console - NOT in application code. KPU Configuration:
- Each KPU: 1 vCPU, 4 GB memory
- Parallelism per KPU: 1-4 (typically 1-2 for optimal performance)
- Auto-scaling: Min/Max KPU count
- Managed Service for Apache Flink automatically manages task slot allocation

**Managed Service for Apache Flink Service Configuration (Console/API Only)**
Managed Service for Apache Flink manages these through service configuration:

State backend:
  - RocksDB (configrable via support case)
Checkpoint Configuration:
  - Interval: 60 seconds (configurable)
  - Storage: S3 (Managed Service for Apache Flink-managed bucket)
Savepoint Configuration:
  - Storage: S3 (Managed Service for Apache Flink-managed)
  - Triggered through Managed Service for Apache Flink console/API
Parallelism Configuration:
  - KPU count: Service-level setting
  - Parallelism per KPU: 1-4 (service setting)
  - Auto-scaling: Min/Max KPU bounds
