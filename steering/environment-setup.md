<!-- Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
     SPDX-License-Identifier: Apache-2.0 -->

# Managed Service for Apache Flink Development Environment Setup

## Overview

This guide provides setup instructions for Apache Flink development targeting Amazon Managed Service for Apache Flink deployment. The setup ensures compatibility with Managed Service for Apache Flink requirements while enabling efficient local development and testing workflows using Docker containers that mirror the Managed Service for Apache Flink environment. For new applications, default to Flink 2.2. For existing applications, use the user's current Flink version.

## Prerequisites

Before starting, ensure you have:
- Administrative access to your development machine
- Stable internet connection for downloading dependencies
- At least 16GB RAM and 30GB free disk space (for Docker containers)
- Docker Desktop installed and running
- Java 11 (for Flink 1.20) or Java 17 (for Flink 2.2) installed
- Basic familiarity with command-line operations and Docker concepts

## Core Development Environment

### 1. Docker Desktop Installation

Docker is required for running Flink, Kafka, LocalStack, and other infrastructure locally for testing. Ensure Docker is installed.

**Verification:**
```bash
docker --version
docker-compose --version
docker run hello-world
```

### 2. Kiro IDE Setup

Kiro IDE provides integrated Managed Service for Apache Flink development capabilities with built-in Docker support and Managed Service for Apache Flink-specific tooling.

**Installation:**
1. Download Kiro IDE from the official Kiro website
2. Install following platform-specific instructions
3. Launch Kiro IDE and complete initial setup

**Managed Service for Apache Flink Development Configuration:**
- Java Development Kit (JDK): Java 11 for Flink 1.20, Java 17+ for Flink 2.2. We recommend Amazon Corretto for a free, production-ready distribution of OpenJDK.
- Build Tool: Maven or Gradle for dependency management and build automation.

**Required Extensions/Plugins:**

- Extension Pack for Java: Popular extensions for Java development in Visual Studio Code. Includes Language Support for Java, Debugger for Java, Test Runner for Java, Maven for Java, Project Manager for Java, and IntelliCode.
- Maven for Java: Manage Maven projects, run Maven tasks and provide better Maven project authoring experience in Kiro.


**Plugin Installation in Kiro:**
1. Open Kiro IDE
2. Navigate to Extensions/Plugins panel
3. Search and install required plugins
4. Restart Kiro IDE

### 3. Java Development Kit (JDK) for Container Development

While Flink runs in containers, JDK is still needed for compilation and IDE support. Flink 1.20 requires Java 11. Flink 2.2 requires Java 17 (Java 21 also supported).

**Verification:**
```bash
java -version
javac -version
echo $JAVA_HOME
```

## Docker-Based Flink Development Environment

### Docker Compose Configuration

Create a comprehensive Docker Compose setup that mirrors the Managed Service for Apache Flink environment. Adjust the Flink image tag to match your target version:
- Flink 1.20: `flink:1.20-java11`
- Flink 2.2: `flink:2.2-java17`

**Create `docker-compose.yml` in your project root:**

```yaml
version: '3.8'

# Set FLINK_IMAGE_TAG in your environment or .env file:
#   Flink 1.20: FLINK_IMAGE_TAG=1.20-java11
#   Flink 2.2:  FLINK_IMAGE_TAG=2.2-java17

services:
  # Flink JobManager
  jobmanager:
    image: flink:${FLINK_IMAGE_TAG:-1.20-java11}
    hostname: jobmanager
    container_name: flink-jobmanager
    ports:
      - "8081:8081"
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        jobmanager.memory.process.size: 1600m
        jobmanager.execution.failover-strategy: region
      - RUNTIME_ENVIRONMENT=local
    volumes:
      - ./flink-jobs:/opt/flink/jobs
      - ./flink-conf:/opt/flink/conf
      - flink-checkpoints:/tmp/flink-checkpoints
      - flink-savepoints:/tmp/flink-savepoints
    networks:
      - flink-network

  # Flink TaskManager
  taskmanager:
    image: flink:${FLINK_IMAGE_TAG:-1.20-java11}
    hostname: taskmanager
    container_name: flink-taskmanager
    depends_on:
      - jobmanager
    command: taskmanager
    scale: 2
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 2
        parallelism.default: 2        
        taskmanager.memory.process.size: 1728m
        taskmanager.memory.managed.fraction: 0.4
        state.backend: rocksdb
        state.checkpoints.dir: file:///tmp/flink-checkpoints
        state.savepoints.dir: file:///tmp/flink-savepoints
        execution.checkpointing.interval: 60000
        execution.checkpointing.mode: EXACTLY_ONCE
        restart-strategy: exponential-delay
        restart-strategy.exponential-delay.initial-backoff: 10s
        restart-strategy.exponential-delay.max-backoff: 2min
        restart-strategy.exponential-delay.backoff-multiplier: 2.0
      - RUNTIME_ENVIRONMENT=local
    volumes:
      - ./flink-jobs:/opt/flink/jobs
      - ./flink-conf:/opt/flink/conf
      - flink-checkpoints:/tmp/flink-checkpoints
      - flink-savepoints:/tmp/flink-savepoints
    networks:
      - flink-network

  # Kafka (for streaming data sources)
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    networks:
      - flink-network

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    hostname: kafka
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "9101:9101"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_JMX_PORT: 9101
      KAFKA_JMX_HOSTNAME: localhost
    networks:
      - flink-network

volumes:
  flink-checkpoints:
  flink-savepoints:

networks:
  flink-network:
    driver: bridge
```

### Starting the Development Environment

**1. Create Project Structure:**
```bash
# Create Managed Service for Apache Flink project directory
mkdir my-msf-app
cd my-msf-app

# Create required directories
mkdir -p flink-jobs flink-conf src/main/java src/test/java

# Copy docker-compose.yml and flink-conf.yaml to project root
```

**2. Start Docker Environment:**
```bash
# Start all services
docker-compose up -d

# Verify services are running
docker-compose ps

# Check taskmanager logs for submitted Flink jobs 
docker logs flink-taskmanager

# Check Flink Web UI
open http://localhost:8081

# Check Kafka
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

## Build Tools and Dependencies

### Maven Project Template for Managed Service for Apache Flink

Build the JAR in local mode to include Managed Service for Apache Flink provided dependencies: 

```bash
# For Flink 1.20 (Java 11):
mvn clean package -Plocal -q

# For Flink 2.2 (Java 17):
export JAVA_HOME=$(/usr/libexec/java_home -v 17) && mvn clean package -Plocal -q

docker cp target/my-msf-app-1.0-SNAPSHOT.jar flink-jobmanager:/opt/flink/jobs/

docker exec flink-jobmanager flink run /opt/flink/jobs/my-msf-app-1.0-SNAPSHOT.jar

# OR run directly with:
java -jar target/kinesis-aggregator-1.0.jar

# OR Run in Kiro with "Run and Debug"
```

See `dependency-management.md` for examples of dependencies and pom.xml requried for Managed Service for Apache Flink.

### Managed Service for Apache Flink-Specific Tools

**AWS CLI Managed Service for Apache Flink Commands:**
```bash
# List Managed Service for Apache Flink applications
aws kinesisanalyticsv2 list-applications

# Describe Managed Service for Apache Flink application
aws kinesisanalyticsv2 describe-application --application-name my-app

# Create savepoint
aws kinesisanalyticsv2 create-application-snapshot --application-name my-app --snapshot-name my-snapshot
```



### 3. Kiro IDE Integration Test

```bash
# Open project in Kiro IDE
kiro open .

# Verify Kiro can connect to Docker services
# Check Kiro → Docker → Services panel
# Test debugging capabilities with breakpoints
```
