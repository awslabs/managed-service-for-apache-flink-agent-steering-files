<!-- Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
     SPDX-License-Identifier: Apache-2.0 -->

# Logging Configuration Guide

## Overview

This guide covers logging best practices for Amazon Managed Service for Apache Flink applications, including Managed Service for Apache Flink service-level vs. application-level logging, local development logging, production log level guidance, querying in CloudWatch Logs Insights, and rate-limited logging patterns for high-throughput operators. Use it when setting up logging for a new application, tuning log levels for production, or adding custom logging within Flink operators without flooding CloudWatch Logs.

### 1. Managed Service for Apache Flink Service-Level Monitoring Log Level

This is the log level configured through the Managed Service for Apache Flink API or console. It controls the application's log level.

- Set via the `MonitoringConfiguration.LogLevel` parameter in `CreateApplication` or `MonitoringConfigurationUpdate.LogLevelUpdate` in `UpdateApplication`.
- Also configurable in the console under the "Monitoring log level" section of the application configuration page.
- Valid values: `ERROR`, `WARN`, `INFO`, `DEBUG`.
- AWS recommends `INFO` because Apache Flink logs some errors at the INFO level rather than ERROR. Using ERROR alone may cause you to miss important failure information.
- `DEBUG` should only be used temporarily for troubleshooting — it significantly impacts application performance.

### 2. Application-Level Log4j2 Configuration

This is the Log4j2 configuration bundled inside your application JAR at `src/main/resources/log4j2.properties` (or `log4j2.xml`). It is only used for local applications - Managed Service for Apache Flink controls the log configuration at the service level and does not apply your in-application log4j configurations.

## Enabling CloudWatch Logging for an Managed Service for Apache Flink Application

To configure logging, you need:

1. Create the CloudWatch log group and log stream.
2. Add the logging option to your application using `CreateApplication` (with `CloudWatchLoggingOptions`) or `AddApplicationCloudWatchLoggingOption` for an existing application.
3. Add the required IAM permissions to your application's service execution role:
   - `logs:PutLogEvents`
   - `logs:DescribeLogGroups`
   - `logs:DescribeLogStreams`

## Local Development Logging vs Managed Service for Apache Flink Logging

### Local Development (IDE / Docker / Flink Mini-Cluster)

When running locally, logging behaves differently from Managed Service for Apache Flink in several ways:

- Logs go to local stdout/stderr (your terminal or IDE console). There is no CloudWatch Logs integration.
- The Managed Service for Apache Flink service-level `MonitoringConfiguration` does not exist locally. Only your Log4j2 configuration applies.
- You can change log levels by editing `log4j2.properties` and restarting the application without redeploying a JAR.

### Managed Service for Apache Flink (Production / Staging)

- All stdout/stderr output from TaskManagers and JobManager is captured and forwarded to CloudWatch Logs automatically.
- Both the service-level monitoring log level and the application-level Log4j2 configuration apply.
- CloudWatch Logs charges $0.50/GB ingested and $0.03/GB stored per month — verbose logging has direct cost impact.
- Log level changes in Log4j2 require redeploying the application JAR.
- The Managed Service for Apache Flink service-level log level can be changed via `UpdateApplication` without redeploying the JAR

## Writing Custom Log Messages to CloudWatch Logs

You can write custom messages from your application code using either Log4j2 directly or SLF4J (which delegates to Log4j2 under the hood in Flink).

### Using SLF4J (Recommended)

SLF4J is the standard logging facade used throughout Flink's own codebase. Using it keeps your code consistent with Flink internals and avoids direct Log4j2 API coupling.

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyFlinkJob {
    private static final Logger LOG = LoggerFactory.getLogger(MyFlinkJob.class);

    public static void main(String[] args) {
        LOG.info("Application starting with configuration: {}", config);
        // NOTE: If config contains user-controlled values, sanitize to prevent log injection:
        // String sanitized = config.toString().replaceAll("[\\r\\n]", "");
    }
}
```

No additional Maven dependencies are needed — SLF4J and the Log4j2 binding are included in the Flink runtime.

### Using Log4j2 Directly

If you prefer the Log4j2 API directly, add these dependencies to your `pom.xml`:

```xml
<dependency>
    <groupId>org.apache.logging.log4j</groupId>
    <artifactId>log4j-api</artifactId>
    <version>2.17.1</version>
</dependency>
<dependency>
    <groupId>org.apache.logging.log4j</groupId>
    <artifactId>log4j-core</artifactId>
    <version>2.17.1</version>
</dependency>
```

```java
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MyFlinkJob {
    private static final Logger LOG = LogManager.getLogger(MyFlinkJob.class);

    public static void main(String[] args) {
        LOG.info("This message will appear in CloudWatch Logs");
    }
}
```

In Managed Service for Apache Flink, custom log messages appear in CloudWatch Logs as structured JSON entries containing fields like `locationInformation`, `logger`, `message`, `threadName`, `applicationARN`, `applicationVersionId`, and `messageType`.

AWS recommends using the `INFO` level for custom messages because the application log contains a large volume of entries — INFO-level messages are easier to filter.

## Managed Service for Apache Flink-Specific CloudWatch Logs Insights Queries

These queries target the structured log format that Managed Service for Apache Flink produces natively (with `applicationARN`, `messageType`, etc.) and are useful for operational analysis beyond application-level logging.

```
# Distribution of tasks across TaskManagers (set time range to a single job run)
fields @timestamp, message
| filter message like /Deploying/
| parse message " to flink-taskmanager-*" as @tmid
| stats count(*) by @tmid
| sort @timestamp desc
| limit 2000

# Subtasks assigned to each TaskManager
fields @timestamp, @tmid, @subtask
| filter message like /Deploying/
| parse message "Deploying * to flink-taskmanager-*" as @subtask, @tmid
| sort @timestamp desc
| limit 2000

# Detect parallelism changes (auto-scaling or manual)
fields @timestamp, @parallelism
| filter message like /property: parallelism.default, /
| parse message "default, *" as @parallelism
| sort @timestamp asc

# Access denied errors
fields @timestamp, @message, @messageType
| filter @message like /AccessDenied/
| sort @timestamp desc

# Source or sink not found (Kinesis stream/resource missing)
fields @timestamp, @message
| filter @message like /ResourceNotFoundException/
| sort @timestamp desc

# Task failures — application switching from RUNNING to RESTARTING
fields @timestamp, @message
| filter @message like /switched from RUNNING to RESTARTING/
| sort @timestamp desc
```
