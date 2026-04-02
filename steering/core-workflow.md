<!-- Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
     SPDX-License-Identifier: Apache-2.0 -->

# Core Managed Service for Apache Flink Development Workflow

## Overview

This workflow guides developers through Managed Service for Apache Flink application development using Kiro IDE with Docker-based local development.

## Workflow Adaptation System

### Step 1: Background Assessment

Before starting, ensure you have a clear understanding of the user persona, use case, and requirements:

STOP: Determine the users background and use case before proceeding:

- Are they new to Flink? New to Managed Service for Apache Flink?
- Are they familiar with Java development?
- Is the use case complex with lots of business logic? Or simple and declarative?

These will inform how to organize the project, and whether to use Flink Table API or DataStream API. In general, assume the DataStream API.

### Step 2: Project Complexity Assessment

The workflow adjusts depth and scope based on your project:

**Simple Projects** (Basic stream processing, single data source):
- Quick start templates and essential patterns only
- Streamlined deployment with minimal configuration
- Focus on getting to production quickly

**Moderate Projects** (Multiple sources, complex transformations):
- Comprehensive guidance with multiple implementation options
- Detailed testing and validation approaches
- Performance optimization considerations

**Complex Projects** (Enterprise-scale, multiple teams, high availability):
- Full systematic approach across all development phases
- Advanced architecture patterns and operational excellence
- Comprehensive monitoring and troubleshooting setup

STOP: Determine the users use case and desired approach before continuing.

#### Step 3: Architecture / Design
**Decision Point**: Select architecture approach based on project complexity.

STOP: **User Control Gate**: ✋ Review and approve architecture plan before implementation begins.

### Step 4: Development and Initial Testing

Create the initial development project, and ensure that project compiles, tests run, and Flink app can be started up at all stages of the project.

STOP: **User Control Gate**: ✋ Confirm local environment setup is complete if the user needs it for their use case, and that user understands Managed Service for Apache Flink service-level vs local Docker configuration separation, and Kiro IDE is configured for Managed Service for Apache Flink development before proceeding.


- [ ] Implement data sources and connectors 
- [ ] Build core stream processing logic using appropriate APIs (DataStream/Table/SQL) 
- [ ] Implement data sinks and output formatting with Managed Service for Apache Flink-optimized patterns
- [ ] Use Kiro's debugging and profiling tools, Docker, and command line to validate the project works as expected

STOP: **User Control Gate**: ✋ Validate core functionality works locally before proceeding to testing.

## Step 5: Detailed Testng
**All Projects**:
- [ ] Set up local Flink cluster with Docker Compose in Kiro environment (→ `local-development.md`)
- [ ] Implement unit tests for core business logic using Kiro's testing framework with Docker containers
- [ ] Validate data processing correctness with test datasets in Kiro's Docker environment
- [ ] **Step-by-Step Process for Data Engineering Teams**:
  1. Create test data that mirrors production patterns in Docker containers
  2. Use Kiro's debugging tools to trace data flow in containerized Flink cluster
  3. Validate transformations match expected business logic using Docker-based testing
  4. Test error handling and data quality scenarios in isolated Docker environment
**Moderate/Complex Projects**:
- [ ] Set up integration testing with realistic data volumes using Kiro's Docker Compose integration
- [ ] Implement performance testing and profiling with Kiro's performance tools in Docker environment

**User Control Gate**: ✋ Confirm all tests pass in Kiro's Docker environment, performance meets requirements, and you understand the strict separation between local Docker configuration and Managed Service for Apache Flink service-level configuration before deployment preparation.

### Step 6: Deployment and Operations

Build and deploy the production JAR to Managed Service for Apache Flink, and review CloudWatch logs for deployment.

## Examples
### Example Workflow for New Applications

```
1. User asks to build a Flink application
2. Confirm user's goals and use case
3. READ best-practices.md using kiroPowers readSteering
4. READ dependency-management.md using kiroPowers readSteering  
5. READ relevant connector guides using kiroPowers readSteering
6. Generate code following the loaded guidance
7. Validate against best practices
8. READ environment-setup.md using kiroPowers readSteering
9. Compile and test locally
```
