# Amazon Managed Service for Apache Flink — Kiro Power / Agent Skill

Comprehensive guidance for developing, optimizing, and operating Apache Flink applications on AWS Managed Service for Apache Flink. Works as a **Kiro Power** and an **Agent Skill** (compatible with Cursor, Claude Code, and other [Agent Skills](https://agentskills.io/)-compatible agents).

## Overview

- **New Customer Onboarding** — Overcome the steep learning curve for Flink's Java-centric development model
- **Best Practices** — Application design, serialization, state management, and configuration separation
- **Dependency Management** — Maven pom.xml setup for Flink projects
- **Resource Optimization** — KPU sizing, operator parallelism tuning, checkpointing
- **Job Graph Architecture** — Optimization patterns, anti-patterns, operator-to-task-slot mapping
- **Monitoring & Logging** — CloudWatch metrics, alarms, Log4j2 configuration for Managed Service for Apache Flink
- **Flink 2.x Migration** — Upgrading from Flink 1.x to 2.x with state compatibility guidance
- **Connector Guides** — Kinesis source/sink development

## Installation

### Kiro Power

Install from [kiro.dev/powers](https://kiro.dev/powers), from the IDE, or directly from the GitHub URL. See the [documentation](https://kiro.dev/docs/powers/installation/) for detailed instructions.

### Agent Skill (Cursor, Claude Code, and other compatible agents)

The `SKILL.md` file follows the open [Agent Skills](https://agentskills.io/) standard, which is supported by Cursor, Claude Code, and a growing number of agent products.

**Per-project** (available in one project):
```bash
# From your project root — pick the directory your agent discovers:

# Cursor
git clone https://github.com/awslabs/managed-service-for-apache-flink-agent-steering-files .cursor/skills/flink

# Claude Code
git clone https://github.com/awslabs/managed-service-for-apache-flink-agent-steering-files .claude/skills/flink

# Agent Skills standard (works with any compatible agent)
git clone https://github.com/awslabs/managed-service-for-apache-flink-agent-steering-files .agents/skills/flink
```

**Personal** (available across all projects):
```bash
# Cursor
git clone https://github.com/awslabs/managed-service-for-apache-flink-agent-steering-files ~/.cursor/skills/flink

# Claude Code
git clone https://github.com/awslabs/managed-service-for-apache-flink-agent-steering-files ~/.claude/skills/flink
```

The agent will automatically discover the skill via `SKILL.md`. In Claude Code you can also invoke it with `/flink`. In Cursor, type `/` in Agent chat and search for `flink`.

## Quick Start

Once installed, try:

```
"Help me build a Flink application that reads from Kinesis, processes events, and writes to another Kinesis stream"
```

The AI will load the appropriate reference files and guide you through:
1. Setting up the project with correct Maven dependencies
2. Implementing the pipeline following Managed Service for Apache Flink best practices
3. Configuring Kinesis connectors with proper serialization
4. Sizing resources (KPUs, parallelism) for your workload
5. Setting up monitoring and logging

## Keywords That Activate This Power

When you mention these keywords, Kiro will automatically load this power:

- Flink, Apache Flink, Managed Service for Apache Flink, streaming, stream-processing, real-time
- Kafka, Kinesis

## Repository Structure

```
├── POWER.md                  # Kiro Power entrypoint
├── SKILL.md                  # Agent Skill entrypoint (Cursor, Claude Code, etc.)
└── steering/                 # Shared reference files (all platforms)
    ├── core-workflow.md
    ├── best-practices.md
    ├── dependency-management.md
    ├── environment-setup.md
    ├── msf-overview.md
    ├── flink-2x-migration.md
    ├── resource-optimization.md
    ├── job-graph-architecture.md
    ├── monitoring-and-metrics.md
    ├── logging-configuration.md
    └── kinesis-connector-guide.md
```

`POWER.md` and `SKILL.md` are separate entrypoints tailored to each platform's conventions. Both reference the same `steering/` files — no content duplication.

## How It Works Across Platforms

| Platform | Entrypoint | Reference Loading | Activation |
|---|---|---|---|
| Kiro | `POWER.md` | `kiroPowers readSteering` action | Keyword-based auto-activation |
| Cursor | `SKILL.md` | Agent reads `steering/` files on demand | Agent decides from description, or `/flink` |
| Claude Code | `SKILL.md` | Markdown links to `steering/` files | Skill discovery, or `/flink` |
| Other agents | `SKILL.md` | Relative path references | Per the [Agent Skills](https://agentskills.io/) standard |

## Resources

- [Amazon Managed Service for Apache Flink Documentation](https://docs.aws.amazon.com/managed-flink/latest/java/)
- [Apache Flink Documentation](https://nightlies.apache.org/flink/flink-docs-stable/)
- [Kiro Powers Documentation](https://kiro.dev/docs/powers/create/)
- [Agent Skills Standard](https://agentskills.io/)

## License

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

This project is licensed under the Apache-2.0 License. See the [LICENSE](LICENSE) file for details.
