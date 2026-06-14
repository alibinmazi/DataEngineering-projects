# UEH Data Management Layer

## Unified Vulnerability Exposure Hub — Data Platform

The backend data engine for UEH. This repository contains the **generic, configuration-driven data platform** that ingests, processes, and serves cybersecurity vulnerability data.

> **This is NOT adapter-specific code.** Adapters are configured via the UEH Dashboard (separate repo). This repo contains the platform engine that reads those configurations and executes generically.

---

## Architecture Overview

```
┌────────────────────────────────────────────────────────────────────────────┐
│                        UEH DATA MANAGEMENT LAYER                            │
│                                                                            │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                 │
│  │  INGESTION   │    │  PROCESSING  │    │ ORCHESTRATION│                 │
│  │  ENGINE      │    │  ENGINE      │    │              │                 │
│  │              │    │              │    │  Generic     │                 │
│  │  Generic     │    │  Bronze      │    │  Airflow     │                 │
│  │  NiFi Flow   │    │  Silver      │    │  DAGs        │                 │
│  │  (config-    │    │  Gold        │    │  (config-    │                 │
│  │   driven)    │    │  (config-    │    │   driven)    │                 │
│  │              │    │   driven)    │    │              │                 │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘                 │
│         │                   │                   │                          │
│         └───────────────────┼───────────────────┘                          │
│                             │                                              │
│                             ▼                                              │
│              ┌──────────────────────────────┐                              │
│              │       CONTROL TABLES          │                              │
│              │   (Iceberg / Hive Metastore)  │                              │
│              │                              │                              │
│              │  adapter_config ← UI writes  │                              │
│              │  adapter_state ← Engine R/W  │                              │
│              │  batch_registry ← Engine R/W │                              │
│              │  field_mapping ← UI writes   │                              │
│              │  schema_registry ← Managed   │                              │
│              └──────────────────────────────┘                              │
│                             ▲                                              │
│                             │                                              │
│              ┌──────────────────────────────┐                              │
│              │     UEH DASHBOARD (UI)        │  ← SEPARATE REPO            │
│              │     Analyst configures here    │                              │
│              └──────────────────────────────┘                              │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## Repository Structure

```
ueh-data-platform/
├── infrastructure/          ← DDL, databases, HDFS setup
├── ingestion_engine/        ← Generic NiFi flows (one flow, all adapters)
├── processing_engine/       ← Generic Spark jobs (Bronze, Silver, Gold)
├── orchestration/           ← Generic Airflow DAGs + plugins
├── adapter_registry/        ← Source definitions, schemas, default mappings
├── config/                  ← Environment configs (dev/uat/prod)
├── scripts/                 ← Deployment + operational scripts
├── tests/                   ← Unit + integration tests
└── docs/                    ← Architecture documentation
```

---

## Key Principles

| Principle | Description |
|-----------|-------------|
| **Configuration-driven** | No code-per-adapter. Engine reads control tables. |
| **Analyst self-service** | Adapters configured via UEH Dashboard UI (separate repo) |
| **Generic engine** | ONE NiFi flow, ONE Bronze loader, ONE Silver transformer |
| **Control table coupling** | NiFi → `RAW_COMPLETE` → Spark → `BRONZE_COMPLETE` |
| **Metadata-first** | All runtime decisions from Iceberg control tables |
| **Schema-tolerant Bronze** | `payload_json` absorbs any source schema change |
| **Mapping-driven Silver** | `field_mapping` table drives parsing (analyst-defined) |

---

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Storage | HDFS + Apache Iceberg |
| Catalog | Hive Metastore |
| Ingestion | Apache NiFi (generic parameterized flows) |
| Processing | Apache Spark / PySpark (CDE) |
| Orchestration | Apache Airflow |
| Configuration | Iceberg control tables (written by UI API) |
| Secrets | HashiCorp Vault |

---

## Quick Start (Dev)

```bash
# 1. Create databases + control tables
spark-sql -f infrastructure/databases/01_create_databases.sql
spark-sql -f infrastructure/control_tables/01_adapter_config.sql
spark-sql -f infrastructure/control_tables/02_adapter_state.sql
spark-sql -f infrastructure/control_tables/03_batch_registry.sql
spark-sql -f infrastructure/control_tables/04_field_mapping.sql
spark-sql -f infrastructure/control_tables/05_schema_registry.sql

# 2. Create HDFS structure
./infrastructure/hdfs/init_hdfs_structure.sh dev

# 3. Deploy NiFi generic flow (import template)
# See: ingestion_engine/docs/deployment_guide.md

# 4. Deploy Spark jobs to HDFS
./scripts/deploy/deploy_spark_jobs.sh dev

# 5. Deploy Airflow DAGs
./scripts/deploy/deploy_airflow_dags.sh dev

# 6. Configure first adapter via UEH Dashboard (or seed SQL for testing)
spark-sql -f adapter_registry/default_mappings/seed_nvd_for_testing.sql
```

---

## How Adapters Get Onboarded

1. **Analyst** opens UEH Dashboard
2. Selects source system type (e.g., Tenable)
3. Fills connection details (URL, auth, schedule)
4. Maps source fields to canonical schema (drag-drop)
5. Clicks **"Activate"**
6. UI API writes to `adapter_config` + `field_mapping` tables
7. **Next scheduled run:** Generic engine picks up new adapter automatically
8. **No code deployment. No engineer involvement.**

---

## Related Repositories

| Repo | Purpose |
|------|---------|
| `ueh-data-platform` (this) | Data engine, DDL, Spark, NiFi, Airflow |
| `ueh-dashboard` (separate) | UI frontend + API backend for analyst self-service |
