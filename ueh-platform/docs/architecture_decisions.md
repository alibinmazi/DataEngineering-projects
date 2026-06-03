# UEH Architecture Decision Records (ADRs)

## ADR-001: Medallion Architecture (Bronze → Silver → Gold)

**Status:** Accepted  
**Date:** 2026-06-01

### Context
Enterprise vulnerability data from 27+ sources needs centralized processing with clear separation of concerns.

### Decision
Adopt a 3-layer medallion architecture:
- **Bronze:** Raw immutable ingestion (schema-agnostic, payload_json)
- **Silver:** Standardized transformation (field extraction, canonical model, DQ)
- **Gold:** Business-ready analytics (aggregation, reporting, dashboards)

### Rationale
- Bronze never breaks due to source schema changes
- Silver can be reprocessed without re-ingesting
- Gold is decoupled from transformation logic
- Clear ownership boundaries per layer

---

## ADR-002: One Bronze Table Per Source Adapter

**Status:** Accepted  
**Date:** 2026-06-01

### Context
Multiple adapters produce vulnerability data. Should they share a table?

### Decision
One Bronze Iceberg table per `adapter_name` (not per instance), partitioned by `adapter_instance_id` where applicable.

### Rationale
- Source isolation for troubleshooting
- Independent onboarding and deprecation
- Cleaner replay per source
- No cross-source schema contamination
- Instance-level partition pruning within same table

---

## ADR-003: 2-DAG Decoupled Pattern

**Status:** Accepted  
**Date:** 2026-06-01

### Context
Should ingestion and Bronze loading be in one DAG or two?

### Decision
Two separate DAGs per adapter:
- **DAG 1 (Ingestion):** Triggers NiFi, monitors completion, writes `RAW_COMPLETE`
- **DAG 2 (Bronze Load):** Polls for `RAW_COMPLETE`, runs Spark job, writes `BRONZE_COMPLETE`

### Coupling Mechanism
Control table status field (`batch_registry.status`). No file sensors, no ExternalTaskSensor.

### Rationale
| Factor | Benefit |
|--------|---------|
| Failure isolation | NiFi failure ≠ Bronze failure |
| Independent retry | Bronze can retry from existing HDFS data |
| Replay support | Can re-run Bronze without re-ingestion |
| SLA tracking | Separate SLA per stage |
| Team ownership | Ingestion team vs Data Engineering team |
| Schedule flexibility | Ingestion daily, Bronze load every 30 min |

---

## ADR-004: Control Table-Driven Orchestration

**Status:** Accepted  
**Date:** 2026-06-01

### Context
How should DAGs detect upstream completion?

### Decision
SQL polling against Iceberg control tables using PythonSensor/ShortCircuitOperator.

### Rejected Alternatives
| Pattern | Why Rejected |
|---------|-------------|
| FileSensor | Doesn't understand business status |
| HiveSensor | Partition existence ≠ processing complete |
| ExternalTaskSensor | Creates hard DAG-to-DAG coupling |
| Airflow Datasets | Vendor lock-in, less visibility |

### Rationale
- Queries business-level status (BRONZE_COMPLETE), not technical artifacts
- Works across different orchestration tools (portable)
- Full audit trail in control tables
- Supports replay and backfill without fake triggers

---

## ADR-005: Environment Separation at Database Level

**Status:** Accepted  
**Date:** 2026-06-01

### Context
How to separate dev/uat/prod without code changes?

### Decision
Environment encoded in **database name**, not table name:
```
ueh_dev_bronze.t01_ueh_brz_nvd_vulnerabilities
ueh_prod_bronze.t01_ueh_brz_nvd_vulnerabilities
```

### Rationale
- Same table names across environments (code portability)
- Environment injected as configuration variable
- Access control at database level (Ranger/Sentry)
- CI/CD promotion = change config, not code

---

## ADR-006: Bronze Stores Complete payload_json

**Status:** Accepted  
**Date:** 2026-06-01

### Context
Should Bronze parse any fields from the API response?

### Decision
Bronze stores the COMPLETE raw record as a single `payload_json` STRING column. No business field extraction.

### Exception
`source_record_id` is extracted for operational deduplication reference only — it is NOT considered business logic.

### Rationale
- Schema drift in source API cannot break Bronze
- If parsing logic changes, Silver can be rerun from Bronze
- Forensic completeness preserved
- Simplifies Bronze loader implementation

---

## ADR-007: Append-Only Bronze (Immutable)

**Status:** Accepted  
**Date:** 2026-06-01

### Context
Should Bronze records ever be updated or deleted?

### Decision
Bronze is strictly append-only. Records are NEVER updated or deleted.

### Implications
- Same CVE modified on different dates = multiple Bronze records (expected)
- Deduplication/latest-version logic belongs in Silver
- Supports full audit trail and forensic investigation
- Iceberg time-travel provides historical access

---

## ADR-008: Dead Letter Architecture

**Status:** Accepted  
**Date:** 2026-06-01

### Context
What happens when ingestion fails?

### Decision
Failed payloads are NEVER discarded. They are written to a dead-letter folder that mirrors the Bronze raw structure, with a corresponding `failed_ingestions` control table record.

### Dead Letter Path Pattern
```
/data-lake/{env}/bronze/{adapter_type}/{adapter_name}/dead_letter/
    ingestion_date={date}/batch_id={batch_id}/
```

### Resolution Flow
1. Failure → dead-letter + `failed_ingestions` record (PENDING)
2. Root cause investigation
3. Fix applied (code, config, or source)
4. Replay queued (`replay_queue` table)
5. Replay executed → new batch created
6. Failed ingestion resolved (REPLAYED)

---

## ADR-009: ingestion_mode vs load_type Separation

**Status:** Accepted  
**Date:** 2026-06-01

### Context
Need to distinguish what a source supports vs what a specific run executed.

### Decision
Two distinct concepts:
- **ingestion_mode** (adapter_config): What the source supports (INCREMENTAL, SNAPSHOT, FULL, HYBRID)
- **load_type** (batch_registry): What THIS specific run executed (FULL_LOAD, INCREMENTAL, SNAPSHOT, REPLAY)

### Example
NVD supports INCREMENTAL mode. First run executes with a wide date range (effectively a large incremental). load_type = 'INCREMENTAL'. If manually triggered for full reload, load_type = 'FULL_LOAD'.

---

## ADR-010: NVD as First Adapter

**Status:** Accepted  
**Date:** 2026-06-01

### Context
Which adapter should be built first to prove the framework?

### Decision
NVD (National Vulnerability Database) — a public vulnerability intelligence feed.

### Rationale
- Public API (no enterprise security approvals)
- Free API key (instant onboarding)
- Well-documented, stable schema
- Simple pagination model (offset-based)
- No org_id/multi-instance complexity
- Proves full Bronze pipeline without organizational overhead
- Framework validated, then reused for enterprise adapters
