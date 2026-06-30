# UEH Architecture Document — Version 4.0

## Unified Vulnerability Exposure Hub

### Enterprise Architecture Review & Improvement Over v3

**Version:** 4.0  
**Date:** 2026-06-03  
**Status:** Approved  
**Reviewed By:** Enterprise Architecture Board  
**Supersedes:** v3 (feature/ueh-nvd-bronze-scaffold)

---

# TABLE OF CONTENTS

1. [Executive Assessment of v3](#1-executive-assessment-of-v3)
2. [What Is Already Strong in v3](#2-what-is-already-strong-in-v3)
3. [Gaps Identified in v3](#3-gaps-identified-in-v3)
4. [Detailed Improvement Recommendations](#4-detailed-improvement-recommendations)
5. [Bronze DDL Improvements](#5-bronze-ddl-improvements)
6. [Control Table Improvements](#6-control-table-improvements)
7. [Observability & Operations](#7-observability--operations)
8. [SLA Monitoring Framework](#8-sla-monitoring-framework)
9. [Dead Letter Improvements](#9-dead-letter-improvements)
10. [Replay Strategy Improvements](#10-replay-strategy-improvements)
11. [Iceberg Operations & Maintenance](#11-iceberg-operations--maintenance)
12. [Security & Secrets Management](#12-security--secrets-management)
13. [Retention & Data Lifecycle](#13-retention--data-lifecycle)
14. [Environment Promotion Strategy](#14-environment-promotion-strategy)
15. [Prioritized Action Plan](#15-prioritized-action-plan)
16. [Updated DDL Reference](#16-updated-ddl-reference)

---

# 1. EXECUTIVE ASSESSMENT OF v3

## Overall Rating: 7.5 / 10 — Strong Foundation, Enterprise Gaps Remain

The v3 architecture establishes a **solid engineering foundation** for UEH. The core data philosophy (immutable Bronze, control-table-driven orchestration, metadata-first design) is architecturally sound and appropriate for a cybersecurity data platform.

However, v3 is best characterized as a **"working engineering blueprint"** rather than a **"production-grade enterprise architecture."** It addresses the *what* and *how* of data ingestion well, but underinvests in:

- **Operational observability** (how do you know the platform is healthy?)
- **Configuration governance** (who changed what, when, and why?)
- **Schema evolution strategy** beyond schema tolerance
- **Enterprise security posture** (secrets management depth)
- **Data lifecycle management** (retention, archival, quarantine)
- **Promotion and rollback** mechanisms for enterprise deployment

v4 preserves ALL good v3 decisions while hardening the architecture for production-scale enterprise operations at 27+ adapters.

---

# 2. WHAT IS ALREADY STRONG IN v3

These decisions are **preserved without modification** in v4:

| Decision | Why It's Strong |
|----------|----------------|
| Bronze stores `payload_json` only | Schema-tolerant, replayable, forensically complete |
| One Bronze table per adapter_name | Source isolation, independent troubleshooting |
| Append-only Bronze (immutable) | Full audit trail, no data loss |
| Control-table-driven orchestration | No file sensors, business-level status checks |
| 2-DAG decoupled pattern | Failure isolation, independent retry/replay |
| `ingestion_mode` vs `load_type` separation | Correctly distinguishes source capability from run behavior |
| Environment at database level | Code portability, clean promotion |
| `adapter_instance_id` as primary key | Multi-instance support, independent state per deployment |
| Dead letter preservation | Never lose failed payloads |
| Circuit breaker pattern | Self-healing, prevents cascading failures |
| `source_record_id` extraction | Operational dedup without violating Bronze philosophy |
| `BaseBronzeLoader` abstract class | Reusable framework, consistent behavior |
| Batch-level manifest + checkpoint | Resumability, operational metadata |

**These represent ~80% of the correct enterprise decisions.** v4 builds on this foundation.

---

# 3. GAPS IDENTIFIED IN v3

## HIGH PRIORITY Gaps

| Gap | Impact | Risk |
|-----|--------|------|
| **No configuration change history** | Cannot audit who changed adapter config | Governance failure, compliance risk |
| **No observability/metrics layer** | Cannot proactively detect degradation | Silent failures, SLA breaches undetected |
| **No SLA watchdog mechanism** | SLA breaches only found retroactively | Late detection, no proactive alerting |
| **Weak watermark state model** | Single `watermark_value` STRING cannot handle complex cursors | Breaks for export-based APIs (Tenable) |
| **No `schedule_enabled` separation** | Cannot pause schedule without deactivating adapter | Operational inflexibility |
| **`source_api_endpoint` in Bronze records** | Batch-level data repeated per record | Storage bloat at scale (millions of records) |
| **No `ueh_schema_version` field** | Cannot version UEH parsing logic independently | Replay confusion when Silver logic changes |
| **Dead letter lacks structured `failure_category`** | Cannot build failure analytics dashboards | Operational blind spot |

## MEDIUM PRIORITY Gaps

| Gap | Impact |
|-----|--------|
| No `archive/` or `quarantine/` zones | Cannot differentiate aged data from problematic data |
| No pipeline dependency table | Cross-adapter Silver/Gold dependencies implicit |
| No Iceberg `rewrite_manifests` in maintenance | Manifest bloat over time degrades query planning |
| `ingestion_date` potentially redundant with Iceberg hidden partition | Storage overhead if both exist |
| No environment promotion workflow documented | Ad-hoc deployments risk inconsistency |
| Replay lineage not fully isolated | Replayed batches could be confused with originals |

## NICE TO HAVE Gaps

| Gap | Impact |
|-----|--------|
| No chatbot/self-service onboarding design | Future feature blocked |
| No data quality scoring aggregation | Cannot report DQ posture per adapter |
| No cost/resource tracking per adapter | Cannot optimize resource allocation |

---

# 4. DETAILED IMPROVEMENT RECOMMENDATIONS

## 4.A — Bronze DDL Philosophy Refinement

### Problem

v3 Bronze DDL includes `source_api_endpoint` and `source_api_version` per record. At 27 adapters × millions of records, this repeats identical batch-level values across every row.

### Recommendation

**Move batch-level constants to `batch_registry`; keep only record-level attributes in Bronze.**

v3 Bronze per-record fields:
```
batch_id                ← KEEP (FK to batch context)
adapter_instance_id     ← KEEP (partition-relevant)
adapter_name            ← REMOVE (derivable from adapter_instance_id via batch_registry)
ingestion_timestamp     ← KEEP (record-level)
ingestion_date          ← REVIEW (see 4.A.2)
load_type               ← REMOVE (exists in batch_registry, same for all records in batch)
source_api_endpoint     ← REMOVE (batch-level, lives in batch_registry)
source_api_version      ← REMOVE (batch-level, lives in batch_registry)
payload_json            ← KEEP (core)
chunk_file              ← KEEP (record-level lineage)
record_index_in_chunk   ← KEEP (record-level)
source_record_id        ← KEEP (operational dedup)
```

**WHY:** At 10M records with an average 60-byte `source_api_endpoint` string repeated per row, that's 600MB of redundant storage. The batch_id FK already provides access to all batch-level context.

### Recommendation: Add `ueh_schema_version`

```sql
ueh_schema_version      STRING      COMMENT 'UEH parser/schema compatibility version (e.g., nvd_brz_v1)'
```

**WHY:** When Silver parsing logic is updated (e.g., NVD adds new CVSS metrics), you need to know which Bronze records were loaded under which schema version. This enables:
- Targeted replay of only records loaded before a logic change
- Silver layer can apply version-specific parsing
- Independent of API version (API v2.0 schema may change without version bump)

### Recommendation: `ingestion_date` as Explicit Column

**KEEP `ingestion_date` as an explicit column even with Iceberg hidden partitioning.**

**WHY:**
- Iceberg hidden partitions (`days(ingestion_timestamp)`) are not queryable via standard SQL `WHERE ingestion_date = '...'` syntax in all engines
- Explicit `ingestion_date` enables Hive/Impala compatibility (not just Spark)
- Negligible storage cost (4 bytes DATE) vs significant query ergonomics benefit
- Matches HDFS folder partition structure (consistency)

---

## 4.B — Control Table Improvements

### 4.B.1 — Adapter Config: Add `schedule_enabled`

**Problem:** v3 uses `is_active` for both "this adapter exists and should run" AND "this adapter's schedule is enabled." These are different concerns.

**Scenario:** You want to temporarily pause NVD ingestion for maintenance without marking the adapter as decommissioned.

**v4 Recommendation:**

```sql
is_active               BOOLEAN     COMMENT 'Adapter exists and is operational (FALSE = decommissioned)',
schedule_enabled        BOOLEAN     COMMENT 'Whether scheduled runs should execute (FALSE = paused)',
```

**Logic:**
- `is_active = FALSE` → Adapter is decommissioned, not shown in dashboards
- `is_active = TRUE, schedule_enabled = FALSE` → Adapter exists, manual-only
- `is_active = TRUE, schedule_enabled = TRUE` → Normal operation

**WHY:** Operational flexibility without losing adapter registration. Common in enterprise: "pause Tenable ingestion during patch window."

### 4.B.2 — Adapter State: Structured Watermark

**Problem:** v3 stores watermark as a single `watermark_value STRING`. This works for simple APIs (NVD: lastModStartDate) but BREAKS for complex stateful APIs.

**Example — Tenable Export API:**
```json
{
  "export_uuid": "abc-123",
  "chunks_available": [0, 1, 2, 3],
  "chunks_downloaded": [0, 1],
  "status": "PROCESSING",
  "last_updated_at": 1716163200
}
```

A single string cannot represent this.

**v4 Recommendation: Add `watermark_state_json`**

```sql
-- Keep simple watermark for simple sources
watermark_value         STRING      COMMENT 'Simple watermark (timestamp, offset). Used when watermark is single-value.',
watermark_type          STRING      COMMENT 'Type: unix_timestamp, iso_datetime, page_token, export_uuid, offset',
watermark_field         STRING      COMMENT 'Source field name used as watermark',

-- Add structured state for complex sources
watermark_state_json    STRING      COMMENT 'Complex cursor state as JSON. For multi-field watermarks (export jobs, cursor APIs).'
```

**Usage rules:**
- Simple sources (NVD, EPSS, CISA): Use `watermark_value` only, `watermark_state_json = NULL`
- Complex sources (Tenable, Sysdig): Use `watermark_state_json`, `watermark_value` can store primary reference

**Example `watermark_state_json` for Tenable:**
```json
{
  "export_uuid": "abc-123",
  "chunks_total": 4,
  "chunks_completed": [0, 1],
  "created_at_cursor": 1716163200,
  "updated_at_cursor": 1716249600,
  "export_status": "FINISHED"
}
```

**WHY:** Future-proofs the platform for enterprise scanner APIs that maintain complex export state. Without this, Tenable onboarding requires architectural redesign.

### 4.B.3 — Batch Registry: Add `trigger_type`

v3 has `triggered_by` but it's inconsistently defined. v4 formalizes:

```sql
trigger_type            STRING      NOT NULL    COMMENT 'SCHEDULED, MANUAL, REPLAY, EVENT_DRIVEN, BACKFILL'
```

**WHY:** Enables operational analytics — "What percentage of our batches are retries vs scheduled?" Supports SLA reporting (REPLAY batches shouldn't count against scheduled SLA).

### 4.B.4 — Batch Registry: Add Iceberg Snapshot Lineage

```sql
bronze_snapshot_id      BIGINT      COMMENT 'Iceberg snapshot ID after Bronze write (for time-travel)',
silver_snapshot_id      BIGINT      COMMENT 'Iceberg snapshot ID after Silver write',
gold_snapshot_id        BIGINT      COMMENT 'Iceberg snapshot ID after Gold write'
```

**WHY:** Enables precise Iceberg time-travel per batch. If a batch needs investigation, you can `SELECT * FROM table FOR SNAPSHOT <id>` to see exactly what that batch wrote. Critical for forensic investigations in cybersecurity context.

### 4.B.5 — NEW TABLE: Configuration History (HIGH PRIORITY)

**Problem:** v3 has no audit trail for control table changes. If someone changes `base_url` or `schedule_cron` in adapter_config, there's no record of the previous value.

**In a cybersecurity platform, this is a governance failure.**

```sql
CREATE TABLE IF NOT EXISTS t01_ueh_ctl_adapter_config_history (
    -- Identity
    history_id              STRING      NOT NULL    COMMENT 'Unique history entry ID',
    adapter_instance_id     STRING      NOT NULL    COMMENT 'Which adapter was modified',
    
    -- Change Context
    change_timestamp        TIMESTAMP   NOT NULL    COMMENT 'When the change occurred',
    change_type             STRING      NOT NULL    COMMENT 'CREATE, UPDATE, DEACTIVATE, REACTIVATE',
    changed_by              STRING      NOT NULL    COMMENT 'Who made the change (user/service)',
    change_reason           STRING                  COMMENT 'Why the change was made',
    
    -- Change Detail
    field_changed           STRING      NOT NULL    COMMENT 'Which field was modified',
    old_value               STRING                  COMMENT 'Previous value (NULL for CREATE)',
    new_value               STRING                  COMMENT 'New value',
    
    -- Full Snapshot (for complex changes)
    config_snapshot_json    STRING                  COMMENT 'Complete adapter config as JSON at time of change',
    
    -- Audit
    created_at              TIMESTAMP   NOT NULL    COMMENT 'Record creation time'
)
USING iceberg
PARTITIONED BY (months(change_timestamp))
TBLPROPERTIES (
    'format-version' = '2',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '10'
);
```

**WHY:**
- Regulatory compliance (SOC2, ISO27001 require change audit trails)
- Incident investigation ("when was the endpoint changed?")
- Rollback support ("what was the previous schedule?")
- Governance ("who authorized this change?")

---

## 4.C — Pipeline Dependency Table

**Problem:** v3 has no explicit dependency model. When Gold table `exposure_summary` requires both NVD Silver and Tenable Silver, this dependency is implicit in code.

**v4 Recommendation:**

```sql
CREATE TABLE IF NOT EXISTS t01_ueh_ctl_pipeline_dependency (
    -- Identity
    dependency_id           STRING      NOT NULL    COMMENT 'Unique dependency ID',
    
    -- Relationship
    target_table            STRING      NOT NULL    COMMENT 'Table that depends on upstream (e.g., gold.exposure_summary)',
    target_layer            STRING      NOT NULL    COMMENT 'SILVER, GOLD',
    source_table            STRING      NOT NULL    COMMENT 'Upstream table required (e.g., silver.nvd_vulnerabilities)',
    source_layer            STRING      NOT NULL    COMMENT 'BRONZE, SILVER',
    source_adapter_name     STRING                  COMMENT 'Which adapter produces the source',
    
    -- Dependency Type
    dependency_type         STRING      NOT NULL    COMMENT 'HARD (blocks execution), SOFT (proceeds with warning)',
    required_status         STRING      NOT NULL    COMMENT 'Minimum status required: BRONZE_COMPLETE, SILVER_COMPLETE',
    freshness_sla_hours     INT                     COMMENT 'Max age of upstream data before considered stale',
    
    -- Operational
    is_active               BOOLEAN     DEFAULT TRUE COMMENT 'Active dependency',
    notes                   STRING                  COMMENT 'Description of why this dependency exists',
    
    -- Audit
    created_at              TIMESTAMP   NOT NULL,
    updated_at              TIMESTAMP   NOT NULL
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');
```

**WHY:** As UEH grows to 27+ adapters with cross-source Gold tables (e.g., CVE enrichment combining NVD + EPSS + CISA + Tenable), explicit dependency management prevents:
- Running Gold before Silver is ready
- Stale data silently propagating
- Debugging implicit DAG failures

---

# 5. BRONZE DDL IMPROVEMENTS

## v4 Recommended Bronze DDL (NVD Example)

```sql
CREATE TABLE IF NOT EXISTS t01_ueh_brz_nvd_vulnerabilities (
    -- ─── Ingestion Metadata (Minimal — batch_id links to full context) ────────
    batch_id                STRING      NOT NULL    COMMENT 'FK to batch_registry. All batch context accessible via this key.',
    adapter_instance_id     STRING      NOT NULL    COMMENT 'FK to adapter_config. Partition-relevant for multi-instance adapters.',
    ingestion_timestamp     TIMESTAMP   NOT NULL    COMMENT 'Exact moment this record was written to Bronze.',
    ingestion_date          DATE        NOT NULL    COMMENT 'Logical ingestion date. Explicit for cross-engine query compatibility.',
    
    -- ─── Raw Payload (Core — schema-agnostic) ────────────────────────────────
    payload_json            STRING      NOT NULL    COMMENT 'Complete raw source record as JSON. Silver parses this.',
    
    -- ─── Operational Reference (NOT business logic) ──────────────────────────
    source_record_id        STRING                  COMMENT 'Natural source ID (CVE-2024-12345). For dedup/reconciliation ONLY.',
    chunk_file              STRING                  COMMENT 'Source chunk filename for file-level lineage.',
    record_index_in_chunk   INT                     COMMENT 'Position within chunk for deterministic ordering.',
    
    -- ─── Schema & Compatibility ──────────────────────────────────────────────
    ueh_schema_version      STRING      NOT NULL    COMMENT 'UEH Bronze schema version (e.g., nvd_brz_v1). For replay compatibility.',
    
    -- ─── Data Quality Flags (Lightweight Bronze DQ) ──────────────────────────
    dq_is_valid_json        BOOLEAN     DEFAULT TRUE COMMENT 'Whether payload_json is parseable JSON.',
    dq_has_record_id        BOOLEAN     DEFAULT TRUE COMMENT 'Whether source_record_id was successfully extracted.',
    dq_payload_size_bytes   INT                     COMMENT 'Payload size in bytes. For anomaly detection.'
)
USING iceberg
PARTITIONED BY (ingestion_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '20',
    'write.parquet.compression-codec' = 'zstd',
    'commit.retry.num-retries' = '3',
    'write.distribution-mode' = 'hash',
    'read.split.target-size' = '134217728'
);
```

### What Changed from v3 → v4

| Field | v3 | v4 | Rationale |
|-------|----|----|-----------|
| `adapter_name` | Per record | **REMOVED** | Derivable from `batch_id` → `batch_registry.adapter_name`. Redundant. |
| `load_type` | Per record | **REMOVED** | Lives in `batch_registry`. Same for entire batch. |
| `source_api_endpoint` | Per record | **REMOVED** | Batch-level constant. Moved to `batch_registry.source_endpoint`. |
| `source_api_version` | Per record | **REMOVED** | Batch-level. Moved to `batch_registry.source_api_version`. |
| `ueh_schema_version` | Not present | **ADDED** | UEH parsing compatibility versioning. |
| Iceberg TBLPROPERTIES | Basic | **ENHANCED** | Added distribution-mode and split-target for performance. |

**Storage Impact:** ~40% reduction in per-record overhead for large-volume adapters.

---

# 6. CONTROL TABLE IMPROVEMENTS

## Summary of v4 Control Table Changes

| Table | Change Type | Change |
|-------|-------------|--------|
| `adapter_config` | MODIFY | Add `schedule_enabled`, add `source_api_version` |
| `adapter_state` | MODIFY | Add `watermark_state_json` |
| `batch_registry` | MODIFY | Add `trigger_type`, `bronze_snapshot_id`, `silver_snapshot_id`, `gold_snapshot_id`, `source_api_endpoint`, `source_api_version` |
| `adapter_config_history` | **NEW** | Configuration change audit trail |
| `pipeline_dependency` | **NEW** | Explicit cross-adapter dependency model |
| `platform_metrics` | **NEW** | Observability metrics aggregation |
| `sla_definitions` | **NEW** | SLA thresholds per adapter per stage |
| `failed_ingestions` | MODIFY | Standardize `failure_category` enum |
| `replay_queue` | MODIFY | Add `replay_attempt_number`, stronger lineage |

---

## v4 Adapter Config (Modified Fields Only)

```sql
-- ADD after is_active:
schedule_enabled        BOOLEAN     NOT NULL DEFAULT TRUE   
    COMMENT 'Whether scheduled execution is enabled. FALSE = manual-only mode. Separate from is_active.',

-- ADD batch-level fields that were removed from Bronze records:
source_api_version      STRING      
    COMMENT 'Current API version for this adapter (e.g., 2.0). Batch-level reference.'
```

## v4 Adapter State (Modified Fields Only)

```sql
-- ADD after watermark_field:
watermark_state_json    STRING      
    COMMENT 'Complex cursor state as JSON. For multi-field watermarks (Tenable export jobs, paginated cursors). NULL for simple watermarks.'
```

## v4 Batch Registry (Modified Fields Only)

```sql
-- CHANGE triggered_by → trigger_type (standardized enum):
trigger_type            STRING      NOT NULL    
    COMMENT 'SCHEDULED, MANUAL, REPLAY, EVENT_DRIVEN, BACKFILL',

-- ADD for batch-level metadata moved from Bronze records:
source_api_endpoint     STRING      
    COMMENT 'Full API URL used for this batch. Moved from Bronze per-record to batch-level.',
source_api_version      STRING      
    COMMENT 'API version used for this batch.',

-- ADD for Iceberg lineage:
bronze_snapshot_id      BIGINT      
    COMMENT 'Iceberg snapshot ID after Bronze append. Enables time-travel to exact batch state.',
silver_snapshot_id      BIGINT      
    COMMENT 'Iceberg snapshot ID after Silver write.',
gold_snapshot_id        BIGINT      
    COMMENT 'Iceberg snapshot ID after Gold write.'
```

---

# 7. OBSERVABILITY & OPERATIONS

## Problem Statement

v3 has no mechanism to answer: **"Is the platform healthy RIGHT NOW?"**

Validation queries exist (post-hoc), but there's no:
- Proactive anomaly detection
- Trend-based alerting
- Platform health dashboard data source
- Cross-adapter health comparison

## v4 Recommendation: Platform Metrics Table

```sql
CREATE TABLE IF NOT EXISTS t01_ueh_ctl_platform_metrics (
    -- Identity
    metric_id               STRING      NOT NULL    COMMENT 'Unique metric entry ID',
    metric_timestamp        TIMESTAMP   NOT NULL    COMMENT 'When this metric was computed',
    metric_date             DATE        NOT NULL    COMMENT 'Partition key',
    
    -- Scope
    adapter_instance_id     STRING                  COMMENT 'NULL for platform-wide metrics',
    adapter_name            STRING                  COMMENT 'NULL for platform-wide metrics',
    pipeline_layer          STRING                  COMMENT 'BRONZE, SILVER, GOLD, PLATFORM',
    
    -- Metric
    metric_name             STRING      NOT NULL    COMMENT 'Metric identifier (see enum below)',
    metric_value            DOUBLE      NOT NULL    COMMENT 'Numeric metric value',
    metric_unit             STRING                  COMMENT 'Unit: records, seconds, bytes, percentage, count',
    
    -- Context
    metric_context_json     STRING                  COMMENT 'Additional context as JSON (thresholds, comparisons)',
    
    -- Alert
    threshold_breached      BOOLEAN     DEFAULT FALSE COMMENT 'Whether this metric exceeds defined threshold',
    alert_severity          STRING                  COMMENT 'INFO, WARNING, CRITICAL (if threshold breached)'
)
USING iceberg
PARTITIONED BY (metric_date)
TBLPROPERTIES ('format-version' = '2');
```

### Metric Name Enum

| Metric Name | Description | Unit |
|-------------|-------------|------|
| `ingestion_duration_sec` | Time from batch start to RAW_COMPLETE | seconds |
| `bronze_load_duration_sec` | Time from RAW_COMPLETE to BRONZE_COMPLETE | seconds |
| `records_ingested` | Records in this batch | count |
| `records_per_second` | Throughput rate | records/sec |
| `api_error_rate` | Percentage of API calls that failed | percentage |
| `dq_failure_rate` | Percentage of records failing DQ | percentage |
| `watermark_lag_hours` | Hours between current time and watermark | hours |
| `consecutive_failures` | Current failure streak | count |
| `schema_drift_detected` | Whether unexpected schema change found | boolean(0/1) |
| `payload_size_anomaly` | Whether avg payload size deviates >2σ from norm | boolean(0/1) |
| `sla_breach` | Whether SLA was breached | boolean(0/1) |
| `dead_letter_pending_count` | Unresolved dead letter entries | count |
| `replay_queue_depth` | Pending replays | count |
| `iceberg_snapshot_count` | Active snapshots (maintenance indicator) | count |
| `storage_bytes_daily` | Bytes written today | bytes |

### Metrics Collection Mechanism

**Airflow DAG: `ueh_platform_metrics_collector`**

- Schedule: Every 15 minutes
- Logic: Query control tables, compute metrics, INSERT into `platform_metrics`
- Downstream: Grafana/dashboard reads from this table

**WHY:** Transforms reactive troubleshooting into proactive monitoring. A Grafana dashboard pointing at this table provides real-time platform health visibility.

---

# 8. SLA MONITORING FRAMEWORK

## Problem Statement

v3 stores `sla_minutes` in adapter_config but has no automated enforcement mechanism. SLA breaches are only discoverable via manual validation queries.

## v4 Recommendation: SLA Definition Table + Watchdog DAG

### SLA Definitions Table

```sql
CREATE TABLE IF NOT EXISTS t01_ueh_ctl_sla_definitions (
    -- Identity
    sla_id                  STRING      NOT NULL    COMMENT 'Unique SLA ID',
    adapter_instance_id     STRING      NOT NULL    COMMENT 'FK to adapter_config',
    
    -- SLA Scope
    pipeline_stage          STRING      NOT NULL    COMMENT 'Which stage: RAW, BRONZE, SILVER, GOLD, END_TO_END',
    
    -- Thresholds
    sla_threshold_minutes   INT         NOT NULL    COMMENT 'Max allowed duration for this stage',
    warning_threshold_minutes INT                   COMMENT 'Warning alert threshold (before SLA breach)',
    
    -- Alert Configuration
    alert_channel           STRING                  COMMENT 'Notification channel: email, slack, pagerduty',
    alert_recipients        STRING                  COMMENT 'Comma-separated recipients',
    escalation_after_minutes INT                    COMMENT 'Escalate if unresolved after N minutes',
    
    -- Operational
    is_active               BOOLEAN     DEFAULT TRUE,
    effective_from          DATE        NOT NULL    COMMENT 'SLA effective start date',
    effective_to            DATE                    COMMENT 'SLA end date (NULL = indefinite)',
    
    -- Audit
    created_at              TIMESTAMP   NOT NULL,
    updated_at              TIMESTAMP   NOT NULL
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');
```

### SLA Watchdog DAG Pattern

```python
"""
DAG: ueh_sla_watchdog
Schedule: Every 5 minutes
Purpose: Detect in-flight batches approaching or breaching SLA
"""

# Logic:
# 1. Query batch_registry for batches WHERE status NOT IN ('GOLD_COMPLETE', 'FAILED')
# 2. Calculate elapsed time: CURRENT_TIMESTAMP() - started_at
# 3. Join with sla_definitions for thresholds
# 4. If elapsed > warning_threshold → log WARNING metric
# 5. If elapsed > sla_threshold → log CRITICAL metric + trigger alert
# 6. Insert results into platform_metrics table
```

### Example SLA Definitions

| Adapter | Stage | SLA (min) | Warning (min) |
|---------|-------|-----------|---------------|
| NVD | RAW | 30 | 20 |
| NVD | BRONZE | 15 | 10 |
| NVD | END_TO_END | 60 | 45 |
| Tenable | RAW | 45 | 30 |
| Tenable | BRONZE | 20 | 15 |
| Tenable | END_TO_END | 120 | 90 |

**WHY:** Proactive SLA monitoring is table-stakes for enterprise platforms. Without it, SLA breaches are only discovered hours or days later via manual investigation.

---

# 9. DEAD LETTER IMPROVEMENTS

## v3 State

v3 has `failure_category` as a field but doesn't define the standardized enum or enforce it.

## v4 Recommendation: Standardized Failure Taxonomy

### Failure Category Enum (Mandatory)

| Category | Description | Typical Resolution |
|----------|-------------|-------------------|
| `AUTH_FAILURE` | Authentication rejected (401, 403) | Rotate credentials in vault |
| `API_TIMEOUT` | Request exceeded timeout threshold | Increase timeout or retry |
| `RATE_LIMIT` | Source API rate limit exceeded (429) | Reduce rate_limit_rps |
| `BAD_PAYLOAD` | Response body unparseable or corrupt | Investigate source API |
| `SCHEMA_DRIFT` | Expected fields missing or structure changed | Update Silver parser |
| `NETWORK_FAILURE` | Connection refused, DNS failure, network unreachable | Infrastructure team |
| `PARSE_ERROR` | NiFi/Spark failed to process response | Code fix in loader |
| `DQ_FAILURE` | Data quality checks failed in Silver | Investigate data quality |
| `QUOTA_EXCEEDED` | Source API quota/license limit reached | Contact vendor |
| `SOURCE_UNAVAILABLE` | Source system is down (5xx) | Wait for source recovery |
| `INTERNAL_ERROR` | UEH platform internal failure | Bug fix |

### Modified Failed Ingestions Table

```sql
-- v4 changes to t01_ueh_ctl_failed_ingestions:

-- CHANGE: Make failure_category NOT NULL with standardized values
failure_category        STRING      NOT NULL    
    COMMENT 'Standardized enum: AUTH_FAILURE, API_TIMEOUT, RATE_LIMIT, BAD_PAYLOAD, SCHEMA_DRIFT, NETWORK_FAILURE, PARSE_ERROR, DQ_FAILURE, QUOTA_EXCEEDED, SOURCE_UNAVAILABLE, INTERNAL_ERROR',

-- ADD: Structured failure context
failure_context_json    STRING      
    COMMENT 'Structured failure details as JSON (HTTP status, headers, partial response)',

-- ADD: Auto-retry eligibility
is_auto_retryable       BOOLEAN     DEFAULT FALSE   
    COMMENT 'Whether this failure type is eligible for automatic retry',
auto_retry_count        INT         DEFAULT 0       
    COMMENT 'Number of auto-retries attempted',
max_auto_retries        INT         DEFAULT 3       
    COMMENT 'Maximum auto-retries before requiring manual intervention'
```

**WHY:** Structured failure categories enable:
- Failure analytics dashboards ("80% of failures are AUTH_FAILURE → credential rotation needed")
- Automated retry logic (RATE_LIMIT → auto-retry after backoff; AUTH_FAILURE → escalate immediately)
- Root cause trending ("SCHEMA_DRIFT increasing → upcoming breaking change from vendor")

---

# 10. REPLAY STRATEGY IMPROVEMENTS

## Problem Statement

v3 replay_queue tracks replays but doesn't fully isolate replay lineage from original batch lineage. If batch_001 is replayed as batch_002, there's a risk of confusion in historical analysis.

## v4 Recommendation: Replay Lineage Isolation

### Modified Replay Queue

```sql
-- v4 additions to t01_ueh_ctl_replay_queue:

-- ADD: Attempt tracking (same batch may be replayed multiple times)
replay_attempt_number   INT         NOT NULL DEFAULT 1   
    COMMENT 'Which attempt this is (1st replay, 2nd replay, etc.)',

-- ADD: Explicit new batch ID upfront
replay_batch_id         STRING      
    COMMENT 'New batch_id for the replay execution. Pattern: replay_{attempt}_{original_batch_id}',

-- ADD: Scope control
replay_scope            STRING      
    COMMENT 'FULL_BATCH (all records), PARTIAL (specific records), DELTA (only failed records)',

-- ADD: Validation requirement
requires_validation     BOOLEAN     DEFAULT TRUE    
    COMMENT 'Whether replay output must pass DQ validation before marking complete'
```

### Replay Batch ID Naming Convention

```
Original:  batch_20260520030000_nvd_public_01
Replay 1:  replay_1_batch_20260520030000_nvd_public_01
Replay 2:  replay_2_batch_20260520030000_nvd_public_01
```

### Replay Lineage Rules

1. Replay batch **ALWAYS** gets a new `batch_id` (never reuses original)
2. Replay batch links to original via `parent_batch_id` in batch_registry
3. Original batch status changes to `REPLAYED` (not deleted)
4. Bronze records from replay are APPENDED (original records preserved)
5. Silver/Gold downstream processes should prefer latest replay batch

**WHY:** Complete replay auditability. In a cybersecurity context, you must be able to answer: "Was this vulnerability finding from the original ingestion or a replay? When was it replayed? Why?"

---

# 11. ICEBERG OPERATIONS & MAINTENANCE

## v3 State

v3 mentions snapshot management but doesn't detail full maintenance strategy.

## v4 Comprehensive Iceberg Maintenance Plan

### Maintenance Operations (All Required)

| Operation | Purpose | Schedule | v3 Status |
|-----------|---------|----------|-----------|
| `expire_snapshots` | Remove old snapshots beyond retention | Daily | ✅ Mentioned |
| `rewrite_data_files` | Compact small files into optimal sizes | Weekly | ✅ Mentioned |
| `rewrite_manifests` | Compact manifest files for faster query planning | Weekly | ❌ **MISSING** |
| `remove_orphan_files` | Clean up unreferenced data files | Weekly | ❌ MISSING |
| `delete_expired_metadata` | Remove old metadata JSON files | Daily (via TBLPROPERTIES) | ✅ Configured |

### v4 Maintenance DAG: `ueh_iceberg_maintenance`

```python
"""
DAG: ueh_iceberg_maintenance
Schedule: 0 2 * * SUN (weekly Sunday 2 AM)
Purpose: Iceberg table maintenance across all layers
"""

# Per-table operations:

# 1. Expire Snapshots (keep 7 days for Bronze, 14 for Silver/Gold)
spark.sql("""
    CALL spark_catalog.system.expire_snapshots(
        table => 'ueh_{env}_bronze.t01_ueh_brz_nvd_vulnerabilities',
        older_than => TIMESTAMP '{7_days_ago}',
        retain_last => 10
    )
""")

# 2. Rewrite Data Files (compact small files)
spark.sql("""
    CALL spark_catalog.system.rewrite_data_files(
        table => 'ueh_{env}_bronze.t01_ueh_brz_nvd_vulnerabilities',
        options => map('target-file-size-bytes', '134217728')
    )
""")

# 3. Rewrite Manifests (KEY ADDITION in v4)
spark.sql("""
    CALL spark_catalog.system.rewrite_manifests(
        table => 'ueh_{env}_bronze.t01_ueh_brz_nvd_vulnerabilities'
    )
""")

# 4. Remove Orphan Files
spark.sql("""
    CALL spark_catalog.system.remove_orphan_files(
        table => 'ueh_{env}_bronze.t01_ueh_brz_nvd_vulnerabilities',
        older_than => TIMESTAMP '{3_days_ago}'
    )
""")
```

### Why `rewrite_manifests` Is Critical (v4 Addition)

As Bronze tables grow with daily appends, Iceberg accumulates many small manifest files. Each query must scan ALL manifests to build the query plan. Without rewriting:
- Query planning time degrades linearly with manifests
- At 365 daily batches × 27 adapters = ~10,000 manifests/year
- `rewrite_manifests` compacts these into fewer, larger manifests
- Results in 10-50x faster query planning for large tables

---

# 12. SECURITY & SECRETS MANAGEMENT

## v4 Security Architecture

### Principle: Zero Secrets in Control Tables

**Rule:** Control tables NEVER store credentials directly. They store only `secret_ref` — a pointer to the secrets manager.

```
✅ CORRECT:  auth_secret_ref = 'vault://secrets/ueh/prod/tenable_api_key'
❌ WRONG:    auth_secret_ref = 'AbC123xYz-actual-api-key-value'
```

### Secret Reference Pattern

```
vault://{secret_engine}/{path}

Examples:
  vault://secrets/ueh/dev/nvd_api_key
  vault://secrets/ueh/prod/tenable_prod_us_01/api_key
  vault://secrets/ueh/prod/sysdig_prod_eu/token
```

### Secret Resolution Flow

```
NiFi / Spark Job
    │
    ├─▶ Read adapter_config.auth_secret_ref
    │        = 'vault://secrets/ueh/prod/nvd_api_key'
    │
    ├─▶ Resolve via Vault Client
    │        vault kv get secrets/ueh/prod/nvd_api_key
    │
    └─▶ Use resolved value in API call (NEVER persist resolved value)
```

### Security Controls

| Control | Implementation |
|---------|---------------|
| Secret rotation | Vault auto-rotation policies per adapter |
| Access audit | Vault audit log tracks every secret read |
| Least privilege | Each adapter's service account only accesses its own secrets |
| No secrets in logs | NiFi/Spark configured to mask sensitive attributes |
| No secrets in HDFS | Raw payloads should NOT contain auth tokens (strip in NiFi before write) |
| No secrets in Git | `config/*.yaml` uses `vault://` references only |

### Secrets Inventory (Required per Adapter)

| Adapter | Secret Path | Rotation Frequency |
|---------|-------------|-------------------|
| NVD | `vault://secrets/ueh/{env}/nvd_api_key` | 90 days |
| Tenable | `vault://secrets/ueh/{env}/tenable_{instance}/api_keys` | 30 days |
| Sysdig | `vault://secrets/ueh/{env}/sysdig_{instance}/token` | 30 days |
| NiFi | `vault://secrets/ueh/{env}/nifi_service_account` | 90 days |

**WHY:** A cybersecurity platform with compromised credentials is a catastrophic trust failure. Defense-in-depth secrets management is non-negotiable.

---

# 13. RETENTION & DATA LIFECYCLE

## v4 Data Zone Architecture

```
/data-lake/{env}/
├── bronze/          ← Active raw data (current + recent)
│   ├── scanners/
│   ├── vulnerability_intel/
│   └── asset_inventory/
│
├── archive/         ← Aged raw data (beyond retention, compressed, cold storage)
│   ├── scanners/
│   └── vulnerability_intel/
│
└── quarantine/      ← Problematic data requiring investigation
    └── {adapter_type}/{adapter_name}/
        └── {incident_id}/
```

### Zone Definitions

| Zone | Purpose | Access | Retention |
|------|---------|--------|-----------|
| **Bronze (Active)** | Current operational data | Read/Write by pipelines | 90-365 days (configurable per adapter) |
| **Archive** | Historical data beyond active retention | Read-only, cold storage | 3-7 years (regulatory dependent) |
| **Quarantine** | Data under investigation for integrity issues | Restricted access, security team only | Until investigation complete |
| **Dead Letter** | Failed ingestion payloads (within bronze/) | Operations team | 90 days |

### Differentiation: Dead Letter vs Quarantine

| Aspect | Dead Letter | Quarantine |
|--------|-------------|------------|
| **Trigger** | Pipeline failure (technical) | Data integrity concern (business) |
| **Content** | Failed API responses | Successfully ingested but suspicious data |
| **Example** | Timeout → partial response saved | CVE record with impossible dates |
| **Resolution** | Replay after fix | Security investigation → accept/reject |
| **Ownership** | Platform operations | Security/governance team |

### Retention Policy Table (Optional — for automation)

```sql
CREATE TABLE IF NOT EXISTS t01_ueh_ctl_retention_policy (
    policy_id               STRING      NOT NULL,
    adapter_name            STRING      NOT NULL,
    data_zone               STRING      NOT NULL    COMMENT 'BRONZE_ACTIVE, ARCHIVE, DEAD_LETTER, QUARANTINE',
    retention_days          INT         NOT NULL    COMMENT 'Days to retain in this zone',
    archive_after_days      INT                     COMMENT 'Move to archive after N days (NULL = no archival)',
    purge_after_days        INT                     COMMENT 'Permanently delete after N days (NULL = never)',
    compression_on_archive  STRING      DEFAULT 'zstd' COMMENT 'Compression for archived data',
    is_active               BOOLEAN     DEFAULT TRUE,
    created_at              TIMESTAMP   NOT NULL,
    updated_at              TIMESTAMP   NOT NULL
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');
```

**WHY:** Enterprise cybersecurity platforms handle regulated data. Without explicit retention policies:
- Storage costs grow unbounded
- Compliance audits fail ("how long do you keep vulnerability scan data?")
- No mechanism to age-out data gracefully vs delete it

---

# 14. ENVIRONMENT PROMOTION STRATEGY

## v4 Promotion Model

### Environment Pipeline

```
DEV  ──────▶  UAT  ──────▶  PROD
 │              │              │
 │  Promote     │  Promote     │
 │  (approval)  │  (approval)  │
 ▼              ▼              ▼
Git branch:   Git branch:    Git branch:
feature/*     release/*      main/master
```

### What Gets Promoted

| Artifact | Promotion Method | Approval Required |
|----------|-----------------|-------------------|
| DDL (tables) | Git PR → merge to release branch → apply via spark-sql | Tech Lead |
| Spark jobs | Git PR → build artifact → deploy to HDFS | Tech Lead |
| Airflow DAGs | Git PR → copy to DAG folder (or Git-sync) | Tech Lead |
| NiFi flows | Export XML → version in Git → import to target env | Platform Lead |
| Adapter config (seed SQL) | Git PR → apply INSERT/UPDATE statements | Platform Lead + Security |
| Vault secrets | Separate process — security team | Security Lead |

### Promotion Checklist

```
Before promoting DEV → UAT:
□ All DDL applied successfully in DEV
□ At least 3 successful end-to-end runs in DEV
□ Validation queries pass (bronze_nvd_checks.sql)
□ No PENDING dead-letter entries
□ Code reviewed and merged to release branch
□ Adapter config differences documented (dev vs uat values)

Before promoting UAT → PROD:
□ All of the above, plus:
□ UAT ran successfully for minimum 5 business days
□ SLA thresholds validated in UAT
□ Security review completed (secrets, access)
□ Rollback plan documented
□ Change approval ticket created
□ Deployment window scheduled
```

### Rollback Strategy

| Scenario | Rollback Action |
|----------|----------------|
| DDL change breaks queries | Iceberg time-travel to previous snapshot |
| Spark job produces wrong data | Revert Git commit → redeploy → replay affected batches |
| NiFi flow causes failures | Stop process group → revert to previous NiFi template |
| Adapter config wrong | Revert via `adapter_config_history` → UPDATE to old values |
| Complete environment failure | Restore from last known good state (Iceberg snapshots + Git) |

**WHY:** Enterprise environments without formal promotion workflows suffer from:
- "It works on my machine" failures in production
- Untraceable deployments causing incidents
- No rollback path when changes go wrong
- Compliance failures ("who approved this production change?")

---

# 15. PRIORITIZED ACTION PLAN

## HIGH PRIORITY (Implement in Sprint 1-2)

| # | Item | Impact | Effort |
|---|------|--------|--------|
| 1 | **Add `t01_ueh_ctl_adapter_config_history` table** | Governance compliance, audit trail | 2 days |
| 2 | **Slim Bronze DDL** (remove batch-level fields, add `ueh_schema_version`) | Storage reduction, cleaner architecture | 1 day |
| 3 | **Add `schedule_enabled` to adapter_config** | Operational flexibility | 0.5 day |
| 4 | **Add `watermark_state_json` to adapter_state** | Unblocks Tenable onboarding | 0.5 day |
| 5 | **Standardize `failure_category` enum in failed_ingestions** | Enables failure analytics | 1 day |
| 6 | **Add `trigger_type` to batch_registry** | Operational reporting accuracy | 0.5 day |
| 7 | **Add Iceberg snapshot IDs to batch_registry** | Forensic time-travel per batch | 0.5 day |
| 8 | **Create `t01_ueh_ctl_platform_metrics` table** | Foundation for observability | 1 day |

**Total HIGH PRIORITY effort: ~7 days**

## MEDIUM PRIORITY (Implement in Sprint 3-4)

| # | Item | Impact | Effort |
|---|------|--------|--------|
| 9 | Create `t01_ueh_ctl_sla_definitions` table | SLA enforcement foundation | 1 day |
| 10 | Build SLA watchdog Airflow DAG | Proactive breach detection | 2 days |
| 11 | Build platform metrics collector DAG | Dashboard data source | 2 days |
| 12 | Create `t01_ueh_ctl_pipeline_dependency` table | Cross-adapter Gold dependencies | 1 day |
| 13 | Add `rewrite_manifests` to Iceberg maintenance | Query performance at scale | 0.5 day |
| 14 | Add `remove_orphan_files` to maintenance | Storage hygiene | 0.5 day |
| 15 | Document secrets management pattern | Security posture | 1 day |
| 16 | Enhance replay queue with `replay_attempt_number` | Replay auditability | 0.5 day |

**Total MEDIUM PRIORITY effort: ~9 days**

## NICE TO HAVE (Implement in Sprint 5+)

| # | Item | Impact | Effort |
|---|------|--------|--------|
| 17 | Create `t01_ueh_ctl_retention_policy` table | Data lifecycle automation | 1 day |
| 18 | Build archive/quarantine zone structure | Storage optimization | 2 days |
| 19 | Document full promotion workflow | Deployment governance | 1 day |
| 20 | Build Grafana dashboard from platform_metrics | Visual observability | 3 days |
| 21 | Self-service adapter onboarding UI design | Future scalability | 5+ days |

**Total NICE TO HAVE effort: ~12 days**

---

# 16. UPDATED DDL REFERENCE

## Complete v4 Control Tables DDL

Below is the consolidated v4 DDL incorporating all improvements. This supersedes `ddl/02_control_tables.sql` from v3.

### File: `ddl/02_control_tables_v4.sql`

```sql
-- =============================================================================
-- UEH Platform v4: Control Framework Tables
-- =============================================================================
-- Version: 4.0
-- Changes from v3:
--   - adapter_config: Added schedule_enabled, source_api_version
--   - adapter_state: Added watermark_state_json
--   - batch_registry: Added trigger_type, snapshot IDs, source_api_endpoint/version
--   - failed_ingestions: Standardized failure_category, added retry fields
--   - replay_queue: Added replay_attempt_number, replay_scope
--   - NEW: adapter_config_history
--   - NEW: pipeline_dependency
--   - NEW: platform_metrics
--   - NEW: sla_definitions
-- =============================================================================

USE ueh_dev_control;

-- [Tables defined in sections 6-8 above]
-- See individual section DDL for complete column definitions.
```

---

# CONCLUSION

## v3 → v4 Improvement Summary

| Dimension | v3 Score | v4 Score | Key Improvement |
|-----------|----------|----------|----------------|
| Core Architecture | 9/10 | 9/10 | Preserved (already strong) |
| Bronze Design | 7/10 | 9/10 | Slimmer records, schema versioning |
| Control Framework | 7/10 | 9/10 | Config history, structured watermarks, SLA |
| Observability | 3/10 | 8/10 | Metrics table, watchdog DAG, dashboards |
| Security | 6/10 | 9/10 | Formalized secrets management pattern |
| Governance | 5/10 | 8/10 | Config history, promotion workflow, retention |
| Replay/Recovery | 7/10 | 9/10 | Stronger lineage, attempt tracking |
| Scalability | 7/10 | 9/10 | Manifest rewrites, dependency model |
| **Overall** | **7.5/10** | **8.8/10** | +1.3 enterprise readiness improvement |

## Guiding Principle for v4 Implementation

> **"v3 built the engine. v4 adds the dashboard, the safety systems, and the maintenance manual."**

The core data philosophy remains unchanged. What v4 adds is the operational, governance, and observability infrastructure required to run this platform at enterprise scale with 27+ adapters, multiple teams, regulatory requirements, and 24/7 availability expectations.

---

*End of Document*
