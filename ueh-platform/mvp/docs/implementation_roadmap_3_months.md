# UEH Implementation Roadmap — 3-Month Plan (Data Engineer Guide)

## Phase 1 (Current) → Phase 2 (Tenable + Asset Inventory)

**Document Version:** 2.0  
**Date:** June 2026  
**Scope:** Complete implementation plan with control table phasing  
**Duration:** 12 weeks (3 months)  
**Team:** 2 Senior DE, 1 Junior DE, 1 Full-Stack Developer  
**Reference:** UEH Architecture v4 (`docs/UEH_Architecture_v4.md`)

---

## Control Table Phasing Overview

### Which Tables, When, and Where They're Used

| Phase | Control Table | When to Create | Used By | Purpose |
|-------|--------------|----------------|---------|---------|
| **Phase 1 (Week 1-2)** | `adapter_config` | Day 1 | NiFi, DAG Factory, Silver | "What to ingest and how" |
| **Phase 1 (Week 1-2)** | `adapter_state` | Day 1 | NiFi, DAG Factory | "Where to resume (watermark)" |
| **Phase 1 (Week 1-2)** | `batch_registry` | Day 1 | All DAGs, Bronze, Silver | "What happened in each run" (lifecycle coupling) |
| **Phase 1 (Week 1-2)** | `field_mapping` | Day 1 | Silver Spark jobs | "How to parse payload_json → Silver columns" |
| **Phase 2 (Week 5-6)** | `failed_ingestions` | Week 5 | NiFi (failure), Ops DAG | "What failed and why" (dead letter registry) |
| **Phase 2 (Week 7-8)** | `pipeline_dependency` | Week 7 | Gold DAG, Silver DAG | "Which Silver tables must complete before Gold runs" |
| **Phase 3 (Week 9-10)** | `adapter_config_history` | Week 9 | UI Backend, Audit | "Who changed config, when, old→new values" |
| **Phase 3 (Week 9-10)** | `platform_metrics` | Week 9 | Metrics DAG, Grafana | "Platform health KPIs over time" |
| **Phase 3 (Week 11-12)** | `sla_definitions` | Week 11 | SLA Watchdog DAG | "What's the SLA per adapter per stage" |
| **Phase 3 (Week 11-12)** | `replay_queue` | Week 11 | Replay DAG | "What needs reprocessing and from which stage" |

---


## PHASE 1: Weeks 1-2 — MVP Completion (NVD End-to-End)

### Control Tables Needed: 4

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ PHASE 1 CONTROL TABLES (Create on Day 1)                                     │
│                                                                             │
│  1. adapter_config      ← NiFi reads (URL, auth, pagination)               │
│                         ← DAG Factory reads (schedule_cron)                 │
│                         ← Silver reads (source_system for routing)          │
│                                                                             │
│  2. adapter_state       ← NiFi reads (watermark_state_json)                 │
│                         ← NiFi writes (new watermark after success)         │
│                         ← DAG Factory reads (state_status for preflight)    │
│                                                                             │
│  3. batch_registry      ← NiFi writes (status = RAW_COMPLETE)              │
│                         ← Bronze DAG polls (WHERE status = RAW_COMPLETE)    │
│                         ← Bronze Spark writes (status = BRONZE_COMPLETE)    │
│                         ← Silver DAG polls (WHERE status = BRONZE_COMPLETE) │
│                         ← Silver Spark writes (status = SILVER_COMPLETE)    │
│                         ← Gold reads (for enrichment context)               │
│                                                                             │
│  4. field_mapping       ← Silver Spark reads (source_json_path → target)    │
│                         ← UI writes (analyst configures mappings)           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### How Each Table Is Used in Phase 1 Pipeline

```
NiFi Flow:
    READ  adapter_config   → get base_url, auth_secret_ref, pagination_config
    READ  adapter_state    → get watermark_state_json (where to resume)
    WRITE batch_registry   → INSERT with batch_status = 'RAW_COMPLETE'
    WRITE adapter_state    → UPDATE watermark_state_json (advance cursor)

DAG Factory (dag_factory_raw_ingestion.py):
    READ  adapter_config   → get schedule_cron (creates DAG per adapter)
    READ  adapter_state    → check state_status (preflight: skip if FAILING)
    READ  batch_registry   → check if already ran today (avoid duplicates)

Generic Bronze DAG (dag_generic_bronze_load.py):
    READ  batch_registry   → poll WHERE batch_status = 'RAW_COMPLETE'
    READ  adapter_config   → JOIN to get source_system (determine Bronze table)

Generic Bronze Spark (generic_bronze_loader.py):
    READ  batch_registry   → get bronze_path, adapter_instance_id
    READ  adapter_config   → get source_system (to pick target table name)
    WRITE batch_registry   → UPDATE batch_status = 'BRONZE_COMPLETE'
    WRITE adapter_state    → UPDATE last_batch_id

Silver DAG (dag_silver_transform.py):
    READ  batch_registry   → poll WHERE batch_status = 'BRONZE_COMPLETE'
    READ  adapter_config   → JOIN to get source_system (route to correct job)

Silver Spark (silver_vulnerability_intel.py):
    READ  batch_registry   → get batch context
    READ  adapter_config   → get source_system, org_id
    READ  field_mapping    → get transformation rules (source_json_path → target)
    READ  Bronze table     → get payload_json records for this batch
    WRITE Silver table     → MERGE/APPEND transformed records
    WRITE batch_registry   → UPDATE batch_status = 'SILVER_COMPLETE'
```

### Phase 1 Tasks

| # | Task | Owner | Days | Control Table Involved |
|---|------|-------|------|------------------------|
| 1.1 | Create 4 control tables (DDL) | DE1 | 0.5 | adapter_config, adapter_state, batch_registry, field_mapping |
| 1.2 | Seed NVD adapter config + state | DE1 | 0.5 | adapter_config, adapter_state |
| 1.3 | Seed NVD field mappings (6 rules) | DE1 | 0.5 | field_mapping |
| 1.4 | Fix Bronze NVD loader for CDE | DE1 | 1 | batch_registry (reads/writes) |
| 1.5 | NiFi NVD flow operational | DE2 | 2 | adapter_config, adapter_state, batch_registry |
| 1.6 | Deploy DAG Factory + Generic Bronze DAG | JDE | 1 | adapter_config (factory reads) |
| 1.7 | Deploy Silver DAG + Sync DAG | JDE | 1 | batch_registry (polls) |
| 1.8 | Validate end-to-end pipeline | All | 1 | All 4 tables |
| 1.9 | Run 3+ consecutive days stable | All | 3 | Validate lifecycle transitions |

### Exit Criteria
- batch_registry shows: RAW_COMPLETE → BRONZE_COMPLETE → SILVER_COMPLETE
- adapter_state.watermark_state_json advances daily
- slv_vulnerability_intel has real CVE data
- Gold tables populated

---


## PHASE 2: Weeks 3-8 — Tenable + ADDM + EPSS/CISA

### Control Tables Needed: +1 (failed_ingestions)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ PHASE 2 NEW CONTROL TABLE (Week 5)                                           │
│                                                                             │
│  5. failed_ingestions   ← NiFi writes (when API call fails)                 │
│                         ← Bronze Spark writes (when load fails)             │
│                         ← Silver Spark writes (when transform fails)        │
│                         ← Ops dashboard reads (failure analytics)           │
│                         ← Replay logic reads (what needs reprocessing)      │
│                                                                             │
│  Purpose: Track ALL failures with structured failure_category:              │
│    AUTH_FAILURE | API_TIMEOUT | RATE_LIMIT | BAD_PAYLOAD | SCHEMA_DRIFT     │
│    NETWORK_FAILURE | PARSE_ERROR | DQ_FAILURE | INTERNAL_ERROR              │
│                                                                             │
│  Why Week 5 (not earlier):                                                  │
│    - Phase 1 focuses on happy path (get it working first)                   │
│    - Phase 2 introduces complex sources (Tenable) that WILL fail            │
│    - You need failure tracking before production-like volumes               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### `failed_ingestions` DDL (Create in Week 5)

```sql
CREATE TABLE IF NOT EXISTS t01_ueh_dev_ctl.t01_ueh_ctl_failed_ingestions (
    failure_id              STRING      NOT NULL,
    org_id                  STRING      NOT NULL,
    batch_id                STRING      NOT NULL,
    adapter_instance_id     STRING      NOT NULL,

    failure_timestamp       TIMESTAMP   NOT NULL,
    failure_stage           STRING      NOT NULL
        COMMENT 'RAW_INGESTION | BRONZE_LOAD | SILVER_TRANSFORM | GOLD_COMPUTE',
    failure_category        STRING      NOT NULL
        COMMENT 'AUTH_FAILURE | API_TIMEOUT | RATE_LIMIT | BAD_PAYLOAD | SCHEMA_DRIFT | NETWORK_FAILURE | PARSE_ERROR | DQ_FAILURE | INTERNAL_ERROR',
    failure_reason          STRING      NOT NULL,
    failure_context_json    STRING
        COMMENT 'HTTP status, headers, partial response, stack trace snippet',

    dead_letter_path        STRING
        COMMENT 'HDFS path where failed payload was preserved',
    records_affected        BIGINT,
    is_auto_retryable       BOOLEAN     DEFAULT FALSE,
    auto_retry_count        INT         DEFAULT 0,

    resolution_status       STRING      NOT NULL DEFAULT 'PENDING'
        COMMENT 'PENDING | REPLAYED | RESOLVED | IGNORED',
    resolved_at             TIMESTAMP,
    resolved_by             STRING,
    replay_batch_id         STRING
        COMMENT 'New batch_id if replayed',

    created_at              TIMESTAMP   NOT NULL
)
USING iceberg
PARTITIONED BY (days(failure_timestamp))
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd');
```

### Where `failed_ingestions` Is Used

```
NiFi (raw ingestion failure):
    IF InvokeHTTP returns 401/403:
        INSERT INTO failed_ingestions (failure_category = 'AUTH_FAILURE')
    IF InvokeHTTP times out:
        INSERT INTO failed_ingestions (failure_category = 'API_TIMEOUT')

Bronze Spark (load failure):
    IF batch not found / file read error:
        INSERT INTO failed_ingestions (failure_category = 'INTERNAL_ERROR')

Silver Spark (transform failure):
    IF get_json_object returns NULL for required fields:
        INSERT INTO failed_ingestions (failure_category = 'SCHEMA_DRIFT')
    IF type cast fails:
        INSERT INTO failed_ingestions (failure_category = 'PARSE_ERROR')

Operational Dashboard:
    SELECT failure_category, COUNT(*) FROM failed_ingestions
    WHERE failure_timestamp > DATE_SUB(CURRENT_DATE(), 7)
    GROUP BY failure_category;
    → "80% of failures are AUTH_FAILURE → credential rotation needed"
```

### Phase 2 Tasks (Weeks 3-8)

| # | Task | Owner | Days | Control Tables Used |
|---|------|-------|------|---------------------|
| 3.1 | Request Tenable API access | DE1 | Day 1 | — (security process) |
| 3.2 | Seed tenable_prod_us_01 in adapter_config | DE1 | 0.5 | adapter_config, adapter_state |
| 3.3 | Build NiFi Tenable flow (async export) | DE2 | 3 | adapter_config, adapter_state, batch_registry |
| 3.4 | Create t01_ueh_brz_tenable_raw | JDE | 0.5 | — (DDL only) |
| 3.5 | Test Tenable Bronze end-to-end | DE1 | 1 | batch_registry (RAW→BRONZE) |
| 4.1 | **Create failed_ingestions table** | DE1 | 0.5 | **NEW table** |
| 4.2 | Add failure handling to NiFi Tenable flow | DE2 | 1 | failed_ingestions (write on failure) |
| 4.3 | Add failure handling to Bronze Spark | DE1 | 0.5 | failed_ingestions |
| 5.1 | Seed Tenable field_mapping (18 rules) | DE1 | 1 | field_mapping |
| 5.2 | Test silver_vulnerability_findings.py | DE1 | 2 | field_mapping, batch_registry |
| 5.3 | Seed addm_prod_01 in adapter_config | DE2 | 0.5 | adapter_config, adapter_state |
| 5.4 | Build NiFi ADDM flow | DE2 | 2 | adapter_config, adapter_state, batch_registry |
| 5.5 | Create t01_ueh_brz_bmc_addm_raw | JDE | 0.5 | — (DDL only) |
| 7.1 | EPSS + CISA Bronze (NiFi flows) | DE1 | 1.5 | adapter_config, adapter_state, batch_registry |
| 7.2 | Seed EPSS/CISA field_mapping | DE1 | 1 | field_mapping |
| 7.3 | Seed ADDM field_mapping (15 rules) | DE2 | 1 | field_mapping |
| 7.4 | Test silver_assets.py with ADDM data | DE2 | 2 | field_mapping, batch_registry |
| 7.5 | Add failure handling to Silver Spark | DE1 | 0.5 | failed_ingestions |

### Exit Criteria (Week 8)
- 5 adapters flowing (NVD, EPSS, CISA, Tenable, ADDM)
- failed_ingestions tracking all errors with categories
- All 3 Silver tables populated with real data

---


## PHASE 3: Weeks 9-12 — Gold + Observability + Production Readiness

### Control Tables Needed: +4

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ PHASE 3 NEW CONTROL TABLES                                                   │
│                                                                             │
│  6. pipeline_dependency (Week 9)                                             │
│     ← Gold DAG reads: "Which Silver tables must be complete before I run?"  │
│     ← Silver DAG reads: "Which Bronze batches feed this Silver table?"      │
│                                                                             │
│  7. adapter_config_history (Week 9)                                          │
│     ← UI Backend writes: every adapter_config change is logged              │
│     ← Audit reads: "who changed Tenable base_url last week?"                │
│     ← Rollback: "what was the previous schedule_cron?"                      │
│                                                                             │
│  8. platform_metrics (Week 10)                                               │
│     ← Metrics DAG writes: computes KPIs every 15 minutes                   │
│     ← Grafana reads: dashboard displays platform health                    │
│     ← Alerting reads: triggers on threshold_breached = TRUE                │
│                                                                             │
│  9. sla_definitions (Week 11)                                                │
│     ← SLA Watchdog DAG reads: "What's the max allowed duration?"            │
│     ← Watchdog compares: actual_duration vs sla_threshold_minutes           │
│     ← Alert: if breached → write to platform_metrics + notify              │
│                                                                             │
│  10. replay_queue (Week 11)                                                  │
│      ← Ops team writes: "Replay batch_xyz from BRONZE stage"               │
│      ← Replay DAG reads: picks up PENDING replays                          │
│      ← Replay DAG writes: creates new batch, links to original             │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Table 6: `pipeline_dependency` (Week 9)

**Why needed now:** Gold layer JOINs slv_vulnerability_intel + slv_vulnerability_findings + slv_assets. Without explicit dependencies, Gold might run before Silver completes.

```sql
CREATE TABLE IF NOT EXISTS t01_ueh_dev_ctl.t01_ueh_ctl_pipeline_dependency (
    dependency_id           STRING      NOT NULL,
    target_table            STRING      NOT NULL
        COMMENT 'e.g., gld_exposure_summary',
    target_layer            STRING      NOT NULL
        COMMENT 'SILVER or GOLD',
    source_table            STRING      NOT NULL
        COMMENT 'e.g., slv_vulnerability_intel',
    source_layer            STRING      NOT NULL
        COMMENT 'BRONZE or SILVER',
    source_adapter_name     STRING
        COMMENT 'Which adapter produces the source (NVD, TENABLE, etc.)',
    dependency_type         STRING      NOT NULL
        COMMENT 'HARD (blocks execution) | SOFT (proceeds with warning)',
    required_status         STRING      NOT NULL
        COMMENT 'BRONZE_COMPLETE | SILVER_COMPLETE',
    freshness_sla_hours     INT
        COMMENT 'Max age of upstream data before considered stale',
    is_active               BOOLEAN     DEFAULT TRUE,
    created_at              TIMESTAMP   NOT NULL,
    updated_at              TIMESTAMP   NOT NULL
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');
```

**Used by Gold DAG:**
```python
# In dag_gold_compute.py (check_silver_ready task):
dependencies = spark.sql("""
    SELECT source_table, source_adapter_name, freshness_sla_hours, dependency_type
    FROM t01_ueh_dev_ctl.t01_ueh_ctl_pipeline_dependency
    WHERE target_table = 'gld_exposure_summary' AND is_active = TRUE
""").collect()

for dep in dependencies:
    # Check if source has fresh SILVER_COMPLETE data
    latest = spark.sql(f"""
        SELECT MAX(end_time) as last_completed
        FROM t01_ueh_dev_ctl.t01_ueh_ctl_batch_registry
        WHERE adapter_instance_id IN (
            SELECT adapter_instance_id FROM t01_ueh_dev_ctl.t01_ueh_ctl_adapter_config
            WHERE source_system = '{dep.source_adapter_name}'
        ) AND batch_status = 'SILVER_COMPLETE'
    """).first()
    
    hours_stale = (now - latest.last_completed).hours
    if hours_stale > dep.freshness_sla_hours and dep.dependency_type == 'HARD':
        raise Exception(f"Dependency stale: {dep.source_table} is {hours_stale}h old")
```

### Table 7: `adapter_config_history` (Week 9)

**Why needed now:** With 5+ adapters in production, config changes happen frequently. Without audit trail, incident investigation is blind.

```sql
CREATE TABLE IF NOT EXISTS t01_ueh_dev_ctl.t01_ueh_ctl_adapter_config_history (
    history_id              STRING      NOT NULL,
    adapter_instance_id     STRING      NOT NULL,
    org_id                  STRING      NOT NULL,
    change_timestamp        TIMESTAMP   NOT NULL,
    change_type             STRING      NOT NULL
        COMMENT 'CREATE | UPDATE | DEACTIVATE | REACTIVATE | PAUSE_SCHEDULE',
    changed_by              STRING      NOT NULL,
    change_reason           STRING,
    field_changed           STRING      NOT NULL,
    old_value               STRING,
    new_value               STRING,
    config_snapshot_json    STRING
        COMMENT 'Full adapter config at time of change',
    created_at              TIMESTAMP   NOT NULL
)
USING iceberg
PARTITIONED BY (months(change_timestamp))
TBLPROPERTIES ('format-version' = '2');
```

**Used by UI Backend:**
```python
# Every time analyst changes adapter config via UI:
def update_adapter_config(adapter_instance_id, field, new_value, changed_by, reason):
    old_value = get_current_value(adapter_instance_id, field)
    
    # 1. Write history FIRST
    insert_config_history(adapter_instance_id, field, old_value, new_value, changed_by, reason)
    
    # 2. Then apply change
    update_adapter_config_table(adapter_instance_id, field, new_value)
```

### Table 8: `platform_metrics` (Week 10)

**Why needed now:** 5 adapters running = need proactive health monitoring, not reactive debugging.

```sql
CREATE TABLE IF NOT EXISTS t01_ueh_dev_ctl.t01_ueh_ctl_platform_metrics (
    metric_id               STRING      NOT NULL,
    metric_timestamp        TIMESTAMP   NOT NULL,
    metric_date             DATE        NOT NULL,
    adapter_instance_id     STRING
        COMMENT 'NULL for platform-wide metrics',
    pipeline_layer          STRING
        COMMENT 'BRONZE | SILVER | GOLD | PLATFORM',
    metric_name             STRING      NOT NULL
        COMMENT 'ingestion_duration_sec | records_ingested | dq_failure_rate | watermark_lag_hours | sla_breach | consecutive_failures',
    metric_value            DOUBLE      NOT NULL,
    metric_unit             STRING,
    threshold_breached      BOOLEAN     DEFAULT FALSE,
    alert_severity          STRING
        COMMENT 'INFO | WARNING | CRITICAL'
)
USING iceberg
PARTITIONED BY (metric_date)
TBLPROPERTIES ('format-version' = '2');
```

**Used by Metrics Collector DAG (runs every 15 min):**
```python
# Compute and write metrics for each adapter
for adapter in active_adapters:
    # Watermark lag
    lag_hours = compute_watermark_lag(adapter)
    insert_metric(adapter, 'watermark_lag_hours', lag_hours)
    
    # Consecutive failures
    failures = get_consecutive_failures(adapter)
    insert_metric(adapter, 'consecutive_failures', failures,
                  threshold_breached=(failures >= 3),
                  alert_severity='CRITICAL' if failures >= 5 else 'WARNING')
    
    # DQ failure rate (from last batch)
    dq_rate = compute_dq_failure_rate(adapter)
    insert_metric(adapter, 'dq_failure_rate', dq_rate)
```

**Used by Grafana Dashboard:**
```sql
-- Real-time adapter health panel:
SELECT adapter_instance_id, metric_name, metric_value, threshold_breached
FROM t01_ueh_dev_ctl.t01_ueh_ctl_platform_metrics
WHERE metric_date = CURRENT_DATE()
  AND metric_name IN ('watermark_lag_hours', 'consecutive_failures', 'sla_breach')
ORDER BY threshold_breached DESC, metric_timestamp DESC;
```

### Table 9: `sla_definitions` (Week 11)

**Why needed now:** Different adapters have different SLA expectations. Need formalized thresholds.

```sql
CREATE TABLE IF NOT EXISTS t01_ueh_dev_ctl.t01_ueh_ctl_sla_definitions (
    sla_id                  STRING      NOT NULL,
    adapter_instance_id     STRING      NOT NULL,
    pipeline_stage          STRING      NOT NULL
        COMMENT 'RAW | BRONZE | SILVER | GOLD | END_TO_END',
    sla_threshold_minutes   INT         NOT NULL,
    warning_threshold_minutes INT,
    alert_channel           STRING
        COMMENT 'email | slack | pagerduty',
    alert_recipients        STRING,
    is_active               BOOLEAN     DEFAULT TRUE,
    created_at              TIMESTAMP   NOT NULL,
    updated_at              TIMESTAMP   NOT NULL
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');
```

**Seed example:**
```sql
-- NVD: 60 min end-to-end, warn at 45
INSERT INTO t01_ueh_ctl_sla_definitions VALUES
('sla_001', 'nvd_prod_01', 'END_TO_END', 60, 45, 'slack', '#ueh-alerts', true, now(), now());

-- Tenable: 45 min for raw, 20 for bronze
INSERT INTO t01_ueh_ctl_sla_definitions VALUES
('sla_002', 'tenable_prod_us_01', 'RAW', 45, 30, 'slack', '#ueh-alerts', true, now(), now());
('sla_003', 'tenable_prod_us_01', 'BRONZE', 20, 15, 'slack', '#ueh-alerts', true, now(), now());
```

**Used by SLA Watchdog DAG (runs every 5 min):**
```python
# Find in-flight batches exceeding SLA
in_flight = spark.sql("""
    SELECT b.batch_id, b.adapter_instance_id, b.batch_status, b.start_time,
           s.sla_threshold_minutes, s.warning_threshold_minutes, s.pipeline_stage
    FROM batch_registry b
    JOIN sla_definitions s ON b.adapter_instance_id = s.adapter_instance_id
    WHERE b.batch_status NOT IN ('SILVER_COMPLETE', 'GOLD_COMPLETE', 'FAILED')
      AND s.is_active = TRUE
""").collect()

for batch in in_flight:
    elapsed = (now - batch.start_time).minutes
    if elapsed > batch.sla_threshold_minutes:
        alert("SLA BREACHED", batch)
        insert_metric(batch.adapter_instance_id, 'sla_breach', 1, alert_severity='CRITICAL')
```

### Table 10: `replay_queue` (Week 11)

```sql
CREATE TABLE IF NOT EXISTS t01_ueh_dev_ctl.t01_ueh_ctl_replay_queue (
    replay_id               STRING      NOT NULL,
    original_batch_id       STRING      NOT NULL,
    adapter_instance_id     STRING      NOT NULL,
    org_id                  STRING      NOT NULL,
    replay_from_stage       STRING      NOT NULL
        COMMENT 'RAW (re-ingest) | BRONZE (re-load) | SILVER (re-transform)',
    replay_reason           STRING      NOT NULL
        COMMENT 'SCHEMA_FIX | LOGIC_CHANGE | FAILURE_RECOVERY | BACKFILL',
    replay_attempt_number   INT         DEFAULT 1,
    priority                INT         DEFAULT 5,
    replay_status           STRING      NOT NULL DEFAULT 'PENDING'
        COMMENT 'PENDING | IN_PROGRESS | COMPLETED | FAILED',
    new_batch_id            STRING,
    requested_at            TIMESTAMP   NOT NULL,
    requested_by            STRING      NOT NULL,
    started_at              TIMESTAMP,
    completed_at            TIMESTAMP,
    notes                   STRING
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');
```

---



## Control Table Data Flow Diagram (All Phases)

```
┌───────────────────────────────────────────────────────────────────────────────────────────────┐
│                          UEH CONTROL TABLE DATA FLOW                                           │
│                                                                                               │
│  WRITERS (who inserts/updates)              READERS (who queries)                              │
│  ─────────────────────────────              ───────────────────────                            │
│                                                                                               │
│  ┌──────────────┐                           ┌───────────────────────┐                         │
│  │  UI Backend   │──writes──▶ adapter_config ◀──reads── │ NiFi Flow          │                │
│  │  (analyst)    │──writes──▶ field_mapping  ◀──reads── │ DAG Factory        │                │
│  │              │──writes──▶ config_history             │ Silver Spark       │                │
│  └──────────────┘                                      └───────────────────────┘              │
│                                                                                               │
│  ┌──────────────┐                           ┌───────────────────────┐                         │
│  │  NiFi Flow    │──writes──▶ batch_registry ◀──reads── │ Bronze DAG (poll)  │                │
│  │  (ingestion)  │──writes──▶ adapter_state  ◀──reads── │ Silver DAG (poll)  │                │
│  │              │──writes──▶ failed_ingestions          │ Gold DAG (check)   │                │
│  └──────────────┘                                      └───────────────────────┘              │
│                                                                                               │
│  ┌──────────────┐                           ┌───────────────────────┐                         │
│  │  Spark Jobs   │──writes──▶ batch_registry ◀──reads── │ Metrics DAG        │                │
│  │  (Bronze/     │──writes──▶ adapter_state             │ SLA Watchdog DAG   │                │
│  │   Silver/Gold)│──writes──▶ failed_ingestions         │ Grafana Dashboard  │                │
│  └──────────────┘──writes──▶ platform_metrics          └───────────────────────┘              │
│                                                                                               │
│  ┌──────────────┐                           ┌───────────────────────┐                         │
│  │  Ops Team     │──writes──▶ replay_queue   ◀──reads── │ Replay DAG         │                │
│  │  (manual)     │──writes──▶ sla_definitions          │ SLA Watchdog       │                │
│  └──────────────┘                                      └───────────────────────┘              │
│                                                                                               │
└───────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Implementation Priority Summary

| Priority | Table | Phase | Week | Blocking? |
|----------|-------|-------|------|-----------|
| P0 (Must have Day 1) | adapter_config | 1 | 1 | YES — nothing works without it |
| P0 (Must have Day 1) | adapter_state | 1 | 1 | YES — NiFi can't resume |
| P0 (Must have Day 1) | batch_registry | 1 | 1 | YES — DAGs can't communicate |
| P0 (Must have Day 1) | field_mapping | 1 | 1 | YES — Silver can't parse |
| P1 (Need for Tenable) | failed_ingestions | 2 | 5 | YES — can't track failures at scale |
| P2 (Need for Gold) | pipeline_dependency | 3 | 9 | SOFT — Gold can run without, but risky |
| P2 (Governance) | adapter_config_history | 3 | 9 | NO — but audit compliance requires it |
| P2 (Observability) | platform_metrics | 3 | 10 | NO — but flying blind without it |
| P3 (Production) | sla_definitions | 3 | 11 | NO — manual SLA checks work temporarily |
| P3 (Recovery) | replay_queue | 3 | 11 | NO — manual replay works temporarily |

---

## Team Allocation (Updated)

| Week | DE1 | DE2 | JDE | FSD |
|------|-----|-----|-----|-----|
| 1-2 | Control tables DDL + NVD fix | NiFi NVD operational | DAG Factory + deploy | UI adapter config |
| 3-4 | Tenable config + Bronze test | NiFi Tenable (async) | Bronze DDL + DAGs | UI field mapping |
| 5-6 | **failed_ingestions DDL** + Tenable Silver | ADDM NiFi + Bronze | Error handling | UI status dashboard |
| 7-8 | EPSS/CISA Bronze + Silver enrichment | ADDM Silver + field_mapping | Validation queries | UI |
| 9-10 | **pipeline_dependency + config_history** | Gold exposure + metrics | **platform_metrics** table | API for chatbot |
| 11-12 | **sla_definitions** + SLA watchdog | **replay_queue** + replay DAG | Testing + docs | Demo prep |

---

## Key Risks

| Risk | Impact | Mitigation | Control Table Related? |
|------|--------|-----------|------------------------|
| Tenable API access delayed | Blocks Phase 2 | Request in Week 1 | adapter_config (can't seed) |
| Large Tenable exports timeout | NiFi failures | Increase timeouts in runtime_config_json | adapter_config.runtime_config_json |
| Field mapping wrong for Tenable | Silver produces bad data | Test with 100 records first | field_mapping (validate early) |
| Asset correlation fails | Gold exposure incomplete | Use multiple match keys (ip OR hostname OR fqdn) | pipeline_dependency (mark as SOFT) |
| No failure tracking before Week 5 | Blind to errors in Phase 1 | Accept risk for NVD (simple source) | failed_ingestions (delay acceptable for MVP) |

---

## Success Metrics (End of 3 Months)

| Metric | Target | Measured From |
|--------|--------|---------------|
| Adapters operational | 5+ | adapter_config WHERE is_active = TRUE |
| Daily success rate | >95% | batch_registry WHERE status != 'FAILED' |
| Gold exposure records | 10,000+ | gld_exposure_summary row count |
| Manual intervention | <1x/week | failed_ingestions WHERE resolution_status = 'PENDING' |
| Watermark lag (max) | <24 hours | platform_metrics WHERE metric_name = 'watermark_lag_hours' |
| SLA breaches | 0 in last 7 days | platform_metrics WHERE metric_name = 'sla_breach' |
| Mean time to remediate failure | <4 hours | failed_ingestions (resolved_at - failure_timestamp) |
