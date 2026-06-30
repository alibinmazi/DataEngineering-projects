# UEH Data Engineering Team — In-Depth Implementation Guide

## MVP Step-by-Step with Bi-Weekly Sprint Planning

**Document Version:** 1.0
**Audience:** Data Engineers (DE1, DE2, JDE), Full-Stack Developer (FSD)
**Platform:** Unified Vulnerability Exposure Hub (UEH)
**Stack:** NiFi · Airflow · Spark · Iceberg · Cloudera CDP · PostgreSQL
**Duration:** 12 weeks (6 sprints × 2 weeks)

---

## How to Use This Document

Each sprint section contains:
1. **Sprint Goal** — what "done" looks like
2. **Tasks** — granular, with owner + estimate
3. **Technical Detail** — exact fields, configs, logic
4. **Validation** — how to prove it works
5. **Exit Criteria** — gate to next sprint

Read the **Foundational Concepts** section first. It explains incremental load,
idempotency, ingestion logging, and orchestration — referenced throughout.

---

## Table of Contents

1. Foundational Concepts (READ FIRST)
2. Sprint 1 (Weeks 1-2): NVD End-to-End
3. Sprint 2 (Weeks 3-4): Tenable Bronze + Staging
4. Sprint 3 (Weeks 5-6): ADDM + Tenable Canonical
5. Sprint 4 (Weeks 7-8): EPSS/CISA + Asset Canonical
6. Sprint 5 (Weeks 9-10): Gold + PostgreSQL Serving
7. Sprint 6 (Weeks 11-12): Hardening + Production
8. Appendix: Field Reference, Troubleshooting


---

# 1. FOUNDATIONAL CONCEPTS (READ FIRST)

## 1.1 The Medallion Flow

```
Source API → NiFi → HDFS (raw chunks) → Bronze (Iceberg) → Silver Stage 1
   (staging) → Silver Stage 2 (canonical) → Gold → PostgreSQL (serving)
```

| Layer | Database | What Lives Here | Mutability |
|-------|----------|-----------------|------------|
| Bronze | `t01_ueh_dev_brz` | Raw payload_json, one table per adapter | Append-only, immutable |
| Silver Stage 1 | `t01_ueh_dev_slv` | Typed staging, one per adapter (`slv_stg_*`) | DELETE+INSERT per batch |
| Silver Stage 2 | `t01_ueh_dev_slv` | Canonical entities (`slv_vulnerability_intel`, etc.) | MERGE (upsert) |
| Gold | `t01_ueh_dev_gld` | Enriched analytics, KPIs | OVERWRITE partition |
| Control | `t01_ueh_dev_ctl` | Orchestration metadata | UPDATE/INSERT |

## 1.2 Control Tables (The Brain)

| Table | Written By | Read By | Purpose |
|-------|-----------|---------|---------|
| `adapter_config` | UI / seed SQL | NiFi, DAGs, Spark | What/how to ingest |
| `adapter_state` | NiFi, Spark | NiFi, DAGs | Watermark + health |
| `batch_registry` | NiFi, Spark | All DAGs | Lifecycle + DQ status |
| `field_mapping` | UI / seed SQL | Stage 2 Spark | Canonical mappings |

## 1.3 batch_status Lifecycle

```
RAW_COMPLETE → BRONZE_COMPLETE → STAGING_COMPLETE → SILVER_COMPLETE → GOLD_COMPLETE
     │              │                  │                  │              │
   NiFi          Bronze Spark      Stage 1 Spark      Stage 2 Spark    Gold Spark
   writes        writes            writes             writes           writes
```

Each DAG polls `batch_registry` for the PREVIOUS status, then advances it.
This is the **coupling mechanism** — no file sensors, no hardcoded dependencies.

## 1.4 dq_status (Quality Gate)

```
NOT_CHECKED → PASSED | WARNING | FAILED

Downstream proceeds when: dq_status IN ('PASSED', 'WARNING')
Downstream BLOCKS when:    dq_status = 'FAILED'
```

`dq_status` is computed by Spark jobs after each layer's validation and written
to `batch_registry`. The Airflow PythonSensor checks BOTH `batch_status` AND
`dq_status` before proceeding.


## 1.5 Incremental Load — How It Works

UEH never re-pulls all data daily. Each adapter resumes from its last position.

### The Watermark Mechanism

`adapter_state.watermark_state_json` stores WHERE the adapter last stopped:

```json
NVD:     {"lastModStartDate": "2026-06-19T00:00:00.000", "watermark_type": "iso_datetime"}
Tenable: {"last_found": 1718755200, "watermark_type": "unix_timestamp", "export_uuid": null}
ADDM:    {"modified_since": "2026-06-19T00:00:00.000", "watermark_type": "iso_datetime"}
```

### Incremental Flow (Step by Step)

```
1. NiFi reads adapter_state.watermark_state_json
       → "last pulled up to 2026-06-19"

2. NiFi calls API with that watermark as filter
       → GET /cves?lastModStartDate=2026-06-19&lastModEndDate=2026-06-20

3. API returns ONLY records modified since watermark
       → ~200 records (not the full 250,000 CVE catalog)

4. NiFi writes chunks to HDFS + batch_registry = RAW_COMPLETE

5. NiFi UPDATES adapter_state.watermark_state_json
       → "last pulled up to 2026-06-20" (new watermark = now)

6. Next run resumes from 2026-06-20
```

### ingestion_mode vs load_type (Critical Distinction)

| Concept | Stored In | Meaning | Example |
|---------|-----------|---------|---------|
| `ingestion_mode` | adapter_config | What source SUPPORTS | INCREMENTAL |
| `load_type` | batch_registry | What THIS run DID | FULL_LOAD (first run) or INCREMENTAL |

First run: ingestion_mode=INCREMENTAL but load_type=FULL_LOAD (watermark starts at epoch).
Daily runs: load_type=INCREMENTAL (watermark advances each day).

### Watermark Types Per Adapter

| Adapter | Watermark Field | Type | API Parameter |
|---------|----------------|------|---------------|
| NVD | lastModStartDate | iso_datetime | `lastModStartDate` / `lastModEndDate` |
| EPSS | date | iso_date | `date` |
| Tenable | last_found | unix_timestamp | export filter `last_found` |
| ADDM | modified_since | iso_datetime | `modified_since` |
| CISA KEV | (none) | full_snapshot | downloads full catalog each time |

## 1.6 Idempotency — Re-Running Same batch_id Is Safe

Every layer must produce the SAME result when re-run. No duplicates, no corruption.

| Layer | Mechanism | Code Pattern |
|-------|-----------|--------------|
| Bronze | Check-exists → SKIP | `IF count(batch_id) > 0: skip` |
| Stage 1 | DELETE + INSERT | `DELETE WHERE batch_id=X; INSERT fresh` |
| Stage 2 | MERGE on key | `MERGE ON cve_id` (natural idempotent) |
| Gold | OVERWRITE partition | `writeTo().overwritePartitions()` |

### Why Each Works

- **Bronze**: If batch already loaded, skip. Raw data never changes for a batch_id.
- **Stage 1**: Delete prior staging rows for this batch, write fresh. Re-run = clean rewrite.
- **Stage 2**: MERGE updates existing keys to same values. Running twice = identical result.
- **Gold**: Overwrites the whole day partition. Recomputing = same output.

### Replay Scenario

```
Problem: Silver parsing logic had a bug. Fixed it. Need to reprocess batch_123.

Solution (no re-ingestion needed):
   spark-submit silver_stage1_processor.py --batch_id batch_123
       → DELETE old staging rows, re-parse with fixed logic
   spark-submit silver_stage2_vuln_intel.py --batch_id batch_123
       → MERGE corrected canonical records

Bronze untouched. Idempotency guarantees clean reprocess.
```

## 1.7 Ingestion Logging & Observability

### Where Logs Live

| Log Type | Location | Used For |
|----------|----------|----------|
| Batch lifecycle | `batch_registry` | Status, records, timing per batch |
| DQ results | `batch_registry.dq_details_json` | Why DQ passed/warned/failed |
| Failures | `failed_ingestions` (Sprint 3+) | Categorized error tracking |
| Spark job logs | YARN / CDE logs | Detailed execution trace |
| NiFi logs | NiFi data provenance | Flow-level lineage |

### What batch_registry Captures Per Run

```sql
SELECT
    batch_id,              -- batch_20260620030000_nvd_prod_01
    adapter_instance_id,   -- nvd_prod_01
    batch_status,          -- SILVER_COMPLETE
    dq_status,             -- PASSED
    load_type,             -- INCREMENTAL
    records_expected,      -- 200 (from API totalResults)
    records_processed,     -- 200 (actually written)
    start_time,            -- when batch started
    end_time,              -- when batch finished
    failure_reason,        -- NULL (or error if FAILED)
    failure_category       -- NULL (or AUTH_FAILURE, etc.)
FROM t01_ueh_dev_ctl.t01_ueh_ctl_batch_registry
WHERE adapter_instance_id = 'nvd_prod_01'
ORDER BY created_at DESC;
```

### Reconciliation Check (records_expected vs records_processed)

```
If records_expected = 200 but records_processed = 195:
   → 5 records lost. Investigate dq_details_json for filtered records.
```


## 1.8 Orchestration — How DAGs Work Together

### The 7 DAGs

```
1. ueh_raw_<adapter>__<instance>   (DAG Factory — one per adapter, own schedule)
2. ueh_generic_bronze_load          (polls RAW_COMPLETE, every 10 min)
3. ueh_silver_pipeline              (Stage 1 → DQ gate → Stage 2, every 10 min)
4. ueh_dag4_gold_compute            (daily 06:00)
5. ueh_platform_sync_adapter_config (syncs config → Airflow Variable, every 5 min)
```

### DAG Factory Pattern (Raw Ingestion)

ONE Python file generates a separate DAG per active adapter:

```
adapter_config has 3 active adapters →
   ueh_raw_nvd__nvd_prod_01            (schedule: 0 3 * * *)
   ueh_raw_tenable__tenable_prod_us_01 (schedule: 0 */4 * * *)
   ueh_raw_bmc_addm__addm_prod_01      (schedule: 0 2 * * *)
```

How: `dag_sync_adapter_config` writes active adapters to Airflow Variable
`ueh_active_adapters`. The factory reads this Variable at parse time and
generates DAGs. Adding adapter = INSERT into adapter_config → new DAG appears.

### PythonSensor Gate (mode="reschedule")

Every downstream DAG starts with a sensor that frees the worker slot while waiting:

```python
PythonSensor(
    task_id='sensor_bronze_ready',
    python_callable=sensor_bronze_ready,
    mode='reschedule',     # frees worker between pokes (vs 'poke' which holds slot)
    poke_interval=60,      # check every 60s
    timeout=600,           # give up after 10 min
)
```

Sensor logic checks BOTH:
```sql
WHERE batch_status = 'BRONZE_COMPLETE'
  AND (dq_status IS NULL OR dq_status IN ('PASSED','WARNING'))
```

### Why mode="reschedule" Matters

- `mode='poke'` → holds a worker slot the entire wait (wasteful)
- `mode='reschedule'` → releases slot between checks (scalable for many adapters)

---

# 2. SPRINT 1 (Weeks 1-2): NVD End-to-End

## Sprint Goal

A single CVE flows from NVD API → Bronze → Silver Staging → Canonical Silver → Gold,
fully automated, idempotent, with DQ gating. Runs unattended for 3+ days.

## Pre-Sprint Setup (Day 1)

| Task | Owner | Detail |
|------|-------|--------|
| Get NVD API key | DE1 | Register at nvd.nist.gov/developers/request-an-api-key |
| Store key in vault | DE1 | `vault kv put secrets/ueh/dev/nvd_api_key value=<key>` |
| Verify Cloudera access | DE2 | Confirm HDFS write + Hive + Spark + Iceberg |
| Verify Airflow access | JDE | Confirm DAG deploy folder + Variables UI |

## Task Breakdown

### Task 1.1 — Create Control Tables (DE1, 0.5 day)

```bash
spark-sql -f infrastructure/control_tables/01_adapter_config.sql
spark-sql -f infrastructure/control_tables/02_adapter_state.sql
spark-sql -f infrastructure/control_tables/03_batch_registry.sql
spark-sql -f infrastructure/control_tables/04_field_mapping.sql
```

Then run the migration to add dq_status:
```bash
spark-sql -f infrastructure/migrations/001_add_dq_status_to_batch_registry.sql
```

**Validate:**
```sql
DESCRIBE t01_ueh_dev_ctl.t01_ueh_ctl_batch_registry;
-- Must show: batch_status, dq_status, dq_details_json
```

### Task 1.2 — Create Bronze + Silver + Gold Databases & Tables (DE1, 0.5 day)

```bash
# Databases
spark-sql -f infrastructure/databases/01_create_databases.sql
# Bronze
spark-sql -f infrastructure/bronze_tables/t01_ueh_brz_nvd_raw.sql
# Silver staging
spark-sql -f infrastructure/silver_staging_tables/01_slv_stg_nvd_vulnerability.sql
# Silver canonical
spark-sql -f infrastructure/silver_tables/01_create_silver_database.sql
spark-sql -f infrastructure/silver_tables/02_slv_vulnerability_intel.sql
# Gold
spark-sql -f infrastructure/gold_tables/01_create_gold_database.sql
spark-sql -f infrastructure/gold_tables/04_gld_cve_enriched.sql
```

### Task 1.3 — Seed NVD Adapter Config + Field Mappings (DE1, 0.5 day)

```bash
spark-sql -f seed/01_nvd_seed.sql                 # adapter_config + adapter_state
spark-sql -f infrastructure/seed/02_nvd_field_mappings.sql   # field_mapping (6 rules)
```

**Validate:**
```sql
SELECT adapter_instance_id, source_system, ingestion_mode, schedule_cron
FROM t01_ueh_dev_ctl.t01_ueh_ctl_adapter_config WHERE source_system='NVD';
-- nvd_prod_01 | NVD | INCREMENTAL | 0 3 * * *

SELECT count(*) FROM t01_ueh_dev_ctl.t01_ueh_ctl_field_mapping WHERE source_system='NVD';
-- 6
```


### Task 1.4 — Build NiFi NVD Flow (DE2, 2 days)

Follow `nifi/NVD_Ingestion_Step_by_Step.md` (9 processors). Key points:

```
1. ExecuteSQL → read adapter_config (base_url, auth_secret_ref, pagination)
2. ExecuteSQL → read adapter_state.watermark_state_json (lastModStartDate)
3. UpdateAttribute → generate batch_id, ingestion_date, build API URL
4. InvokeHTTP → call NVD API (loop for pagination: startIndex += 2000)
5. PutHDFS → write chunk_NNN.json to bronze raw path
6. PutSQL → INSERT batch_registry (batch_status='RAW_COMPLETE')
7. PutSQL → UPDATE adapter_state (advance watermark to now)
```

**The HDFS path NiFi writes to:**
```
/data-lake/dev/bronze/vulnerability_intel/nvd/raw/
   ingestion_date=2026-06-20/batch_id=batch_20260620030000_nvd_prod_01/
       chunk_001.json
```

**Critical NiFi config — watermark advance (Processor 7):**
```sql
UPDATE t01_ueh_dev_ctl.t01_ueh_ctl_adapter_state
SET watermark_state_json = '{"lastModStartDate":"${last_mod_end}","watermark_type":"iso_datetime"}',
    last_successful_run = CURRENT_TIMESTAMP(),
    records_last_pulled = ${total_results},
    state_status = 'HEALTHY'
WHERE adapter_instance_id = 'nvd_prod_01';
```

**Validate:** After manual run, check HDFS has chunk files + batch_registry has RAW_COMPLETE.

### Task 1.5 — Deploy Bronze Spark Job (DE1, 1 day)

```bash
hdfs dfs -put processing_engine/bronze/generic_bronze_loader.py /apps/ueh/spark/
```

Test manually:
```bash
spark-submit --conf ueh.environment=dev \
    /apps/ueh/spark/generic_bronze_loader.py <batch_id_from_nifi>
```

**What it does:** Reads chunks from `bronze_path`, explodes `vulnerabilities[]` array,
writes one row per CVE with `payload_json`, updates batch_registry=BRONZE_COMPLETE.

**Validate:**
```sql
SELECT count(*), batch_id FROM t01_ueh_dev_brz.t01_ueh_brz_nvd_raw GROUP BY batch_id;
```

### Task 1.6 — Deploy Silver Stage 1 + Parser (DE1, 1 day)

```bash
# Package parsers as zip for --py-files
cd processing_engine/silver && zip -r parsers.zip parsers/
hdfs dfs -put parsers.zip /apps/ueh/spark/
hdfs dfs -put staging/silver_stage1_processor.py /apps/ueh/spark/
```

Test:
```bash
spark-submit --conf ueh.environment=dev --py-files /apps/ueh/spark/parsers.zip \
    /apps/ueh/spark/silver_stage1_processor.py --batch_id <batch_id>
```

**What it does:** Loads nvd_parser_v1, parses payload_json → typed columns,
DELETE+INSERT into `slv_stg_nvd_vulnerability`, computes dq_status, sets STAGING_COMPLETE.

**Validate:**
```sql
SELECT cve_id, cvss31_base_score, cvss31_severity, dq_has_cve_id
FROM t01_ueh_dev_slv.t01_ueh_slv_stg_nvd_vulnerability LIMIT 5;
```

### Task 1.7 — Deploy Silver Stage 2 Canonical (DE2, 1 day)

```bash
hdfs dfs -put canonical/silver_stage2_vuln_intel.py /apps/ueh/spark/
```

Test:
```bash
spark-submit --conf ueh.environment=dev \
    /apps/ueh/spark/silver_stage2_vuln_intel.py --batch_id <batch_id>
```

**What it does:** Reads staging table, maps NVD-specific → canonical schema,
MERGE on cve_id into `slv_vulnerability_intel`, sets SILVER_COMPLETE.

**Validate:**
```sql
SELECT cve_id, severity, cvss_base_score, source_systems_json
FROM t01_ueh_dev_slv.t01_ueh_slv_vulnerability_intel LIMIT 5;
```

### Task 1.8 — Deploy Airflow DAGs (JDE, 1 day)

```bash
cp orchestration/dags/platform/dag_sync_adapter_config.py $AIRFLOW_HOME/dags/
cp orchestration/dags/raw/dag_factory_raw_ingestion.py $AIRFLOW_HOME/dags/
cp orchestration/dags/bronze/dag_generic_bronze_load.py $AIRFLOW_HOME/dags/
cp orchestration/dags/silver/dag_silver_pipeline.py $AIRFLOW_HOME/dags/
cp orchestration/dags/gold/dag_gold_compute.py $AIRFLOW_HOME/dags/

# Set Airflow Variables
airflow variables set ueh_environment dev
airflow variables set ueh_nifi_base_url http://nifi-dev:8080
airflow variables set ueh_nifi_pg_nvd <process-group-id>
```

**Validate:** `airflow dags list | grep ueh` shows all DAGs. Sync DAG populates
`ueh_active_adapters` Variable → factory generates `ueh_raw_nvd__nvd_prod_01`.

### Task 1.9 — Idempotency Test (DE1, 0.5 day)

```bash
# Run Stage 1 twice with same batch_id
spark-submit ... silver_stage1_processor.py --batch_id batch_X
spark-submit ... silver_stage1_processor.py --batch_id batch_X   # re-run

# Verify NO duplicates
SELECT cve_id, count(*) FROM slv_stg_nvd_vulnerability
WHERE batch_id='batch_X' GROUP BY cve_id HAVING count(*) > 1;
-- Expected: ZERO rows
```

### Task 1.10 — DQ Gate Test (DE2, 0.5 day)

```sql
-- Manually set a batch to dq FAILED
UPDATE batch_registry SET dq_status='FAILED' WHERE batch_id='batch_X';
-- Trigger silver pipeline → verify Stage 2 is SKIPPED (ShortCircuit blocks)
```

## Sprint 1 Exit Criteria

- [ ] Full pipeline: RAW → BRONZE → STAGING → SILVER → GOLD (one CVE traced through)
- [ ] batch_registry shows all 5 status transitions
- [ ] dq_status populated at staging
- [ ] Re-running any batch = no duplicates
- [ ] dq_status=FAILED blocks Stage 2
- [ ] 3 consecutive unattended daily runs succeed


---

# 3. SPRINT 2 (Weeks 3-4): Tenable Bronze + Staging

## Sprint Goal

Tenable findings (async export API) flow into Bronze and Stage 1 staging.
Handle 10,000+ records. Multi-chunk download. Proves the framework scales
to a complex enterprise scanner.

## Why Tenable Is Harder Than NVD

| Aspect | NVD | Tenable |
|--------|-----|---------|
| Auth | Single API key | access_key + secret_key |
| Pagination | Offset (startIndex) | Async export job (request→poll→download) |
| Volume | ~200/day | 10,000-100,000 findings |
| Watermark | lastModStartDate | last_found (unix timestamp) |

## Task Breakdown

### Task 2.1 — Tenable API Access (DE1 + Security, Day 1)

Request from security team:
- Tenable.io access_key + secret_key
- Firewall rule: NiFi → cloud.tenable.com (443)
- Store in vault: `vault kv put secrets/ueh/dev/tenable_prod_us_01 access_key=X secret_key=Y`

**START THIS DAY 1 — approvals take time.**

### Task 2.2 — Seed Tenable Adapter (DE1, 0.5 day)

```bash
spark-sql -f infrastructure/seed/03_tenable_adapter_seed.sql
```

This registers `tenable_prod_us_01` with:
- `pagination_config_json`: export job config (poll interval, endpoints)
- `watermark_state_json`: `{"last_found": 1704067200, "export_uuid": null}`
- `schedule_cron`: `0 */4 * * *` (every 4 hours)

### Task 2.3 — Create Tenable Bronze + Staging Tables (JDE, 1 day)

```bash
spark-sql -f infrastructure/bronze_tables/t01_ueh_brz_tenable_raw.sql
spark-sql -f infrastructure/silver_staging_tables/02_slv_stg_tenable_finding.sql
```

### Task 2.4 — Build NiFi Tenable Async Export Flow (DE2, 3 days)

The async export pattern:

```
1. POST /vulns/export
       body: {"filters":{"last_found":<watermark>},"num_assets":50}
       → returns export_uuid

2. LOOP: GET /vulns/export/{export_uuid}/status
       → poll every 30s until status="FINISHED"
       → response has chunks_available: [1,2,3]

3. FOR EACH chunk: GET /vulns/export/{export_uuid}/chunks/{chunk_id}
       → returns array of findings
       → PutHDFS chunk_NNN.json

4. PutSQL → batch_registry RAW_COMPLETE
5. PutSQL → adapter_state (new last_found watermark + clear export_uuid)
```

**NiFi processors for async:**
- `InvokeHTTP` (POST export request)
- `EvaluateJsonPath` (extract export_uuid)
- `InvokeHTTP` (GET status) + `RouteOnAttribute` (loop if not FINISHED) + `ControlRate` (30s)
- `SplitJson` (chunks_available array)
- `InvokeHTTP` (download each chunk) → `PutHDFS`

**Resumability:** Store `export_uuid` in watermark_state_json. If NiFi crashes
mid-download, next run resumes from saved export_uuid + chunks already downloaded.

### Task 2.5 — Register TENABLE in Stage 1 Processor (DE1, 0.5 day)

Already in `ADAPTER_REGISTRY` in `silver_stage1_processor.py`:
```python
'TENABLE': {
    'parser_module': 'parsers.tenable_parser_v1',
    'staging_table': 't01_ueh_slv_stg_tenable_finding',
},
```

### Task 2.6 — Test tenable_parser_v1 End-to-End (DE1, 1.5 days)

The parser extracts:
```
$.plugin.id → plugin_id
$.plugin.cve[0] → primary_cve
$.severity (0-4) → severity_id   (canonical maps to enum in Stage 2)
$.asset.uuid → asset_uuid
$.plugin.vpr.score → vpr_score
$.state → state (open/reopened/fixed)
```

**Validate:**
```sql
SELECT plugin_id, primary_cve, severity_id, asset_hostname, vpr_score, state
FROM t01_ueh_dev_slv.t01_ueh_slv_stg_tenable_finding LIMIT 10;
```

### Task 2.7 — Volume Test (DE1+DE2, 1 day)

Run full export (all open findings). Verify:
- 10,000+ records in Bronze
- Multi-chunk download succeeded
- No NiFi timeout (increase timeouts in runtime_config_json if needed)

## Sprint 2 Exit Criteria

- [ ] Tenable export API working (auth + export + poll + download)
- [ ] 10,000+ findings in `t01_ueh_brz_tenable_raw`
- [ ] Stage 1 produces typed records in `slv_stg_tenable_finding`
- [ ] export_uuid resumability tested (kill mid-run, resume)
- [ ] Watermark advances correctly (last_found)

---

# 4. SPRINT 3 (Weeks 5-6): ADDM + Tenable Canonical

## Sprint Goal

Build Tenable Stage 2 canonical (findings). Onboard ADDM Bronze + Stage 1.
Introduce `failed_ingestions` table for error tracking.

## Task Breakdown

### Task 3.1 — Tenable Stage 2 Canonical (DE1, 2 days)

Create `canonical/silver_stage2_vuln_findings.py`. Key logic:

```python
# finding_id = deterministic hash (idempotent + unique across scanners)
finding_id = md5(concat(source_system, source_finding_id, source_asset_id))

# severity_id (0-4) → canonical enum
severity = CASE severity_id
    WHEN 4 THEN 'CRITICAL' WHEN 3 THEN 'HIGH'
    WHEN 2 THEN 'MEDIUM' WHEN 1 THEN 'LOW' ELSE 'INFORMATIONAL'

# Write strategy: APPEND (point-in-time scan snapshots, preserve history)
```

**Apply the schema-alignment fix pattern** (avoid type/missing column errors):
```python
target_schema = spark.table(canonical_table).schema  # NOT describe
# add missing cols as NULL, cast all to target types, filter NOT NULL
```

### Task 3.2 — ADDM API Access + NiFi Flow (DE2, 3 days)

ADDM uses standard REST offset pagination (simpler than Tenable):
```
GET /data/hosts?limit=500&offset=0&modified_since=<watermark>
   → loop offset += 500 until total_count reached
```

### Task 3.3 — ADDM Stage 1 (DE2, 2 days)

ADDM parser already exists (`addm_parser_v1.py`). Create staging table,
register in ADAPTER_REGISTRY (already done), test.

### Task 3.4 — Create failed_ingestions Table (JDE, 0.5 day)

```bash
spark-sql -f infrastructure/control_tables/05_failed_ingestions.sql
```

Categories: AUTH_FAILURE, API_TIMEOUT, RATE_LIMIT, BAD_PAYLOAD, SCHEMA_DRIFT,
NETWORK_FAILURE, PARSE_ERROR, DQ_FAILURE, INTERNAL_ERROR.

### Task 3.5 — Add Failure Handling to All Spark Jobs (DE1, 1 day)

In each Spark job's except block:
```python
INSERT INTO failed_ingestions (failure_id, batch_id, adapter_instance_id,
    failure_stage, failure_category, failure_reason, ...)
VALUES (...)
```

## Sprint 3 Exit Criteria

- [ ] Tenable findings in canonical `slv_vulnerability_finding`
- [ ] finding_id deterministic + unique
- [ ] ADDM Bronze + Stage 1 working
- [ ] failed_ingestions tracking errors with categories


---

# 5. SPRINT 4 (Weeks 7-8): EPSS/CISA + Asset Canonical

## Sprint Goal

Multi-source canonical enrichment. EPSS + CISA KEV enrich the SAME CVE records
that NVD created. ADDM → canonical asset. Introduce pipeline_dependency table.

## Task Breakdown

### Task 4.1 — EPSS + CISA KEV Bronze + Parsers (DE1, 2 days)

```
EPSS:  GET https://api.first.org/data/v1/epss?offset=0&limit=1000
       → parser extracts: cve, epss_score, percentile
CISA:  GET full KEV JSON (no pagination)
       → parser extracts: cveID, dateAdded, dueDate, knownRansomware
```

Create staging tables: `slv_stg_epss_score`, `slv_stg_cisa_kev`.
Register both in ADAPTER_REGISTRY.

### Task 4.2 — Extend Stage 2 Intel: Multi-Source Merge (DE1, 2 days)

This is the KEY cross-source enrichment. `silver_stage2_vuln_intel.py` gets
branches for EPSS and CISA that MERGE only their fields onto existing CVE rows:

```python
if source_system == 'EPSS':
    # MERGE — update ONLY epss_score, epss_percentile (don't touch NVD fields)
    MERGE INTO slv_vulnerability_intel target USING epss_staging source
    ON target.cve_id = source.cve_id
    WHEN MATCHED THEN UPDATE SET
        target.epss_score = source.epss_score,
        target.epss_percentile = source.epss_percentile,
        target.epss_batch_id = source.batch_id,
        target.source_systems_json = <append EPSS to array>
    WHEN NOT MATCHED THEN INSERT (...)  # EPSS-only CVE not yet seen

if source_system == 'CISA_KEV':
    # MERGE — update ONLY KEV fields
    WHEN MATCHED THEN UPDATE SET
        target.is_in_kev = true,
        target.kev_date_added = source.dateAdded,
        target.kev_due_date = source.dueDate,
        target.is_actively_exploited = source.is_ransomware
```

**Result:** One CVE row enriched by 3 sources:
```sql
SELECT cve_id, cvss_base_score, epss_score, is_in_kev, source_systems_json
FROM slv_vulnerability_intel WHERE cve_id='CVE-2024-3400';
-- CVE-2024-3400 | 10.0 | 0.97 | true | ["NVD","EPSS","CISA_KEV"]
```

### Task 4.3 — ADDM → Canonical Asset (DE2, 2 days)

Create `canonical/silver_stage2_assets.py`. MERGE on asset_id:
```python
asset_id = md5(concat(source_system, source_asset_id))
# MERGE on asset_id (Type 1 — latest asset state wins)
```

### Task 4.4 — pipeline_dependency Table + Gold Gating (DE2, 1 day)

```bash
spark-sql -f infrastructure/control_tables/06_pipeline_dependency.sql
```

Seed: Gold exposure_summary depends on NVD+Tenable+ADDM Silver complete.
Gold DAG checks this before running.

## Sprint 4 Exit Criteria

- [ ] CVE records enriched by NVD + EPSS + CISA (one row, multi-source)
- [ ] source_systems_json shows all contributing sources
- [ ] ADDM assets in canonical `slv_asset`
- [ ] pipeline_dependency gates Gold readiness

---

# 6. SPRINT 5 (Weeks 9-10): Gold + PostgreSQL Serving

## Sprint Goal

Gold layer produces enriched exposure analytics. Sync to PostgreSQL for
fast UI/chatbot serving. Add observability (platform_metrics).

## Task Breakdown

### Task 5.1 — Gold Exposure Summary (DE1, 2 days)

`gold_exposure_summary.py` JOINs the 3 canonical Silver tables:
```sql
SELECT f.*, i.epss_score, i.is_in_kev, a.criticality, a.owner
FROM slv_vulnerability_finding f
LEFT JOIN slv_vulnerability_intel i ON f.cve_id = i.cve_id
LEFT JOIN slv_asset a ON f.asset_id = a.asset_id
```
Computes risk_score (CVSS + EPSS + KEV + asset criticality), OVERWRITE partition.

### Task 5.2 — Gold CVE Enriched + Risk Metrics (DE2, 2 days)

`gold_cve_enriched.py` + `gold_risk_metrics.py`. Aggregate by severity, BU, env.

### Task 5.3 — adapter_config_history Table (DE2, 0.5 day)

```bash
spark-sql -f infrastructure/control_tables/07_adapter_config_history.sql
```

### Task 5.4 — platform_metrics Table + Collector DAG (JDE, 2.5 days)

```bash
spark-sql -f infrastructure/control_tables/08_platform_metrics.sql
```
Collector DAG (every 15 min): watermark_lag, consecutive_failures, dq_failure_rate.

### Task 5.5 — PostgreSQL Sync (FSD, 2 days)

```python
# Daily sync: Gold Iceberg → PostgreSQL serving tables
gold_df.write.format("jdbc") \
    .option("url", "jdbc:postgresql://cloudsql:5432/ueh") \
    .option("dbtable", "exposure_findings") \
    .mode("overwrite").save()
```

Two table types in PostgreSQL:
- READ (synced from Gold): exposure_findings, cve_intelligence, asset_inventory
- OPERATIONAL (UI-managed): remediation_assignments, risk_exceptions, audit_log

### Task 5.6 — Dashboard API Endpoints (FSD, 3 days)

REST API reads PostgreSQL serving tables + writes operational tables.

## Sprint 5 Exit Criteria

- [ ] Gold exposure_summary with risk scoring
- [ ] PostgreSQL serving tables synced daily
- [ ] platform_metrics collecting health KPIs
- [ ] API endpoints functional

---

# 7. SPRINT 6 (Weeks 11-12): Hardening + Production

## Sprint Goal

Production-ready: SLA monitoring, replay capability, idempotency validated
across all adapters, security review, UAT deployment.

## Task Breakdown

| # | Task | Owner | Days |
|---|------|-------|------|
| 6.1 | sla_definitions table + SLA watchdog DAG (5-min poll) | DE1 | 2 |
| 6.2 | replay_queue table + replay DAG | DE2 | 2.5 |
| 6.3 | Idempotency validation (all 5 adapters, all layers) | DE1 | 1 |
| 6.4 | Iceberg maintenance DAG (compaction, expire snapshots, rewrite manifests) | JDE | 1 |
| 6.5 | Security review (secrets, no creds in logs/code) | DE1 | 1 |
| 6.6 | Performance tuning (Spark partitioning, file sizes) | DE2 | 1 |
| 6.7 | UAT deployment (promote dev → uat) | All | 1 |
| 6.8 | Stakeholder demo + Phase 3 planning | All | 1 |

### Task 6.1 — SLA Watchdog Detail

```bash
spark-sql -f infrastructure/control_tables/09_sla_definitions.sql
```
Watchdog finds in-flight batches exceeding sla_threshold_minutes → alert + metric.

### Task 6.2 — Replay DAG Detail

```bash
spark-sql -f infrastructure/control_tables/10_replay_queue.sql
```
Ops inserts replay request → DAG picks PENDING → re-runs from specified stage.

### Task 6.4 — Iceberg Maintenance

```sql
CALL system.rewrite_data_files(table => '...');   -- compact small files
CALL system.rewrite_manifests(table => '...');     -- compact manifests
CALL system.expire_snapshots(table => '...', older_than => TIMESTAMP '...');
CALL system.remove_orphan_files(table => '...');
```

## Sprint 6 Exit Criteria

- [ ] 5+ adapters running unattended
- [ ] SLA breaches detected automatically
- [ ] Replay works (reprocess any batch from any stage)
- [ ] Idempotency proven across all adapters
- [ ] Deployed to UAT


---

# 8. APPENDIX

## 8.1 Control Table Field Reference

### adapter_config (key fields)

| Field | Type | Example | Purpose |
|-------|------|---------|---------|
| adapter_instance_id | STRING | nvd_prod_01 | Unique adapter ID (PK) |
| source_system | STRING | NVD | Source enum (routing key) |
| adapter_type | STRING | REST_API | Implementation type |
| base_url | STRING | https://services.nvd.nist.gov/... | API endpoint |
| auth_method | STRING | API_KEY | Auth type |
| auth_secret_ref | STRING | vault://secrets/ueh/dev/nvd_api_key | Vault pointer (NEVER raw key) |
| ingestion_mode | STRING | INCREMENTAL | What source supports |
| schedule_cron | STRING | 0 3 * * * | Cron for DAG factory |
| schedule_enabled | BOOLEAN | true | Pause without decommission |
| pagination_config_json | STRING | {"type":"offset","page_size":2000} | Pagination rules |
| runtime_config_json | STRING | {"timeout_sec":120,"max_retries":3} | Retry/timeout/rate |
| is_active | BOOLEAN | true | Decommission flag |

### adapter_state (key fields)

| Field | Type | Example | Purpose |
|-------|------|---------|---------|
| adapter_instance_id | STRING | nvd_prod_01 | FK |
| watermark_state_json | STRING | {"lastModStartDate":"2026-06-19..."} | Resume point |
| last_batch_id | STRING | batch_2026... | Last completed batch |
| consecutive_failures | INT | 0 | Circuit breaker counter |
| state_status | STRING | HEALTHY | NEW/HEALTHY/FAILING/DISABLED |

### batch_registry (key fields)

| Field | Type | Example | Purpose |
|-------|------|---------|---------|
| batch_id | STRING | batch_2026..._nvd_prod_01 | Unique batch (PK) |
| batch_status | STRING | SILVER_COMPLETE | Lifecycle state |
| dq_status | STRING | PASSED | Quality gate |
| dq_details_json | STRING | {"dq_has_cvss":{...}} | DQ check results |
| load_type | STRING | INCREMENTAL | What this run did |
| records_expected | BIGINT | 200 | From API totalResults |
| records_processed | BIGINT | 200 | Actually written |
| bronze_path | STRING | /data-lake/dev/bronze/... | Where raw landed |
| failure_category | STRING | NULL | Error classification |

### field_mapping (key fields)

| Field | Type | Example | Purpose |
|-------|------|---------|---------|
| source_system | STRING | NVD | Which source |
| source_json_path | STRING | $.cve.id | Where in payload_json |
| target_field | STRING | cve_id | Canonical column |
| transformation_type | STRING | DIRECT/CAST/UPPER/LOOKUP | How to transform |
| transformation_config | STRING | {"cast_to":"DOUBLE"} | Transform params |

## 8.2 Transformation Types Reference

| Type | Use | Config Example |
|------|-----|----------------|
| DIRECT | Use value as-is | (none) |
| CAST | Type conversion | {"cast_to":"DOUBLE"} |
| UPPER / LOWER | Case standardize | (none) |
| TRIM | Strip whitespace | (none) |
| TO_JSON | Keep nested as JSON string | (none) |
| LOOKUP | Map values | {"map":{"0":"INFO","4":"CRITICAL"}} |
| EXPRESSION | Spark SQL expr | {"expr":"CASE WHEN ..."} |

## 8.3 Common Errors & Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| Cannot cast string to double | get_json_object returns STRING | Explicit .cast(field.dataType) using target schema |
| Cannot find column X in INSERT | DataFrame missing target columns | Add missing cols as lit(None) before MERGE |
| 'Part 0' in column list | DESCRIBE returns partition metadata | Use spark.table().schema.fields, not DESCRIBE |
| Cannot write null to non-null | NULL in NOT NULL column | Filter NOT NULL cols before write |
| Indentation error in CDE | Leading space on def/if | Ensure column-0 alignment |
| NameError: args not defined | logger before parse_args | Move argparse before usage |

## 8.4 Schema-Alignment Fix Pattern (Use in EVERY Silver Job)

```python
# Before any MERGE/write to Iceberg:
target_schema = spark.table(target_table).schema           # NOT describe
target_cols = [f.name for f in target_schema.fields]

# 1. Add missing columns as NULL
for c in target_cols:
    if c not in df.columns:
        df = df.withColumn(c, lit(None).cast(StringType()))

# 2. Reorder to match target
df = df.select(*[col(c) for c in target_cols])

# 3. Cast all to target types
for field in target_schema.fields:
    df = df.withColumn(field.name, col(field.name).cast(field.dataType))

# 4. Filter NOT NULL columns
not_null = [f.name for f in target_schema.fields if not f.nullable]
if not_null:
    df = df.filter(" AND ".join([f"{c} IS NOT NULL" for c in not_null]))
```

## 8.5 Daily Operational Queries

```sql
-- Pipeline status across all adapters today
SELECT adapter_instance_id, batch_status, dq_status, records_processed
FROM t01_ueh_dev_ctl.t01_ueh_ctl_batch_registry
WHERE ingestion_date = CURRENT_DATE() ORDER BY created_at DESC;

-- Adapters that are failing
SELECT adapter_instance_id, state_status, consecutive_failures, last_failure_reason
FROM t01_ueh_dev_ctl.t01_ueh_ctl_adapter_state
WHERE state_status IN ('FAILING','DEGRADED');

-- Pending failures needing attention (Sprint 3+)
SELECT failure_category, count(*) FROM t01_ueh_dev_ctl.t01_ueh_ctl_failed_ingestions
WHERE resolution_status='PENDING' GROUP BY failure_category;

-- Watermark health (is any adapter stuck?)
SELECT adapter_instance_id, watermark_state_json, last_successful_run
FROM t01_ueh_dev_ctl.t01_ueh_ctl_adapter_state ORDER BY last_successful_run;
```

## 8.6 Sprint Summary Table

| Sprint | Weeks | Goal | Adapters Added | Control Tables Added |
|--------|-------|------|----------------|---------------------|
| 1 | 1-2 | NVD end-to-end | NVD | (4 core) + dq_status |
| 2 | 3-4 | Tenable Bronze+Staging | Tenable | — |
| 3 | 5-6 | ADDM + Tenable canonical | ADDM | failed_ingestions |
| 4 | 7-8 | EPSS/CISA + asset canonical | EPSS, CISA KEV | pipeline_dependency |
| 5 | 9-10 | Gold + PostgreSQL | — | config_history, platform_metrics |
| 6 | 11-12 | Hardening + production | — | sla_definitions, replay_queue |

## 8.7 Definition of Done (Every Task)

- [ ] Code committed to Git (feature branch)
- [ ] Validation query run + result confirmed
- [ ] Idempotency tested (re-run safe)
- [ ] batch_registry status transitions correctly
- [ ] dq_status populated
- [ ] Peer reviewed via PR
- [ ] Documented in run log

---

*End of Implementation Guide*
