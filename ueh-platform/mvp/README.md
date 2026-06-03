# UEH MVP: Minimum Viable Pipeline

## The Simplest Path to a Working NVD Bronze Pipeline

This folder contains the **absolute minimum** needed to run the first UEH adapter end-to-end. No extra complexity. Build this first, then grow.

---

## Architecture (MVP)

```
┌─────────────┐         ┌─────────────┐         ┌─────────────┐
│  STEP 1     │         │  STEP 2     │         │  STEP 3     │
│             │         │             │         │             │
│  NiFi       │────────▶│  HDFS       │────────▶│  Spark      │
│  (API→HDFS) │         │  (Raw JSON) │         │  (→Iceberg) │
│             │         │             │         │             │
└──────┬──────┘         └─────────────┘         └──────┬──────┘
       │                                                │
       │         ┌─────────────────────┐                │
       └────────▶│  CONTROL TABLES     │◀───────────────┘
                 │                     │
                 │  adapter_config     │
                 │  adapter_state      │
                 │  batch_registry     │
                 └─────────────────────┘
                          ▲        ▲
                          │        │
                 ┌────────┘        └────────┐
                 │                          │
          ┌──────┴──────┐           ┌──────┴──────┐
          │  DAG 1      │           │  DAG 2      │
          │  (Trigger + │           │  (Poll +    │
          │   Monitor)  │           │   Load)     │
          └─────────────┘           └─────────────┘
```

---

## Files in This Folder

```
mvp/
├── ddl/
│   ├── 01_minimal_control_tables.sql   ← 3 tables (config, state, batch)
│   └── 02_bronze_nvd_table.sql         ← Bronze Iceberg table
│
├── seed/
│   └── 01_nvd_seed.sql                 ← Register NVD adapter
│
├── nifi/
│   └── NVD_Ingestion_Step_by_Step.md   ← 9-processor build guide
│
├── airflow/dags/
│   ├── dag1_nvd_raw_ingestion.py       ← Trigger NiFi + wait for RAW_COMPLETE
│   └── dag2_nvd_bronze_load.py         ← Poll + Spark + verify BRONZE_COMPLETE
│
├── spark/
│   └── bronze_nvd_loader.py            ← Single self-contained loader
│
└── README.md                           ← This file
```

---

## Implementation Order (Do This Exactly)

### Phase A: Setup (30 minutes)

```bash
# 1. Create control tables
spark-sql -f mvp/ddl/01_minimal_control_tables.sql

# 2. Create Bronze table
spark-sql -f mvp/ddl/02_bronze_nvd_table.sql

# 3. Seed NVD adapter
spark-sql -f mvp/seed/01_nvd_seed.sql

# 4. Create HDFS folders (auto-created by NiFi, but you can pre-create)
hdfs dfs -mkdir -p /data-lake/dev/bronze/vulnerability_intel/nvd/raw
```

### Phase B: NiFi Flow (2-4 hours)

Follow `nifi/NVD_Ingestion_Step_by_Step.md` — build 9 processors.

**Quick test:** Set watermark to yesterday, run once, check HDFS for chunk files.

### Phase C: DAG 1 Deploy (30 minutes)

```bash
# Copy DAG
cp mvp/airflow/dags/dag1_nvd_raw_ingestion.py $AIRFLOW_HOME/dags/

# Set Airflow variables
airflow variables set ueh_environment dev
airflow variables set ueh_nifi_base_url http://your-nifi:8080
airflow variables set ueh_nifi_pg_nvd your-process-group-id

# Test
airflow dags trigger ueh_dag1_nvd_raw_ingestion
```

### Phase D: DAG 2 + Spark Deploy (1 hour)

```bash
# Upload Spark job to HDFS
hdfs dfs -mkdir -p /apps/ueh/spark/
hdfs dfs -put mvp/spark/bronze_nvd_loader.py /apps/ueh/spark/

# Copy DAG
cp mvp/airflow/dags/dag2_nvd_bronze_load.py $AIRFLOW_HOME/dags/

# Test manually first:
spark-submit --conf ueh.environment=dev \
    mvp/spark/bronze_nvd_loader.py <batch_id_from_phase_B>

# Then let DAG 2 run automatically (every 30 min)
```

### Phase E: Validate (15 minutes)

```sql
-- Check batch went through entire lifecycle
SELECT batch_id, status, records_ingested
FROM ueh_dev_control.t01_ueh_ctl_batch_registry
WHERE adapter_instance_id = 'nvd_public_01';
-- Expected: status = BRONZE_COMPLETE

-- Check Bronze table has data
SELECT COUNT(*), ingestion_date
FROM ueh_dev_bronze.t01_ueh_brz_nvd_vulnerabilities
GROUP BY ingestion_date;

-- Spot check a record
SELECT source_record_id, ueh_schema_version, dq_payload_size_bytes
FROM ueh_dev_bronze.t01_ueh_brz_nvd_vulnerabilities
LIMIT 5;
```

---

## How the Two DAGs Work Together

```
Timeline:
─────────────────────────────────────────────────────────────────
03:00  DAG 1 fires → triggers NiFi
03:01  NiFi starts calling NVD API
03:15  NiFi finishes → writes RAW_COMPLETE to batch_registry
03:15  DAG 1 detects RAW_COMPLETE → task completes ✓
─────────────────────────────────────────────────────────────────
03:30  DAG 2 runs (scheduled every 30 min)
03:30  DAG 2 finds RAW_COMPLETE batch → submits Spark job
03:35  Spark reads chunks from HDFS → writes to Iceberg
03:35  Spark updates batch_registry → BRONZE_COMPLETE
03:36  DAG 2 verifies → task completes ✓
─────────────────────────────────────────────────────────────────
```

**Coupling mechanism:** `batch_registry.status` field. That's it. No file sensors, no DAG-to-DAG dependency.

---

## What to Do Next (After MVP Works)

| Step | What | Files to Add |
|------|------|--------------|
| 1 | Add dead-letter handling in NiFi | NiFi failure routing |
| 2 | Add EPSS adapter (second source) | New seed SQL + new NiFi flow |
| 3 | Add Silver layer (parse payload_json) | New Spark job + new DAG |
| 4 | Add observability (platform_metrics) | New control table + metrics DAG |
| 5 | Add more control tables (config_history, SLA) | From v4 architecture doc |

---

## Common Issues

| Problem | Solution |
|---------|----------|
| NiFi can't reach NVD API | Check firewall, DNS, proxy settings |
| NiFi writes but no RAW_COMPLETE | Check PutSQL processor — is Hive JDBC working? |
| DAG 2 never finds pending batches | Verify `adapter_instance_id` matches between NiFi SQL and DAG |
| Spark fails with "table not found" | Ensure `ueh_dev_bronze` database exists |
| Zero records after Bronze load | Check if NVD response has empty `vulnerabilities` array |
| Watermark not advancing | Check NiFi's final PutSQL (adapter_state UPDATE) |
