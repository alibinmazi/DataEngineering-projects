# UEH Implementation Guide

## Step-by-Step Deployment for NVD Bronze Pipeline

This guide walks through deploying the first UEH adapter (NVD) end-to-end in a dev environment.

---

## Prerequisites

| Requirement | Details |
|-------------|---------|
| HDFS access | Service account with write to `/data-lake/dev/` and `/warehouse/dev/` |
| Spark/PySpark | Version 3.x with Iceberg support |
| Hive Metastore | For Iceberg catalog |
| Apache NiFi | Instance with access to NVD API and HDFS |
| Apache Airflow | Instance for DAG deployment |
| NVD API Key | Register at https://nvd.nist.gov/developers/request-an-api-key |
| Vault/Secrets | Location to store NVD API key securely |

---

## Deployment Steps

### Step 1: HDFS Folder Structure

```bash
cd /path/to/ueh-platform
chmod +x scripts/hdfs_init.sh
./scripts/hdfs_init.sh dev
```

**Verify:**
```bash
hdfs dfs -ls /data-lake/dev/bronze/vulnerability_intel/nvd/
# Should show: metadata/, raw/, dead_letter/
```

---

### Step 2: Create Iceberg Databases

```bash
spark-sql -f ddl/01_databases.sql
```

**Verify:**
```sql
SHOW DATABASES LIKE 'ueh_dev_*';
-- Should show: ueh_dev_control, ueh_dev_bronze, ueh_dev_silver, ueh_dev_gold
```

---

### Step 3: Create Control Tables

```bash
spark-sql -f ddl/02_control_tables.sql
```

**Verify:**
```sql
USE ueh_dev_control;
SHOW TABLES;
-- Should show: t01_ueh_ctl_adapter_config, t01_ueh_ctl_adapter_state,
--              t01_ueh_ctl_batch_registry, t01_ueh_ctl_ingestion_log,
--              t01_ueh_ctl_failed_ingestions, t01_ueh_ctl_replay_queue
```

---

### Step 4: Create Bronze Table

```bash
spark-sql -f ddl/03_bronze_nvd.sql
```

**Verify:**
```sql
USE ueh_dev_bronze;
DESCRIBE EXTENDED t01_ueh_brz_nvd_vulnerabilities;
```

---

### Step 5: Seed NVD Adapter Configuration

```bash
spark-sql -f seed/01_nvd_adapter_seed.sql
```

**Verify:**
```sql
SELECT adapter_instance_id, is_active, state_status
FROM ueh_dev_control.t01_ueh_ctl_adapter_config ac
JOIN ueh_dev_control.t01_ueh_ctl_adapter_state ast
  ON ac.adapter_instance_id = ast.adapter_instance_id
WHERE ac.adapter_name = 'nvd';
-- Should show: nvd_public_01 | true | NEW
```

---

### Step 6: Store NVD API Key in Vault

```bash
# Example using HashiCorp Vault
vault kv put secrets/ueh/dev/nvd_api_key value="your-nvd-api-key-here"

# Or using environment-specific secret manager
# Ensure the path matches auth_secret_ref in adapter_config:
# vault://secrets/ueh/dev/nvd_api_key
```

---

### Step 7: Deploy NiFi Flow

1. Open NiFi UI
2. Create Process Group: `UEH_Bronze_NVD_Ingestion`
3. Follow specification in `nifi/nvd_ingestion_flow.md`
4. Configure connection services:
   - `UEH_Hive_DBCP`: JDBC connection to HiveServer2
   - HDFS configuration: core-site.xml + hdfs-site.xml
5. Configure NiFi Variable Registry:
   ```
   ueh.environment = dev
   ueh.hdfs.base_path = /data-lake/dev/bronze
   ueh.control.database = ueh_dev_control
   ueh.nifi.adapter_instance_id = nvd_public_01
   ```
6. Test with a SMALL date range first (modify watermark to yesterday)
7. Verify chunk files written to HDFS
8. Verify batch_registry updated to RAW_COMPLETE

---

### Step 8: Deploy Spark Jobs

```bash
# Copy Spark jobs to HDFS (accessible by CDE/Spark cluster)
hdfs dfs -mkdir -p /apps/ueh/spark/bronze/
hdfs dfs -put spark/bronze/base_bronze_loader.py /apps/ueh/spark/bronze/
hdfs dfs -put spark/bronze/bronze_nvd_loader.py /apps/ueh/spark/bronze/

# Test manually:
spark-submit \
    --conf ueh.environment=dev \
    --py-files /apps/ueh/spark/bronze/base_bronze_loader.py \
    /apps/ueh/spark/bronze/bronze_nvd_loader.py \
    <batch_id_from_step_7>
```

**Verify:**
```sql
SELECT COUNT(*), batch_id
FROM ueh_dev_bronze.t01_ueh_brz_nvd_vulnerabilities
GROUP BY batch_id;

SELECT status FROM ueh_dev_control.t01_ueh_ctl_batch_registry
WHERE batch_id = '<batch_id>';
-- Should show: BRONZE_COMPLETE
```

---

### Step 9: Deploy Airflow DAGs

```bash
# Copy DAGs to Airflow DAGs folder
cp airflow/dags/ueh_ingest_nvd.py $AIRFLOW_HOME/dags/
cp airflow/dags/ueh_bronze_load_nvd.py $AIRFLOW_HOME/dags/

# Set Airflow variables
airflow variables set ueh_environment dev
airflow variables set ueh_nifi_base_url http://your-nifi-host:8080
airflow variables set ueh_nifi_pg_nvd <process-group-id-from-nifi>
airflow variables set ueh_nifi_username <nifi-user>
airflow variables set ueh_nifi_password <nifi-pass>
```

**Verify DAGs appear:**
```bash
airflow dags list | grep ueh
# Should show: ueh_ingest_nvd, ueh_bronze_load_nvd
```

---

### Step 10: End-to-End Validation

```bash
# Trigger DAG 1 manually
airflow dags trigger ueh_ingest_nvd

# Monitor in Airflow UI or CLI
airflow tasks states-for-dag-run ueh_ingest_nvd <execution_date>

# Once RAW_COMPLETE, DAG 2 will pick it up within 30 minutes
# Or trigger manually:
airflow dags trigger ueh_bronze_load_nvd

# Run validation queries
spark-sql -f validation/bronze_nvd_checks.sql
```

---

## Troubleshooting

### NiFi Flow Not Triggering
- Check NiFi process group is ENABLED (not just created)
- Verify GenerateFlowFile scheduler is correct
- Check NiFi logs: `tail -f /var/log/nifi/nifi-app.log`

### API Key Rejected (HTTP 403)
- Verify API key in vault matches NVD registration
- NVD rate limit without key: 5 req/30s → with key: 50 req/30s
- Wait and retry (NVD occasionally has temporary blocks)

### Spark Job Fails: "Batch not found or not RAW_COMPLETE"
- Check batch_registry: `SELECT status FROM batch_registry WHERE batch_id = '...'`
- If status is FAILED, check failure_reason
- Ensure NiFi completed successfully (check manifest.json on HDFS)

### Zero Records After Bronze Load
- Check NVD API returned data: verify watermark range has modified CVEs
- Inspect chunk files on HDFS: `hdfs dfs -cat /path/to/chunk_001.json | head`
- Check if "vulnerabilities" array is empty in response

### DAG 2 Not Picking Up Batches
- Verify DAG 2 schedule is running (every 30 min)
- Check ShortCircuitOperator logs (is it finding RAW_COMPLETE?)
- Verify `adapter_instance_id` matches between DAGs and control table

### Circuit Breaker Open
- Check adapter_state: `SELECT * FROM adapter_state WHERE adapter_instance_id = 'nvd_public_01'`
- Review last_failure_reason
- Fix root cause, then manually reset:
  ```sql
  UPDATE t01_ueh_ctl_adapter_state
  SET circuit_breaker_open = FALSE,
      consecutive_failures = 0,
      state_status = 'HEALTHY'
  WHERE adapter_instance_id = 'nvd_public_01';
  ```

---

## Adding the Next Adapter (EPSS)

Once NVD is proven, add EPSS using the same framework:

1. **HDFS folders** already created by `hdfs_init.sh`
2. **Create Bronze table:** `ddl/04_bronze_epss.sql` (copy NVD pattern)
3. **Seed config:**
   ```sql
   INSERT INTO t01_ueh_ctl_adapter_config VALUES ('epss_public_01', 'epss', 'vulnerability_intel', ...);
   INSERT INTO t01_ueh_ctl_adapter_state VALUES ('epss_public_01', ...);
   ```
4. **Create Spark loader:** `spark/bronze/bronze_epss_loader.py` (subclass BaseBronzeLoader)
5. **Create NiFi flow:** New process group or parameterize existing
6. **Create DAGs:** Copy NVD DAGs, change adapter_instance_id
7. **Test and validate**

**Estimated time for second adapter: 2-3 days** (framework already exists)

---

## Configuration Reference

### Environment Config (`config/dev.yaml`)

```yaml
environment: dev
hdfs:
  base_path: /data-lake/dev/bronze
  warehouse_path: /warehouse/dev
databases:
  control: ueh_dev_control
  bronze: ueh_dev_bronze
  silver: ueh_dev_silver
  gold: ueh_dev_gold
nifi:
  base_url: http://nifi-dev:8080
  process_groups:
    nvd: <pg-id>
spark:
  jobs_path: /apps/ueh/spark
  conf:
    driver_memory: 4g
    executor_memory: 8g
    executor_instances: 2
```

---

## Pipeline Flow Diagram

```
┌─────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│ Airflow  │────▶│   NiFi   │────▶│   HDFS   │────▶│  Spark   │
│  DAG 1   │     │ NVD Flow │     │  Bronze  │     │  Bronze  │
│(Trigger) │     │(Ingest)  │     │  (Raw)   │     │ (Load)   │
└─────────┘     └──────────┘     └──────────┘     └──────────┘
     │                │                                   │
     │                ▼                                   ▼
     │         ┌──────────┐                        ┌──────────┐
     │         │ Control  │◀───────────────────────│ Control  │
     │         │  Table   │                        │  Table   │
     │         │(RAW_COMP)│                        │(BRZ_COMP)│
     │         └──────────┘                        └──────────┘
     │                ▲                                   ▲
     │                │                                   │
     │         ┌──────────┐                        ┌──────────┐
     └────────▶│ Airflow  │                        │ Airflow  │
               │  DAG 1   │                        │  DAG 2   │
               │(Monitor) │                        │ (Poll)   │
               └──────────┘                        └──────────┘
```
