# UEH NiFi Flow Specification: NVD Ingestion

## Process Group: `UEH_Bronze_NVD_Ingestion`

### Overview

This NiFi process group handles the complete ingestion of NVD CVE data from the
public API into HDFS Bronze raw storage. It is triggered by Airflow (DAG 1) and
updates control tables upon completion.

---

## Flow Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Process Group: UEH_Bronze_NVD_Ingestion                                    │
│                                                                             │
│  Input Port ──→ [Get Config] ──→ [Get State] ──→ [Generate Runtime]         │
│       │                                                                     │
│       ▼                                                                     │
│  [Pagination Loop] ──→ [Write Chunks] ──→ [Write Manifest]                  │
│       │                                                                     │
│       ▼                                                                     │
│  [Update Control Tables] ──→ Output Port                                    │
│       │                                                                     │
│       └──→ (failure) ──→ [Dead Letter] ──→ [Log Failure]                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Environment Variables (NiFi Variable Registry)

| Variable | Dev Value | Prod Value | Description |
|----------|-----------|------------|-------------|
| `ueh.environment` | `dev` | `prod` | Active environment |
| `ueh.hdfs.base_path` | `/data-lake/dev/bronze` | `/data-lake/prod/bronze` | HDFS base path |
| `ueh.control.database` | `ueh_dev_control` | `ueh_prod_control` | Control table database |
| `ueh.hive.jdbc.url` | `jdbc:hive2://hiveserver:10000` | `jdbc:hive2://...` | Hive JDBC connection |
| `ueh.nifi.adapter_instance_id` | `nvd_public_01` | `nvd_public_01` | Target adapter instance |

---

## Processor Configuration (Step-by-Step)

---

### Processor 1: `GenerateFlowFile` (Trigger)

**Purpose:** Entry point. Triggered by Airflow via NiFi REST API or on schedule.

| Property | Value |
|----------|-------|
| Name | `Trigger_NVD_Ingestion` |
| Scheduling Strategy | `Timer driven` |
| Run Schedule | `0 sec` (triggered externally) / `0 3 * * * ?` (self-scheduled) |
| Custom Text | `{"trigger": "scheduled", "timestamp": "${now()}"}` |

**Relationships:**
- success → `Query_Adapter_Config`

---

### Processor 2: `ExecuteSQL` (Get Adapter Config)

**Purpose:** Read adapter configuration from control table.

| Property | Value |
|----------|-------|
| Name | `Query_Adapter_Config` |
| Database Connection Pooling Service | `UEH_Hive_DBCP` |
| SQL select query | See below |

**SQL:**
```sql
SELECT 
    adapter_instance_id,
    adapter_name,
    adapter_type,
    base_url,
    auth_method,
    auth_secret_ref,
    ingestion_mode,
    default_load_type,
    chunk_size,
    page_size,
    rate_limit_rps,
    request_timeout_sec,
    max_retries,
    path_template,
    sla_minutes
FROM ${ueh.control.database}.t01_ueh_ctl_adapter_config
WHERE adapter_instance_id = '${ueh.nifi.adapter_instance_id}'
  AND is_active = TRUE
```

**Relationships:**
- success → `Convert_Config_To_JSON`
- failure → `Log_Config_Failure`

---

### Processor 3: `ConvertAvroToJSON` → `EvaluateJsonPath`

**Purpose:** Extract config values into FlowFile attributes.

| Property | Value |
|----------|-------|
| Name | `Extract_Config_Attributes` |
| Destination | `flowfile-attribute` |

**JsonPath Expressions:**

| Attribute | JsonPath |
|-----------|----------|
| `adapter_instance_id` | `$.adapter_instance_id` |
| `adapter_name` | `$.adapter_name` |
| `adapter_type` | `$.adapter_type` |
| `base_url` | `$.base_url` |
| `auth_secret_ref` | `$.auth_secret_ref` |
| `ingestion_mode` | `$.ingestion_mode` |
| `default_load_type` | `$.default_load_type` |
| `chunk_size` | `$.chunk_size` |
| `page_size` | `$.page_size` |
| `rate_limit_rps` | `$.rate_limit_rps` |
| `request_timeout_sec` | `$.request_timeout_sec` |
| `max_retries` | `$.max_retries` |
| `path_template` | `$.path_template` |

**Relationships:**
- matched → `Query_Adapter_State`

---

### Processor 4: `ExecuteSQL` (Get Adapter State / Watermark)

**Purpose:** Read current watermark to determine where to resume.

| Property | Value |
|----------|-------|
| Name | `Query_Adapter_State` |
| SQL select query | See below |

**SQL:**
```sql
SELECT 
    watermark_value,
    watermark_type,
    watermark_field,
    state_status,
    circuit_breaker_open
FROM ${ueh.control.database}.t01_ueh_ctl_adapter_state
WHERE adapter_instance_id = '${adapter_instance_id}'
```

**Post-processing:** Extract `watermark_value` into FlowFile attribute.

**Circuit Breaker Check:** If `circuit_breaker_open = TRUE`, route to failure and log.

**Relationships:**
- success → `Extract_State_Attributes` → `Generate_Runtime_Attributes`
- failure → `Log_State_Failure`

---

### Processor 5: `UpdateAttribute` (Generate Runtime Context)

**Purpose:** Generate batch_id, ingestion_date, compute API parameters.

| Property | Value |
|----------|-------|
| Name | `Generate_Runtime_Attributes` |

**Attribute Expressions:**

| Attribute | Expression | Example Value |
|-----------|------------|---------------|
| `ingestion_date` | `${now():format('yyyy-MM-dd')}` | `2026-05-20` |
| `batch_id` | `batch_${now():format('yyyyMMddHHmmss')}_${adapter_instance_id}` | `batch_20260520030000_nvd_public_01` |
| `start_index` | `0` | `0` |
| `chunk_index` | `1` | `1` |
| `total_records_fetched` | `0` | `0` |
| `last_mod_start_date` | `${watermark_value}` | `2024-01-01T00:00:00.000` |
| `last_mod_end_date` | `${now():format("yyyy-MM-dd'T'HH:mm:ss.SSS")}` | `2026-05-20T03:00:00.000` |
| `hdfs_output_path` | `${ueh.hdfs.base_path}/${path_template}` | `/data-lake/dev/bronze/vulnerability_intel/nvd/raw/ingestion_date=2026-05-20/batch_id=batch_20260520030000_nvd_public_01` |
| `has_more_pages` | `true` | `true` |

**Relationships:**
- success → `Call_NVD_API`

---

### Processor 6: `InvokeHTTP` (NVD API Call — Inside Pagination Loop)

**Purpose:** Call NVD API with pagination parameters.

| Property | Value |
|----------|-------|
| Name | `Call_NVD_API` |
| HTTP Method | `GET` |
| Remote URL | `${base_url}?lastModStartDate=${last_mod_start_date}&lastModEndDate=${last_mod_end_date}&startIndex=${start_index}&resultsPerPage=${page_size}` |
| Read Timeout | `${request_timeout_sec} sec` |
| Connection Timeout | `30 sec` |

**Request Headers:**

| Header | Value |
|--------|-------|
| `apiKey` | `${auth_secret_ref:resolved}` |
| `Accept` | `application/json` |
| `User-Agent` | `UEH-Platform/1.0` |

**NOTE:** `auth_secret_ref:resolved` means the API key is fetched from vault.
In practice, use a NiFi Parameter Context or HashiCorp Vault lookup processor.

**Relationships:**
- Response → `Parse_NVD_Response`
- Retry → `Wait_Rate_Limit` → `Call_NVD_API` (loop back)
- No Retry → `Handle_API_Failure`
- Failure → `Handle_API_Failure`

---

### Processor 7: `EvaluateJsonPath` (Parse Response Metadata)

**Purpose:** Extract pagination metadata from NVD response to control the loop.

| Property | Value |
|----------|-------|
| Name | `Parse_NVD_Response` |
| Destination | `flowfile-attribute` |

**JsonPath Expressions:**

| Attribute | JsonPath | Description |
|-----------|----------|-------------|
| `total_results` | `$.totalResults` | Total matching CVEs |
| `results_per_page` | `$.resultsPerPage` | Results in this page |
| `start_index_returned` | `$.startIndex` | Current offset |

**Relationships:**
- matched → `Calculate_Pagination`

---

### Processor 8: `UpdateAttribute` (Calculate Pagination State)

**Purpose:** Determine if more pages exist and increment counters.

| Property | Value |
|----------|-------|
| Name | `Calculate_Pagination` |

**Attribute Expressions:**

| Attribute | Expression |
|-----------|------------|
| `next_start_index` | `${start_index:plus(${results_per_page})}` |
| `has_more_pages` | `${next_start_index:lt(${total_results})}` |
| `chunk_filename` | `chunk_${chunk_index:padLeft(3, '0')}.json` |
| `records_this_page` | `${results_per_page}` |
| `total_records_fetched` | `${total_records_fetched:plus(${results_per_page})}` |

**Relationships:**
- success → `Write_Chunk_To_HDFS`

---

### Processor 9: `PutHDFS` (Write Chunk File)

**Purpose:** Write the raw API response as a chunk file to HDFS.

| Property | Value |
|----------|-------|
| Name | `Write_Chunk_To_HDFS` |
| Hadoop Configuration Resources | `/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml` |
| Directory | `${hdfs_output_path}` |
| Conflict Resolution Strategy | `replace` |
| Block Size | `128 MB` |
| Replication | `3` |
| Permissions umask | `077` |
| Owner | (leave default — inherited from service account) |

**Filename:** `${chunk_filename}`

**Auto-creates directory:** Yes (NiFi creates parent dirs automatically)

**Relationships:**
- success → `Route_Pagination`
- failure → `Handle_Write_Failure`

---

### Processor 10: `RouteOnAttribute` (Pagination Decision)

**Purpose:** Decide whether to fetch next page or finalize batch.

| Property | Value |
|----------|-------|
| Name | `Route_Pagination` |
| Routing Strategy | `Route to Property name` |

**Route Conditions:**

| Route Name | Expression | Destination |
|------------|------------|-------------|
| `more_pages` | `${has_more_pages:equals('true')}` | → `Increment_Page` → `Call_NVD_API` (loop) |
| `all_pages_done` | `${has_more_pages:equals('false')}` | → `Generate_Manifest` |

---

### Processor 11: `UpdateAttribute` (Increment for Next Page)

**Purpose:** Advance pagination counters before looping back.

| Attribute | Expression |
|-----------|------------|
| `start_index` | `${next_start_index}` |
| `chunk_index` | `${chunk_index:plus(1)}` |

**Relationships:**
- success → `Wait_Rate_Limit`

---

### Processor 12: `ControlRate` (Rate Limiting)

**Purpose:** Respect NVD API rate limits (5 req/30s with API key).

| Property | Value |
|----------|-------|
| Name | `Wait_Rate_Limit` |
| Rate Control Criteria | `flowfile count` |
| Maximum Rate | `${rate_limit_rps}` |
| Time Duration | `1 second` |

**Relationships:**
- success → `Call_NVD_API` (loops back to Processor 6)

---

### Processor 13: `ReplaceText` + `PutHDFS` (Write Manifest)

**Purpose:** Generate and write manifest.json after all pages are fetched.

| Property | Value |
|----------|-------|
| Name | `Generate_Manifest` |
| Replacement Strategy | `Always Replace` |

**Manifest Template:**
```json
{
  "batch_id": "${batch_id}",
  "adapter_instance_id": "${adapter_instance_id}",
  "adapter_name": "${adapter_name}",
  "adapter_type": "${adapter_type}",
  "ingestion_date": "${ingestion_date}",
  "load_type": "${default_load_type}",
  "source_api": "${base_url}",
  "total_records": ${total_records_fetched},
  "total_chunks": ${chunk_index},
  "total_results_from_api": ${total_results},
  "watermark_start": "${last_mod_start_date}",
  "watermark_end": "${last_mod_end_date}",
  "started_at": "${batch_started_at}",
  "completed_at": "${now():format("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")}",
  "hdfs_path": "${hdfs_output_path}",
  "nifi_flow_version": "1.0.0"
}
```

**Write to:** `${hdfs_output_path}/manifest.json`

**Relationships:**
- success → `Generate_Checkpoint`

---

### Processor 14: `ReplaceText` + `PutHDFS` (Write Checkpoint)

**Purpose:** Write checkpoint.json with new watermark for adapter_state update.

**Checkpoint Template:**
```json
{
  "adapter_instance_id": "${adapter_instance_id}",
  "batch_id": "${batch_id}",
  "new_watermark_value": "${last_mod_end_date}",
  "watermark_type": "iso_datetime",
  "records_fetched": ${total_records_fetched},
  "status": "RAW_COMPLETE"
}
```

**Write to:** `${hdfs_output_path}/checkpoint.json`

**Relationships:**
- success → `Insert_Batch_Registry`

---

### Processor 15: `PutSQL` (Insert Batch Registry)

**Purpose:** Register completed batch in control table.

**SQL:**
```sql
INSERT INTO ${ueh.control.database}.t01_ueh_ctl_batch_registry VALUES (
    '${batch_id}',
    '${adapter_instance_id}',
    '${adapter_name}',
    NULL,
    '${ingestion_date}',
    '${default_load_type}',
    'RAW_COMPLETE',
    ${total_records_fetched},
    ${chunk_index},
    NULL,
    '${hdfs_output_path}',
    NULL,
    '${last_mod_start_date}',
    '${last_mod_end_date}',
    TIMESTAMP '${batch_started_at}',
    CURRENT_TIMESTAMP(),
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    0,
    NULL,
    'SCHEDULE',
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
)
```

**Relationships:**
- success → `Update_Adapter_State`

---

### Processor 16: `PutSQL` (Update Adapter State)

**Purpose:** Advance watermark and update health status.

**SQL:**
```sql
UPDATE ${ueh.control.database}.t01_ueh_ctl_adapter_state
SET watermark_value = '${last_mod_end_date}',
    last_successful_run = CURRENT_TIMESTAMP(),
    last_attempted_run = CURRENT_TIMESTAMP(),
    records_last_pulled = ${total_records_fetched},
    consecutive_failures = 0,
    state_status = 'HEALTHY',
    last_updated = CURRENT_TIMESTAMP()
WHERE adapter_instance_id = '${adapter_instance_id}'
```

**Relationships:**
- success → `Output_Port` (flow complete)

---

## Failure Handling

### Dead Letter Path

When any processor fails:

1. **Preserve payload** → Write to dead letter folder:
   ```
   ${ueh.hdfs.base_path}/vulnerability_intel/nvd/dead_letter/ingestion_date=${ingestion_date}/batch_id=${batch_id}/
   ```

2. **Update batch_registry** → Set `status = 'FAILED'`, populate `failure_reason`

3. **Update adapter_state** → Increment `consecutive_failures`, set `state_status`:
   - 1-2 failures → `DEGRADED`
   - 3+ failures → `FAILING`
   - 5+ failures → `circuit_breaker_open = TRUE`

4. **Insert failed_ingestions record** → For operational tracking and replay

### Failure SQL (adapter_state):
```sql
UPDATE ${ueh.control.database}.t01_ueh_ctl_adapter_state
SET last_attempted_run = CURRENT_TIMESTAMP(),
    consecutive_failures = consecutive_failures + 1,
    state_status = CASE 
        WHEN consecutive_failures + 1 >= 5 THEN 'FAILING'
        WHEN consecutive_failures + 1 >= 2 THEN 'DEGRADED'
        ELSE state_status
    END,
    circuit_breaker_open = CASE 
        WHEN consecutive_failures + 1 >= 5 THEN TRUE
        ELSE FALSE
    END,
    circuit_breaker_opened_at = CASE 
        WHEN consecutive_failures + 1 >= 5 THEN CURRENT_TIMESTAMP()
        ELSE circuit_breaker_opened_at
    END,
    last_failure_reason = '${error_message}',
    last_updated = CURRENT_TIMESTAMP()
WHERE adapter_instance_id = '${adapter_instance_id}'
```

---

## Connection Services Required

| Service | Type | Configuration |
|---------|------|---------------|
| `UEH_Hive_DBCP` | `DBCPConnectionPool` | JDBC URL to HiveServer2 for control table queries |
| `UEH_HDFS_Config` | `HdfsResources` | core-site.xml + hdfs-site.xml paths |
| `UEH_Vault_Lookup` | `HashiCorpVaultClientService` | For resolving `auth_secret_ref` |

---

## NiFi REST API Trigger (Called by Airflow)

Airflow DAG 1 triggers this flow via NiFi REST API:

```bash
# Start the process group
curl -X PUT \
  "${NIFI_URL}/nifi-api/flow/process-groups/${PG_ID}" \
  -H "Content-Type: application/json" \
  -d '{"id": "'${PG_ID}'", "state": "RUNNING"}'
```

Alternatively, use NiFi's **Input Port** with `PostHTTP` from Airflow to send trigger message.

---

## FlowFile Attributes Summary (Complete List)

| Attribute | Source | Example |
|-----------|--------|---------|
| `adapter_instance_id` | Control table | `nvd_public_01` |
| `adapter_name` | Control table | `nvd` |
| `adapter_type` | Control table | `vulnerability_intel` |
| `base_url` | Control table | `https://services.nvd.nist.gov/rest/json/cves/2.0` |
| `auth_secret_ref` | Control table | `vault://secrets/ueh/dev/nvd_api_key` |
| `ingestion_mode` | Control table | `INCREMENTAL` |
| `default_load_type` | Control table | `INCREMENTAL` |
| `page_size` | Control table | `2000` |
| `chunk_size` | Control table | `2000` |
| `rate_limit_rps` | Control table | `5` |
| `path_template` | Control table | `vulnerability_intel/nvd/raw/...` |
| `watermark_value` | Adapter state | `2024-01-01T00:00:00.000` |
| `ingestion_date` | Runtime | `2026-05-20` |
| `batch_id` | Runtime | `batch_20260520030000_nvd_public_01` |
| `start_index` | Runtime (loop) | `0`, `2000`, `4000`, ... |
| `chunk_index` | Runtime (loop) | `1`, `2`, `3`, ... |
| `chunk_filename` | Runtime (loop) | `chunk_001.json`, `chunk_002.json` |
| `total_results` | API response | `25000` |
| `total_records_fetched` | Accumulated | `25000` |
| `has_more_pages` | Calculated | `true` / `false` |
| `last_mod_start_date` | From watermark | `2024-01-01T00:00:00.000` |
| `last_mod_end_date` | Runtime | `2026-05-20T03:00:00.000` |
| `hdfs_output_path` | Constructed | `/data-lake/dev/bronze/vulnerability_intel/nvd/raw/...` |

---

## Testing Checklist

- [ ] Verify NiFi can connect to Hive (control table queries)
- [ ] Verify NiFi can reach NVD API (network/firewall)
- [ ] Verify NiFi can write to HDFS (permissions)
- [ ] Verify vault integration for API key
- [ ] Test with small date range first (1 day of changes)
- [ ] Verify manifest.json content after successful run
- [ ] Verify batch_registry record created with RAW_COMPLETE
- [ ] Verify adapter_state watermark advanced
- [ ] Test failure scenario → verify dead_letter write
- [ ] Test rate limiting (reduce to 1 req/sec and observe)
