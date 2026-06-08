# UEH MVP: NiFi NVD Ingestion — Step-by-Step Build Guide

## Database: `t01_ueh_dev_ctl`
## Tables Used: `t01_ueh_ctl_adapter_config`, `t01_ueh_ctl_adapter_state`, `t01_ueh_ctl_batch_registry`

---

## What This Flow Does

```
Read Config → Read Watermark → Call NVD API (paginate) → Write Chunks to HDFS → Update Control Tables
```

**End Result:** Raw JSON files on HDFS + `batch_registry.batch_status = 'RAW_COMPLETE'`

---

## Prerequisites

Before building this flow, ensure:
- [ ] NiFi can reach NVD API: `https://services.nvd.nist.gov`
- [ ] NiFi can write to HDFS: `/data-lake/dev/bronze/`
- [ ] NiFi can query Hive/Iceberg: `t01_ueh_dev_ctl` database
- [ ] NVD API key obtained: https://nvd.nist.gov/developers/request-an-api-key
- [ ] Control tables created: `spark-sql -f mvp/ddl/01_control_tables.sql`
- [ ] NVD adapter seeded: `spark-sql -f mvp/seed/01_nvd_seed.sql`

---

## NiFi Connection Services (Create First)

| Service Name | Type | Purpose |
|---|---|---|
| `UEH_Hive_DBCP` | DBCPConnectionPool | Query/update control tables |
| `UEH_HDFS` | (Built-in via hadoop config) | Write to HDFS |

**DBCPConnectionPool Settings:**
```
Database Connection URL: jdbc:hive2://your-hiveserver:10000/t01_ueh_dev_ctl
Database Driver Class Name: org.apache.hive.jdbc.HiveDriver
Database User: ueh_svc
```

---

## FLOW: Build These 9 Processors In Order

---

### PROCESSOR 1: GenerateFlowFile (Trigger)

**Purpose:** Start the flow. One empty FlowFile = one ingestion run.

| Setting | Value |
|---|---|
| Processor Type | `GenerateFlowFile` |
| Schedule | `0 3 * * * ?` (cron) OR `Run Once` for testing |
| Custom Text | `trigger` |

**Output:** → `success` to Processor 2

---

### PROCESSOR 2: ExecuteSQL (Read Adapter Config)

**Purpose:** Get NVD connection details from `t01_ueh_ctl_adapter_config`.

| Setting | Value |
|---|---|
| Processor Type | `ExecuteSQL` |
| Database Connection | `UEH_Hive_DBCP` |
| SQL select query | (see below) |

**SQL:**
```sql
SELECT
    org_id,
    adapter_instance_id,
    source_system,
    adapter_type,
    base_url,
    auth_method,
    auth_secret_ref,
    ingestion_mode,
    pagination_config_json,
    runtime_config_json,
    path_template
FROM t01_ueh_dev_ctl.t01_ueh_ctl_adapter_config
WHERE adapter_instance_id = 'nvd_prod_01'
  AND is_active = TRUE
```

**Output:** → `success` to Processor 3

---

### PROCESSOR 3: ConvertAvroToJSON + EvaluateJsonPath (Extract Config)

**Purpose:** Extract config values into FlowFile attributes.

**Step 3a: ConvertAvroToJSON**
| Setting | Value |
|---|---|
| Processor Type | `ConvertAvroToJSON` |

**Step 3b: EvaluateJsonPath**
| Setting | Value |
|---|---|
| Processor Type | `EvaluateJsonPath` |
| Destination | `flowfile-attribute` |

| Property Name | JsonPath |
|---|---|
| `org_id` | `$[0].org_id` |
| `adapter_instance_id` | `$[0].adapter_instance_id` |
| `source_system` | `$[0].source_system` |
| `base_url` | `$[0].base_url` |
| `auth_secret_ref` | `$[0].auth_secret_ref` |
| `ingestion_mode` | `$[0].ingestion_mode` |
| `pagination_config_json` | `$[0].pagination_config_json` |
| `runtime_config_json` | `$[0].runtime_config_json` |
| `path_template` | `$[0].path_template` |

**Output:** → `matched` to Processor 4

---

### PROCESSOR 4: ExecuteSQL (Read Watermark from adapter_state)

**Purpose:** Get last sync point so we know where to resume.

| Setting | Value |
|---|---|
| Processor Type | `ExecuteSQL` |

**SQL:**
```sql
SELECT watermark_state_json, state_status
FROM t01_ueh_dev_ctl.t01_ueh_ctl_adapter_state
WHERE adapter_instance_id = 'nvd_prod_01'
```

**Then ConvertAvroToJSON + EvaluateJsonPath to extract:**

| Property | JsonPath |
|---|---|
| `watermark_state_json` | `$[0].watermark_state_json` |
| `state_status` | `$[0].state_status` |

**After extraction, use another EvaluateJsonPath on the `watermark_state_json` attribute value to get the actual watermark:**

| Property | JsonPath (on watermark_state_json content) |
|---|---|
| `last_mod_start` | `$.lastModStartDate` |

> **Note:** `watermark_state_json` contains: `{"lastModStartDate":"2024-01-01T00:00:00.000","watermark_type":"iso_datetime"}`
> We extract `lastModStartDate` as the API parameter.

**Output:** → to Processor 5

---

### PROCESSOR 5: UpdateAttribute (Generate Runtime Values)

**Purpose:** Create batch_id, dates, and build the HDFS path.

| Setting | Value |
|---|---|
| Processor Type | `UpdateAttribute` |

| Attribute | Expression |
|---|---|
| `ingestion_date` | `${now():format('yyyy-MM-dd')}` |
| `batch_id` | `batch_${now():format('yyyyMMddHHmmss')}_nvd_prod_01` |
| `last_mod_end` | `${now():format("yyyy-MM-dd'T'HH:mm:ss.SSS")}` |
| `start_index` | `0` |
| `chunk_index` | `1` |
| `page_size` | `2000` |
| `hdfs_path` | `/data-lake/dev/bronze/vulnerability_intel/nvd/raw/ingestion_date=${ingestion_date}/batch_id=${batch_id}` |
| `batch_started_at` | `${now():format("yyyy-MM-dd'T'HH:mm:ss.SSS")}` |

> **Note:** `page_size` can also be extracted from `pagination_config_json` if you parse it. For MVP, hardcode 2000 (NVD max).

**Output:** → to Processor 6

---

### PROCESSOR 6: InvokeHTTP (Call NVD API)

**Purpose:** Fetch one page of CVE data.

| Setting | Value |
|---|---|
| Processor Type | `InvokeHTTP` |
| HTTP Method | `GET` |
| Remote URL | `${base_url}?lastModStartDate=${last_mod_start}&lastModEndDate=${last_mod_end}&startIndex=${start_index}&resultsPerPage=${page_size}` |
| Read Timeout | `120 sec` |
| Connection Timeout | `30 sec` |

**Request Headers:**
| Header | Value |
|---|---|
| `apiKey` | `YOUR_NVD_API_KEY` |

> For production: resolve from vault using `${auth_secret_ref}` via HashiCorp Vault NiFi integration.

**Output:**
- `Response` → to Processor 7
- `Failure` / `Retry` → handle errors (dead-letter or retry)

---

### PROCESSOR 7: Parse Response + Write Chunk to HDFS

**Step 7a: EvaluateJsonPath** — Extract pagination metadata from response body

| Setting | Value |
|---|---|
| Processor Type | `EvaluateJsonPath` |
| Destination | `flowfile-attribute` |

| Property | JsonPath |
|---|---|
| `total_results` | `$.totalResults` |
| `results_per_page` | `$.resultsPerPage` |

**Step 7b: UpdateAttribute** — Calculate next page + chunk filename

| Attribute | Expression |
|---|---|
| `next_start_index` | `${start_index:plus(${results_per_page})}` |
| `has_more` | `${next_start_index:lt(${total_results})}` |
| `chunk_filename` | `chunk_${chunk_index:padLeft(3, '0')}.json` |

**Step 7c: PutHDFS** — Write raw response to HDFS

| Setting | Value |
|---|---|
| Processor Type | `PutHDFS` |
| Directory | `${hdfs_path}` |
| Conflict Resolution | `replace` |

**Filename:** `${chunk_filename}`

**Output:** → to Processor 8

---

### PROCESSOR 8: RouteOnAttribute (More Pages?)

**Purpose:** Loop back for next page, or finalize when done.

| Setting | Value |
|---|---|
| Processor Type | `RouteOnAttribute` |
| Routing Strategy | `Route to Property name` |

| Route | Condition | Destination |
|---|---|---|
| `more_pages` | `${has_more:equals('true')}` | → Increment → back to Processor 6 |
| `done` | `${has_more:equals('false')}` | → Processor 9 (Update Control Tables) |

**Increment UpdateAttribute (for loop-back):**
| Attribute | Expression |
|---|---|
| `start_index` | `${next_start_index}` |
| `chunk_index` | `${chunk_index:plus(1)}` |

Then connect back to **Processor 6 (InvokeHTTP)**.

> **Rate Limiting:** Add a `ControlRate` processor before the loop-back to InvokeHTTP:
> - Rate Control Criteria: `flowfile count`
> - Maximum Rate: `5`
> - Time Duration: `1 second`

---

### PROCESSOR 9: PutSQL (Update Control Tables)

**Purpose:** After all pages fetched, register batch and advance watermark.

---

**Step 9a: INSERT into batch_registry** — Mark batch as RAW_COMPLETE

| Setting | Value |
|---|---|
| Processor Type | `PutSQL` |
| Database Connection | `UEH_Hive_DBCP` |

**SQL Statement:**
```sql
INSERT INTO t01_ueh_dev_ctl.t01_ueh_ctl_batch_registry (
    org_id,
    batch_id,
    adapter_instance_id,
    trigger_type,
    load_type,
    batch_status,
    ingestion_date,
    bronze_path,
    checkpoint_path,
    watermark_state_json,
    records_expected,
    records_processed,
    chunks_written,
    start_time,
    end_time,
    failure_reason,
    failure_category,
    parent_batch_id,
    created_at
) VALUES (
    '${org_id}',
    '${batch_id}',
    '${adapter_instance_id}',
    'SCHEDULED',
    '${ingestion_mode}',
    'RAW_COMPLETE',
    DATE '${ingestion_date}',
    '${hdfs_path}',
    NULL,
    '{"lastModStartDate":"${last_mod_start}","lastModEndDate":"${last_mod_end}"}',
    ${total_results},
    ${total_results},
    ${chunk_index},
    TIMESTAMP '${batch_started_at}',
    CURRENT_TIMESTAMP(),
    NULL,
    NULL,
    NULL,
    CURRENT_TIMESTAMP()
)
```

---

**Step 9b: UPDATE adapter_state** — Advance watermark

| Setting | Value |
|---|---|
| Processor Type | `PutSQL` |

**SQL Statement:**
```sql
UPDATE t01_ueh_dev_ctl.t01_ueh_ctl_adapter_state
SET watermark_state_json = '{"lastModStartDate":"${last_mod_end}","watermark_type":"iso_datetime"}',
    last_batch_id = '${batch_id}',
    last_successful_run = CURRENT_TIMESTAMP(),
    records_last_pulled = ${total_results},
    consecutive_failures = 0,
    state_status = 'HEALTHY',
    updated_at = CURRENT_TIMESTAMP()
WHERE adapter_instance_id = '${adapter_instance_id}'
  AND org_id = '${org_id}'
```

---

## COMPLETE FLOW DIAGRAM

```
[1. GenerateFlowFile]
        │
        ▼
[2. ExecuteSQL: Read adapter_config]
        │
        ▼
[3. Extract Config → FlowFile Attributes]
        │
        ▼
[4. ExecuteSQL: Read adapter_state.watermark_state_json]
        │
        ▼
[5. UpdateAttribute: batch_id, dates, hdfs_path]
        │
        ▼
┌───────────────────────────────────────────────┐
│  PAGINATION LOOP                               │
│                                                │
│  [6. InvokeHTTP: GET NVD API]                  │
│          │                                     │
│          ▼                                     │
│  [7a. EvaluateJsonPath: totalResults]          │
│  [7b. UpdateAttribute: next page calc]         │
│  [7c. PutHDFS: write chunk_NNN.json]           │
│          │                                     │
│          ▼                                     │
│  [8. RouteOnAttribute: has_more?]              │
│      │              │                          │
│      │ more_pages   │ done                     │
│      ▼              │                          │
│  [Increment +       │                          │
│   Rate Limit]       │                          │
│      │              │                          │
│      └── back to 6  │                          │
│                     │                          │
└─────────────────────┼──────────────────────────┘
                      │
                      ▼
[9a. PutSQL: INSERT batch_registry → RAW_COMPLETE]
                      │
                      ▼
[9b. PutSQL: UPDATE adapter_state → new watermark]
```

---

## HDFS Output After Successful Run

```
/data-lake/dev/bronze/vulnerability_intel/nvd/raw/
└── ingestion_date=2026-06-08/
    └── batch_id=batch_20260608030000_nvd_prod_01/
        ├── chunk_001.json    (first 2000 CVEs)
        ├── chunk_002.json    (next 2000 CVEs)
        └── chunk_003.json    (remaining CVEs)
```

---

## Control Table State After Successful Run

```sql
-- batch_registry shows RAW_COMPLETE:
SELECT batch_id, batch_status, records_expected, chunks_written, bronze_path
FROM t01_ueh_dev_ctl.t01_ueh_ctl_batch_registry
WHERE adapter_instance_id = 'nvd_prod_01'
ORDER BY created_at DESC LIMIT 1;

-- Result:
-- batch_20260608030000_nvd_prod_01 | RAW_COMPLETE | 5432 | 3 | /data-lake/dev/bronze/.../batch_id=batch_20260608030000_nvd_prod_01


-- adapter_state shows advanced watermark:
SELECT adapter_instance_id, watermark_state_json, state_status, records_last_pulled
FROM t01_ueh_dev_ctl.t01_ueh_ctl_adapter_state
WHERE adapter_instance_id = 'nvd_prod_01';

-- Result:
-- nvd_prod_01 | {"lastModStartDate":"2026-06-08T03:00:00.000",...} | HEALTHY | 5432
```

---

## Failure Handling (Simple MVP Version)

If InvokeHTTP fails:
1. Route `failure` relationship to a **LogAttribute** processor (logs the error)
2. Route to **PutSQL** that updates adapter_state:

```sql
UPDATE t01_ueh_dev_ctl.t01_ueh_ctl_adapter_state
SET consecutive_failures = consecutive_failures + 1,
    state_status = CASE
        WHEN consecutive_failures + 1 >= 5 THEN 'FAILING'
        ELSE 'HEALTHY'
    END,
    last_failure_reason = '${invokehttp.status.code}: ${invokehttp.status.message}',
    updated_at = CURRENT_TIMESTAMP()
WHERE adapter_instance_id = '${adapter_instance_id}'
  AND org_id = '${org_id}'
```

3. Optionally INSERT into batch_registry with `batch_status = 'FAILED'`

> For MVP, just log failures. Add dead-letter HDFS routing in next iteration.

---

## Testing Checklist

- [ ] Set watermark to yesterday for small test:
  ```sql
  UPDATE t01_ueh_dev_ctl.t01_ueh_ctl_adapter_state
  SET watermark_state_json = '{"lastModStartDate":"2026-06-07T00:00:00.000","watermark_type":"iso_datetime"}'
  WHERE adapter_instance_id = 'nvd_prod_01';
  ```
- [ ] Run NiFi flow once
- [ ] Verify chunk files on HDFS: `hdfs dfs -ls /data-lake/dev/bronze/vulnerability_intel/nvd/raw/`
- [ ] Verify batch_registry has RAW_COMPLETE record
- [ ] Verify adapter_state watermark advanced
- [ ] Verify chunk file content is valid JSON: `hdfs dfs -cat <path>/chunk_001.json | python -m json.tool | head`

---

## Next Step

After NiFi produces `RAW_COMPLETE`, **Airflow DAG 2** will detect it and submit the **Spark Bronze Loader** to read chunks from HDFS and write to Iceberg.
