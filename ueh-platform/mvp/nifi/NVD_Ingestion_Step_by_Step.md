# UEH MVP: NiFi NVD Ingestion — Step-by-Step Build Guide

## What This Flow Does

```
Read Config → Read Watermark → Call NVD API (paginate) → Write Chunks to HDFS → Update Control Tables
```

**End Result:** Raw JSON files on HDFS + `batch_registry.status = 'RAW_COMPLETE'`

---

## Prerequisites

Before building this flow, ensure:
- [ ] NiFi can reach NVD API: `https://services.nvd.nist.gov`
- [ ] NiFi can write to HDFS: `/data-lake/dev/bronze/`
- [ ] NiFi can query Hive/Iceberg: `ueh_dev_control` database
- [ ] NVD API key obtained: https://nvd.nist.gov/developers/request-an-api-key

---

## NiFi Connection Services Needed (Create First)

| Service Name | Type | Purpose |
|---|---|---|
| `UEH_Hive_DBCP` | DBCPConnectionPool | Query control tables |
| `UEH_HDFS` | (Built-in) | Write to HDFS |

**DBCPConnectionPool Settings:**
```
Database Connection URL: jdbc:hive2://your-hiveserver:10000/ueh_dev_control
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
| Run Schedule | `60 sec` (throttle for testing) |

**Output:** → Connect `success` to Processor 2

---

### PROCESSOR 2: ExecuteSQL (Read Adapter Config)

**Purpose:** Get NVD connection details from control table.

| Setting | Value |
|---|---|
| Processor Type | `ExecuteSQL` |
| Database Connection | `UEH_Hive_DBCP` |
| SQL select query | (see below) |

**SQL:**
```sql
SELECT
    adapter_instance_id,
    adapter_name,
    adapter_type,
    base_url,
    auth_secret_ref,
    ingestion_mode,
    page_size,
    rate_limit_rps,
    path_template
FROM t01_ueh_ctl_adapter_config
WHERE adapter_instance_id = 'nvd_public_01'
  AND is_active = TRUE
```

**Output:** → `success` to Processor 3 (ConvertAvroToJSON)

---

### PROCESSOR 3: ConvertAvroToJSON + EvaluateJsonPath

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
| `adapter_instance_id` | `$[0].adapter_instance_id` |
| `base_url` | `$[0].base_url` |
| `auth_secret_ref` | `$[0].auth_secret_ref` |
| `page_size` | `$[0].page_size` |
| `path_template` | `$[0].path_template` |

**Output:** → `matched` to Processor 4

---

### PROCESSOR 4: ExecuteSQL (Read Watermark)

**Purpose:** Get last sync point so we know where to resume.

| Setting | Value |
|---|---|
| Processor Type | `ExecuteSQL` |

**SQL:**
```sql
SELECT watermark_value
FROM t01_ueh_ctl_adapter_state
WHERE adapter_instance_id = 'nvd_public_01'
```

**Then EvaluateJsonPath to extract:**
| Property | JsonPath |
|---|---|
| `watermark_value` | `$[0].watermark_value` |

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
| `batch_id` | `batch_${now():format('yyyyMMddHHmmss')}_nvd_public_01` |
| `last_mod_start` | `${watermark_value}` |
| `last_mod_end` | `${now():format("yyyy-MM-dd'T'HH:mm:ss.SSS")}` |
| `start_index` | `0` |
| `chunk_index` | `1` |
| `hdfs_path` | `/data-lake/dev/bronze/vulnerability_intel/nvd/raw/ingestion_date=${ingestion_date}/batch_id=${batch_id}` |

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

**Request Headers:**
| Header | Value |
|---|---|
| `apiKey` | `YOUR_NVD_API_KEY` (or resolved from vault) |

**Output:**
- `Response` → to Processor 7 (EvaluateJsonPath for pagination)
- `Failure/Retry` → handle errors (log or dead-letter)

---

### PROCESSOR 7: EvaluateJsonPath (Parse Pagination) + PutHDFS (Write Chunk)

**Step 7a: EvaluateJsonPath** — Get pagination info from response

| Property | JsonPath |
|---|---|
| `total_results` | `$.totalResults` |
| `results_per_page` | `$.resultsPerPage` |

**Step 7b: UpdateAttribute** — Calculate next page

| Attribute | Expression |
|---|---|
| `next_start_index` | `${start_index:plus(${results_per_page})}` |
| `has_more` | `${next_start_index:lt(${total_results})}` |
| `chunk_filename` | `chunk_${chunk_index:padLeft(3, '0')}.json` |

**Step 7c: PutHDFS** — Write this page to HDFS

| Setting | Value |
|---|---|
| Processor Type | `PutHDFS` |
| Directory | `${hdfs_path}` |
| Conflict Resolution | `replace` |

**Filename:** `${chunk_filename}`

**Output:** → to Processor 8 (RouteOnAttribute)

---

### PROCESSOR 8: RouteOnAttribute (More Pages?)

**Purpose:** Loop back for next page, or finalize.

| Setting | Value |
|---|---|
| Processor Type | `RouteOnAttribute` |

| Route | Condition | Destination |
|---|---|---|
| `more_pages` | `${has_more:equals('true')}` | → UpdateAttribute (increment) → back to Processor 6 |
| `done` | `${has_more:equals('false')}` | → Processor 9 (Update Control Tables) |

**Increment UpdateAttribute (for loop-back):**
| Attribute | Expression |
|---|---|
| `start_index` | `${next_start_index}` |
| `chunk_index` | `${chunk_index:plus(1)}` |

---

### PROCESSOR 9: PutSQL (Update Control Tables)

**Purpose:** After all pages fetched, mark batch as RAW_COMPLETE.

**Step 9a: Insert into batch_registry**

| Setting | Value |
|---|---|
| Processor Type | `PutSQL` |
| Database Connection | `UEH_Hive_DBCP` |

**SQL:**
```sql
INSERT INTO ueh_dev_control.t01_ueh_ctl_batch_registry VALUES (
    '${batch_id}',
    'nvd_public_01',
    DATE '${ingestion_date}',
    'INCREMENTAL',
    'RAW_COMPLETE',
    ${total_results},
    ${chunk_index},
    '${hdfs_path}',
    '${last_mod_start}',
    '${last_mod_end}',
    TIMESTAMP '${batch_started_at}',
    CURRENT_TIMESTAMP(),
    NULL,
    'SCHEDULED',
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
)
```

**Step 9b: Update adapter_state (advance watermark)**

```sql
UPDATE ueh_dev_control.t01_ueh_ctl_adapter_state
SET watermark_value = '${last_mod_end}',
    last_successful_run = CURRENT_TIMESTAMP(),
    records_last_pulled = ${total_results},
    consecutive_failures = 0,
    state_status = 'HEALTHY',
    last_updated = CURRENT_TIMESTAMP()
WHERE adapter_instance_id = 'nvd_public_01'
```

---

## COMPLETE FLOW DIAGRAM

```
[1. GenerateFlowFile]
        │
        ▼
[2. ExecuteSQL: Get Config]
        │
        ▼
[3. Extract Config Attributes]
        │
        ▼
[4. ExecuteSQL: Get Watermark]
        │
        ▼
[5. UpdateAttribute: Runtime Values]
        │
        ▼
┌───────────────────────────────────────┐
│ PAGINATION LOOP                        │
│                                        │
│  [6. InvokeHTTP: Call NVD API]         │
│          │                             │
│          ▼                             │
│  [7. Parse Response + Write to HDFS]   │
│          │                             │
│          ▼                             │
│  [8. RouteOnAttribute]                 │
│      │           │                     │
│      │more       │done                 │
│      ▼           │                     │
│  [Increment]     │                     │
│      │           │                     │
│      └─── back to [6]                  │
│                  │                     │
└──────────────────┼─────────────────────┘
                   │
                   ▼
[9. PutSQL: batch_registry = RAW_COMPLETE]
[9. PutSQL: adapter_state = new watermark]
```

---

## After This Flow Runs Successfully

**On HDFS you'll see:**
```
/data-lake/dev/bronze/vulnerability_intel/nvd/raw/
└── ingestion_date=2026-06-03/
    └── batch_id=batch_20260603030000_nvd_public_01/
        ├── chunk_001.json
        ├── chunk_002.json
        └── chunk_003.json
```

**In control tables you'll see:**
```sql
-- batch_registry:
SELECT * FROM t01_ueh_ctl_batch_registry WHERE batch_id = 'batch_20260603030000_nvd_public_01';
-- → status = 'RAW_COMPLETE', records_ingested = 6000, chunks_written = 3

-- adapter_state:
SELECT * FROM t01_ueh_ctl_adapter_state WHERE adapter_instance_id = 'nvd_public_01';
-- → watermark_value = '2026-06-03T03:00:00.000', state_status = 'HEALTHY'
```

**Next step:** Airflow DAG 2 will detect `RAW_COMPLETE` and trigger the Bronze Spark loader.

---

## Testing Tips

1. **First test:** Set watermark to yesterday → only ~50-200 CVEs modified per day
   ```sql
   UPDATE t01_ueh_ctl_adapter_state
   SET watermark_value = '2026-06-02T00:00:00.000'
   WHERE adapter_instance_id = 'nvd_public_01';
   ```

2. **Verify HDFS write:** After flow runs, check:
   ```bash
   hdfs dfs -ls /data-lake/dev/bronze/vulnerability_intel/nvd/raw/
   hdfs dfs -cat /path/to/chunk_001.json | python -m json.tool | head -50
   ```

3. **Verify control tables:** After flow completes, query batch_registry.

4. **Common issues:**
   - NVD returns 403 → API key wrong or rate limited
   - NiFi can't write HDFS → check permissions for NiFi service user
   - SQL fails → check Hive JDBC connection string
