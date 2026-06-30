# UEH Silver & Gold Data Modeling Guide

## Evolutionary Modeling: MVP Start → Enterprise Scale

**Document Version:** 1.0
**Audience:** Data Engineers, Data Architects
**Platform:** Unified Vulnerability Exposure Hub (UEH)
**Purpose:** Guide the team on WHAT to model now, WHAT to add later, and WHEN

---

## Core Principle: Start Narrow, Evolve Wide

```
MVP (Now):        3 Silver canonical + 3 Gold tables
Phase 2 (3mo):    + asset subtypes, + finding history
Phase 3 (6mo):    + Gold star schema (dimensions + facts)
Phase 4 (9mo+):   + identity, configuration, exposure events
```

**Rule:** Never add a table until a real query or source demands it.
Premature modeling creates maintenance burden. Evolve based on signal.

---

## Table of Contents

1. Modeling Philosophy
2. Silver Layer — MVP (Phase 1)
3. Silver Layer — Evolution (Phase 2-4)
4. Asset Subtype Strategy (the key evolution)
5. Gold Layer — MVP (Phase 1)
6. Gold Layer — Star Schema Evolution
7. Control Tables Per Modeling Stage
8. Decision Framework: When to Split a Table
9. Migration Patterns (how to evolve safely)


---

# 1. MODELING PHILOSOPHY

## Silver = Business Entities (Canonical)

Silver canonical tables represent **enterprise cybersecurity entities**, not adapters.
Multiple adapters feed ONE canonical entity table.

```
Tenable + Sysdig + Qualys  →  slv_vulnerability_finding  (one entity)
NVD + EPSS + CISA + MSRC    →  slv_vulnerability_intel    (one entity)
CMDB + ADDM + scanners      →  slv_asset                  (one entity)
```

## Gold = Analytics-Optimized (Star Schema Eventually)

Gold starts as **wide flat tables** (easy MVP). It evolves into a **star schema**
(dimensions + facts) when reporting complexity demands it.

```
MVP:        Wide denormalized tables (gld_exposure_summary)
Evolution:  Star schema (dim_asset, dim_cve, dim_time + fact_exposure)
```

## The Two-Stage Silver (Already Implemented)

```
Stage 1 (staging):   slv_stg_<adapter>_*    — adapter-specific, typed
Stage 2 (canonical): slv_<entity>           — merged, deduplicated, enriched
```

Stage 1 is per-adapter. Stage 2 is per-entity. This guide focuses on **Stage 2
canonical** and **Gold** modeling evolution.

---

# 2. SILVER LAYER — MVP (PHASE 1)

## The 3 Starting Canonical Tables

| Table | Entity | Fed By (MVP) | Key | Write |
|-------|--------|--------------|-----|-------|
| `slv_vulnerability_intel` | CVE knowledge | NVD | cve_id | MERGE |
| `slv_vulnerability_finding` | Scanner findings | Tenable | finding_id | APPEND |
| `slv_asset` | Asset inventory | ADDM | asset_id | MERGE |

## Why These 3 First

```
slv_vulnerability_intel   → "What do we know about CVE-2024-1234?"
slv_vulnerability_finding → "Which assets have this vulnerability?"
slv_asset                 → "What assets do we have?"
```

These 3 answer the core UEH question:
**"Which of our assets are exposed to which vulnerabilities?"**
(JOIN finding + intel + asset in Gold)

## MVP Asset Table — Single Table, All Types

For MVP, `slv_asset` holds ALL asset types in one table using:
- Common typed columns (ip, hostname, os_family) — queryable
- `asset_type` discriminator column (COMPUTE, NETWORK, etc.)
- `asset_attributes_json` — type-specific extras (flexible)

```sql
-- MVP: one row per asset, type as a column
asset_id | asset_type | hostname    | ip_address | asset_attributes_json
a001     | COMPUTE    | srv-prod-01 | 10.0.1.50  | {"cpu":8,"ram_gb":32}
a002     | CONTAINER  | NULL        | NULL       | {"image":"nginx:1.25","pod":"web-x"}
a003     | NETWORK    | sw-core-01  | 10.0.0.1   | {"device":"switch","vendor":"Cisco"}
```

This is sufficient for MVP. Evolution to subtypes covered in Section 4.


---

# 3. SILVER LAYER — EVOLUTION (PHASE 2-4)

## Evolution Roadmap

| Phase | New Canonical Tables | Trigger (when to add) |
|-------|---------------------|----------------------|
| Phase 1 (MVP) | intel, finding, asset | Day 1 |
| Phase 2 | finding_history, software_inventory | Multiple scanners + need point-in-time |
| Phase 3 | configuration, exposure_event | Compliance/config scanning onboarded |
| Phase 4 | identity, relationship | IAM sources + dependency mapping |

## Phase 2 Additions

### slv_software_inventory
**When:** ADDM/scanners report installed software/packages.
**Why:** "Which assets run OpenSSL 1.1.1?" (vulnerable package tracking)
```
Fed by: ADDM (software_instances), Sysdig (container packages)
Key: software_id = hash(asset_id + package_name + version)
```

### slv_finding_history (SCD Type 2 for findings)
**When:** Need to track finding state changes over time (OPEN→FIXED→REOPENED).
**Why:** "How long was CVE-X open on asset-Y?" MTTR reporting.
```
Note: MVP uses APPEND for findings (point-in-time). This adds explicit
SCD2 with valid_from/valid_to ONLY if MTTR analytics needs it.
```

## Phase 3 Additions

### slv_configuration
**When:** Config/compliance scanners (CIS benchmarks) onboarded.
**Why:** "Which assets fail CIS hardening?"
```
Fed by: Tenable compliance, Qualys policy compliance
Key: config_finding_id = hash(asset_id + check_id)
```

### slv_exposure_event
**When:** Need temporal event stream (finding appeared/disappeared/changed).
**Why:** Time-series exposure analytics, trend detection.
```
Fed by: Derived from finding state transitions
Key: event_id (append-only event log)
```

## Phase 4 Additions

### slv_identity
**When:** IAM sources (AD, Okta) onboarded.
**Why:** "Which users can access this exposed asset?"

### slv_relationship
**When:** Dependency mapping needed (ADDM dependencies).
**Why:** "If this asset is compromised, what's the blast radius?"


---

# 4. ASSET SUBTYPE STRATEGY (KEY EVOLUTION)

This is the most important evolution. You asked about asset subtypes:
network, storage, host, applications, containers, databases.

## Three Modeling Options (Choose Based on Phase)

### Option A — Single Table + Discriminator (MVP / Phase 1)

```
slv_asset (asset_type column + asset_attributes_json)
```
- ✅ Simplest, fastest to build
- ✅ Easy cross-type queries ("all assets")
- ❌ Type-specific fields buried in JSON (not directly queryable)
- **Use when:** <500K assets, type-specific queries are rare

### Option B — Single Base Table + Subtype Extension Tables (Phase 2-3)

```
slv_asset                  (common fields — ALL assets)
  ├── slv_asset_host        (cpu, ram, os_patch_level)
  ├── slv_asset_container   (image, namespace, pod, registry)
  ├── slv_asset_network     (device_type, firmware, port_count)
  ├── slv_asset_database    (engine, version, instance_count)
  ├── slv_asset_storage     (capacity, raid_level, protocol)
  └── slv_asset_application (app_name, version, vendor, license)
```
- ✅ Type-specific fields are typed columns (queryable, indexable)
- ✅ Base table still enables cross-type queries (JOIN to base)
- ✅ Each subtype evolves independently
- **Use when:** type-specific queries become frequent, >500K assets

**This is the recommended target model for asset subtypes.**

### Option C — Fully Separate Tables (NOT recommended)

```
slv_host, slv_container, slv_network (no shared base)
```
- ❌ No unified "all assets" view (requires UNION across all)
- ❌ Common logic duplicated
- **Avoid** — loses the canonical entity benefit

## Recommended Subtype Evolution Path

```
Phase 1 (MVP):
   slv_asset (asset_type discriminator + JSON)
   → COMPUTE, NETWORK only (from ADDM)

Phase 2 (when CMDB + containers arrive):
   slv_asset (base, common fields)
   + slv_asset_host        ← extension (cpu, ram, os details)
   + slv_asset_container   ← extension (image, namespace, k8s)

Phase 3 (when DB/storage scanners arrive):
   + slv_asset_database    ← extension
   + slv_asset_storage     ← extension
   + slv_asset_network     ← extension (promote from JSON to typed)

Phase 4 (application portfolio):
   + slv_asset_application ← extension
```

## Base + Extension Pattern (DDL Example)

```sql
-- BASE: common to ALL asset types
CREATE TABLE slv_asset (
    asset_id        STRING,   -- PK, hash-generated
    asset_type      STRING,   -- COMPUTE|CONTAINER|NETWORK|DATABASE|STORAGE|APPLICATION
    hostname        STRING,
    ip_address      STRING,
    fqdn            STRING,
    os_family       STRING,
    criticality     STRING,
    business_unit   STRING,
    owner           STRING,
    environment     STRING,
    first_seen      TIMESTAMP,
    last_seen       TIMESTAMP,
    is_active       BOOLEAN,
    source_systems_json STRING,
    ingestion_date  DATE
) USING iceberg PARTITIONED BY (ingestion_date);

-- EXTENSION: host-specific (1:1 with base where asset_type='COMPUTE')
CREATE TABLE slv_asset_host (
    asset_id        STRING,   -- FK to slv_asset
    cpu_count       INT,
    ram_mb          BIGINT,
    disk_total_gb   INT,
    is_virtual      BOOLEAN,
    hypervisor      STRING,
    cluster         STRING,
    os_version      STRING,
    patch_level     STRING,
    ingestion_date  DATE
) USING iceberg PARTITIONED BY (ingestion_date);

-- EXTENSION: container-specific
CREATE TABLE slv_asset_container (
    asset_id        STRING,   -- FK to slv_asset
    image_name      STRING,
    image_tag       STRING,
    image_digest    STRING,
    namespace       STRING,
    pod_name        STRING,
    cluster_name    STRING,
    registry        STRING,
    ingestion_date  DATE
) USING iceberg PARTITIONED BY (ingestion_date);
```

## Querying Base + Extension

```sql
-- All assets (base only)
SELECT * FROM slv_asset;

-- Hosts with their hardware details
SELECT a.*, h.cpu_count, h.ram_mb, h.hypervisor
FROM slv_asset a
JOIN slv_asset_host h ON a.asset_id = h.asset_id
WHERE a.asset_type = 'COMPUTE';

-- Containers in production
SELECT a.hostname, c.image_name, c.namespace
FROM slv_asset a
JOIN slv_asset_container c ON a.asset_id = c.asset_id
WHERE a.environment = 'PRODUCTION';
```

## Migration: JSON → Extension Table (How to Evolve Safely)

```
When asset_attributes_json for containers gets queried often:

1. CREATE slv_asset_container (new extension table)
2. Backfill: parse existing asset_attributes_json → extension columns
       INSERT INTO slv_asset_container
       SELECT asset_id, get_json_object(asset_attributes_json,'$.image'), ...
       FROM slv_asset WHERE asset_type='CONTAINER'
3. Update Stage 2 asset job: write container fields to extension table
4. Keep asset_attributes_json (backward compat) OR deprecate gradually
5. NO Bronze change. NO re-ingestion. Pure Silver evolution.
```


---

# 5. GOLD LAYER — MVP (PHASE 1)

## The 3 Starting Gold Tables

| Table | Purpose | Source (Silver) | Consumer |
|-------|---------|-----------------|----------|
| `gld_exposure_summary` | Every finding enriched | finding + intel + asset | Dashboard, prioritization |
| `gld_cve_enriched` | CVE intelligence lookup | intel + finding counts | Chatbot, CVE lookup |
| `gld_risk_metrics` | Aggregated KPIs | exposure_summary | Executive dashboard |

## MVP Gold = Wide Flat Tables

For MVP, Gold tables are **denormalized wide tables** (not star schema yet).

```sql
-- gld_exposure_summary: one row per finding, fully enriched
finding_id | cve_id | cvss | epss | is_in_kev | asset_hostname |
           | asset_criticality | risk_score | priority_rank | ...
```

**Why wide flat first:**
- Simple to build and query
- Dashboard reads one table (no JOINs at query time)
- Fast for MVP volumes (5-10 GB/day)

## Gold Rules (Never Violate)

- Gold reads ONLY from canonical Silver (never Bronze, never staging)
- Gold NEVER parses payload_json
- Gold NEVER does entity resolution (that's Silver Stage 2)
- Gold is rebuildable (OVERWRITE partition daily)

---

# 6. GOLD LAYER — STAR SCHEMA EVOLUTION

## When to Evolve to Star Schema

```
Trigger: Reporting complexity grows. Multiple dashboards. Slicing by
many dimensions (time, BU, asset_type, severity, source). Wide tables
become expensive to scan repeatedly.
```

## Phase 3 Star Schema Design

```
                    ┌─────────────────┐
                    │  dim_time        │
                    └────────┬─────────┘
                             │
   ┌──────────────┐   ┌──────┴───────────┐   ┌──────────────┐
   │  dim_asset    │───│  fact_exposure   │───│  dim_cve      │
   └──────────────┘   └──────┬───────────┘   └──────────────┘
                             │
                    ┌────────┴─────────┐
                    │  dim_adapter      │
                    └─────────────────┘
```

## Dimension Tables (Phase 3)

| Dimension | Grain | Source | SCD |
|-----------|-------|--------|-----|
| `gld_dim_asset` | One per asset | slv_asset | Type 2 (track changes) |
| `gld_dim_cve` | One per CVE | slv_vulnerability_intel | Type 1 |
| `gld_dim_time` | One per day | generated | Static |
| `gld_dim_adapter` | One per source | adapter_config | Type 1 |
| `gld_dim_business_unit` | One per BU | slv_asset (derived) | Type 2 |

## Fact Tables (Phase 3)

| Fact | Grain | Measures |
|------|-------|----------|
| `gld_fact_exposure` | finding × day | risk_score, days_open, is_sla_breach |
| `gld_fact_remediation` | remediation event | time_to_remediate, sla_met |
| `gld_fact_daily_posture` | asset × day | open_count, critical_count, avg_risk |

## Fact Table Example

```sql
CREATE TABLE gld_fact_exposure (
    -- Dimension keys (foreign keys to dims)
    asset_key       STRING,   -- → gld_dim_asset
    cve_key         STRING,   -- → gld_dim_cve
    time_key        DATE,     -- → gld_dim_time
    adapter_key     STRING,   -- → gld_dim_adapter

    -- Degenerate dimension
    finding_id      STRING,

    -- Measures (additive facts)
    risk_score      DOUBLE,
    cvss_score      DOUBLE,
    epss_score      DOUBLE,
    days_open       INT,
    is_open         INT,      -- 1/0 for SUM aggregation
    is_critical     INT,      -- 1/0
    is_kev          INT,      -- 1/0
    is_sla_breach   INT,      -- 1/0

    ingestion_date  DATE
) USING iceberg PARTITIONED BY (ingestion_date);
```

## Star Schema Query Benefit

```sql
-- "Critical open exposures by BU over last 30 days"
SELECT t.month, bu.bu_name, SUM(f.is_critical) as critical_count
FROM gld_fact_exposure f
JOIN gld_dim_time t ON f.time_key = t.time_key
JOIN gld_dim_asset a ON f.asset_key = a.asset_key
JOIN gld_dim_business_unit bu ON a.bu_key = bu.bu_key
WHERE f.is_open = 1 AND t.date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY t.month, bu.bu_name;
```

## Wide Table vs Star Schema (Coexist)

Keep wide `gld_exposure_summary` for operational dashboard (fast single-table read).
Add star schema for analytical/historical reporting (flexible slicing).
**Both can coexist** — wide for ops, star for analytics.


---

# 7. CONTROL TABLES PER MODELING STAGE

Each modeling layer is governed/driven by specific control tables.

## Control Tables Attached to Silver

| Control Table | Attached To | Role |
|---------------|-------------|------|
| `batch_registry` | All Silver tables | Tracks STAGING_COMPLETE, SILVER_COMPLETE per batch |
| `field_mapping` | Stage 2 canonical | Maps source_json_path → canonical target_field |
| `adapter_config` | Stage 1 staging | source_system → which parser, which staging table |
| `failed_ingestions` | All Silver (Phase 2+) | Captures parse/transform failures |
| `pipeline_dependency` | Stage 2 canonical (Phase 3+) | Multi-source readiness (NVD+EPSS+CISA before intel complete) |
| `schema_registry` | Canonical (Phase 3+) | Defines canonical schema versions for validation |

## Control Tables Attached to Gold

| Control Table | Attached To | Role |
|---------------|-------------|------|
| `batch_registry` | All Gold tables | Tracks GOLD_COMPLETE |
| `pipeline_dependency` | Gold tables | Which Silver tables must be SILVER_COMPLETE first |
| `platform_metrics` | Gold (Phase 3+) | Tracks Gold rebuild duration, freshness |

## How field_mapping Drives Canonical Modeling

When you add a new subtype (e.g., container), field_mapping defines the parse:

```sql
-- Container-specific mappings (Phase 2)
INSERT INTO field_mapping VALUES
  ('map_sysdig_img','SYSDIG','$.image.name','image_name','asset_container','DIRECT',...),
  ('map_sysdig_ns','SYSDIG','$.kubernetes.namespace','namespace','asset_container','DIRECT',...);
```

`target_schema = 'asset_container'` tells Stage 2 which extension table to write.

## How pipeline_dependency Drives Multi-Source Canonical

```sql
-- intel canonical needs NVD (hard) + EPSS (soft) + CISA (soft)
INSERT INTO pipeline_dependency VALUES
  ('dep_001','slv_vulnerability_intel','SILVER','slv_stg_nvd_vulnerability','SILVER','NVD','HARD','STAGING_COMPLETE',24,...),
  ('dep_002','slv_vulnerability_intel','SILVER','slv_stg_epss_score','SILVER','EPSS','SOFT','STAGING_COMPLETE',48,...);
```
HARD = must complete. SOFT = proceed with warning if stale.

---

# 8. DECISION FRAMEWORK: WHEN TO SPLIT A TABLE

Use this checklist before adding any new table:

| Signal | Action |
|--------|--------|
| A source reports a NEW entity type not covered | Add new canonical table |
| Type-specific fields queried frequently (buried in JSON) | Promote JSON → extension table |
| One asset_type is >60% of rows + has unique query patterns | Create subtype extension |
| Reporting needs flexible multi-dimension slicing | Introduce Gold star schema |
| Need point-in-time history (MTTR, trend) | Add SCD2 history table |
| Cross-source merge logic differs significantly per type | Separate Stage 2 jobs (not tables) |

**Anti-signals (do NOT split):**
- "It might be useful later" → wait for real demand
- "Other platforms do it" → model YOUR queries
- "The table has many NULL columns" → NULLs are cheap in Parquet/Iceberg

---

# 9. MIGRATION PATTERNS (EVOLVE SAFELY)

## Pattern 1: Add Column to Canonical (Backward Compatible)

```sql
ALTER TABLE slv_vulnerability_intel ADD COLUMN epss_score DOUBLE;
-- Existing rows = NULL. New batches populate. No rewrite. No re-ingestion.
```

## Pattern 2: JSON Field → Typed Column (Promote)

```
1. ALTER TABLE ADD COLUMN (new typed column)
2. Backfill: UPDATE ... SET col = get_json_object(json_col, '$.field')
3. Update Stage 2 job to write typed column going forward
4. Optionally deprecate JSON field later
```

## Pattern 3: Single Table → Base + Extension (Subtype Split)

```
1. CREATE extension table (slv_asset_container)
2. Backfill from base JSON for that asset_type
3. Update Stage 2 to write base + extension
4. Bronze UNCHANGED. Re-run Silver from staging if needed (idempotent).
```

## Pattern 4: Wide Gold → Star Schema (Add, Don't Replace)

```
1. Build dim_* and fact_* tables ALONGSIDE wide table
2. Dual-write during transition (wide + star)
3. Migrate dashboards to star gradually
4. Retire wide table only when no consumers remain
```

## Golden Rule of Evolution

```
Bronze NEVER changes during Silver/Gold evolution.
All re-modeling re-runs from Bronze/staging (idempotent).
No re-ingestion from source APIs ever needed for modeling changes.
```

---

# SUMMARY: MODELING ROADMAP AT A GLANCE

| Phase | Silver Canonical | Asset Model | Gold Model | New Control Tables |
|-------|-----------------|-------------|------------|-------------------|
| **1 (MVP)** | intel, finding, asset | Single table + JSON | 3 wide tables | field_mapping (core 4) |
| **2** | + software_inventory | Base + host/container ext | wide tables | failed_ingestions |
| **3** | + configuration, exposure_event | + network/db/storage ext | + star schema (dims+facts) | pipeline_dependency, schema_registry |
| **4** | + identity, relationship | + application ext | full star + marts | platform_metrics |

---

*End of Modeling Guide*
