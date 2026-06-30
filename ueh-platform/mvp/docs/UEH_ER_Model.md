# UEH End-to-End ER Model

## Entity-Relationship Reference Across All Layers

**Document Version:** 1.0
**Audience:** Data Engineers, Architects, Analysts
**Scope:** Control + Bronze + Silver (Stage 1/2) + Gold + Serving
**Notation:** PK = primary key, FK = foreign key, (P) = partition key

---

## Table of Contents

1. ER Overview (Layer Map)
2. Control Plane ER
3. Bronze Layer ER
4. Silver Stage 1 (Staging) ER
5. Silver Stage 2 (Canonical) ER
6. Gold Layer ER
7. Serving Layer (PostgreSQL) ER
8. Cross-Layer Key Lineage
9. Ownership Relationships
10. Full Relationship Matrix


---

# 1. ER OVERVIEW (LAYER MAP)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          CONTROL PLANE (drives everything)                    │
│  adapter_config ──1:1── adapter_state                                         │
│       │ 1:N                                                                   │
│       ▼                                                                       │
│  batch_registry ──1:N── failed_ingestions                                     │
│       │                                                                       │
│  field_mapping   pipeline_dependency   ownership_rules                        │
└───────────────────────────────┬───────────────────────────────────────────────┘
                                │ batch_id links control → data
                                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  BRONZE (1 table per adapter)                                                 │
│  brz_nvd_raw    brz_tenable_raw    brz_addm_raw    (payload_json)             │
└───────────────────────────────┬───────────────────────────────────────────────┘
                                │ batch_id
                                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  SILVER STAGE 1 — Staging (1 per adapter, typed)                              │
│  slv_stg_nvd_vulnerability   slv_stg_tenable_finding   slv_stg_addm_asset     │
└───────────────────────────────┬───────────────────────────────────────────────┘
                                │ merge by business key
                                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  SILVER STAGE 2 — Canonical (business entities)                               │
│                                                                               │
│  slv_asset ──1:N── slv_vulnerability_finding ──N:1── slv_vulnerability_intel  │
│  (asset_id)         (finding_id, FK asset_id,         (cve_id)                │
│                      FK cve_id)                                               │
│     │ 1:1 (subtype extensions, Phase 2+)                                      │
│     ├── slv_asset_host                                                        │
│     ├── slv_asset_container                                                   │
│     ├── slv_asset_network                                                     │
│     ├── slv_asset_database                                                    │
│     ├── slv_asset_storage                                                     │
│     └── slv_asset_application                                                 │
└───────────────────────────────┬───────────────────────────────────────────────┘
                                │ JOIN finding + intel + asset
                                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  GOLD                                                                         │
│  MVP: gld_exposure_summary  gld_cve_enriched  gld_risk_metrics (wide)         │
│  Evolution: dim_asset, dim_cve, dim_time + fact_exposure (star)               │
└───────────────────────────────┬───────────────────────────────────────────────┘
                                │ daily sync
                                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  POSTGRESQL SERVING                                                           │
│  exposure_findings (synced) ──1:N── remediation_assignments (UI-managed)      │
│  cve_intelligence (synced)          risk_exceptions, audit_log (UI-managed)   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

# 2. CONTROL PLANE ER

```
t01_ueh_ctl_adapter_config                t01_ueh_ctl_adapter_state
┌──────────────────────────┐              ┌──────────────────────────┐
│ adapter_instance_id (PK) │◄───1:1──────►│ adapter_instance_id (PK) │
│ org_id                   │              │ org_id                   │
│ source_system            │              │ watermark_state_json     │
│ adapter_type             │              │ last_batch_id            │
│ base_url                 │              │ consecutive_failures     │
│ auth_secret_ref          │              │ state_status             │
│ ingestion_mode           │              └──────────────────────────┘
│ schedule_cron            │
│ pagination_config_json   │
│ runtime_config_json      │
│ is_active                │
└────────────┬─────────────┘
             │ 1:N (one adapter → many batches)
             ▼
t01_ueh_ctl_batch_registry
┌──────────────────────────┐              t01_ueh_ctl_failed_ingestions
│ batch_id (PK)            │              ┌──────────────────────────┐
│ adapter_instance_id (FK) │◄───1:N──────►│ failure_id (PK)          │
│ org_id                   │              │ batch_id (FK)            │
│ batch_status             │              │ adapter_instance_id (FK) │
│ dq_status                │              │ failure_stage            │
│ dq_details_json          │              │ failure_category         │
│ load_type                │              │ failure_reason           │
│ records_expected         │              │ resolution_status        │
│ records_processed        │              └──────────────────────────┘
│ bronze_path              │
│ ingestion_date (P)       │
└──────────────────────────┘

t01_ueh_ctl_field_mapping              t01_ueh_ctl_pipeline_dependency
┌──────────────────────────┐          ┌──────────────────────────┐
│ mapping_id (PK)          │          │ dependency_id (PK)       │
│ source_system            │          │ target_table             │
│ source_json_path         │          │ source_table             │
│ target_field             │          │ source_adapter_name      │
│ target_schema            │          │ dependency_type          │
│ transformation_type      │          │ required_status          │
└──────────────────────────┘          └──────────────────────────┘

Relationships:
  adapter_config 1:1 adapter_state    (on adapter_instance_id)
  adapter_config 1:N batch_registry   (on adapter_instance_id)
  batch_registry 1:N failed_ingestions(on batch_id)
  field_mapping  N:1 source_system    (many mappings per source)
```


---

# 3. BRONZE LAYER ER

One table per adapter. No relationships between Bronze tables (source-isolated).
Linked to control plane via `batch_id` + `adapter_instance_id`.

```
t01_ueh_brz_nvd_raw           t01_ueh_brz_tenable_raw      t01_ueh_brz_bmc_addm_raw
┌────────────────────┐        ┌────────────────────┐       ┌────────────────────┐
│ batch_id (FK)      │        │ batch_id (FK)      │       │ batch_id (FK)      │
│ adapter_instance_id│        │ adapter_instance_id│       │ adapter_instance_id│
│ ingestion_ts       │        │ ingestion_ts       │       │ ingestion_ts       │
│ ingestion_date (P) │        │ adapter_instance(P)│       │ ingestion_date (P) │
│ source_record_id   │        │ source_record_id   │       │ source_record_id   │
│ payload_json       │        │ payload_json       │       │ payload_json       │
│ payload_hash       │        │ payload_hash       │       │ payload_hash       │
│ schema_version     │        │ schema_version     │       │ schema_version     │
└────────────────────┘        └────────────────────┘       └────────────────────┘

source_record_id values:  CVE-2024-1234   |  plugin_id (12345)  |  ADDM key (HOST-x)

Relationship to control: batch_id → batch_registry.batch_id (N:1)
```

---

# 4. SILVER STAGE 1 (STAGING) ER

One staging table per adapter. Typed columns. Still adapter-specific schema.
No cross-table relationships (merge happens in Stage 2).

```
slv_stg_nvd_vulnerability     slv_stg_tenable_finding      slv_stg_addm_asset
┌────────────────────┐        ┌────────────────────┐       ┌────────────────────┐
│ batch_id           │        │ batch_id           │       │ batch_id           │
│ cve_id             │        │ plugin_id          │       │ addm_key           │
│ cvss31_base_score  │        │ primary_cve        │       │ addm_type          │
│ cvss31_severity    │        │ severity_id (0-4)  │       │ hostname           │
│ description_en     │        │ asset_uuid         │       │ ip_address         │
│ published_date     │        │ asset_hostname     │       │ os_class           │
│ references_json    │        │ vpr_score          │       │ cpu_count          │
│ dq_has_cve_id      │        │ state              │       │ business_service   │
│ parser_version     │        │ dq_has_plugin_id   │       │ support_group      │
└─────────┬──────────┘        └─────────┬──────────┘       └─────────┬──────────┘
          │ merge on cve_id              │ merge on finding_id        │ merge on asset_id
          ▼                              ▼                            ▼
   (Stage 2 canonical)           (Stage 2 canonical)          (Stage 2 canonical)
```

---

# 5. SILVER STAGE 2 (CANONICAL) ER — THE CORE MODEL

This is the heart of UEH. Three entities + asset subtype extensions.

```
                    slv_vulnerability_intel
                    ┌──────────────────────────┐
                    │ cve_id (PK)              │
                    │ cvss_base_score          │
                    │ severity                 │
                    │ epss_score               │
                    │ is_in_kev                │
                    │ description              │
                    │ org_id = 'global'        │  ← NO owner (public data)
                    │ source_systems_json      │
                    └────────────┬─────────────┘
                                 │ N:1 (many findings reference one CVE)
                                 │ (cve_id is nullable in finding)
                                 │
              slv_vulnerability_finding
              ┌──────────────────────────┐
              │ finding_id (PK)          │
              │ source_finding_id        │
              │ source_system            │
              │ cve_id (FK, nullable)    │──────────┘
              │ asset_id (FK)            │──────────┐
              │ severity                 │          │
              │ status                   │          │ N:1 (many findings on one asset)
              │ first_seen / last_seen   │          │
              │ org_id (inherited)       │          │
              │ owner (inherited)        │          │
              └──────────────────────────┘          │
                                                     ▼
                            slv_asset (OWNERSHIP ANCHOR)
                            ┌──────────────────────────┐
                            │ asset_id (PK)            │
                            │ asset_type               │  COMPUTE|CONTAINER|...
                            │ hostname / ip_address    │
                            │ os_family                │
                            │ org_id                   │  ← tenant boundary
                            │ owner                    │  ← responsible team
                            │ business_unit            │  ← org grouping
                            │ owner_resolved_from      │  ← lineage
                            │ criticality              │
                            └────────────┬─────────────┘
                                         │ 1:1 subtype extensions (Phase 2+)
        ┌──────────────┬─────────────────┼─────────────────┬──────────────┐
        ▼              ▼                 ▼                 ▼              ▼
  slv_asset_host  slv_asset_container slv_asset_network slv_asset_db  slv_asset_storage
  ┌───────────┐   ┌───────────┐      ┌───────────┐     ┌──────────┐  ┌───────────┐
  │ asset_id  │   │ asset_id  │      │ asset_id  │     │ asset_id │  │ asset_id  │
  │ cpu_count │   │ image_name│      │ device_typ│     │ db_engine│  │ capacity  │
  │ ram_mb    │   │ namespace │      │ firmware  │     │ version  │  │ raid_level│
  │ hypervisor│   │ pod_name  │      │ port_count│     │ port     │  │ protocol  │
  └───────────┘   └───────────┘      └───────────┘     └──────────┘  └───────────┘

KEY RELATIONSHIPS:
  slv_asset 1:N slv_vulnerability_finding   (asset_id)    one asset, many findings
  slv_vulnerability_intel 1:N finding       (cve_id)      one CVE, many findings
  slv_asset 1:1 slv_asset_<subtype>         (asset_id)    extension per asset_type

CARDINALITY NOTES:
  - finding.cve_id is NULLABLE (some findings have no CVE)
  - finding.asset_id should always resolve (else owner=UNASSIGNED)
  - intel is global (shared across all orgs, no ownership)
```


---

# 6. GOLD LAYER ER

## MVP — Wide Denormalized (Phase 1)

```
gld_exposure_summary (one row per finding, fully enriched)
┌──────────────────────────────────────────┐
│ finding_id (PK)                          │
│ cve_id            ← from finding/intel    │
│ cvss_base_score   ← from intel            │
│ epss_score        ← from intel            │
│ is_in_kev         ← from intel            │
│ asset_id          ← from finding          │
│ asset_hostname    ← from asset            │
│ asset_criticality ← from asset            │
│ org_id            ← from asset (ownership)│
│ owner             ← from asset (ownership)│
│ business_unit     ← from asset (ownership)│
│ risk_score        ← computed              │
│ priority_rank     ← computed              │
│ ingestion_date (P)│                       │
└──────────────────────────────────────────┘
   = JOIN of (finding + intel + asset), denormalized for fast dashboard read
```

## Evolution — Star Schema (Phase 3)

```
                  gld_dim_time
                  ┌────────────┐
                  │ time_key PK│
                  │ date,month │
                  │ quarter    │
                  └─────┬──────┘
                        │
  gld_dim_asset    gld_fact_exposure    gld_dim_cve
  ┌────────────┐   ┌──────────────────┐ ┌────────────┐
  │ asset_key PK│◄─│ asset_key (FK)   │ │ cve_key PK │
  │ hostname   │   │ cve_key (FK)     │►│ cvss_score │
  │ criticality│   │ time_key (FK)    │ │ epss_score │
  │ org_id     │   │ adapter_key (FK) │ │ is_in_kev  │
  │ owner      │   │ finding_id       │ └────────────┘
  │ business_un│   │ risk_score (M)   │
  │ (SCD2)     │   │ days_open (M)    │ gld_dim_adapter
  └────────────┘   │ is_open (M)      │ ┌────────────┐
                   │ is_critical (M)  │ │ adapter_key│
                   │ is_kev (M)       │◄│ source_sys │
                   │ is_sla_breach(M) │ └────────────┘
                   └──────────────────┘
                   (M) = measure/fact

Ownership in star: dim_asset carries org_id/owner. fact_exposure inherits
via asset_key. Filtering by owner = JOIN fact → dim_asset → WHERE owner.
```

---

# 7. SERVING LAYER (POSTGRESQL) ER

```
SYNCED FROM GOLD (read-only, refreshed daily):

exposure_findings                    cve_intelligence
┌──────────────────────┐            ┌──────────────────────┐
│ finding_id (PK)      │            │ cve_id (PK)          │
│ cve_id (FK)          │───────────►│ cvss_score           │
│ asset_id (FK)        │            │ epss_score           │
│ risk_score           │            │ is_in_kev            │
│ org_id  ← RLS        │            └──────────────────────┘
│ owner   ← RLS        │
│ business_unit ← RLS  │            asset_inventory
└──────────┬───────────┘            ┌──────────────────────┐
           │ 1:N                    │ asset_id (PK)        │
           ▼                        │ org_id, owner        │
UI-MANAGED (operational, RLS-enforced):                    │
                                    └──────────────────────┘
remediation_assignments
┌──────────────────────┐            risk_exceptions       audit_log
│ id (PK)              │            ┌──────────────┐      ┌──────────────┐
│ finding_id (FK)      │            │ id (PK)      │      │ id (PK)      │
│ assigned_to          │            │ cve_id       │      │ action       │
│ assigned_by          │            │ approved_by  │      │ entity_id    │
│ status               │            │ expiry_date  │      │ performed_by │
│ due_date             │            └──────────────┘      └──────────────┘
│ org_id  ← RLS        │
└──────────────────────┘

RLS (Row Level Security) enforced on: org_id, business_unit, owner
  → User only sees rows matching their org + business unit
```

---

# 8. CROSS-LAYER KEY LINEAGE

How a single record's keys flow across all layers:

```
SOURCE API           NVD returns CVE-2024-3400
     │
BRONZE               brz_nvd_raw.source_record_id = 'CVE-2024-3400'
     │               brz_nvd_raw.batch_id = 'batch_X'
     │
STAGE 1              slv_stg_nvd_vulnerability.cve_id = 'CVE-2024-3400'
     │
STAGE 2 (intel)      slv_vulnerability_intel.cve_id = 'CVE-2024-3400' (PK)
     │
     │   ← Tenable finding references this CVE on an asset
     │
STAGE 2 (finding)    slv_vulnerability_finding.cve_id = 'CVE-2024-3400' (FK)
     │               finding_id = md5('TENABLE'+plugin_id+asset_uuid)
     │               asset_id = md5('BMC_ADDM'+addm_key)
     │
GOLD                 gld_exposure_summary.finding_id + cve_id + asset_id
     │
SERVING              exposure_findings.finding_id (PostgreSQL)

KEY GENERATION RULES:
  cve_id     = natural key from source (CVE-YYYY-NNNNN)
  finding_id = md5(source_system + source_finding_id + source_asset_id)
  asset_id   = md5(source_system + source_asset_id)
  batch_id   = batch_<timestamp>_<adapter_instance_id>
```

---

# 9. OWNERSHIP RELATIONSHIPS

```
                    OWNERSHIP FLOWS DOWNWARD FROM ASSET

slv_asset (OWNERSHIP ORIGIN)
   org_id = 'org001'
   owner = 'Finance-Team'
   business_unit = 'Finance'
        │
        │ asset_id (FK) propagates ownership
        ▼
slv_vulnerability_finding (INHERITS)
   org_id = 'org001'        ← copied from asset during Stage 2 JOIN
   owner = 'Finance-Team'   ← copied from asset
        │
        ▼
gld_exposure_summary (CARRIES)
   org_id, owner, business_unit  ← for filtering/RLS
        │
        ▼
PostgreSQL exposure_findings (ENFORCES)
   RLS policy: WHERE org_id = user.org AND business_unit = user.bu

slv_vulnerability_intel = NO OWNERSHIP (org_id='global', public CVE data)

OWNERSHIP RESOLUTION PRIORITY (in slv_asset):
   1. CMDB owner (authoritative)
   2. ADDM support_group
   3. ownership_rules table match (hostname/IP pattern)
   4. 'UNASSIGNED' (flagged for review)
```

---

# 10. FULL RELATIONSHIP MATRIX

| Parent Table | Child Table | Key | Cardinality | Layer |
|--------------|-------------|-----|-------------|-------|
| adapter_config | adapter_state | adapter_instance_id | 1:1 | Control |
| adapter_config | batch_registry | adapter_instance_id | 1:N | Control |
| batch_registry | failed_ingestions | batch_id | 1:N | Control |
| batch_registry | brz_*_raw | batch_id | 1:N | Bronze |
| brz_nvd_raw | slv_stg_nvd_vulnerability | batch_id | 1:N | Bronze→Silver |
| slv_stg_nvd | slv_vulnerability_intel | cve_id | N:1 (merge) | Silver |
| slv_stg_tenable | slv_vulnerability_finding | finding_id | N:1 (merge) | Silver |
| slv_stg_addm | slv_asset | asset_id | N:1 (merge) | Silver |
| slv_asset | slv_vulnerability_finding | asset_id | 1:N | Silver |
| slv_vulnerability_intel | slv_vulnerability_finding | cve_id | 1:N (nullable) | Silver |
| slv_asset | slv_asset_host | asset_id | 1:1 | Silver (P2) |
| slv_asset | slv_asset_container | asset_id | 1:1 | Silver (P2) |
| slv_asset | slv_asset_network | asset_id | 1:1 | Silver (P3) |
| slv_asset | slv_asset_database | asset_id | 1:1 | Silver (P3) |
| (finding+intel+asset) | gld_exposure_summary | composite | JOIN | Gold |
| gld_dim_asset | gld_fact_exposure | asset_key | 1:N | Gold (P3) |
| gld_dim_cve | gld_fact_exposure | cve_key | 1:N | Gold (P3) |
| gld_dim_time | gld_fact_exposure | time_key | 1:N | Gold (P3) |
| exposure_findings | remediation_assignments | finding_id | 1:N | Serving |

(P2) = Phase 2, (P3) = Phase 3

---

# RELATIONSHIP CARDINALITY SUMMARY

```
1 adapter_config    →  N batches
1 batch             →  N bronze records
1 asset             →  N findings          (one server, many vulnerabilities)
1 CVE               →  N findings          (one CVE, found on many assets)
1 asset             →  1 subtype extension (host/container/network/...)
1 finding           →  N remediation assignments (reassignments over time)
1 org_id            →  N assets/findings   (tenant isolation boundary)
```

---

*End of ER Model Document*
