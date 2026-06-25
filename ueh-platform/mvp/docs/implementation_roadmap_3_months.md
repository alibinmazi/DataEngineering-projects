# UEH Implementation Roadmap — 3-Month Plan

## Phase 1 (Current) → Phase 2 (Tenable + Asset Inventory)

**Document Version:** 1.0  
**Date:** June 2026  
**Scope:** Post-MVP implementation plan for Tenable scanner + ADDM/CMDB asset inventory  
**Duration:** 12 weeks (3 months)  
**Team:** 2 Senior DE, 1 Junior DE, 1 Full-Stack Developer

---

## Current State (Phase 1 — MVP In Progress)

| Component | Status |
|-----------|--------|
| Control Tables (adapter_config, adapter_state, batch_registry, field_mapping) | Created |
| Bronze NVD (NiFi → HDFS → Spark → Iceberg) | In progress |
| Silver NVD (vulnerability_intel) | Testing with dummy data |
| Gold Layer (exposure_summary, cve_enriched, risk_metrics) | Code ready |
| UEH Dashboard (separate repo) | Parallel development |

**Phase 1 Completion Target:** End of Week 2

---

## 12-Week Sprint Breakdown

```
Week  1-2:  Phase 1 completion (NVD end-to-end, fix issues)
Week  3-4:  Tenable Bronze (API integration, NiFi, raw ingestion)
Week  5-6:  ADDM/CMDB Bronze + Tenable Silver
Week  7-8:  Asset Silver + EPSS/CISA KEV Bronze
Week  9-10: Gold layer enrichment + cross-source correlation
Week 11-12: Hardening, testing, observability, production prep
```

---

# WEEKS 1-2: Complete Phase 1 (NVD MVP)

## Objective
Get NVD flowing end-to-end: API → NiFi → HDFS → Bronze Iceberg → Silver → Gold

## Tasks
| # | Task | Owner | Days |
|---|------|-------|------|
| 1.1 | Fix Bronze NVD loader (CDE compatibility) | DE1 | 1 |
| 1.2 | NiFi NVD flow operational (API → HDFS → RAW_COMPLETE) | DE2 | 2 |
| 1.3 | Validate NVD Bronze end-to-end (DAG 1 → DAG 2) | DE1 | 1 |
| 1.4 | Run Silver NVD against real data (not dummy) | DE1 | 1 |
| 1.5 | Validate Gold layer with real NVD data | DE2 | 1 |
| 1.6 | Airflow DAG 1-4 deployed and stable | JDE | 2 |
| 1.7 | Document learnings, fix pain points | All | 1 |

## Exit Criteria
- NVD daily run successful for 3+ consecutive days
- batch_registry lifecycle: RAW_COMPLETE → BRONZE_COMPLETE → SILVER_COMPLETE
- Gold tables populated with real CVE data
- No manual intervention needed for daily run

---

# WEEKS 3-4: Tenable Bronze Integration

## Objective
Ingest Tenable vulnerability scan data into Bronze

## Tenable API Complexity
| Aspect | NVD | Tenable |
|--------|-----|---------|
| Auth | Simple API key | access_key + secret_key |
| Pagination | Offset-based | Async export (request → poll → download chunks) |
| Volume | ~200 CVEs/day | 10,000-100,000+ findings |
| Watermark | lastModStartDate | last_found (unix timestamp) |
| Instances | Single global | Multi-instance (US, EU, APAC) |

## Tasks
| # | Task | Owner | Days |
|---|------|-------|------|
| 3.1 | Tenable API access: Request keys, firewall rules | DE1 + Security | Day 1 |
| 3.2 | Study Tenable Export API (async pattern) | DE1 | 1 |
| 3.3 | Create adapter_config entry for tenable_prod_us_01 | DE1 | 0.5 |
| 3.4 | Design NiFi flow for Tenable async export | DE2 | 2 |
| 3.5 | Build NiFi Tenable flow (request/poll/download) | DE2 | 3 |
| 3.6 | Create Bronze DDL: t01_ueh_brz_tenable_raw | JDE | 0.5 |
| 3.7 | Test NiFi flow (small 24h export) | DE2 | 1 |
| 3.8 | Validate Bronze end-to-end | DE1 | 0.5 |
| 3.9 | DAG 1 + DAG 2 for Tenable | JDE | 1.5 |
| 3.10 | First full ingestion (all active findings) | DE1+DE2 | 1 |

## Tenable NiFi Flow (Async Export)
```
[POST /vulns/export] → Get export_uuid
    ↓
[POLL /vulns/export/{uuid}/status] → Wait for FINISHED (loop 30s)
    ↓
[FOR EACH chunk: GET .../chunks/{id}] → PutHDFS
    ↓
[Update batch_registry → RAW_COMPLETE]
```

## Exit Criteria (Week 4)
- Tenable export API working (auth + export + download)
- NiFi handles full async lifecycle
- Bronze table populated with real findings (>10K records)
- Handles multi-chunk downloads without failure

---

# WEEKS 5-6: ADDM/CMDB Bronze + Tenable Silver

## Track A: Tenable Silver (DE1)
| # | Task | Days |
|---|------|------|
| 5.1 | Design Tenable field mappings (study payload_json) | 1 |
| 5.2 | Seed field_mapping table (18 mappings) | 1 |
| 5.3 | Test silver_vulnerability_findings.py with real data | 2 |
| 5.4 | Handle Tenable severity LOOKUP (0-4 → enum) | 0.5 |
| 5.5 | Validate finding_id uniqueness + determinism | 0.5 |
| 5.6 | DAG 3 routes Tenable correctly | 0.5 |

## Track B: ADDM/CMDB Bronze (DE2)
| # | Task | Days |
|---|------|------|
| 5.7 | ADDM API access (credentials, firewall) | Day 1 |
| 5.8 | Study ADDM API (endpoints, pagination, data model) | 1 |
| 5.9 | Register bmc_addm_prod_01 in adapter_config | 0.5 |
| 5.10 | Build NiFi flow for ADDM (standard REST) | 2 |
| 5.11 | Create Bronze DDL (t01_ueh_brz_bmc_addm_raw) | 0.5 |
| 5.12 | Test + validate Bronze | 1.5 |
| 5.13 | DAG 1 + DAG 2 for ADDM | 1 |

## Exit Criteria (Week 6)
- Tenable Silver producing findings in slv_vulnerability_findings
- ADDM/CMDB Bronze operational (daily ingestion)
- Field mappings tested and validated

---

# WEEKS 7-8: Asset Silver + EPSS/CISA KEV Enrichment

## Track A: EPSS + CISA KEV (DE1)
| # | Task | Days |
|---|------|------|
| 7.1 | EPSS Bronze (NiFi flow for FIRST.org API) | 1 |
| 7.2 | CISA KEV Bronze (single JSON download) | 0.5 |
| 7.3 | Create Bronze tables for EPSS + CISA | 0.5 |
| 7.4 | Seed field_mapping (EPSS → epss_score, percentile) | 0.5 |
| 7.5 | Seed field_mapping (CISA → is_in_kev, dates) | 0.5 |
| 7.6 | Test Silver enrichment (NVD + EPSS + CISA merge) | 1 |
| 7.7 | Validate: CVEs have EPSS + KEV status | 0.5 |

## Track B: Asset Silver (DE2)
| # | Task | Days |
|---|------|------|
| 7.8 | Design ADDM field mappings → slv_assets | 1 |
| 7.9 | Seed field_mapping for ADDM | 1 |
| 7.10 | Test silver_assets.py with ADDM data | 2 |
| 7.11 | Validate MERGE on asset_id (upsert works) | 1 |
| 7.12 | Asset type classification (COMPUTE/NETWORK) | 0.5 |
| 7.13 | CMDB field mapping + test | 1 |

## Exit Criteria (Week 8)
- slv_vulnerability_intel has NVD + EPSS + CISA enrichment
- slv_vulnerability_findings has Tenable findings
- slv_assets has ADDM/CMDB records
- All 3 Silver tables populated with real data

---

# WEEKS 9-10: Gold Layer + Cross-Source Correlation

## Tasks
| # | Task | Owner | Days |
|---|------|-------|------|
| 9.1 | Run gold_exposure_summary.py (real JOIN of all 3 Silver) | DE1 | 2 |
| 9.2 | Validate risk_score computation | DE1 | 1 |
| 9.3 | Asset correlation: findings → assets by IP/hostname | DE2 | 2 |
| 9.4 | Run gold_cve_enriched.py (full CVE intel + org exposure) | DE1 | 1 |
| 9.5 | Run gold_risk_metrics.py (aggregated KPIs) | DE2 | 1 |
| 9.6 | DAG 4 end-to-end (daily Gold rebuild) | JDE | 1 |
| 9.7 | Dashboard SQL queries for Grafana/Superset | FSD | 2 |
| 9.8 | API endpoints for chatbot/dashboard | FSD | 3 |

## Exit Criteria (Week 10)
- Gold tables producing meaningful enriched data
- Risk scoring validated by security team
- Dashboard queries returning actionable insights
- Chatbot can query "top 10 critical vulnerabilities"

---

# WEEKS 11-12: Hardening & Production Prep

## Tasks
| # | Task | Owner | Days |
|---|------|-------|------|
| 11.1 | SLA monitoring for all adapters | DE1 | 1 |
| 11.2 | Error handling audit (dead-letter, retry) | DE2 | 2 |
| 11.3 | Performance tuning (Spark, Iceberg compaction) | DE1 | 2 |
| 11.4 | Data quality report across Silver | JDE | 2 |
| 11.5 | Security review (secrets, logs) | DE1 | 1 |
| 11.6 | Documentation update | JDE | 2 |
| 11.7 | UAT deployment plan | DE1+DE2 | 1 |
| 11.8 | Demo to stakeholders | All | 1 |
| 11.9 | Phase 3 planning (next 5 adapters) | All | 1 |

## Exit Criteria (Week 12)
- Platform running 7+ days without manual intervention
- 5+ adapters operational (NVD, EPSS, CISA, Tenable, ADDM/CMDB)
- Full Bronze → Silver → Gold for all
- Ready for UAT deployment

---

## Team Allocation

| Week | DE1 | DE2 | JDE | FSD |
|------|-----|-----|-----|-----|
| 1-2 | NVD fix + validate | NiFi NVD | DAGs | UI |
| 3-4 | Tenable API + prep | NiFi Tenable | Bronze DDL + DAGs | UI |
| 5-6 | Tenable Silver | ADDM NiFi + Bronze | Monitoring | UI |
| 7-8 | EPSS + CISA | Asset Silver | Operational stability | UI |
| 9-10 | Gold enrichment | Gold metrics + correlation | DAG 4 + validation | API + dashboard |
| 11-12 | Performance + security | Error handling | DQ + docs | Demo |

---

## Key Risks

| Risk | Mitigation |
|------|-----------|
| Tenable API access delayed | Request in Week 1 — don't wait |
| ADDM/CMDB access delayed | Request in Week 2 |
| Large Tenable exports timeout | Increase NiFi/Spark timeouts |
| Asset correlation fails (IP mismatch) | Fuzzy matching (hostname OR ip OR fqdn) |

---

## Success Metrics (End of 3 Months)

| Metric | Target |
|--------|--------|
| Adapters operational | 5+ |
| Daily success rate | >95% |
| Gold exposure records | 10,000+ |
| Manual intervention | <1x/week |
| Time to onboard next adapter | <2 weeks |
