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

Please refer to the full document pushed to the repository for complete details including task breakdowns, team allocation, exit criteria, and risk register.
