# UEH Implementation Roadmap — 3-Month Plan (Data Engineer Guide)

## Post-Refactoring: Two-Stage Silver Architecture

**Document Version:** 3.0  
**Date:** June 2026  
**Scope:** Complete implementation with two-stage Silver, parser classes, DQ gates  
**Duration:** 12 weeks (3 months)  
**Team:** 2 Senior DE, 1 Junior DE, 1 Full-Stack Developer  
**Reference:** UEH Architecture v4 + Refactoring Spec

---

## Architecture (Post-Refactoring)

```
Bronze (per adapter, raw)
    ↓ Generic Bronze DAG (polls RAW_COMPLETE)
Silver Stage 1 (per adapter, typed staging)
    ↓ PythonSensor(batch_status + dq_status) + TriggerDagRunOperator
Silver Stage 2 (canonical entities, cross-source merge)
    ↓ Triggered by Stage 1
Gold (enriched analytics, KPIs)
    ↓ Daily scheduled
PostgreSQL (serving layer + operational decisions)
```

---

## Control Table Lifecycle (Updated for Two-Stage Silver)

```
batch_registry.batch_status flow:

RAW_COMPLETE → BRONZE_COMPLETE → STAGING_COMPLETE → SILVER_COMPLETE → GOLD_COMPLETE
                                        ↑ NEW              ↑ renamed context

batch_registry.dq_status flow (NEW):

NOT_CHECKED → PASSED/WARNING/FAILED (set at each stage)
```

---

## 12-Week Sprint Breakdown

```
Week  1-2:  Complete NVD end-to-end (new two-stage pipeline)
Week  3-4:  Tenable Bronze + Stage 1 parser
Week  5-6:  ADDM/CMDB Bronze + Stage 1 parser + Tenable Stage 2
Week  7-8:  EPSS/CISA KEV + Asset canonical (Stage 2)
Week  9-10: Gold layer + cross-source correlation
Week 11-12: Hardening, idempotency validation, production prep
```

---


## Incremental Load & SCD Strategy

| Layer | Load Strategy | SCD Type | Idempotency |
|-------|--------------|----------|-------------|
| **Bronze** | APPEND always (immutable) | N/A | CHECK exists → SKIP |
| **Silver Stage 1** | DELETE batch + INSERT fresh | N/A | DELETE WHERE batch_id + INSERT |
| **Silver Stage 2** | MERGE on business key | **Type 1** (latest wins) | MERGE is naturally idempotent |
| **Gold** | OVERWRITE partition daily | N/A | Naturally idempotent |

**Why SCD Type 1 (not Type 2):**
- Bronze preserves FULL history (every version of every record)
- Iceberg time-travel provides historical access on Silver tables
- Type 2 doubles storage with minimal benefit when Bronze exists
- Gold queries are simpler without is_current filters

---

## Control Tables — Phased Introduction

| Phase | Table | When | Used By | Purpose |
|-------|-------|------|---------|---------|
| Phase 1 (Week 1) | adapter_config | Day 1 | NiFi, DAG Factory, Parsers | What to ingest |
| Phase 1 (Week 1) | adapter_state | Day 1 | NiFi (watermark) | Where to resume |
| Phase 1 (Week 1) | batch_registry (+dq_status) | Day 1 | ALL DAGs, ALL Spark jobs | Lifecycle + DQ gate |
| Phase 1 (Week 1) | field_mapping | Day 1 | Stage 2 canonical mappings | Configurable transforms |
| Phase 2 (Week 5) | failed_ingestions | Week 5 | Error tracking | Dead letter registry |
| Phase 2 (Week 7) | pipeline_dependency | Week 7 | Stage 2 multi-adapter deps | Gold readiness |
| Phase 3 (Week 9) | adapter_config_history | Week 9 | Governance audit | Who changed what |
| Phase 3 (Week 10) | platform_metrics | Week 10 | Observability | Platform health |
| Phase 3 (Week 11) | sla_definitions | Week 11 | SLA watchdog | Breach detection |
| Phase 3 (Week 11) | replay_queue | Week 11 | Controlled reprocessing | Safe replay |

---

## WEEKS 1-2: NVD Two-Stage Pipeline

### Tasks
| # | Task | Owner | Days |
|---|------|-------|------|
| 1.1 | ALTER TABLE: add dq_status to batch_registry | DE1 | 0.5 |
| 1.2 | Create slv_stg_nvd_vulnerability table | DE1 | 0.5 |
| 1.3 | Deploy nvd_parser_v1.py + stage1_processor.py | DE1 | 0.5 |
| 1.4 | Test Stage 1: Bronze → staging | DE1 | 1 |
| 1.5 | Deploy stage2_vuln_intel.py | DE2 | 0.5 |
| 1.6 | Test Stage 2: staging → canonical MERGE | DE2 | 1 |
| 1.7 | Deploy refactored DAGs (PythonSensor + TriggerDagRun) | JDE | 1 |
| 1.8 | End-to-end validation (5 stages) | All | 2 |
| 1.9 | Idempotency test: re-run same batch | DE1 | 0.5 |
| 1.10 | DQ gate test: inject bad data → verify block | DE2 | 0.5 |

### Exit Criteria
- Full pipeline: RAW → BRONZE → STAGING → SILVER → GOLD
- dq_status populated at each transition
- Re-run = no duplicates (idempotent)
- DQ=FAILED blocks downstream

---

## WEEKS 3-4: Tenable Bronze + Stage 1

| # | Task | Owner | Days |
|---|------|-------|------|
| 3.1 | Tenable API access | DE1 | Day 1 |
| 3.2 | NiFi Tenable async export flow | DE2 | 3 |
| 3.3 | Seed adapter_config + adapter_state | DE1 | 0.5 |
| 3.4 | Create t01_ueh_brz_tenable_raw | JDE | 0.5 |
| 3.5 | Create slv_stg_tenable_finding | JDE | 0.5 |
| 3.6 | Register TENABLE in ADAPTER_REGISTRY | DE1 | 0.5 |
| 3.7 | Test tenable_parser_v1 end-to-end | DE1 | 1.5 |
| 3.8 | Validate 10K+ records in staging | DE1+DE2 | 1 |

---

## WEEKS 5-6: ADDM + Tenable Stage 2

| # | Task | Owner | Days |
|---|------|-------|------|
| 5.1 | silver_stage2_vuln_findings.py | DE1 | 2 |
| 5.2 | finding_id generation + severity mapping | DE1 | 1 |
| 5.3 | ADDM API access + NiFi flow | DE2 | 3 |
| 5.4 | addm_parser_v1 + Stage 1 test | DE2 | 2 |
| 5.5 | Create failed_ingestions table | JDE | 0.5 |
| 5.6 | Add failure tracking to all Spark jobs | DE1 | 1 |

---

## WEEKS 7-8: Multi-Source Canonical

| # | Task | Owner | Days |
|---|------|-------|------|
| 7.1 | EPSS + CISA KEV Bronze + parsers | DE1 | 2 |
| 7.2 | Extend Stage 2 intel: NVD + EPSS + CISA merge | DE1 | 2 |
| 7.3 | silver_stage2_assets.py (ADDM → canonical) | DE2 | 2 |
| 7.4 | pipeline_dependency table + Gold gating | DE2 | 1 |
| 7.5 | Validate cross-source enrichment | All | 1 |

---

## WEEKS 9-10: Gold + Observability

| # | Task | Owner | Days |
|---|------|-------|------|
| 9.1 | Gold exposure_summary (reads canonical Silver) | DE1 | 2 |
| 9.2 | Gold cve_enriched + risk_metrics | DE2 | 2 |
| 9.3 | adapter_config_history table | DE2 | 0.5 |
| 9.4 | platform_metrics table + collector DAG | JDE | 2.5 |
| 9.5 | PostgreSQL sync (Gold → Cloud SQL) | FSD | 2 |
| 9.6 | Dashboard API endpoints | FSD | 3 |

---

## WEEKS 11-12: Production Hardening

| # | Task | Owner | Days |
|---|------|-------|------|
| 11.1 | sla_definitions + watchdog DAG | DE1 | 2 |
| 11.2 | replay_queue + replay DAG | DE2 | 2.5 |
| 11.3 | Idempotency validation (all layers, all adapters) | DE1 | 1 |
| 11.4 | Iceberg maintenance DAG | JDE | 1 |
| 11.5 | Security review + documentation | All | 2 |
| 11.6 | UAT deployment + demo | All | 2 |

---

## Success Metrics

| Metric | Target |
|--------|--------|
| Adapters (full pipeline) | 5+ |
| Daily success rate | >95% |
| DQ gate blocking | 100% of bad batches |
| Idempotency | 100% safe re-runs |
| Gold freshness | <6 hours |
| SLA breaches in prod | 0 |
