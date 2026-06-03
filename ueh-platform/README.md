# Unified Vulnerability Exposure Hub (UEH)

## Enterprise Cybersecurity Data Platform

UEH centralizes vulnerability and exposure data from multiple security tools into a single governed platform built on a medallion architecture (Bronze → Silver → Gold).

---

## Project Structure

```
ueh-platform/
├── ddl/                    # Iceberg table definitions
│   ├── 01_databases.sql        # Database/namespace creation
│   ├── 02_control_tables.sql   # Control framework tables
│   └── 03_bronze_nvd.sql       # Bronze layer table (NVD)
│
├── seed/                   # Initial data population
│   └── 01_nvd_adapter_seed.sql # NVD adapter configuration
│
├── scripts/                # Operational scripts
│   ├── hdfs_init.sh            # HDFS folder structure creation
│   └── deploy_dev.sh           # Dev environment deployment
│
├── nifi/                   # NiFi flow configurations
│   └── nvd_ingestion_flow.md   # NVD process group specification
│
├── spark/                  # PySpark jobs
│   └── bronze/
│       ├── __init__.py
│       ├── base_bronze_loader.py   # Reusable base class
│       └── bronze_nvd_loader.py    # NVD-specific loader
│
├── airflow/                # Airflow DAGs
│   └── dags/
│       ├── ueh_ingest_nvd.py       # DAG 1: Ingestion orchestration
│       └── ueh_bronze_load_nvd.py  # DAG 2: Bronze loading
│
├── validation/             # Validation & testing
│   └── bronze_nvd_checks.sql  # Post-load validation queries
│
├── config/                 # Environment configurations
│   ├── dev.yaml                # Dev environment config
│   └── prod.yaml               # Prod environment config
│
└── docs/                   # Architecture documentation
    ├── architecture_decisions.md   # ADRs
    └── implementation_guide.md     # Step-by-step guide
```

---

## Quick Start (Dev Environment)

```bash
# 1. Create HDFS folder structure
./scripts/hdfs_init.sh dev

# 2. Create Iceberg databases and tables
spark-sql -f ddl/01_databases.sql
spark-sql -f ddl/02_control_tables.sql
spark-sql -f ddl/03_bronze_nvd.sql

# 3. Seed adapter configuration
spark-sql -f seed/01_nvd_adapter_seed.sql

# 4. Deploy NiFi flow (manual via NiFi UI or CLI)
# See nifi/nvd_ingestion_flow.md

# 5. Deploy Airflow DAGs
cp airflow/dags/*.py $AIRFLOW_DAGS_FOLDER/

# 6. Validate
spark-sql -f validation/bronze_nvd_checks.sql
```

---

## Architecture Principles

1. **Bronze = Immutable, Schema-Agnostic** — metadata + payload_json only
2. **One Bronze table per source** — source isolation
3. **Metadata-driven orchestration** — control tables drive everything
4. **2-DAG pattern** — Ingestion DAG + Bronze Load DAG (decoupled)
5. **No file sensors** — PythonSensor polls control tables
6. **Environment at database level** — `ueh_{env}_{layer}`

---

## Technology Stack

- Apache Iceberg + Hive Metastore
- Spark / PySpark (CDE)
- Apache Airflow
- Apache NiFi
- HDFS

---

## First Adapter: NVD (National Vulnerability Database)

- **API:** https://services.nvd.nist.gov/rest/json/cves/2.0
- **Type:** vulnerability_intel
- **Mode:** INCREMENTAL (lastModStartDate)
- **Pagination:** Offset-based (startIndex + resultsPerPage)
- **Rate Limit:** 5 req/30s with API key
