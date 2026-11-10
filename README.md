# Walmart End-to-End Data Engineering Project

An end-to-end data engineering portfolio project using Databricks, Unity Catalog, Delta Lake, PySpark, dbt, Apache Airflow, Docker Compose, SQL, and Git. It ingests retail CSV data into Bronze Delta tables, transforms it through Silver and Gold layers, checks data quality, and tracks dimension history with dbt snapshots.

> **Project status:** The pipeline has completed successfully in the development environment. The row counts and validation outcomes below are observed results, not guarantees for future runs.

## Table of contents

- [Project overview](#project-overview)
- [Architecture and data layers](#architecture-and-data-layers)
- [Data quality](#data-quality)
- [Airflow orchestration](#airflow-orchestration)
- [Repository structure](#repository-structure)
- [Prerequisites and configuration](#prerequisites-and-configuration)
- [Run the pipeline](#run-the-pipeline)
- [Security](#security)
- [Limitations and future improvements](#limitations-and-future-improvements)

## Project overview

The project turns six retail datasets—customers, employees, orders, order items, products, and stores—into analytics-ready data. It uses a Databricks Free Trial workspace. Raw CSV files, Auto Loader schema metadata, and ingestion checkpoints are stored in Unity Catalog Volumes; there is no separate Azure Data Lake Storage or Amazon S3 landing layer.

The Databricks ingestion job is configured in the workspace and triggered by Airflow. The dbt project and orchestration code are in this repository.

## Architecture and data layers

```text
Retail CSV files in a Unity Catalog Volume
                  |
                  v
     Databricks Auto Loader ingestion job
                  |
                  v
          Bronze Delta tables
                  |
                  v
       Silver technical models
                  |
                  v
        Silver business OBT
           /             \
          v               v
  Gold ephemeral models   (order-item grain)
          |
          +--> dbt snapshots --> Gold dimensions
          |
          +--------------------> Gold fact model

       Apache Airflow orchestrates the stages
```

The source files are expected under `/Volumes/de_project/bronze/raw_data`. The ingestion job writes the six Bronze tables in `de_project.bronze`; `_ingested_at` is used by dbt source freshness checks.

The Silver technical models use the `_t` suffix. The Silver business model, `silver_b.obt_b`, has one row per order item and joins order, customer, product, and store data. Employee data follows a separate dimension path to avoid multiplying fact rows when a store has multiple employees.

The Gold layer includes customer, employee, product, store, and order dimensions, plus `de_project.gold.fact_orders`. The fact table has order-item grain and includes order/customer/product/store identifiers and quantity and monetary measures. dbt snapshots track historical dimension records; the order snapshot uses `order_id` and an order-update timestamp.

## Data quality

The dbt project includes source freshness checks, Silver model tests, and a singular OBT test for null business keys. Additional development validation checked keys, uniqueness, referential integrity, fact grain, snapshot metadata, and aggregate measures.

Observed development counts:

| Table | Rows |
|---|---:|
| Bronze customers | 2,000 |
| Bronze employees | 250 |
| Bronze orders | 10,000 |
| Bronze order items | 30,021 |
| Bronze products | 500 |
| Bronze stores | 25 |
| Gold `dim_customers` | 1,991 |
| Gold `dim_employees` | 250 |
| Gold `dim_products` | 500 |
| Gold `dim_stores` | 25 |
| Gold `dim_orders` | 10,000 |
| Gold `fact_orders` | 30,021 |

In the recorded fact validation, there were 30,021 rows, 10,000 distinct orders, 30,021 distinct order-item IDs, no nulls in the checked keys, no duplicate order-item IDs returned by the check, and no orphans in the checked relationships. Re-run validations after code or data changes.

## Airflow orchestration

The `orchestrate` DAG runs these tasks in order:

1. `run_databricks_job` — trigger and wait for the Bronze ingestion job.
2. `clean_target` and `source_freshness`.
3. Run and test `silver_t`, then run and test `silver_b`.
4. Run Gold ephemeral models, execute dbt snapshots, and build the Gold fact model.

The local Docker Compose setup uses Apache Airflow 3.1.8 with CeleryExecutor. It is intended for local development, not production.

## Repository structure

```text
.
├── README.md
├── main.py                         # Convenience launcher for local Airflow Compose
├── pyproject.toml                  # Python project metadata and dbt dependencies
├── uv.lock
└── airflow_dbt_project/
    ├── .env.example
    ├── Dockerfile
    ├── docker-compose.yaml
    ├── dags/
    │   ├── orchestrate.py
    │   └── databricks_connection_test.py
    └── walmart_dbt_project/
        ├── dbt_project.yml
        ├── profiles.example.yml
        ├── macros/
        ├── models/
        │   ├── gold/
        │   ├── silver_b/
        │   ├── silver_t/
        │   └── source/
        ├── snapshots/
        └── tests/
```

Compose mounts `config/` and `plugins/` paths for local Airflow customization; those directories are not currently tracked in this repository. Generated dbt directories such as `target/`, `logs/`, and `dbt_packages/` are excluded from version control.

## Prerequisites and configuration

You need:

- Docker Desktop with Docker Compose and enough resources for the Airflow stack.
- A Databricks workspace with Unity Catalog enabled, a SQL warehouse, and the required catalog, schema, table, and Volume permissions.
- A Databricks job for Bronze ingestion (the DAG currently references a job ID in `airflow_dbt_project/dags/orchestrate.py`).
- Git. Python 3.12 is specified for local project tooling.

Cloud resources may incur charges; stop them when they are not needed.

### Airflow environment

From the repository root, copy `airflow_dbt_project/.env.example` to `airflow_dbt_project/.env`. Set the Databricks host and token, and replace the JWT secret with a unique random value. For example, generate one with:

```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

The example also includes local Airflow UI username/password settings. Change them to values you choose before starting the stack. The Compose defaults of `airflow` / `airflow` are for local development only and must not be used in a shared or production deployment.

### dbt profile

Create a local `profiles.yml` based on `airflow_dbt_project/walmart_dbt_project/profiles.example.yml`. Configure the Databricks host, catalog, schema, SQL warehouse HTTP path, and authentication. Keep the credential-bearing profile out of version control.

## Run the pipeline

After configuring the environment and dbt profile, start the local Airflow stack from the repository root with either the convenience launcher:

```bash
python main.py
```

or directly with Docker Compose:

```bash
cd airflow_dbt_project
docker compose up -d --build
docker compose ps
```

Open the Airflow UI at `http://localhost:8080`, sign in with the local credentials you configured, and trigger the `orchestrate` DAG. The DAG runs the Databricks job and then the dbt stages. To inspect worker logs:

```bash
cd airflow_dbt_project
docker compose logs --tail 100 airflow-worker
```

You can also run dbt commands manually in an environment with dbt installed and the profile configured, from the dbt project directory:

```bash
cd airflow_dbt_project/walmart_dbt_project
dbt source freshness
dbt run --select silver_t
dbt test --select silver_t
dbt run --select silver_b
dbt test --select silver_b
dbt run --select gold/ephemeral
dbt snapshot
dbt run --select gold/fact
```

Check Airflow task logs and dbt output for failures and test results.

## Security

Do not commit `.env` files, Databricks tokens, credential-bearing `profiles.yml` files, private keys, or passwords. Use the example files only as templates, rotate credentials if exposed, and review staged files before committing. The Compose setup, including its local database credentials and UI defaults, is not a production deployment configuration.

## Limitations and future improvements

Potential next steps include more robust incremental and late-arriving-data handling, automated integration tests, CI/CD for dbt validation and deployment, improved secrets management, deferrable Airflow operators or sensors, centralized monitoring and alerting, source-to-target reconciliation, and documented recovery/backfill procedures. These are future improvements, not features currently claimed as implemented.
