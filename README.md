# Walmart End-to-End Data Engineering Project

An end-to-end Data Engineering portfolio project built with Databricks Free Trial, Unity Catalog, Delta Lake, PySpark, dbt, Apache Airflow, Docker Compose, SQL, and Git.

The pipeline ingests retail CSV files, stores them in Bronze Delta tables, transforms them through Silver and Gold layers, validates data quality, tracks historical dimension changes with dbt snapshots, and orchestrates execution with Airflow.

> **Project status:** The pipeline has completed successfully in the development environment. Row counts and validation outcomes below are observed results from the development runs, not guarantees for future executions.

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Source Data](#source-data)
- [Data Layers](#data-layers)
- [Data Quality and Validation](#data-quality-and-validation)
- [Airflow Orchestration](#airflow-orchestration)
- [Repository Structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
- [Running the Pipeline](#running-the-pipeline)
- [Security](#security)
- [Limitations and Future Improvements](#limitations-and-future-improvements)

## Project Overview

The goal is to build a maintainable data pipeline that turns raw retail source files into analytics-ready datasets. The project was implemented manually to develop practical understanding of the tools and how they integrate.

The project uses a Databricks Free Trial workspace. Raw CSV files, Auto Loader schema metadata, and ingestion checkpoints are stored in Unity Catalog Volumes; the pipeline does not use Azure Data Lake Storage or Amazon S3 as a separate raw landing layer.

The pipeline demonstrates:

- CSV ingestion with Databricks Auto Loader.
- Delta Lake tables organized with Unity Catalog.
- A Medallion Architecture with Bronze, Silver, and Gold layers.
- SQL transformations and tests using dbt.
- Historical dimension tracking using dbt snapshots.
- Pipeline orchestration using Apache Airflow.
- Containerized Airflow execution using Docker Compose.
- SQL checks for grain, keys, referential integrity, and row counts.

## Architecture

```text
                         Retail CSV files
                                |
                                v
                    Databricks Auto Loader
                                |
                                v
                    BRONZE: Delta tables
                                |
                                v
                    SILVER: Technical models
                                |
                                v
                    SILVER: Business OBT
                                |
                                v
                    GOLD: Ephemeral models
                         /            \
                        v              v
                 dbt snapshots     Gold fact model
                        |
                        v
                 Gold dimensions

             Apache Airflow orchestrates the workflow
```

Airflow first triggers the Databricks ingestion job. Once it succeeds, Airflow runs dbt source freshness, Silver transformations and tests, Gold ephemeral models, snapshots, and the Gold fact model in sequence.

## Technology Stack

| Technology | Role in the project |
|---|---|
| Python | Supporting pipeline logic and configuration |
| PySpark | Distributed processing in Databricks |
| Databricks Free Trial | Data processing environment |
| Unity Catalog | Catalog, schema, table, and Volume organization |
| Delta Lake | Table storage format |
| Databricks Auto Loader | File discovery and ingestion |
| dbt Core | SQL transformations, tests, and snapshots |
| dbt-databricks | dbt adapter for Databricks |
| Apache Airflow | Workflow orchestration |
| Docker Compose | Containerized Airflow environment |
| SQL | Transformation and data validation |
| Git | Version control |

## Source Data

The pipeline processes six CSV datasets:

| Dataset | Description |
|---|---|
| `customers` | Customer records |
| `employees` | Employee records |
| `orders` | Order-level records |
| `order_items` | Individual items within orders |
| `products` | Product records |
| `stores` | Store records |

The source files are stored in the Unity Catalog Volume at `/Volumes/de_project/bronze/raw_data`. The notebook uses file patterns to discover matching CSV files. Auto Loader schema metadata and per-table streaming checkpoints are also stored under this Volume.

## Data Layers

### 1. Bronze: ingestion

The ingestion notebook uses Databricks Auto Loader with CSV input and Delta output. Raw CSV files, schema metadata, and per-table checkpoint directories are stored within the same Unity Catalog Volume. It uses separate schema and checkpoint locations for each source table, append mode, and `availableNow` processing.

The `_ingested_at` timestamp records ingestion time and is used by dbt source freshness checks. The notebook also includes logging and exception propagation so a failed ingestion can be surfaced to the orchestrator.

Bronze tables:

```text
de_project.bronze.customers
de_project.bronze.employees
de_project.bronze.orders
de_project.bronze.order_items
de_project.bronze.products
de_project.bronze.stores
```

### 2. Silver Technical

The technical layer provides technical representations of Bronze data. Models use the `_t` suffix, for example:

```text
silver_t.customers_t
silver_t.employees_t
silver_t.orders_t
silver_t.order_items_t
silver_t.products_t
silver_t.stores_t
```

These models prepare the source data for downstream business transformations.

### 3. Silver Business

The business layer creates a One Big Table (OBT):

```text
silver_b.obt_b
```

The OBT grain is one row per order item. It combines relevant order, customer, product, and store information while preserving the order-item grain.

Employee data is handled through its own downstream dimension path rather than being joined directly into the order-item fact path. This avoids multiplying fact rows when a store has multiple employees.

### 4. Gold: dimensions and facts

The Gold layer provides analytics-oriented tables.

**Dimension tables**

| Table | Purpose |
|---|---|
| `de_project.gold.dim_customers` | Customer dimension |
| `de_project.gold.dim_employees` | Employee dimension |
| `de_project.gold.dim_products` | Product dimension |
| `de_project.gold.dim_stores` | Store dimension |
| `de_project.gold.dim_orders` | Order dimension |

**Fact table**

`de_project.gold.fact_orders` has an order-item grain and includes order, customer, product, and store identifiers together with quantity and monetary measures.

**Historical tracking**

dbt snapshots maintain historical versions of dimension records. The order snapshot uses `order_id` as its unique key and an order-update timestamp for change tracking. The current snapshot configuration uses a far-future `dbt_valid_to` value to represent current rows.

## Data Quality and Validation

The project includes dbt tests and SQL validation queries for:

- Source freshness.
- Silver model tests.
- Null business keys.
- Dimension key uniqueness.
- Referential integrity and orphan records.
- Fact-table grain and duplicate order-item identifiers.
- Snapshot key counts and validity metadata.
- Aggregate business measures.

### Observed row counts

The following counts were observed during development validation:

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

### Fact-table validation observed

- Total rows: `30,021`
- Distinct orders: `10,000`
- Distinct order-item IDs: `30,021`
- Nulls in the checked order, order-item, customer, product, and store keys: `0`
- Duplicate order-item IDs: none returned by the duplicate-check query
- Orphan rows in the checked relationships: `0`

These are results from the validation queries shown during development. Re-run the checks after future code or data changes.

## Airflow Orchestration

The DAG is named `orchestrate`. The current task sequence is:

1. `run_databricks_job`
2. `clean_target`
3. `source_freshness`
4. `silver_technical`
5. `silver_technical_tests`
6. `silver_business`
7. `silver_business_tests`
8. `gold_ephemeral`
9. `gold_dimensions`
10. `gold_facts`

The Databricks task triggers the ingestion job and waits for a terminal state before allowing downstream tasks to proceed. dbt commands execute sequentially, with tests following the Silver transformations.

The project has completed successfully in the development environment. The Airflow deployment uses Docker Compose and an Airflow 3.1.8 image.

## Repository Structure

The active project layout is expected to resemble:

```text
dbt_walmart/
├── README.md
├── .gitignore
├── .python-version
├── pyproject.toml
├── uv.lock
└── airflow_dbt_project/
    ├── Dockerfile
    ├── docker-compose.yaml
    ├── .env.example
    ├── dags/
    │   ├── orchestrate.py
    │   └── databricks_connection_test.py
    ├── config/
    ├── plugins/
    └── walmart_dbt_project/
        ├── dbt_project.yml
        ├── profiles.example.yml
        ├── models/
        ├── snapshots/
        ├── tests/
        ├── macros/
        ├── analyses/
        └── seeds/
```

Generated dbt directories such as `target/`, `logs/`, and `dbt_packages/` should not be committed. The obsolete standalone `walmart_dbt_project/` copy at the repository root is intentionally excluded from version control; the active copy is inside `airflow_dbt_project/`.

Confirm the filenames and folders against the repository before publishing.

## Prerequisites

Before running the project, configure:

- Docker Desktop with Docker Compose.
- A Databricks Free Trial workspace with Unity Catalog enabled.
- A Databricks job for Bronze ingestion.
- Unity Catalog permissions for the required catalog, schemas, tables, and Volume.
- Sufficient workspace and compute availability for the selected Databricks Free Trial.
- A Databricks SQL warehouse for dbt.
- Python and the local development tools used by this repository.
- Git.

Cloud resources may incur charges. Stop resources when they are no longer needed.

## Configuration

### Airflow environment

Create an `.env` file in `airflow_dbt_project/` based on `.env.example`. Supply the required Databricks settings, including:

```dotenv
DATABRICKS_HOST=your_databricks_workspace_host
DATABRICKS_TOKEN=your_databricks_token
```

Use a valid token with only the permissions required by the pipeline. Do not commit real tokens.

### dbt profile

Create a local `profiles.yml` based on `profiles.example.yml`. Configure the Databricks catalog, schema, SQL warehouse HTTP path, and authentication. Keep the credential-bearing profile out of version control.

Do not put actual tokens or passwords in the README, example files, screenshots, or Git history.

## Running the Pipeline

Complete the environment configuration before running commands.

### Start Airflow

From the Airflow project directory:

```bash
docker compose up -d --build
docker compose ps
```

Inspect worker logs when troubleshooting:

```bash
docker compose logs --tail 100 airflow-worker
```

Service names and availability depend on the checked-in Compose configuration.

### Run dbt commands

Run these in an environment with dbt installed and the correct profile configured. In the Airflow setup, these commands are executed by the DAG.

```bash
dbt source freshness
dbt run --select silver_t
dbt test --select silver_t
dbt run --select silver_b
dbt test --select silver_b
dbt run --select gold/ephemeral
dbt snapshot
dbt run --select gold/fact
```

The DAG runs the stages in sequence. Check the Airflow task logs for failures and the dbt output for test results.

## Security and Version Control

The repository should exclude:

- `.env` files containing credentials.
- dbt `profiles.yml` files containing credentials.
- Local Python virtual environments.
- Generated Airflow logs and local database files.
- Generated dbt artifacts.
- Local IDE settings.
- Private keys and certificates.

Use `.env.example` and `profiles.example.yml` to document required settings without exposing real secrets. Review staged files for credentials before every commit.

## Limitations and Future Improvements

Potential next improvements include:

- More robust incremental processing and late-arriving-data handling.
- Automated integration tests.
- CI/CD for dbt validation and deployment.
- Improved secrets management.
- Deferrable Airflow operators or sensors for long-running Databricks jobs.
- Centralized monitoring and alerting.
- Additional source-to-target reconciliation checks.
- Documented recovery and backfill procedures.

These are potential improvements, not claims that the features are already implemented.

## Project Goals

This project demonstrates practical work with cloud ingestion, distributed processing, Medallion Architecture, Delta Lake, dbt transformations and tests, snapshot-based historical tracking, data-quality validation, workflow orchestration, Docker-based development, and version control.

The aim is to keep the pipeline understandable, testable, and maintainable.
