# RetailStream — End-to-End Retail Data Engineering Platform

RetailStream is an end-to-end batch data engineering project that processes retail e-commerce data using PySpark, RustFS, PostgreSQL, dbt, and Apache Airflow. It implements a multi-layer data pipeline to ingest raw Parquet files, transform and validate data, load it into a relational data warehouse, and build analytics-ready dimensional and fact tables.

The project is designed to demonstrate practical data engineering skills, including ETL development, data quality validation, data warehouse modeling, workflow orchestration, containerization, and analytics engineering.

## Architecture

![RetailStream Architecture]([docs/architecture.png](https://github.com/vikaslokhande2004/retailstream-data-engineering/blob/main/docs/RetailStream%20Data%20Engineering%20Pipeline.png))

```text
              Retail E-commerce Parquet Data
                            |
                            v
                    RustFS Object Storage
                       Raw Data Layer
                            |
                            v
                   Apache Airflow DAG
                            |
                            v
                    PySpark Ingestion
                            |
                            v
                     Bronze Layer
                            |
                            v
                  PySpark Transformations
                            |
                            v
                      Silver Layer
                            |
                            v
                 Data Quality Validation
                            |
                            v
                    PostgreSQL Warehouse
                     Staging Schema
                            |
                            v
                       dbt Models
                            |
                            v
                    Analytics Schema
                  /                   \
             Dimensions             Facts
                            |
                            v
                 Analytics Validation
```

Apache Airflow orchestrates the pipeline and manages task dependencies, while Docker runs the pipeline components in isolated containers.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Pipeline development and automation |
| PySpark | Distributed data processing and transformation |
| RustFS | S3-compatible object storage for raw data |
| PostgreSQL | Relational data warehouse and staging |
| dbt | SQL transformations, dimensional modeling, and data tests |
| Apache Airflow | Workflow orchestration and task monitoring |
| Docker | Containerization and reproducible execution |
| PowerShell | Local development and environment management |
| Parquet | Columnar storage format for data processing |

## Data Pipeline

### 1. Raw Data Ingestion

Raw retail datasets are stored as Parquet files in RustFS. The ingestion script downloads the source files from object storage and processes them using PySpark.

**Script:** `scripts/ingest_bronze.py`

### 2. Bronze Layer

The Bronze layer stores ingested data in Parquet format. It provides a persistent intermediate layer for downstream processing.

### 3. Silver Layer

The transformation script cleans the data by trimming string values, removing duplicate rows, and dropping fully empty rows before writing the transformed datasets to the Silver layer.

**Script:** `scripts/transform_silver.py`

### 4. Data Quality Validation

The validation script checks table completeness, duplicate rows, null values in required columns, and blank values in relevant fields. Expected nullable fields are handled through explicit validation rules.

**Script:** `scripts/validate_silver.py`

The current validation run completed with **77 passed checks and 0 failed checks**.

### 5. PostgreSQL Warehouse Loading

Validated Silver datasets are loaded into PostgreSQL staging tables. The loading process uses temporary tables and transactional replacement to refresh the target data.

**Script:** `scripts/load_silver_postgres.py`

### 6. dbt Transformations

dbt builds analytics models from the PostgreSQL staging tables. The project contains seven staging models and eight analytics models.

The analytics layer includes:

- **Dimensions:** `dim_customer`, `dim_category`, `dim_product`, `dim_date`
- **Fact tables:** `fact_orders`, `fact_sales`, `fact_payments`, `fact_returns`

dbt tests validate the configured model and data-quality rules.

### 7. Airflow Orchestration

The `retailstream_pipeline` DAG orchestrates the complete workflow:

1. Check PostgreSQL connectivity.
2. Ingest raw data into Bronze.
3. Transform Bronze data into Silver.
4. Validate Silver data quality.
5. Load Silver data into PostgreSQL.
6. Validate staging tables.
7. Execute dbt models.
8. Execute dbt tests.
9. Validate analytics tables.

The complete DAG has been executed successfully, with all nine tasks reporting success.

## Verified Results

### Source and Staging Data

| Dataset | Rows |
|---|---:|
| Categories | 24 |
| Customers | 12,000 |
| Order Items | 215,000 |
| Orders | 90,000 |
| Payments | 95,000 |
| Products | 2,000 |
| Returns | 12,000 |
| **Total** | **426,024** |

### Analytics Tables

| Table | Rows |
|---|---:|
| `dim_category` | 24 |
| `dim_customer` | 12,000 |
| `dim_product` | 2,000 |
| `dim_date` | 1,096 |
| `fact_orders` | 90,000 |
| `fact_sales` | 215,000 |
| `fact_payments` | 95,000 |
| `fact_returns` | 12,000 |
| **Combined table rows** | **427,120** |

The combined analytics count represents rows across separate fact and dimension tables, which have different grains and purposes.

## Project Structure

```text
retailstream-data-engineering/
├── config/
├── dags/
│   └── retailstream_pipeline.py
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── .raw_from_rustfs/
├── dbt/
│   └── retailstream_dbt/
│       ├── models/
│       ├── macros/
│       ├── tests/
│       └── dbt_project.yml
├── docker/
│   ├── airflow/
│   └── pipeline/
├── docs/
├── notebooks/
├── scripts/
│   ├── ingest_bronze.py
│   ├── transform_silver.py
│   ├── validate_silver.py
│   └── load_silver_postgres.py
├── src/
├── tests/
├── .gitignore
└── README.md
```

## Prerequisites

Install or configure the following tools before running the project:

- Python 3.11
- Docker Desktop with Docker Compose
- Git
- PostgreSQL client tools, if you want to run database verification commands from the host
- A local RustFS and Airflow environment configured through the project Docker Compose files

The pipeline image includes the Java runtime and PySpark dependencies needed for its processing scripts.

## Setup and Execution

### 1. Clone the Repository

```bash
git clone https://github.com/vikaslokhande2004/retailstream-data-engineering.git
cd retailstream-data-engineering
```

Replace the placeholder with your actual GitHub repository URL.

### 2. Configure Environment Variables

Configure the RustFS endpoint and PostgreSQL connection settings for your local environment. Keep passwords and access keys in environment variables or a local environment file that is excluded from Git.

Do not commit credentials, local environment files, or sensitive configuration to the repository.

### 3. Start the Pipeline Services

Build the pipeline image:

```powershell
docker build -f .\docker\pipeline\Dockerfile -t retailstream-pipeline:latest .
```

Start the Airflow services using the project's Compose file:

```powershell
docker compose -f .\docker\airflow\docker-compose.yml up -d
```

Ensure that PostgreSQL, RustFS, and Airflow are running and accessible from the pipeline containers.

### 4. Open Airflow

Navigate to:

```text
http://localhost:8080
```

Use the local credentials configured for your Airflow environment.

Find the `retailstream_pipeline` DAG and trigger it manually. Monitor each task in the Airflow Graph or Grid view.

### 5. Verify the Results

Use PostgreSQL to verify the staging and analytics tables:

```sql
SELECT COUNT(*) FROM staging.orders;
SELECT COUNT(*) FROM analytics.fact_orders;
SELECT COUNT(*) FROM analytics.fact_sales;
```

The row counts should match the results produced by the current dataset and pipeline run.

## Key Features

- End-to-end batch ETL orchestration
- S3-compatible object storage integration
- Multi-layer Bronze and Silver data processing
- PySpark-based transformations
- Automated data quality checks
- PostgreSQL staging and analytics schemas
- dbt-based dimensional modeling
- Automated dbt tests
- Dockerized pipeline execution
- Airflow task dependency management
- Analytics table verification

## Future Improvements

- Add incremental ingestion and processing.
- Implement retry strategies and failure notifications.
- Add structured logging and pipeline metrics.
- Expand referential-integrity and business-rule validation.
- Add automated tests to CI/CD workflows.
- Add dashboards for retail sales and customer analytics.
- Improve secrets management and deployment configuration.

## Author

**Vikas Lokhande**

Data Engineering | Python | SQL | PySpark | PostgreSQL | dbt | Apache Airflow | Docker

GitHub: https://github.com/vikaslokhande2004
