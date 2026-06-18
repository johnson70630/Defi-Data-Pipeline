# DeFi Data Pipeline

An end-to-end Data Engineering project that ingests Uniswap swap data from The Graph API, stores raw blockchain data in AWS S3 and Snowflake, transforms data using dbt, and orchestrates workflows with Apache Airflow.

The project demonstrates modern ELT architecture, incremental data processing, automated data quality testing, and cloud data warehousing practices commonly used in production data platforms.

---

## Architecture

```text
The Graph API
      ↓
Apache Airflow
      ↓
AWS S3 (Raw Data Lake)
      ↓
Snowflake RAW Layer
      ↓
dbt Staging Layer
      ↓
dbt Incremental Mart Layer
      ↓
Analytics & Reporting
```

---

## Tech Stack

### Data Ingestion
- Python
- The Graph API

### Orchestration
- Apache Airflow

### Storage
- AWS S3
- Snowflake

### Transformation
- dbt

### Infrastructure
- Docker
- Docker Compose

### Security
- RSA Key-Pair Authentication
- Snowflake Roles & Service Accounts

### Development
- Git
- GitHub
- uv

---

## Project Objectives

- Build a production-style ELT pipeline
- Automate blockchain data ingestion
- Store raw and transformed data separately
- Implement scalable data modeling practices
- Validate data quality automatically
- Demonstrate modern cloud data engineering workflows

---

## Data Flow

### 1. Extract

Airflow triggers a Python extraction script that retrieves swap events from the Uniswap protocol through The Graph API.

Output:

```text
Raw swap data
```

---

### 2. Load Raw Data to S3

Raw files are stored in AWS S3.

Purpose:

- Data lake storage
- Historical backup
- Reprocessing capability

Output:

```text
s3://...
```

---

### 3. Transform

Python transformation scripts:

- Clean records
- Standardize fields
- Generate analytics-ready datasets

Output:

```text
uniswap_swaps_processed.csv
```

---

### 4. Load to Snowflake

Processed data is loaded into:

```sql
DEFI_DB.RAW.RAW_UNISWAP_SWAPS
```

using Snowflake RSA key authentication.

---

### 5. dbt Transformation Layer

dbt transforms raw data into analytics-ready models.

#### Staging Layer

```text
STG_UNISWAP_SWAPS
```

Responsibilities:

- Standardize schema
- Rename fields
- Apply data type conversions
- Create reusable models

---

#### Mart Layer

##### DAILY_SWAP_VOLUME

Daily aggregated swap volume metrics.

##### TOKEN_VOLUME

Token pair trading volume analytics.

##### POOL_VOLUME

Liquidity pool volume analytics.

---

## Incremental Processing

The mart models are implemented using dbt incremental materializations.

Benefits:

- Process only new data
- Reduce Snowflake compute costs
- Improve pipeline performance
- Scale efficiently with data growth

Example:

```sql
{% if is_incremental() %}
    WHERE event_date >= (
        SELECT COALESCE(MAX(event_date), '1900-01-01')
        FROM {{ this }}
    )
{% endif %}
```

---

## Data Quality Testing

Implemented using dbt tests.

Current validations:

### STG_UNISWAP_SWAPS

- swap_id is unique
- swap_id is not null
- amount_usd is not null

### DAILY_SWAP_VOLUME

- event_date is unique
- event_date is not null

Run tests:

```bash
uv run dbt test
```

---

## Security

Snowflake access is secured through:

### Service Account

```text
DBT_USER
```

### Role-Based Access Control

```text
DBT_ROLE
```

### RSA Authentication

- Private key authentication
- No password-based access
- Automated Airflow execution

This follows the principle of least privilege commonly used in production environments.

---

## Airflow Workflow

```text
extract_swaps
        ↓
upload_raw_to_s3
        ↓
transform_swaps
        ↓
load_to_snowflake
        ↓
dbt_run
        ↓
dbt_test
```

The workflow is fully automated and scheduled through Apache Airflow.

---

## Running the Project

### Start Infrastructure

```bash
docker compose up -d
```

### Run Airflow

```bash
docker compose -f docker-compose-airflow.yml up -d
```

### Run dbt

```bash
cd defi_dbt

uv run dbt run
uv run dbt test
```

---

## Key Data Engineering Concepts Demonstrated

### Data Engineering

- ELT Pipelines
- Workflow Orchestration
- Incremental Loading
- Data Warehousing
- Data Lakes

### Data Modeling

- Raw Layer
- Staging Layer
- Mart Layer
- Analytical Aggregations

### Data Quality

- Uniqueness Validation
- Null Validation
- Automated Testing

### Security

- Service Accounts
- Role-Based Access Control
- RSA Authentication

### Cloud Data Platforms

- AWS S3
- Snowflake
- Apache Airflow

---

## Future Enhancements

- Streamlit analytics dashboard
- Snowflake COPY INTO from S3
- Spark-based transformations
- CI/CD deployment pipeline
- Real-time ingestion with Kafka
- Kubernetes deployment
- Advanced monitoring and alerting

---

## Author

Chung-Sheng (Johnson) Chang

Built as a hands-on Data Engineering project to demonstrate modern cloud data platform design, orchestration, modeling, testing, and automation practices.