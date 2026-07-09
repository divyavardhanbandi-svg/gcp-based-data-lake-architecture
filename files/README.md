# GCP-Based Data Lake Architecture

A production-style data lake reference implementation on Google Cloud Platform,
supporting batch and streaming ingestion, a multi-zone storage layer, automated
data quality checks, and BigQuery-based analytics — orchestrated end-to-end
with Terraform and Cloud Composer (Airflow).

## Architecture

```
                         ┌──────────────────────────────────────────────┐
                         │                 Ingestion Layer               │
                         │                                                │
   Batch files  ───────► │  Cloud Storage (raw drop zone)                │
   Streaming events ───► │  Pub/Sub  ───►  Cloud Function (validate)     │
                         └───────────────────────┬────────────────────────┘
                                                  │
                                                  ▼
                         ┌──────────────────────────────────────────────┐
                         │              Multi-Zone Data Lake             │
                         │                                                │
                         │  gs://<project>-raw       (immutable landing) │
                         │  gs://<project>-staging   (cleansed/validated)│
                         │  gs://<project>-curated   (business-ready)    │
                         └───────────────────────┬────────────────────────┘
                                                  │
                                                  ▼
                         ┌──────────────────────────────────────────────┐
                         │         Orchestration & Processing            │
                         │                                                │
                         │  Cloud Composer (Airflow) DAG                 │
                         │    -> Dataflow batch pipeline (Apache Beam)   │
                         │    -> Data quality checks                     │
                         │    -> BigQuery load (raw -> staging -> mart)  │
                         └───────────────────────┬────────────────────────┘
                                                  │
                                                  ▼
                         ┌──────────────────────────────────────────────┐
                         │              Analytics / Consumption          │
                         │                                                │
                         │  BigQuery datasets (staging, curated, marts)  │
                         │  Looker Studio / BI tools                     │
                         │  IAM-scoped service accounts per zone         │
                         └──────────────────────────────────────────────┘
```

## Components

| Layer | GCP Service | Purpose |
|---|---|---|
| Ingestion (batch) | Cloud Storage | Landing zone for raw files (CSV, JSON, Parquet) |
| Ingestion (streaming) | Pub/Sub + Cloud Function | Real-time event ingestion & schema validation |
| Storage | Cloud Storage (3 buckets) | Raw / Staging / Curated multi-zone lake |
| Processing | Dataflow (Apache Beam) | Batch transformation, cleansing, dedup |
| Orchestration | Cloud Composer (Airflow) | DAG-driven pipeline scheduling & dependencies |
| Warehouse | BigQuery | Staging tables, curated tables, analytics marts |
| Data Quality | Python (Great Expectations style checks) | Schema, null, and range validation |
| Security | IAM + service accounts | Least-privilege access per zone |
| IaC | Terraform | Reproducible infrastructure provisioning |

## Project Structure

```
gcp-data-lake-architecture/
├── terraform/                  # Infrastructure as Code
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── modules/
│       ├── storage/             # GCS buckets (raw/staging/curated)
│       ├── bigquery/             # Datasets & tables
│       ├── pubsub/                # Topics & subscriptions
│       └── iam/                    # Service accounts & role bindings
├── ingestion/
│   └── cloud_function/          # Pub/Sub triggered validation function
├── pipelines/
│   ├── dataflow/                 # Apache Beam batch pipeline
│   └── airflow/dags/             # Composer DAG orchestrating the lake
├── scripts/
│   └── data_quality_checks.py   # Standalone data quality validator
├── config/
│   └── config.yaml              # Environment/project configuration
└── requirements.txt
```

## Setup

### 1. Prerequisites
- GCP project with billing enabled
- `gcloud` CLI authenticated (`gcloud auth application-default login`)
- Terraform >= 1.5
- Python 3.10+

### 2. Provision infrastructure
```bash
cd terraform
terraform init
terraform plan -var="project_id=<YOUR_PROJECT_ID>"
terraform apply -var="project_id=<YOUR_PROJECT_ID>"
```

### 3. Deploy the ingestion Cloud Function
```bash
cd ingestion/cloud_function
gcloud functions deploy validate_and_land \
  --runtime python311 \
  --trigger-topic <project>-ingest-topic \
  --entry-point validate_and_land \
  --region us-central1
```

### 4. Deploy the DAG to Composer
```bash
gcloud composer environments storage dags import \
  --environment <composer-env-name> \
  --location us-central1 \
  --source pipelines/airflow/dags/data_lake_etl_dag.py
```

### 5. Run the batch Dataflow pipeline manually (optional, outside Composer)
```bash
python pipelines/dataflow/batch_pipeline.py \
  --runner DataflowRunner \
  --project <YOUR_PROJECT_ID> \
  --region us-central1 \
  --input gs://<project>-raw/incoming/*.json \
  --output_dataset staging_dataset \
  --temp_location gs://<project>-staging/tmp
```

## Data Zones

- **Raw** — immutable, append-only landing zone; exact copy of source data.
- **Staging** — schema-validated, deduplicated, type-cast data.
- **Curated** — business-level, joined/aggregated tables ready for BI and ML consumption.

## Data Quality

`scripts/data_quality_checks.py` runs schema conformance, null-rate thresholds,
and range/enum checks before data is promoted from staging to curated. Failed
checks route records to a `quarantine` table/prefix instead of blocking the
whole pipeline.

## Security Model

Each zone has a dedicated service account with least-privilege IAM bindings:
- `raw-ingest-sa`: write-only to raw bucket
- `pipeline-sa`: read raw / write staging & curated
- `analytics-sa`: read-only on curated BigQuery datasets

## Cost & Scaling Notes

- Lifecycle rules auto-transition raw objects to Nearline/Coldline after 30/90 days.
- BigQuery tables are partitioned by ingestion date and clustered by key business fields to control scan costs.
- Dataflow autoscaling is enabled (`--autoscaling_algorithm=THROUGHPUT_BASED`).
