
# Healthcare Data Processing Pipeline

## 📋 Overview

This project implements a robust ETL (Extract, Transform, Load) pipeline using **PySpark** on Google Cloud Platform (GCP). It is designed to ingest raw healthcare data from Google Cloud Storage (GCS), validate records based on test results, and separate data into three distinct streams:

1.  **Normal Data:** Cleaned, transformed, and loaded into **BigQuery** for analysis.
2.  **Abnormal Data:** Flagged and routed to a specific GCS bucket for urgent review.
3.  **Inconclusive Data:** Routed to a separate GCS bucket for manual auditing.

## 🏗️ Architecture

The pipeline follows this logic flow:


    A[Raw CSV in GCS] --> B(Spark Ingestion)
    B --> C{Data Validation}
    C -->|Normal| D[Transformation]
    C -->|Abnormal| E[GCS: /abnormal]
    C -->|Inconclusive| F[GCS: /inconclusive]
    D --> G[BigQuery Analysis Table]


# Prerequisites

Before running this pipeline, ensure you have the following:

Google Cloud Platform (GCP) Account with billing enabled.

APIs Enabled:

Cloud Dataproc API

Cloud Storage API

BigQuery API

Cloud Logging API

Service Account with the following roles:

Storage Object Admin (Read/Write to GCS)

BigQuery Data Editor (Write to BQ)


Logging Log Writer (Write to Cloud Logging)

Python 3.8+ and Apache Spark 3.x installed.

Installation & Setup

1. Clone the Repository

git clone https://github.com/manisha0146/Healthcare_ETL_Pipeline
cd Healthcare_ETL_Pipeline

2. Set up Virtual Environment
   
python3 -m venv venv
source venv/bin/activate

3. Install Dependencies

pip install pyspark google-cloud-logging

Configuration
Open main.py and update the following variables at the top of the script to match your GCP environment:

Variable,Description,Default Value
BUCKET_NAME,Main GCS bucket name,healthcare_bucket_data
BQ_TABLE,Target BigQuery table,project-a2f9a359-1d5f-40ef-a6c.healthcare_dataset.analyse_dataset

Usage
You can run the pipeline locally or submit it as a job to a Dataproc cluster.

Submit to Dataproc

gcloud dataproc jobs submit pyspark main.py \
    --cluster=YOUR_CLUSTER_NAME \
    --region=YOUR_REGION \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar

Data Transformation Details
For valid (Normal) records, the following transformations are applied before loading to BigQuery:

Patient_Key: Anonymized SHA-256 hash of the patient's name.

Dates: Cast Date of Admission and Discharge Date to yyyy-MM-dd format.

Strings: Medical Condition is trimmed and converted to lowercase.

Billing: Billing Amount is cast to Decimal(10,2) for financial precision.

Calculated Field: Length_of_Stay (Discharge Date - Admission Date).

📝 Logging
The pipeline integrates with Google Cloud Logging. You can view logs in the GCP Console under the log name:

Healthcare-data-pipeline

Log Levels:

INFO: General pipeline progress (Steps, Record Counts).

WARNING: Detection of Abnormal or Inconclusive data.

ERROR: Pipeline failures or exceptions.
