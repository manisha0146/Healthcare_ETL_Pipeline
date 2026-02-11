
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


# Healthcare Data Processing Pipeline

## 📋 Overview

This project implements a robust ETL (Extract, Transform, Load) pipeline using **PySpark** on Google Cloud Platform (GCP). It is designed to ingest raw healthcare data from Google Cloud Storage (GCS), validate records based on test results, and separate data into three distinct streams:

1.  **Normal Data:** Cleaned, transformed, and loaded into **BigQuery** for analysis.
2.  **Abnormal Data:** Flagged and routed to a specific GCS bucket for urgent review.
3.  **Inconclusive Data:** Routed to a separate GCS bucket for manual auditing.

## 🏗️ Architecture

The pipeline follows this logic flow:


graph LR
    A[Raw CSV in GCS] --> B(Spark Ingestion)
    B --> C{Data Validation}
    C -->|Normal| D[Transformation]
    C -->|Abnormal| E[GCS: /abnormal]
    C -->|Inconclusive| F[GCS: /inconclusive]
    D --> G[BigQuery Analysis Table]
⚙️ Prerequisites
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

git clone [https://github.com/your-username/healthcare-data-pipeline.git](https://github.com/your-username/healthcare-data-pipeline.git)
cd healthcare-data-pipeline

