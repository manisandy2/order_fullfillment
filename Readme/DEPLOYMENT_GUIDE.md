Deployment Guide
Overview

This guide explains how to deploy the Order Fulfillment Data Ingestion System in different environments.

The system reads data from MySQL, processes it using Python pipelines, and writes the data to Apache Iceberg tables stored in S3-compatible object storage.

Supported deployment methods:

Local development

Docker container deployment

Cloud Run deployment

Kubernetes scheduled jobs

Cron-based job scheduling

Prerequisites

Before deployment ensure the following tools are installed.

Tool	Purpose
Python 3.9+	Runtime environment
Docker	Containerization
Git	Source control
MySQL access	Source database
S3 compatible storage	Iceberg data storage
Environment Variables

Create a .env file in the project root.

HOST=
USERNAME=
PASSWORD=
DATABASE=
PORT=3306

ACCESS_KEY_ID=
SECRET_ACCESS_KEY=
ENDPOINT=

These variables configure:

MySQL connection

Iceberg object storage

S3-compatible endpoint

Example:

HOST=mysql.company.internal
USERNAME=data_user
PASSWORD=secure_password
DATABASE=order_fulfillment
PORT=3306

ACCESS_KEY_ID=minio
SECRET_ACCESS_KEY=minio123
ENDPOINT=http://minio:9000
Local Deployment
Step 1 — Clone Repository
git clone https://github.com/manisandy2/order_fullfillment.git
cd order_fullfillment
Step 2 — Create Virtual Environment
python -m venv venv
source venv/bin/activate

Windows:

venv\Scripts\activate
Step 3 — Install Dependencies
pip install -r requirements.txt
Step 4 — Run Pipeline Job

Example:

python jobs/bluedart_zone_masters_between_date.py

Example output:

Initial Memory: 180 MB

{
  "rows_fetched": 404,
  "chunks_total": 1,
  "chunks_failed": 0,
  "status": "COMPLETED"
}

Final Memory: 210 MB
Docker Deployment

Docker allows pipelines to run consistently across environments.

Step 1 — Create Dockerfile
FROM python:3.10-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "jobs/bluedart_zone_masters_between_date.py"]
Step 2 — Build Image
docker build -t order-fulfillment-pipeline .
Step 3 — Run Container
docker run --env-file .env order-fulfillment-pipeline
Scheduled Pipeline Execution

Production systems typically schedule ingestion pipelines.

Two common approaches:

Cron jobs

Kubernetes scheduled jobs

Cron Job Deployment

Example cron job running every hour:

0 * * * * python /app/jobs/bluedart_zone_masters_between_date.py

Example daily ingestion:

0 2 * * * python /app/jobs/masterorders_between_date.py
Kubernetes Deployment

Kubernetes can run pipelines as batch jobs or scheduled jobs.

Kubernetes Job Example
apiVersion: batch/v1
kind: Job
metadata:
  name: order-ingestion-job
spec:
  template:
    spec:
      containers:
      - name: ingestion
        image: order-fulfillment-pipeline:latest
        envFrom:
        - secretRef:
            name: pipeline-env
      restartPolicy: Never
Kubernetes CronJob Example
apiVersion: batch/v1
kind: CronJob
metadata:
  name: order-ingestion
spec:
  schedule: "0 * * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: ingestion
            image: order-fulfillment-pipeline:latest
          restartPolicy: Never
Cloud Run Deployment (Optional)

For serverless execution, pipelines can run on Google Cloud Run Jobs.

Steps:

Build container image

docker build -t gcr.io/project/order-ingestion .

Push image

docker push gcr.io/project/order-ingestion

Create Cloud Run job

gcloud run jobs create order-ingestion \
  --image gcr.io/project/order-ingestion \
  --region us-central1

Execute job

gcloud run jobs execute order-ingestion
Logging and Monitoring

Logs are stored in:

logs/iceberg_upload/

Logs contain:

failed chunk data

error messages

ingestion metadata

Example log fields:

Field	Description
chunk_index	Failed chunk number
chunk_data	Original data
error	Exception message
Scaling the Pipeline

For large datasets:

Reduce chunk size

Enable streaming MySQL fetch

Run multiple ingestion workers

Schedule pipelines across nodes

Example chunk size configuration:

chunk_size = 1000

Recommended for very large tables:

chunk_size = 500
Production Best Practices

Recommended improvements for production environments:

Docker-based deployment

Kubernetes CronJobs

Centralized logging

Metrics monitoring

Retry pipelines for failed chunks

Security Recommendations

Avoid storing secrets in the repository.

Use:

Kubernetes Secrets

AWS Secrets Manager

Environment variables

Maintainer

Manikandan R
Backend / Data Engineering