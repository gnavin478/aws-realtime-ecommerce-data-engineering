# Project Setup Steps

## Step 1 — Create AWS Account

Create an AWS account and configure billing alerts.

---

## Step 2 — Configure IAM Permissions

Create IAM user and attach permissions for:

* S3
* Lambda
* Glue
* Athena
* CloudWatch
* SNS

---

## Step 3 — Configure AWS CLI

Install AWS CLI and configure credentials.

```bash
aws configure
```

---

## Step 4 — Create Python Virtual Environment

```bash
python -m venv venv
```

Activate virtual environment.

```bash
venv\Scripts\activate
```

---

## Step 5 — Install Dependencies

```bash
pip install -r requirements.txt
```

---

## Step 6 — Create S3 Bucket

Create:

```text
aws-realtime-ecommerce-pipeline
```

Create folders:

* raw/
* bronze/
* silver/
* gold/

---

## Step 7 — Run Producers

Run producers to generate streaming ecommerce events.

```bash
python producers/run_all_producers.py
```

---

## Step 8 — Configure Lambda

Create Lambda function for S3 event processing.

Configure:

* S3 trigger
* IAM role
* CloudWatch logs

---

## Step 9 — Create Glue ETL Jobs

Create:

* Silver ETL jobs
* Gold ETL jobs

Upload PySpark ETL scripts.

---

## Step 10 — Create Glue Crawlers

Create:

* Bronze crawler
* Silver crawler
* Gold crawler

---

## Step 11 — Create Athena Databases

Create:

* ecommerce_bronze_db
* ecommerce_silver_db
* ecommerce_gold_db

---

## Step 12 — Configure Monitoring

Configure:

* CloudWatch alarms
* SNS notifications
* Glue monitoring
* Lambda monitoring

---

## Step 13 — Configure Glue Workflow

Create workflow automation:

* Silver ETL trigger
* Gold ETL trigger
* Gold crawler trigger
