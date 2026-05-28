# Deployment Steps

## Step 1 — Upload Glue Scripts

Upload:

* Silver ETL scripts
* Gold ETL scripts

to AWS Glue jobs.

---

## Step 2 — Configure IAM Roles

Verify Glue and Lambda IAM permissions.

Required permissions:

* S3 access
* Glue access
* CloudWatch access
* SNS access

---

## Step 3 — Configure S3 Event Notifications

Configure:

* S3 bucket trigger
* Lambda integration

for raw layer event processing.

---

## Step 4 — Deploy Lambda Function

Deploy Lambda function code.

Verify:

* Lambda execution
* CloudWatch logs
* S3 trigger execution

---

## Step 5 — Execute Producers

Run producers to generate real-time ecommerce events.

```bash
python producers/run_all_producers.py
```

---

## Step 6 — Run Glue ETL Jobs

Execute:

* Silver ETL jobs
* Gold ETL jobs

Verify successful execution.

---

## Step 7 — Run Glue Crawlers

Execute:

* Bronze crawler
* Silver crawler
* Gold crawler

Verify Athena tables are created.

---

## Step 8 — Validate Athena Queries

Run analytics queries:

* revenue analysis
* top products
* failed payments
* inventory analysis

---

## Step 9 — Configure Monitoring

Configure:

* CloudWatch alarms
* SNS notifications
* Glue monitoring
* Lambda logs

---

## Step 10 — Execute Glue Workflow

Run automated Glue workflow pipeline.

Validate:

* Silver ETL execution
* Gold ETL execution
* Gold crawler execution
* Athena analytics readiness
