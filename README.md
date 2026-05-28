# AWS Real-Time Ecommerce Data Engineering Pipeline

## Project Overview

This project is a real-time AWS-based ecommerce data engineering platform built using Medallion Architecture (Raw → Bronze → Silver → Gold).

The pipeline simulates streaming ecommerce events such as:

* Orders
* Payments
* Inventory updates
* Delivery events
* Cart activities
* Customer activities

The project processes real-time streaming events into analytical datasets using AWS serverless services and PySpark ETL pipelines.

---

# AWS Services Used

* Amazon S3
* AWS Lambda
* AWS Glue
* AWS Glue Crawlers
* Amazon Athena
* Amazon CloudWatch
* Amazon SNS
* AWS IAM

---

# Features

* Real-time streaming event simulation
* Medallion architecture implementation
* Lambda-based event processing
* Glue PySpark ETL pipelines
* Athena SQL analytics
* Glue Workflow orchestration
* CloudWatch monitoring
* SNS email alerts
* Gold analytical datasets

---

# Pipeline Flow

1. Producers generate ecommerce events.
2. Events are uploaded into S3 raw layer.
3. S3 triggers Lambda processing.
4. Lambda stores processed events into Bronze layer.
5. Glue ETL converts Bronze to Silver.
6. Gold ETL creates analytical datasets.
7. Glue crawler scans Gold layer.
8. Athena queries Gold datasets.

---

# Monitoring

CloudWatch alarms monitor:

* Glue job failures
* Lambda errors
* Pipeline execution failures

SNS notifications send email alerts.

---

# Future Enhancements

* Terraform infrastructure automation
* Airflow orchestration
* Step Functions orchestration
* Incremental ETL processing
* Redshift integration
* QuickSight dashboards
