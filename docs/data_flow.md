# Data Flow

1. Producers generate ecommerce events.

2. Events are uploaded into S3 raw layer.

3. S3 event notifications trigger Lambda.

4. Lambda processes and stores files into Bronze layer.

5. Glue ETL converts Bronze data into Silver parquet datasets.

6. Gold ETL creates business analytical datasets.

7. Glue crawler scans Gold layer datasets.

8. Athena queries Gold analytical tables.

9. CloudWatch monitors Glue jobs and Lambda execution.

10. SNS sends email notifications for failures.
