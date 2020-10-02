# Retention_ETL_Datapipeline

#### Overview
Data is captured in real time from GoogleAnalytics Platform. The data collected from GoogleAnalytics API is stored on local disk and is timely moved to the Landing Bucket on GCP Cloud Storge. ETL jobs are written in spark and scheduled in airflow to run every 10 minutes.  

### ETL Flow

 - Data Collected from the API is moved to GCS buckets.
 - Spark job is triggered which reads the data and apply transformation.
 - ETL job execution is completed once the BigQuery is updated. 
 - Airflow DAG runs the data quality check on all Warehouse tables once the ETL job execution is completed.
 - Airflow DAG has Analytics queries configured in a Custom Designed Operator. These queries are run and again a Data Quality Check is done on some selected Analytics Table.
 - Dag execution completes after these Data Quality check.
