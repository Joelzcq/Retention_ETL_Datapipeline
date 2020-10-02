import base64
import json
import os
from datetime import datetime, timedelta
from time import time
from airflow import DAG
from airflow.utils import trigger_rule
from airflow.operators import PythonOperator
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataProcSparkOperator, DataprocClusterDeleteOperator

dag_name = 'jiuzhang'.strip()


def push_cluster_name(**kwargs):
  ti = kwargs['ti']
  cluster_name = dag_name[:27] + '-' + str(int(round(time() * 100)))
  ti.xcom_push(key='cluster_name', value=cluster_name)


with DAG(
    dag_id=dag_name,
    schedule_interval='@daily',
    start_date=datetime.strptime('2019-03-26 00:00:00', "%Y-%m-%d %H:%M:%S"),
    max_active_runs=1,
    concurrency=1,
    default_args={
        'project_id': 'sinuous-set-242504',
        'email': 'test@gmail.com',
        'email_on_failure': True,
        'email_on_retry': False
    }) as dag:

  push_cluster_name = PythonOperator(
      dag=dag,
      task_id="push-cluster-name",
      provide_context=True,
      python_callable=push_cluster_name)

  dataproc_create_cluster = DataprocClusterCreateOperator(
      task_id='dataproc-create-cluster',
      project_id='sinuous-set-242504',
      region='us-west1',
      master_machine_type='n1-standard-2',
      worker_machine_type='n1-standard-2',
      cluster_name='{{ ti.xcom_pull(key="cluster_name", task_ids="push-cluster-name") }}',
      num_workers=2)

  dataproc_spark_process = DataProcSparkOperator(
      task_id='dataproc-test',
      dataproc_spark_jars=['gs://jiuzhangsuanfa/SparkProject-assembly-0.1.jar'],
      main_class='com.jiuzhang.spark.LoanAnalyze',
      job_name='loan',
      region='us-west1',
      cluster_name='{{ ti.xcom_pull(key="cluster_name", task_ids="push-cluster-name") }}',
      arguments=[
          "gs://jiuzhangsuanfa/LendingClub/LoanStats_2019Q1.csv",
          "gs://jiuzhangsuanfa/LendingClub/RejectStats_2019Q1.csv",
          "gs://jiuzhangsuanfa/output"
      ])

  dataproc_destroy_cluster = DataprocClusterDeleteOperator(
      task_id='dataproc-destroy-cluster',
      project_id='sinuous-set-242504',
      region='us-west1',
      cluster_name='{{ ti.xcom_pull(key="cluster_name", task_ids="push-cluster-name") }}',
      trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

  push_cluster_name >> dataproc_create_cluster >> dataproc_spark_process >> dataproc_destroy_cluster
