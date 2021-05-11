"""
## PySpark dag
This dag creates a Dataproc cluster in Google Cloud and runs a series of PySpark jobs.
"""
from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator, DataprocClusterDeleteOperator,
    DataProcPySparkOperator)
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.dates import days_ago

from pyspark_subdag import weekday_subdag

default_arguments = {'owner': 'Semyon Filkin', 'start_date': days_ago(1)}


def assess_day(execution_date=None):
    date = datetime.strptime(execution_date, '%Y-%m-%d')

    if date.isoweekday() < 6:
        return 'weekday_analytics'

    return 'weekend_analytics'


with DAG(
    'bigquery_data_analytics',
    schedule_interval='0 20 * * *',
    catchup=False,
    default_args=default_arguments
) as dag:

    dag.doc_md = __doc__

    create_cluster = DataprocClusterCreateOperator(
        task_id='create_cluster',
        project_id='fsp-airflow',
        cluster_name='spark-cluster-{{ ds_nodash }}',
        num_workers=2,
        master_machine_type='n1-standard-1',
        worker_machine_type='n1-standard-1',
        image_version='1.3.89-debian10',
        storage_bucket='fsp-logistics-spark-bucket',
        region='europe-central2'
    )

    create_cluster.doc_md = """## Create Dataproc cluster
    This task creates a Dataproc cluster in your project.
    """

    weekday_or_weekend = BranchPythonOperator(
        task_id='weekday_or_weekend',
        python_callable=assess_day,
        op_kwargs={'execution_date': '{{ ds }}'}
    )

    weekend_analytics = DataProcPySparkOperator(
        task_id='weekend_analytics',
        main='gs://fsp-logistics-spark-bucket/pyspark/weekend/gas_composition_count.py',
        cluster_name='spark-cluster-{{ ds_nodash }}',
        region='europe-central2',
        dataproc_pyspark_jars='gs://spark-lib/bigquery/spark-bigquery-latest.jar',
    )

    weekday_analytics = SubDagOperator(
        task_id='weekday_analytics',
        subdag=weekday_subdag(
            parent_dag='bigquery_data_analytics',
            task_id='weekday_analytics',
            schedule_interval='0 20 * * *',
            default_args=default_arguments
        )
    )

    delete_cluster = DataprocClusterDeleteOperator(
        task_id='delete_cluster',
        project_id='fsp-airflow',
        cluster_name='spark-cluster-{{ ds_nodash }}',
        trigger_rule='all_done',
        region='europe-central2'
    )

create_cluster >> weekday_or_weekend >> [
    weekday_analytics,
    weekend_analytics
] >> delete_cluster

