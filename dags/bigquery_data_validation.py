from airflow import DAG
from airflow.operators.bigquery_plugin import (BigQueryDataValidationOperator,
                                               BiqQueryDatasetSensor)
from airflow.utils.dates import days_ago


default_arguments = {'owner': 'Semyon Filkin', 'start_date': days_ago(1)}

with DAG(
    'big_query_data_validation',
    schedule_interval='@daily',
    catchup=False,
    default_args=default_arguments,
    user_defined_macros={'project': 'fsp-airflow'}
) as dag:

    is_table_empty = BigQueryDataValidationOperator(
        task_id='is_table_empty',
        sql='SELECT COUNT(1) FROM `{{ project }}.vehicle_analytics.history`',
        location='europe-central2',
    )

    dataset_exists = BiqQueryDatasetSensor(
        task_id='dataset_exists',
        project_id='{{ project }}',
        dataset_id = 'vehicle_analytics',
    )

dataset_exists >> is_table_empty
