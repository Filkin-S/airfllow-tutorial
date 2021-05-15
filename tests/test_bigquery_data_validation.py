import unittest
from datetime import datetime
from unittest.mock import patch

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import TaskInstance
from airflow.operators.biqquery_plugin import BigQueryDataValidationOperator
from airflow.utils.state import State

def mock_run_query():
    def return_empty_list(*args, **kwargs):
        return []
    return return_empty_list


class TestBigQueryDataValidationOperator(unittest.TestCase):
    
    def setUp(self):
        EXEC_DATE = '2021-05-13'

        self.dag = DAG(
            'test_bigquery_data_validation',
            schedule_interval='@daily',
            default_args={'start_date': EXEC_DATE},
        )

        self.op = BigQueryDataValidationOperator(
            task_id='big_query_op',
            sql='SELECT COUNT(1) FROM `example.example.example`',
            location='europe-central2',
            dag=self.dag,
        )

        self.ti = TaskInstance(task=self.op, execution_date=datetime.strptime(EXEC_DATE, '%Y-%m-%d'))

    @patch.object(
        BigQueryDataValidationOperator, 'run_query', new_callable=mock_run_query
    )
    def test_with_empty_result(self, mock):
        with self.assertRaises(AirflowException) as context:
            self.ti.run()
        self.assertEqual(self.ti.state, State.FAILED)
        self.assertEqual(str(context.exception), 'Query returned no results.')
