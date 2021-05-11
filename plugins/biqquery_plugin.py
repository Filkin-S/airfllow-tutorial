from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from google.cloud import bigquery
from googleapiclient.errors import HttpError


class BigQueryDataValidationOperator(BaseOperator):
    template_fields = ['sql']
    ui_color = '#fcf197'

    @apply_defaults
    def __init__(
        self,
        sql,
        gcp_conn_id='google_cloud_default',
        use_legacy_sql=False,
        location=None,
        *args,
        **kwargs
    ):

        super().__init__(*args, **kwargs)
        self.sql = sql
        self.gcp_conn_id = gcp_conn_id
        self.use_legacy_sql = use_legacy_sql
        self.location = location

    def run_query(self, project, credencials):
        client = bigquery.Client(project=project, credentials=credencials)

        query_job = client.query(self.sql)
        results = query_job.result()

        return [list(row.values()) for row in results][0]


    def execute(self, context):
        hook = BigQueryHook(
            bigquery_conn_id=self.gcp_conn_id,
            use_legacy_sql=self.use_legacy_sql,
            location=self.location,
        )

        records = self.run_query(
            project=hook._get_field('project'),
            credencials=hook._get_credentials()
        )

        if not records:
            raise AirflowException('Query returned no results.')
        elif not all([bool(record) for record in records]):
            raise AirflowException(
                f'Test failed.\nQuery: {self.sql}\nRecords: {records}'
            )

        self.log.info(f'Test passed.\nQuery: {self.sql}\nRecords: {records}')


class BiqQueryDatasetSensor(BaseSensorOperator):
    template_fields = ['project_id', 'dataset_id']
    ui_color = '#feeef1'

    def __init__(
        self,
        project_id,
        dataset_id,
        gcp_conn_id='google_cloud_default',
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.project_id=project_id
        self.dataset_id=dataset_id
        self.gcp_conn_id=gcp_conn_id

    def poke(self, contex):
        hook = BigQueryHook(bigquery_conn_id=self.gcp_conn_id)
        service = hook.get_service()

        try:
            service.datasets().get(
                datasetId=self.dataset_id,
                projectId=self.project_id
                ).execute()
            
            return True
        except HttpError as e:
            if e.resp['status'] == '404':
                return False

            raise AirflowException(f'Error: {e}')



class BigQueryPlugin(AirflowPlugin):
    name = 'bigquery_plugin'
    operators = [BigQueryDataValidationOperator]
    sensors = [BiqQueryDatasetSensor]
