# airfllow-tutorial

Some training Apache Airflow DAGs via GCP BigQuery

### DAGS:
**'bigquery_data_load'** - repeating data loading from сsv files to the BiqQuery table. After loading processed files moving to the backup directory.

![](https://github.com/Filkin-S/airfllow-tutorial/blob/master/IMGS/data_load.bmp)

**'bigquery_data_analytics'** - daily Spark analytics job. Running different Spark-jobs on the cluster, depending on the day of the week.

![](https://github.com/Filkin-S/airfllow-tutorial/blob/master/IMGS/data_analytics.bmp)

**'big_query_data_validation'** - DAG with custom airflow-plugin operator and sensor. Checks if table exists, then checks if table empty.

![](https://github.com/Filkin-S/airfllow-tutorial/blob/master/IMGS/data_validation.bmp)
