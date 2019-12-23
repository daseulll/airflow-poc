from airflow import models
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from datetime import datetime

default_args = {
    'owner' : 'seul',
    'start_date' : datetime(2019, 12, 20),
    'email' : ['nldaseul@gmail.com'],
    'retries' : 1,
    'project_id' : 'connecting-208115'
}

query = """
    SELECT *
    FROM `connecting-208115.matchingServer.bunyan_log`
    LIMIT 1000
"""

with models.DAG(
    dag_id='extract_matching_data',
    schedule_interval='@once',
    default_args=default_args) as dag:

    bq_query=BigQueryOperator(
        task_id='extract_maehing_data_once',
        sql=query,
        use_legacy_sql=False,
        destination_dataset_table='connecting-208115.matchingServer.bunyan_log',
        bigquery_conn_id='bigquery_default',
        write_disposition='WRITE_TRUNCATE'
    )

