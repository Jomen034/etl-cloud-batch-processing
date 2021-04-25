from datetime import datetime, timedelta

from airflow import models
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account

project_id = models.Variable.get("project_id")
data_source_project_id = models.Variable.get("data_source_project_id")
credentials_path = models.Variable.get("credentials_path")
bucket_path = models.Variable.get("bucket_path")

credentials_source = service_account.Credentials.from_service_account_file(credentials_path+"data_source_project.json")
client_source = bigquery.Client(credentials= credentials_source,project=data_source_project_id)


def pull_from_bq(**kwargs):
    sql = f"""SELECT * FROM pkl-playing-fields.unified_events.event WHERE event_name = 'purchase_item'
              AND DATE(event_datetime) between '{kwargs.get('ds')}' and DATE_ADD('{kwargs.get('ds')}', INTERVAL 2 DAY)"""
    event_table = client_source.query(sql).result().to_dataframe()
    event_table['transaction_id'], event_table['transaction_detail_id'], event_table['transaction_number'] = '', '', ''
    event_table['purchase_quantity'], event_table['purchase_amount'], event_table['purchase_payment_method'] = '', '', ''
    event_table['purchase_source'], event_table['product_id'] = '', ''
    for i in range(len(event_table)):
        try:
            if len(event_table['event_params'][i]) == 21:
                event_table['transaction_id'][i] = event_table['event_params'][i][0]
                event_table['transaction_detail_id'][i] = event_table['event_params'][i][1]
                event_table['transaction_number'][i] = event_table['event_params'][i][2]
                event_table['purchase_quantity'][i] = event_table['event_params'][i][3]
                event_table['purchase_amount'][i] = event_table['event_params'][i][4]
                event_table['purchase_payment_method'][i] = event_table['event_params'][i][5]
                event_table['purchase_source'][i] = event_table['event_params'][i][6]
                event_table['product_id'][i] = event_table['event_params'][i][7]
            else:
                event_table['transaction_id'][i] = np.NaN
                event_table['transaction_detail_id'][i] = np.NaN
                event_table['transaction_number'][i] = event_table['event_params'][i][0]
                event_table['purchase_quantity'][i] = np.NaN
                event_table['purchase_amount'][i] = np.NaN
                event_table['purchase_payment_method'][i] = np.NaN
                event_table['purchase_source'][i] = np.NaN
                event_table['product_id'][i] = event_table['event_params'][i][1]
        except ValueError:
            pass
    event_table = event_table.drop('event_params', axis=1)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    table_id = project_id + ".transactions.raw"
    credentials_my = service_account.Credentials.from_service_account_file(credentials_path+"my_project.json")
    client_target = bigquery.Client(credentials=credentials_my, project=project_id)
    client_target.load_table_from_dataframe(event_table, table_id, job_config=job_config)

sql_store = """ SELECT  transaction_id.value.int_value `transaction_id`,
                        transaction_detail_id.value.int_value `transaction_detail_id`,
                        transaction_number.value.string_value `transaction_number`,
                        event_datetime `transaction_datetime`,
                        purchase_quantity.value.int_value `purchase_quantity`,
                        purchase_amount.value.float_value `purchase_amount`,
                        purchase_payment_method.value.string_value `purchase_payment_method`,
                        purchase_source.value.string_value `purchase_source`,
                        product_id.value.int_value `product_id`,
                        user_id, state, city, created_at, '{{ ds }}' `ext_created_at`
                FROM    `academi-cloud-etl.transactions.raw`
                WHERE   DATE(event_datetime) BETWEEN '{{ ds }}' AND DATE_ADD('{{ ds }}', INTERVAL 2 DAY)"""

default_args = {
    "start_date": datetime(2021,3,21),
    "end_date": datetime(2021,3,27),
    "depends_on_past": True,
    "dataflow_default_options": {
        "project": project_id,
        "temp_location": bucket_path + "/tmp/",
        "numWorkers": 1,
    },
}


with models.DAG(
    # The id you will see in the DAG airflow page
    "transactions_table_dag",
    default_args=default_args,
    # The interval with which to schedule the DAG
    schedule_interval=timedelta(days=3),  # Override to match your needs
) as dag:

    preprocessed = PythonOperator(
        task_id='storing_preprocessed_data',
        python_callable=pull_from_bq,
        provide_context=True
    )

    store = BigQueryOperator(
        task_id='storing_final_table',
        sql=sql_store,
        write_disposition='WRITE_APPEND',
        destination_dataset_table=project_id + ":transactions.transactions_table",
        use_legacy_sql=False,
    )

    preprocessed >> store