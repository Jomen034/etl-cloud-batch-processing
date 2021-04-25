"""A liveness prober dag for monitoring composer.googleapis.com/environment/healthy."""
import airflow
from airflow import DAG, models
from datetime import timedelta, datetime
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator


bucket_path = models.Variable.get("bucket_path")
project_id = models.Variable.get("project_id")


default_args = {
    # Tell airflow to start one day ago, so that it runs as soon as you upload it
    "start_date": datetime(2021,3,10),
    "end_date": datetime(2021,3,15),
    "depends_on_past": True,
    "dataflow_default_options": {
        "project": project_id,
        # This is a subfolder for storing temporary files, like the staged pipeline job.
        "temp_location": bucket_path + "/tmp/",
        "numWorkers": 1,
    },
}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
    # The id you will see in the DAG airflow page
    "dailiy_search_history",
    default_args=default_args,
    # The interval with which to schedule the DAG
    schedule_interval= "00 21 * * *",  # Override to match your needs
) as dag:

    store_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket='week_2_bs',
        source_objects= ["keyword_search/search_{{ ds_nodash }}.csv"],
        destination_project_dataset_table= "daily_search_history.daily_search_keyword_history",
        source_format="csv",
        skip_leading_rows=1,
        schema_fields=[
            {'name': 'user_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'search_keyword', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'search_result_count', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'created_at', 'type': 'STRING', 'mode': 'REQUIRED'},
        ],
        write_disposition="WRITE_TRUNCATE",
        wait_for_downstream=True,
        depends_on_past=True
    )

    convert_data_type = BigQueryOperator(
        task_id='collect_n_update_data',
        sql = """
        SELECT  
            SAFE_CAST(user_id AS INT64 ) user_id,
            search_keyword,
            SAFE_CAST(search_result_count AS INT64 ) search_result_count,
            created_at
        FROM 
            `pkl-playing-fields.daily_search_history.daily_search_keyword_history`
        """,
        write_disposition='WRITE_APPEND',
        destination_dataset_table=project_id + ":daily_search_history.daily_search_results",
        use_legacy_sql=False,
        dag =dag
    )

    get_most_searched_keyword = BigQueryOperator(
        task_id='most_searched_keywords',
        sql = """
        SELECT  
            user_id, 
            search_keyword, 
            search_result_count, 
            SAFE_CAST(LEFT(created_at, 10) AS DATE) AS `created_date` 
        FROM    
            `pkl-playing-fields.daily_search_history.daily_search_results` 
        WHERE   
            SAFE_CAST(LEFT(created_at, 10) AS DATE) = '{{ ds }}'
        ORDER BY search_result_count DESC
        LIMIT 1
        """,
        write_disposition='WRITE_APPEND',
        destination_dataset_table=project_id + ":daily_search_history.most_search_keyword_history",
        use_legacy_sql=False,
        dag = dag
    )

    store_to_bq >> convert_data_type >> get_most_searched_keyword