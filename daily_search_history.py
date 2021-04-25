

"""A liveness prober dag for monitoring composer.googleapis.com/environment/healthy."""

import airflow
from airflow import DAG, models
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta, datetime
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

bucket_path = models.Variable.get("bucket_path")
project_id = models.Variable.get("project_id")
# gce_zone = models.Variable.get("gce_zone")
# gce_region = models.Variable.get("gce_region")


default_args = {
    "start_date": datetime(2021, 3, 10),
    "end_date": datetime(2021, 3, 15),
    # Free google cloud couldn't handle mass dataflow jobs at a time(Low Workers Quota), so we have to execute the dags one at a time
    "depends_on_past": True,
    "dataflow_default_options": {
        "project": project_id,
        "numWorkers": 1,
        # This is a subfolder for storing temporary files, like the staged pipeline job.
        "tempLocation": bucket_path + "/tmp/"
    }
}

# Define a DAG (directed acyclic graph) of tasks.
with models.DAG(
    # The id you will see in the DAG airflow page
    "composer_dataflow_dag",
    default_args=default_args,
    # The interval with which to schedule the DAG
    schedule_interval='@daily',  # Override to match your needs
) as dag:

    def get_nodash_date(**kwargs):
        # get execution date in format YYYYMMDD
        return kwargs.get('ds_nodash')

    def get_dash_date(**kwargs):
        # get execution date in format YYYY-MM-DD
        return kwargs.get('ds')

    dataflow_job = DataflowTemplateOperator(
        # The task id of your job
        task_id="dataflow_operator_transform_csv_to_bq",
        # The name of the template that you're using.
        # Below is a list of all the templates you can use.
        # For versions in non-production environments, use the subfolder 'latest'
        # https://cloud.google.com/dataflow/docs/guides/templates/provided-batch#gcstexttobigquery
        template="gs://dataflow-templates/latest/GCS_Text_to_BigQuery",
        # Use the link above to specify the correct parameters for your template.
        parameters={
            "javascriptTextTransformFunctionName": "transformCSVtoJSON",
            "JSONPath": bucket_path + "/schema.json",
            "javascriptTextTransformGcsPath": bucket_path + "/transform.js",
            "inputFilePattern": "gs://daily_searched_data/keyword_search/search_" + '{{ ti.xcom_pull("get_execution_date") }}' + ".csv",
            "outputTable": project_id + ":daily_search_history.daily_search_keyword_history",
            "bigQueryLoadingTemporaryDirectory": bucket_path + "/tmp/"
        }
    )

    
    bigquery_job = BigQueryOperator(
        task_id='bq_top_searched_query',
        sql="""
        SELECT 
            *
        FROM #project_id.dataset.table
            `etl-cloud-batch-processing.daily_search_history.daily_search_keyword_history` 
        WHERE 
            created_at LIKE '{}%' 
        ORDER BY search_result_count DESC 
        LIMIT 1
        """.format('{{ ti.xcom_pull("get_dash_date") }}'),
        use_legacy_sql=False,
        destination_dataset_table='etl-cloud-batch-processing.daily_search_history.most_search_keyword_history',
        write_disposition='WRITE_APPEND'
    )

dataflow_job >> bigquery_job