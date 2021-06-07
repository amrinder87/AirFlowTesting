
import datetime
import time
import csv

from airflow import models
from airflow.operators import bash_operator
from airflow.contrib.operators import bigquery_operator
from airflow.contrib.operators import bigquery_to_gcs
from airflow.operators import email_operator
from airflow.operators import dummy_operator
from airflow.utils import trigger_rule



bq_dataset_name = 'airflow_bq_test_data'
bq_recent_questions_table_id = bq_dataset_name + '.comman_name'
output_file = 'gs://{gcs_dest_bucket}/recent_questionsS.csv'.format(
    gcs_dest_bucket=models.Variable.get('gcs_dest_bucket'))

max_query_date = '2018-02-01'
min_query_date = '2018-01-01'



#Query public dataset to find most command names
default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2020, 4, 8),
    'schedule_interval': '@hourly',
    'email_on_failure' : True,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : datetime.timedelta(minutes=5),
    'project_id' : models.Variable.get('project_id')
}

with models.DAG(
        'composer_sample_bq_gs',
        default_args=default_dag_args) as dag:

    start = dummy_operator.DummyOperator(
          task_id='start',
          trigger_rule='all_success'
    )

    end = dummy_operator.DummyOperator(
      task_id='end',
      trigger_rule='all_success'
    )

    delay_task = bash_operator.BashOperator(
    task_id="delay_python_task",
    bash_command= 'sleep 30'
    )

    make_bq_dataset = bash_operator.BashOperator(
       task_id="make_bq_dataset",
       bash_command='bq ls {} || bq mk {}'.format(bq_dataset_name,bq_dataset_name)

    )

    delete_bq_dataset = bash_operator.BashOperator(
       task_id="delete_bq_dataset",
       bash_command='bq rm -r -f %s' % bq_dataset_name,
       Trigger_rule = trigger_rule.TriggerRule.ALL_DONE
    )

    bq_most_common_name_query = bigquery_operator.BigQueryOperator(
    task_id='bq_most_comman_name',
    bql="""
        SELECT owner_display_name, title, view_count
        FROM `bigquery-public-data.stackoverflow.posts_questions`
        WHERE creation_date < CAST('{max_date}' AS TIMESTAMP)
            AND creation_date >= CAST('{min_date}' AS TIMESTAMP)
        ORDER BY view_count DESC
        LIMIT 100
        """.format(max_date=max_query_date, min_date=min_query_date),
        use_legacy_sql=False,
        destination_dataset_table=bq_recent_questions_table_id
    )

    export_data_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
      task_id = 'export_data_to_gcs',
      source_project_dataset_table=bq_recent_questions_table_id,
      destination_cloud_storage_uris=[output_file],
      export_format='CSV'
    )


    start >> make_bq_dataset >> bq_most_common_name_query >> export_data_to_gcs >> delay_task >>  delete_bq_dataset >> end






     #define DAG Dependencies


     #>> make_bq_dataset
     #>> bq_most_common_name_query
     #>> export_most_common_name_to_gcs
     #>> delete_bq_dataset
