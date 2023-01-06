from __future__ import print_function
import datetime
from io import BytesIO
from airflow import models
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
#from google.cloud import storage
import pandas as pd
from sqlalchemy import create_engine
import sqlite3
from sqlite3 import Error

default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2018, 1, 1),
}


def create_connection():
    """ create a database connection to a SQLite database """
    conn = None
    db_file = r"C:\Dev MLsense\airflow-train\dags\data-dump.db"
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


def transform_csv_file():
    df = pd.read_csv('dags/updated.csv')
    df = df.drop(['time_ref', 'code', 'status'], axis=1)
    df = df.drop(df.index[50:])
    print(df)
    # db_conn = create_engine('sqlite:///dags/data-dump.db', connect_args={'timeout': 300})
    db_conn_cloud_sql = create_engine("postgresql+psycopg2://usman:123456789@130.211.206.126:5455/usman-db")
    df.to_sql('trade_data', db_conn_cloud_sql, if_exists='replace', index=False)
    print('records added:', df.count())


# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
        'usman-data-transformation',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    def greeting():
        import logging
        logging.info('Hello World!')

    def download_file_from_gcs(project_id="playground-s-11-f1943c45"):
        """
        When interacting with Google Cloud Client libraries, the library can auto-detect the
        credentials to use.

        // TODO(Developer):
        //  1. Before running this sample,
        //  set up ADC as described in https://cloud.google.com/docs/authentication/external/set-up-adc
        //  2. Replace the project variable.
        //  3. Make sure that the user account or service account that you are using
        //  has the required permissions. For this sample, you must have "storage.buckets.list".
        Args:
            project_id: The project id of your Google Cloud project.
        """

        # This snippet demonstrates how to list buckets.
        # *NOTE*: Replace the client created below with the client required for your application.
        # Note that the credentials are not specified when constructing the client.
        # Hence, the client library will look for credentials using ADC.
        #storage_client = storage.Client(project=project_id)
        #bucket = storage_client.bucket("airflow-project-data-csv")
        #blob = bucket.blob("annual-enterprise-survey-2021-financial-year-provisional-size-bands-csv.csv")
        #blob.download_to_filename("downloaded-csv.csv")
        pass


    db_task = PythonOperator(
        task_id='create_db',
        python_callable=create_connection)

    transform_task = PythonOperator(
        task_id='download-file',
        python_callable=transform_csv_file)

    goodbye_bash = BashOperator(
        task_id='bye',
        bash_command='echo Completed.')

    # db_task >> transform_task >> goodbye_bash
    transform_task >> goodbye_bash


def test_gcs_hook():
    #from airflow.providers.google.cloud.hooks.gcs import GCSHook
    #hook_obj = GCSHook(gcp_conn_id='google_cloud_default', delegate_to=None, impersonation_chain=None)
    #file_handle = hook_obj.download_as_byte_array(bucket_name='airflow-project-data-csv',
    #                                              object_name='annual-enterprise-survey-2021-financial-'
    #                                                          'year-provisional-size-bands-csv (1).csv')
    #df = pd.read_csv(BytesIO(file_handle))
    #print(df.head())
    pass


if __name__ == "__main__":
    # test_gcs_hook()
    transform_csv_file()
    # authenticate_implicit_with_adc(project_id="playground-s-11-f1943c45")
