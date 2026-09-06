"""Books to Scrape Airflow + Spark 파이프라인 DAG."""

from datetime import timedelta

from pendulum import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from data_collection_pipeline.crawling import run_crawling
from data_collection_pipeline.extract import run_extract
from data_collection_pipeline.load import run_load
from data_collection_pipeline.minio_storage import sync_batch_to_minio


def crawl_task(**context):
    """Airflow 실행 시각으로 batch_id를 만들고 HTML을 수집한다."""
    batch_id = context['logical_date'].in_timezone('Asia/Seoul').format('YYYYMMDD_HHmmss')
    return run_crawling(batch_id=batch_id)


def extract_task(**context):
    batch_id = context['ti'].xcom_pull(task_ids='crawl_books')['batch_id']
    return run_extract(batch_id)


def load_task(**context):
    batch_id = context['ti'].xcom_pull(task_ids='crawl_books')['batch_id']
    return run_load(batch_id)


def minio_task(**context):
    batch_id = context['ti'].xcom_pull(task_ids='crawl_books')['batch_id']
    return sync_batch_to_minio(batch_id)


default_args = {'owner': 'education', 'retries': 2, 'retry_delay': timedelta(minutes=1)}

with DAG(
    dag_id='books_airflow_spark_pipeline',
    description='Books to Scrape 수집·Spark 처리·MySQL/MinIO 저장',
    start_date=datetime(2026, 1, 1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=['books', 'spark', 'education'],
) as dag:
    crawl_books = PythonOperator(task_id='crawl_books', python_callable=crawl_task)
    extract_books = PythonOperator(task_id='extract_books', python_callable=extract_task)

    spark_common = {
        'conn_id': 'spark_default',
        'deploy_mode': 'client',
        'verbose': True,
        'conf': {
            'spark.driver.host': 'airflow-scheduler',
            'spark.driver.bindAddress': '0.0.0.0',
            'spark.driver.port': '37777',
            'spark.blockManager.port': '37778',
            'spark.executor.instances': '1',
            'spark.executor.cores': '1',
            'spark.executor.memory': '1g',
        },
    }
    batch_argument = "{{ ti.xcom_pull(task_ids='crawl_books')['batch_id'] }}"
    spark_preprocess = SparkSubmitOperator(
        task_id='spark_preprocess',
        application='/opt/project/spark/preprocess_books.py',
        application_args=['--batch-id', batch_argument],
        name='books-preprocess',
        **spark_common,
    )
    spark_aggregate = SparkSubmitOperator(
        task_id='spark_aggregate',
        application='/opt/project/spark/aggregate_books.py',
        application_args=['--batch-id', batch_argument],
        name='books-aggregate',
        **spark_common,
    )
    load_mysql = PythonOperator(task_id='load_mysql', python_callable=load_task)
    sync_minio = PythonOperator(task_id='sync_minio', python_callable=minio_task)

    crawl_books >> extract_books >> spark_preprocess >> spark_aggregate >> load_mysql >> sync_minio
