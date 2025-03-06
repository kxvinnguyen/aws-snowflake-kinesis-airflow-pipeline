#!/usr/bin/env python3
from datetime import datetime, timedelta
import yaml
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Add scripts directory to path for importing custom modules
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'scripts'))
from kinesis_to_snowflake import process_kinesis_data, load_config

# Load configuration
config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config/config.yaml')
config = load_config()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'kinesis_to_snowflake',
    default_args=default_args,
    description='Load data from Kinesis to Snowflake',
    schedule_interval=config.get('airflow', {}).get('schedule_interval', '@hourly'),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['kinesis', 'snowflake'],
) as dag:

    # Task to extract data from Kinesis and load into Snowflake
    load_data_task = PythonOperator(
        task_id='load_data_from_kinesis',
        python_callable=process_kinesis_data,
        op_kwargs={'config': config},
    )
    
    # Task to run data transformation in Snowflake
    transform_data_task = SnowflakeOperator(
        task_id='transform_data',
        snowflake_conn_id='snowflake_conn',
        sql="""
        -- Refresh the daily event summary table
        CREATE OR REPLACE TABLE daily_event_summary AS
        SELECT 
            DATE(event_timestamp) as event_date,
            event_type,
            COUNT(*) as event_count,
            MIN(event_timestamp) as first_event,
            MAX(event_timestamp) as last_event
        FROM raw_data
        GROUP BY event_date, event_type;
        """,
        warehouse=config['snowflake']['warehouse'],
        database=config['snowflake']['database'],
        schema=config['snowflake']['schema'],
        role=config['snowflake']['role'],
    )
    
    # Task to log summary of processed data
    log_summary_task = PythonOperator(
        task_id='log_summary',
        python_callable=lambda ti: print(f"Processed {ti.xcom_pull(task_ids='load_data_from_kinesis', key='records_processed')} records"),
    )
    
    # Set task dependencies
    load_data_task >> transform_data_task >> log_summary_task 