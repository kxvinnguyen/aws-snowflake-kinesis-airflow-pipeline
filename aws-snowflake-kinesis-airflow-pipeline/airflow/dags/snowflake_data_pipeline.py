"""
Snowflake-Kinesis Data Pipeline DAG

This DAG processes data from the S3 bucket that receives data from Kinesis Firehose
and loads it into Snowflake.

The DAG performs the following steps:
1. Check if new data is available in S3
2. Prepare Snowflake staging tables
3. Copy data from S3 to Snowflake staging
4. Transform and load data to target tables
5. Perform data quality checks
6. Send notification on completion
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.operators.email import EmailOperator

from datetime import datetime, timedelta
import os
import json
import boto3
from botocore.exceptions import ClientError

# Default args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

# Create the DAG
with DAG(
    'snowflake_kinesis_data_pipeline',
    default_args=default_args,
    description='Load data from S3 to Snowflake',
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
    tags=['snowflake', 'kinesis', 's3'],
) as dag:

    # Get configuration from environment or Airflow Variables
    S3_BUCKET = '{{ var.value.s3_bucket_name }}'
    S3_PREFIX = 'raw/year={{ execution_date.strftime("%Y") }}/month={{ execution_date.strftime("%m") }}/day={{ execution_date.strftime("%d") }}/hour={{ execution_date.strftime("%H") }}/'
    SNOWFLAKE_STAGE = '{{ var.value.snowflake_stage }}'
    SNOWFLAKE_ROLE = '{{ var.value.snowflake_role }}'
    SNOWFLAKE_WAREHOUSE = '{{ var.value.snowflake_warehouse }}'
    SNOWFLAKE_DATABASE = '{{ var.value.snowflake_database }}'
    SNOWFLAKE_SCHEMA = '{{ var.value.snowflake_schema }}'
    
    # Task 1: Check if new data is available in S3
    check_for_data = S3KeySensor(
        task_id='check_for_data',
        bucket_key=f'{S3_PREFIX}*.gz',
        wildcard_match=True,
        bucket_name=S3_BUCKET,
        timeout=60 * 60,  # 1 hour
        poke_interval=60,  # 1 minute
        aws_conn_id='aws_default',
    )
    
    # Task 2: Prepare Snowflake staging tables
    create_staging_table = SnowflakeOperator(
        task_id='create_staging_table',
        sql=f"""
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.RAW_DATA_STAGING (
            event_id VARCHAR(255),
            event_timestamp TIMESTAMP_NTZ,
            event_type VARCHAR(50),
            user_id VARCHAR(255),
            device_id VARCHAR(255),
            app_version VARCHAR(50),
            os_version VARCHAR(50),
            ip_address VARCHAR(50),
            location VARCHAR(255),
            payload VARIANT,
            raw_data VARIANT,
            file_name VARCHAR(255),
            load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        );
        """,
        snowflake_conn_id='snowflake_default',
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )
    
    # Task 3: Copy data from S3 to Snowflake
    copy_to_snowflake = S3ToSnowflakeOperator(
        task_id='copy_to_snowflake',
        s3_keys=[f'{S3_PREFIX}'],
        table='RAW_DATA_STAGING',
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format="(TYPE = 'JSON', COMPRESSION = 'AUTO')",
        copy_options=f"PATTERN='.*[.]gz' ON_ERROR='CONTINUE' FORCE=TRUE",
        snowflake_conn_id='snowflake_default',
        role=SNOWFLAKE_ROLE,
    )
    
    # Task 4: Transform and load data to target tables
    transform_data = SnowflakeOperator(
        task_id='transform_data',
        sql=f"""
        -- Insert into events table
        INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.EVENTS (
            EVENT_ID,
            EVENT_TIMESTAMP,
            EVENT_TYPE,
            USER_ID,
            DEVICE_ID,
            APP_VERSION,
            OS_VERSION,
            IP_ADDRESS,
            LOCATION,
            PAYLOAD
        )
        SELECT 
            event_id,
            event_timestamp,
            event_type,
            user_id,
            device_id,
            app_version,
            os_version,
            ip_address,
            location,
            payload
        FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.RAW_DATA_STAGING
        WHERE event_id IS NOT NULL
        AND event_id NOT IN (SELECT EVENT_ID FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.EVENTS);
        """,
        snowflake_conn_id='snowflake_default',
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )
    
    # Task 5: Run data quality checks
    data_quality_check = SnowflakeOperator(
        task_id='data_quality_check',
        sql=f"""
        -- Check for duplicate events
        SELECT COUNT(*) AS DUPLICATE_COUNT
        FROM (
            SELECT EVENT_ID, COUNT(*) AS NUM_OCCURRENCES
            FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.EVENTS
            GROUP BY EVENT_ID
            HAVING COUNT(*) > 1
        );
        
        -- Check for incomplete data
        SELECT COUNT(*) AS INCOMPLETE_DATA_COUNT
        FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.EVENTS
        WHERE EVENT_ID IS NULL
        OR EVENT_TIMESTAMP IS NULL
        OR EVENT_TYPE IS NULL
        OR USER_ID IS NULL;
        """,
        snowflake_conn_id='snowflake_default',
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        do_xcom_push=True,
    )
    
    # Define a Python function to check the result and fail if data quality issues found
    def check_data_quality(**context):
        ti = context['ti']
        result = ti.xcom_pull(task_ids='data_quality_check')
        
        if not result:
            raise ValueError("No data quality check results returned")
            
        duplicate_count = int(result[0]['DUPLICATE_COUNT'])
        incomplete_count = int(result[1]['INCOMPLETE_DATA_COUNT'])
        
        if duplicate_count > 0 or incomplete_count > 0:
            error_message = f"Data quality issues found: {duplicate_count} duplicates, {incomplete_count} incomplete records."
            raise ValueError(error_message)
            
        return "Data quality checks passed."
    
    # Task to evaluate data quality results
    evaluate_data_quality = PythonOperator(
        task_id='evaluate_data_quality',
        python_callable=check_data_quality,
        provide_context=True,
    )
    
    # Task 6: Send notification on completion
    send_success_email = EmailOperator(
        task_id='send_success_email',
        to='data-pipeline-admins@example.com',
        subject='Snowflake Kinesis Data Pipeline Completed Successfully',
        html_content="""
        <p>The Snowflake Kinesis Data Pipeline has completed successfully for execution date: {{ ds }}.</p>
        <p>Time range processed: {{ execution_date.strftime("%Y-%m-%d %H:00:00") }} to {{ execution_date.strftime("%Y-%m-%d %H:59:59") }}</p>
        <p>Please check the Airflow logs for more details.</p>
        """,
    )
    
    # Define the task dependencies
    check_for_data >> create_staging_table >> copy_to_snowflake >> transform_data >> data_quality_check >> evaluate_data_quality >> send_success_email
