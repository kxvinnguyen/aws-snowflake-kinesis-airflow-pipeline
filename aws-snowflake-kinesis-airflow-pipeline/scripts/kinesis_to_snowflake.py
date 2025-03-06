#!/usr/bin/env python3
import boto3
import json
import yaml
import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import time

def load_config():
    """Load configuration from YAML file"""
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config/config.yaml')
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def get_snowflake_credentials(secret_name, region_name):
    """Retrieve Snowflake credentials from AWS Secrets Manager"""
    client = boto3.client('secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret_string = response['SecretString']
    return json.loads(secret_string)

def get_kinesis_records(kinesis_client, stream_name, shard_id, iterator_type='LATEST'):
    """Get records from a Kinesis shard"""
    response = kinesis_client.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType=iterator_type
    )
    shard_iterator = response['ShardIterator']
    
    response = kinesis_client.get_records(
        ShardIterator=shard_iterator,
        Limit=100
    )
    
    records = []
    for record in response['Records']:
        data = json.loads(record['Data'])
        records.append(data)
        
    return records, response['NextShardIterator']

def connect_to_snowflake(config):
    """Connect to Snowflake using configuration"""
    # Get credentials from AWS Secrets Manager if available
    secret_name = config.get('aws', {}).get('secrets_manager', {}).get('snowflake_secret_name')
    
    if secret_name:
        try:
            snowflake_creds = get_snowflake_credentials(
                secret_name, 
                config['aws']['region']
            )
            
            # Override config with credentials from Secrets Manager
            config['snowflake']['account'] = snowflake_creds.get('account', config['snowflake']['account'])
            config['snowflake']['username'] = snowflake_creds.get('username', config['snowflake']['username'])
            config['snowflake']['password'] = snowflake_creds.get('password', config['snowflake']['password'])
        except Exception as e:
            print(f"Error getting credentials from Secrets Manager: {e}")
            print("Using credentials from config file instead")
    
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=config['snowflake']['username'],
        password=config['snowflake']['password'],
        account=config['snowflake']['account'],
        warehouse=config['snowflake']['warehouse'],
        database=config['snowflake']['database'],
        schema=config['snowflake']['schema'],
        role=config['snowflake']['role']
    )
    
    return conn

def load_data_to_snowflake(conn, records):
    """Load data into Snowflake"""
    if not records:
        print("No records to load")
        return 0
    
    # Convert to DataFrame
    df = pd.DataFrame(records)
    
    # Convert data column to string (JSON)
    df['data'] = df['data'].apply(json.dumps)
    
    # Write to Snowflake
    success, num_chunks, num_rows, output = write_pandas(
        conn=conn,
        df=df,
        table_name="raw_data"
    )
    
    print(f"Loaded {num_rows} rows to Snowflake")
    return num_rows

def process_kinesis_data(config, **kwargs):
    """Process data from Kinesis and load into Snowflake"""
    # Get task instance from Airflow context if available
    ti = kwargs.get('ti', None)
    
    # AWS clients
    kinesis_client = boto3.client(
        'kinesis',
        region_name=config['aws']['region']
    )
    
    stream_name = config['aws']['kinesis']['stream_name']
    
    # Get information about the stream
    try:
        response = kinesis_client.describe_stream(StreamName=stream_name)
        shards = response['StreamDescription']['Shards']
    except Exception as e:
        print(f"Error accessing Kinesis stream: {e}")
        if ti:
            ti.xcom_push(key='records_processed', value=0)
        return 0
    
    total_records = 0
    
    # Connect to Snowflake
    try:
        conn = connect_to_snowflake(config)
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        if ti:
            ti.xcom_push(key='records_processed', value=0)
        return 0
    
    # Process each shard
    for shard in shards:
        shard_id = shard['ShardId']
        print(f"Processing shard {shard_id}")
        
        try:
            # Get records from Kinesis
            records, next_shard_iterator = get_kinesis_records(
                kinesis_client, stream_name, shard_id, 'TRIM_HORIZON'
            )
            
            # Load records to Snowflake
            if records:
                rows_loaded = load_data_to_snowflake(conn, records)
                total_records += rows_loaded
        except Exception as e:
            print(f"Error processing shard {shard_id}: {e}")
    
    conn.close()
    
    # If running in Airflow, push the result to XCom
    if ti:
        ti.xcom_push(key='records_processed', value=total_records)
    
    return total_records

if __name__ == "__main__":
    # For testing outside of Airflow
    config = load_config()
    total_records = process_kinesis_data(config)
    print(f"Processed {total_records} records in total") 