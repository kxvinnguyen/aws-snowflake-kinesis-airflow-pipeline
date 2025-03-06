#!/usr/bin/env python3
import json
import random
import time
import uuid
import boto3
import yaml
import os
from datetime import datetime

def load_config():
    """Load configuration from YAML file"""
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config/config.yaml')
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def generate_event():
    """Generate a random event for testing"""
    event_types = ['view', 'click', 'purchase', 'signup']
    products = [
        {'product_id': 'p1', 'price': 19.99},
        {'product_id': 'p2', 'price': 29.99},
        {'product_id': 'p3', 'price': 39.99},
        {'product_id': 'p4', 'price': 49.99},
    ]
    
    event_type = random.choice(event_types)
    user_id = f"user_{random.randint(1, 1000)}"
    product = random.choice(products)
    
    return {
        'event_id': str(uuid.uuid4()),
        'event_timestamp': datetime.now().isoformat(),
        'event_type': event_type,
        'user_id': user_id,
        'data': product
    }

def put_record_to_kinesis(kinesis_client, stream_name, data):
    """Send a record to Kinesis stream"""
    response = kinesis_client.put_record(
        StreamName=stream_name,
        Data=json.dumps(data),
        PartitionKey=data['user_id']
    )
    return response

def main():
    """Main function to generate and send events to Kinesis"""
    config = load_config()
    
    # Initialize AWS client
    kinesis_client = boto3.client(
        'kinesis',
        region_name=config['aws']['region']
    )
    
    stream_name = config['aws']['kinesis']['stream_name']
    
    # Check if the stream exists or use the one created by Terraform
    try:
        kinesis_client.describe_stream(StreamName=stream_name)
        print(f"Stream {stream_name} exists")
    except kinesis_client.exceptions.ResourceNotFoundException:
        print(f"Stream {stream_name} not found. Make sure you've run Terraform to create it.")
        return
    
    print(f"Producing data to Kinesis stream: {stream_name}")
    events_count = 0
    
    try:
        while True:
            event = generate_event()
            response = put_record_to_kinesis(kinesis_client, stream_name, event)
            events_count += 1
            print(f"Sent event {events_count}: {event['event_type']} for user {event['user_id']} - Shard: {response['ShardId']}")
            time.sleep(1)  # Send one event per second
    except KeyboardInterrupt:
        print(f"\nStopped after sending {events_count} events.")

if __name__ == "__main__":
    main() 