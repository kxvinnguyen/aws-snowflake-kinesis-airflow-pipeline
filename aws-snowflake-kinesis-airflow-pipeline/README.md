# AWS Snowflake Data Pipeline with Kinesis and Airflow

Production-ready data pipeline connecting AWS Kinesis streams to Snowflake using Airflow for orchestration and Terraform for infrastructure.

## Project Structure

```
aws-snowflake-kinesis-airflow-pipeline/
├── dags/                       # Airflow DAGs
│   └── kinesis_to_snowflake_dag.py
├── scripts/                    # Python scripts
│   ├── kinesis_data_producer.py
│   └── kinesis_to_snowflake.py
├── sql/                        # SQL scripts for Snowflake
│   └── create_snowflake_tables.sql
├── config/                     # Configuration files
│   └── config.yaml
├── terraform/                  # Terraform files for AWS resources
│   ├── main.tf
│   └── variables.tf
├── docker-compose.yml          # Docker Compose configuration
├── Dockerfile                  # Dockerfile for Airflow
├── requirements.txt            # Python dependencies
└── README.md                   # This file
```

## Prerequisites

- AWS Account with appropriate permissions
- Snowflake Account
- Docker and Docker Compose
- Terraform
- Python 3.9+

## Detailed Setup Instructions

### 0. Setting Up Required Accounts

#### AWS Account Setup (if you don't have one)
1. Visit [AWS Sign-up](https://aws.amazon.com/)
2. Click "Create an AWS Account" and follow the registration process
3. Set up an IAM user with the following permissions:
   - AmazonKinesisFullAccess
   - AmazonS3FullAccess
   - AmazonSecretsManagerFullAccess
   - IAM permissions for creating roles and policies
4. Generate access keys for this user

#### Snowflake Account Setup (if you don't have one)
1. Visit [Snowflake's website](https://www.snowflake.com/free-trial/)
2. Sign up for a 30-day free trial
3. Select your cloud provider (ideally the same as your AWS resources)
4. After activation, log in and note your account identifier from the URL
   - Format is typically: `account_name.region.cloud_provider`
5. Create a user with ACCOUNTADMIN privileges:
   ```sql
   USE ROLE securityadmin;
   CREATE USER pipeline_user PASSWORD='YourStrongPassword' DEFAULT_ROLE=accountadmin;
   GRANT ROLE accountadmin TO USER pipeline_user;
   ```

### 1. Set up AWS Resources with Terraform

Make sure you have AWS CLI and Terraform installed:
```bash
# Install Terraform (on macOS with Homebrew)
brew install terraform

# Configure AWS CLI
aws configure
# Enter your AWS Access Key ID, Secret Access Key, default region, and output format
```

Navigate to the terraform directory and initialize Terraform:

```bash
cd terraform
terraform init
```

Apply the Terraform configuration:

```bash
terraform apply
```

You will be prompted for the following variables:
- aws_region (default: us-east-1)
- project_name (default: snowflake-kinesis-pipeline)
- environment (default: dev)
- kinesis_shard_count (default: 1)
- snowflake_username (required - use the user you created in Snowflake)
- snowflake_password (required - the password you set)
- snowflake_account (required - your Snowflake account identifier)
- snowflake_database (default: KINESIS_DATA)
- snowflake_warehouse (default: KINESIS_WH)
- snowflake_role (default: ACCOUNTADMIN)
- snowflake_schema (default: PUBLIC)

After Terraform completes, it will output several values. **Make note of these values**:
- kinesis_stream_name
- firehose_delivery_stream_name
- s3_bucket_name
- airflow_logs_bucket_name
- secrets_manager_snowflake_secret_name

### 2. Update Configuration File

Open the `config/config.yaml` file and update it with the values from Terraform:

```yaml
aws:
  region: us-east-1  # Update if you changed the default
  kinesis:
    stream_name: YOUR_KINESIS_STREAM_NAME_FROM_TERRAFORM_OUTPUT
    shard_count: 1
  s3:
    data_bucket: YOUR_S3_BUCKET_NAME_FROM_TERRAFORM_OUTPUT
    airflow_logs_bucket: YOUR_AIRFLOW_LOGS_BUCKET_FROM_TERRAFORM_OUTPUT
  secrets_manager:
    snowflake_secret_name: YOUR_SECRETS_MANAGER_SECRET_NAME_FROM_TERRAFORM_OUTPUT
```

The Snowflake section can remain as is since we'll retrieve those credentials from AWS Secrets Manager.

### 3. Set up Snowflake Database and Tables

Log in to your Snowflake account with the user you created:

1. Go to [Snowflake login](https://app.snowflake.com/)
2. Enter your account identifier, username, and password
3. Create a new worksheet and run the SQL script from `sql/create_snowflake_tables.sql`:

```sql
-- Create database and schema
CREATE DATABASE IF NOT EXISTS data_pipeline_db;
USE DATABASE data_pipeline_db;
CREATE SCHEMA IF NOT EXISTS public;
USE SCHEMA public;

-- Create raw data landing table
CREATE TABLE IF NOT EXISTS raw_data (
    event_id VARCHAR,
    event_timestamp TIMESTAMP_NTZ,
    event_type VARCHAR,
    user_id VARCHAR,
    data VARIANT,
    inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create a view for analytics
CREATE OR REPLACE VIEW user_events AS
SELECT
    event_id,
    event_timestamp,
    event_type,
    user_id,
    data:product_id::VARCHAR as product_id,
    data:price::FLOAT as price,
    inserted_at
FROM raw_data
WHERE event_type = 'purchase';

-- Create a daily summary table
CREATE OR REPLACE TABLE daily_event_summary AS
SELECT 
    DATE(event_timestamp) as event_date,
    event_type,
    COUNT(*) as event_count,
    MIN(event_timestamp) as first_event,
    MAX(event_timestamp) as last_event
FROM raw_data
GROUP BY event_date, event_type;
```

### 4. Build and Start Airflow

Make sure Docker and Docker Compose are installed and running:
```bash
# Check Docker installation
docker --version
docker compose version

# If not installed on macOS, install with Homebrew
brew install --cask docker
# Then open Docker Desktop from your Applications folder
```

Build and start the Airflow services:

```bash
# From the project root directory
docker-compose up -d --build
```

This may take a few minutes when run for the first time as it downloads the necessary Docker images and builds your custom Airflow image.

Initialize the Airflow database:

```bash
# Wait for a few seconds to ensure the containers are fully started
sleep 10

# Initialize the database
docker-compose exec airflow-webserver airflow db init

# Create the admin user
docker-compose exec airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

### 5. Set up Snowflake Connection in Airflow

1. Open the Airflow UI at http://localhost:8080
2. Log in with username `admin` and password `admin`
3. Navigate to Admin > Connections
4. Add a new connection with the following details:
   - Connection Id: `snowflake_conn`
   - Connection Type: `Snowflake`
   - Account: Your Snowflake account identifier (from config)
   - Login: Your Snowflake username
   - Password: Your Snowflake password
   - Schema: The schema name (e.g., `public`)
   - Warehouse: Your warehouse name (e.g., `compute_wh` or the one you specified in Terraform)
   - Database: Your database name (e.g., `data_pipeline_db`)
   - Role: Your role (e.g., `accountadmin`)
5. Click "Save"

### 6. Enable the DAG

1. In the Airflow UI, go to the "DAGs" view
2. Find the `kinesis_to_snowflake` DAG in the list
3. Click the toggle switch to enable it
4. The DAG will now run according to the schedule you specified (hourly by default)

### 7. Generate Test Data

Run the Kinesis data producer script to generate test data:

```bash
# From the project root directory
python scripts/kinesis_data_producer.py
```

This will continuously send random events to your Kinesis stream. Press Ctrl+C to stop when you have enough test data.

You should see console output confirming that events are being sent to Kinesis. The Airflow DAG will process these events according to its schedule and load them into Snowflake.

### 8. Verify the Pipeline

1. **Check Airflow:**
   - Go to the Airflow UI
   - Click on the `kinesis_to_snowflake` DAG
   - Check the DAG runs and task status

2. **Check Snowflake:**
   - Log in to your Snowflake account
   - Run a query to see if data has been loaded:
   ```sql
   USE DATABASE data_pipeline_db;
   SELECT * FROM raw_data LIMIT 100;
   SELECT * FROM user_events LIMIT 100;
   SELECT * FROM daily_event_summary;
   ```

## Monitoring

- Monitor Airflow tasks in the Airflow UI
- Check Snowflake tables for the loaded data
- Monitor AWS resources in the AWS Console:
  - Kinesis streams: https://console.aws.amazon.com/kinesis
  - S3 buckets: https://console.aws.amazon.com/s3
  - Secrets Manager: https://console.aws.amazon.com/secretsmanager

## Troubleshooting

- **Airflow Connections Issues:**
  - Check Airflow logs in the `logs` directory
  - Run `docker-compose logs airflow-webserver` to see webserver logs
  - Run `docker-compose logs airflow-scheduler` to see scheduler logs

- **Kinesis Issues:**
  - Verify your AWS credentials with `aws sts get-caller-identity`
  - Check Kinesis stream status with `aws kinesis describe-stream --stream-name YOUR_STREAM_NAME`

- **Snowflake Issues:**
  - Verify your Snowflake connection in Airflow
  - Check that the database, schema, and tables exist in Snowflake

- **Docker Issues:**
  - Restart Docker with `docker-compose down` and then `docker-compose up -d`
  - Check container status with `docker-compose ps`

## Cleanup

To stop all services:

```bash
docker-compose down
```

To destroy AWS resources (to avoid ongoing charges):

```bash
cd terraform
terraform destroy
```

## Further Enhancements

- Add data validation
- Implement error handling and retry mechanisms
- Set up monitoring and alerting
- Implement CI/CD pipelines
- Add data quality checks
- Implement incremental loading
- Add visualization layer with tools like Tableau or PowerBI
- Implement data masking for sensitive information
