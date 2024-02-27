# Facebook Ads Data Extraction and SQL Integration

## Overview

This project aims to streamline the process of extracting data from Facebook Ads and seamlessly integrating it into dinamically mutating tables in a SQL database. By leveraging Python scripts, the project automates data retrieval, normalization, and insertion into the database, simplifying the workflow for analysts and data engineers.

## Features

- **Data Extraction**: Utilizes the Facebook Graph API to extract campaign performance data directly from Facebook Ads Manager.
- **Data Normalization**: Normalizes extracted data to ensure consistent formatting and structure.
- **SQL Integration**: Inserts normalized data into SQL tables for further analysis and reporting.
- **AWS Lambda Compatibility**: Designed to be deployable as an AWS Lambda function for serverless execution.
- **Command-Line Interface**: Provides a command-line interface for manual execution and testing.
- **Customizable Configuration**: Supports configuration options for database credentials, API tokens, and extraction parameters.

## Components

### 1. `facebook.py`

Handles data extraction from Facebook Ads Manager using the Facebook Graph API. The module retrieves campaign performance metrics and structures the data for further processing.

### 2. `postgres.py`

Manages database connections and operations related to PostgreSQL databases. Includes functions for establishing connections, creating tables, and inserting data.

### 3. `utils.py`

Contains utility functions used across the project, including functions for data normalization, key extraction, and error handling.

### 4. `main.py`

Serves as the entry point for the application. Orchestrates the data extraction and insertion processes, leveraging functions from `facebook.py` and `postgres.py`.

### 5. `secrets.py`

Stores sensitive information such as API tokens and database credentials. Ensure this file is properly secured and excluded from version control.

## Usage

### Local Execution

To execute the data extraction and insertion locally:

1. Install the required Python dependencies using `pip install -r requirements.txt`.
2. Update the `secrets.py` file with your Facebook API token and database credentials.
3. Run the `main.py` script with appropriate command-line arguments for start and end dates.

```bash
python main.py --start_date YYYY-MM-DD --end_date YYYY-MM-DD
```

## AWS Lambda Deployment

To deploy the application as an AWS Lambda function:

1. Package the project files along with dependencies into a deployment package.
2. Create a Lambda function using the packaged deployment package.
3. Configure the Lambda function to trigger based on specific events (e.g., scheduled events using CloudWatch Events).
4. Ensure proper IAM permissions for the Lambda function to access Facebook Graph API and PostgreSQL.

## Dependencies

- `requests`: HTTP library for making API requests.
- `psycopg2`: PostgreSQL adapter for Python.
- `boto3`: AWS SDK for Python.
