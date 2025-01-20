#!/usr/bin/env python

import requests
import pandas as pd
import json
import os
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

def execute_sql(sql_query):
    try:
        print("Executing SQL query...")
        conn = psycopg2.connect(
            dbname=os.getenv("REDSHIFT_DB"),
            user=os.getenv("REDSHIFT_USER"),
            password=os.getenv("REDSHIFT_PASSWORD"),
            host=os.getenv("REDSHIFT_HOST"),
            port=os.getenv("REDSHIFT_PORT")
        )
        cur = conn.cursor()
        cur.execute(sql_query)
        conn.commit()
        cur.close()
        conn.close()
        print("SQL query executed successfully.")
    except Exception as e:
        print(f"An error occurred while executing SQL query: {str(e)}")

# Load environment variables
load_dotenv("/home/sameen/qb_scripts/.env")
load_dotenv("/home/sameen/qb_scripts/.env_access")

# Get environment variables
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
refresh_token = os.getenv("REFRESH_TOKEN")
realm_id = os.getenv("REALM_ID")
access_token = os.getenv("CURR_AUTH_TOKEN")

# For debugging: Print environment variables (sensitive data redacted)
print(f"CLIENT_ID: {client_id}")
print(f"REALM_ID: {realm_id}")

# API URL and headers
url_report = f"https://quickbooks.api.intuit.com/v3/company/{realm_id}/reports/TransactionList"
headers_report = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# Query parameters
params = {
    "start_date": "2022-01-01",
    "end_date": datetime.now().strftime('%Y-%m-%d')
}

# Make the request to fetch report data
response_report = requests.get(url_report, headers=headers_report, params=params)

if response_report.status_code == 200:
    print(f"API request successful. Status code: {response_report.status_code}")
    report_data = response_report.json()

    # Extract the header information
    header_info = report_data['Header']
    start_period = header_info['StartPeriod']
    end_period = header_info['EndPeriod']
    
    # Extract the columns
    columns = [col['ColTitle'] for col in report_data['Columns']['Column']]
    
    # Extract the rows
    rows = []
    for row in report_data['Rows']['Row']:
        row_data = [col.get('value', None) for col in row['ColData']]
        rows.append(row_data)
    
    # Create DataFrame
    df = pd.DataFrame(rows, columns=columns)
    
    # Add header information as new columns to the DataFrame
    df['Start Period'] = start_period
    df['End Period'] = end_period
    
    # Display the DataFrame
    print(df)
else:
    print(f"Error: {response_report.status_code}, {response_report.text}")

# Convert non-numeric values in 'Amount' to NaN
df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')

# Optionally, rename the column for consistency
df.rename(columns={'Amount': 'amount'}, inplace=True)

# Check if there are any NaN values in the 'amount' column
nan_amounts = df['amount'].isna().sum()
if nan_amounts > 0:
    print(f"Warning: {nan_amounts} rows contain non-numeric values in the 'amount' column and have been set to NaN.")

# Transform column names to match the required format
transformed_columns = [
    'date',
    'transaction_type',
    'doc_num',
    'is_no_post',
    'name',
    'description',
    'account_name',
    'split',
    'amount',
    'start_period',
    'end_period'
]

df.columns = transformed_columns

# Define data types with fallback handling
def convert_column_to_numeric(df, column_name):
    df[column_name] = pd.to_numeric(df[column_name], errors='coerce')  # Convert to numeric, set invalid parsing as NaN

# Apply data types
df = df.astype({
    'date': 'string',
    'transaction_type': 'string',
    'doc_num': 'string',
    'is_no_post': 'string',
    'name': 'string',
    'description': 'string',
    'account_name': 'string',
    'split': 'string',
    'amount': 'float64',
    'start_period': 'string',
    'end_period': 'string'
})

# Check data types before saving to Parquet
print("Data types after conversion:")
print(df.dtypes)

# Save DataFrame to Parquet file
s3_url = 's3://datalake-medusadistribution/datalake/to_redshift/qb/qb_transactionlist.parquet'
try:
    df.to_parquet(s3_url, index=False, engine='pyarrow')
    print(f"DataFrame successfully saved to Parquet file: {s3_url}")
except Exception as e:
    print(f"An error occurred while saving DataFrame to Parquet file: {str(e)}")

# Define SQL statements
sql_statements = [
    """CREATE TABLE finance.temp_qb_transaction_list(
          date               VARCHAR(255),
          transaction_type   VARCHAR(50),
          doc_num            VARCHAR(50),
          is_no_post         VARCHAR(3),
          name               VARCHAR(255),
          description        VARCHAR(1024),
          account_name       VARCHAR(255),
          split              VARCHAR(255),
          amount             DOUBLE PRECISION,
          start_period       VARCHAR(255),
          end_period         VARCHAR(255)
       );""",
    f"COPY finance.temp_qb_transaction_list FROM '{s3_url}' IAM_ROLE '{os.getenv('REDSHIFT_IAM_ROLE')}' FORMAT AS PARQUET;",
    "TRUNCATE TABLE finance.qb_transaction_list;",
    """INSERT INTO finance.qb_transaction_list
               SELECT 
                    TO_DATE(date, 'YYYY-MM-DD') AS date,
                    transaction_type,
                    doc_num,
                    is_no_post,
                    name,
                    description,
                    account_name,
                    split,
                    amount,
                    TO_DATE(start_period, 'YYYY-MM-DD') AS start_period,
                    TO_DATE(end_period, 'YYYY-MM-DD') AS end_period
               FROM finance.temp_qb_transaction_list;""",
    """DROP TABLE finance.temp_qb_transaction_list;"""
]

for sql in sql_statements:
    print(f"Executing SQL statement: {sql}")
    execute_sql(sql)

else:
    print(f"Failed to retrieve data from API. Status code: {response_report.status_code}")
    print(f"Response content: {response_report.text}")
