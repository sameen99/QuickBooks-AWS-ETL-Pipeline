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
        print(f"SQL Query: {sql_query}")

# Load environment variables
load_dotenv("/home/sameen/qb_scripts/.env")
load_dotenv("/home/sameen/qb_scripts/.env_access")

# Get environment variables
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
refresh_token = os.getenv("REFRESH_TOKEN")
realm_id = os.getenv("REALM_ID")
access_token = os.getenv("CURR_AUTH_TOKEN")

url_report = f"https://quickbooks.api.intuit.com/v3/company/{realm_id}/reports/TransactionListByVendor"

# Ensure your access token is valid
headers_report = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# Initialize pagination control variables
has_more = True
start_position = 1
page_size = 100  # Number of records to fetch per page (change according to API limits)

# Prepare a list to hold all transaction data across pages
all_transaction_data = []

while has_more:
    # Define the query parameters, including start position and page size
    params = {
        "start_date": "2015-01-01",
        "end_date": datetime.now().strftime('%Y-%m-%d'),
        "start_position": start_position,  # Pagination parameter
        "max_results": page_size,          # Limit the number of results per request
        "columns": "Vendor ID, Vendor Name"
    }

    # Make the request
    response_report = requests.get(url_report, headers=headers_report, params=params)

    if response_report.status_code == 200:
        report_data = response_report.json()
        print(f"Fetched page starting from position: {start_position}")
        
        # Extract header data
        header = report_data.get('Header', {})
        report_time = header.get('Time', '')
        start_period = header.get('StartPeriod', '')
        end_period = header.get('EndPeriod', '')

        
    # Extract transaction data from the JSON response
    transactions = []

    for vendor_section in report_data['Rows']['Row']:
        vendor_name = vendor_section['Header']['ColData'][0]['value']
        for transaction in vendor_section['Rows']['Row']:
            transaction_data = {
                'Vendor': vendor_name,
                'Date': transaction['ColData'][0]['value'],
                'Transaction Type': transaction['ColData'][1]['value'],
                'Num': transaction['ColData'][2]['value'],
                'Posting': transaction['ColData'][3]['value'],
                'Memo/Description': transaction['ColData'][4]['value'],
                'Account': transaction['ColData'][5]['value'],
                'Amount': transaction['ColData'][6]['value']
            }
            transactions.append(transaction_data)

    # Create a DataFrame from the extracted transaction data
    df = pd.DataFrame(transactions)
                            # Append the extracted data to the list
                            rows.append({
                                'Vendor': vendor_name,
                                'Date': date,
                                'transaction_type': txn_type,
                                'doc_num': doc_num,
                                'posting': is_posting,
                                'description': memo,
                                'account': account_name,
                                'amount': amount,
                                'start_period': start_period,
                                'end_period': end_period,
                                'report_time': report_time
                            })
        
        # Append the data from the current page to the overall transaction data
        all_transaction_data.extend(rows)
    
        # Check if there is more data to fetch
        has_more = report_data.get('hasMore', False)  # Use 'hasMore' if provided by API
        start_position += page_size  # Move to the next set of results

    else:
        print(f"Failed to retrieve data from API. Status code: {response_report.status_code}")
        print(f"Response content: {response_report.text}")
        has_more = False  # Stop if there is an error

# After fetching all pages, create DataFrame
df = pd.DataFrame(all_transaction_data)

# Replace empty strings with NaN in the amount column
df['amount'].replace('', pd.NA, inplace=True)

# Convert 'amount' column to numeric, setting invalid parsing as NaN
df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

print(df)
 # Apply data types
df = df.astype({
    'vendor_id' : 'int32',
    'vendor_name' : 'string',
    'date': 'string',
    'transaction_type': 'string',
    'doc_num': 'string',
    'posting': 'string',
    'description': 'string',
    'account': 'string',
    'amount': 'float64',
    'start_period': 'string',
    'end_period': 'string',
    'report_time': 'string'
})

print(df.dtypes)

# Save DataFrame to Parquet file
s3_url = 's3://datalake-medusadistribution/datalake/to_redshift/qb/qb_transactionlistbyvendor.parquet'
try:
    df.to_parquet(s3_url, index=False, engine='pyarrow')
    print(f"DataFrame successfully saved to Parquet file: {s3_url}")
except Exception as e:
    print(f"An error occurred while saving DataFrame to Parquet file: {str(e)}")

# SQL statements to load data into Redshift
sql_statements = [
    """CREATE TABLE IF NOT EXISTS finance.temp_qb_transactionlist_by_vendor(
          vendor_id INT,
          vendor_name VARCHAR(1024),
          date VARCHAR(10),
          transaction_type VARCHAR(50),
          doc_num VARCHAR(50),
          posting VARCHAR(10),
          description VARCHAR(625),
          account VARCHAR(100),
          amount DOUBLE PRECISION,
          start_period VARCHAR(10),
          end_period VARCHAR(10),
          report_time VARCHAR(25)
       );""",
    f"COPY finance.temp_qb_transactionlist_by_vendor FROM '{s3_url}' IAM_ROLE '{os.getenv('REDSHIFT_IAM_ROLE')}' FORMAT AS PARQUET;",
    "TRUNCATE TABLE finance.qb_transactionlist_by_vendor;",
    """INSERT INTO finance.qb_transactionlist_by_vendor
               SELECT 
                   vendor_id,
                   vendor_name,
                   TO_DATE(date, 'YYYY-MM-DD') AS date,
                   transaction_type,
                   doc_num,
                   posting,
                   description,
                   account,
                   amount,
                   TO_DATE(start_period, 'YYYY-MM-DD') AS start_period,
                   TO_DATE(end_period, 'YYYY-MM-DD') AS end_period,
                   TO_DATE(report_time, 'YYYY-MM-DD') AS report_time
               FROM finance.temp_qb_transactionlist_by_vendor;""",
    """DROP TABLE IF EXISTS finance.temp_qb_transactionlist_by_vendor;"""
]

for sql in sql_statements:
    print(f"Executing SQL statement: {sql}")
    execute_sql(sql)
