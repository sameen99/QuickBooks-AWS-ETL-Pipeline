#!/usr/bin/env python

import requests
import json
import pandas as pd
import os
from dotenv import load_dotenv
import psycopg2

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

url_report = f"https://quickbooks.api.intuit.com/v3/company/{realm_id}/query"

# Define the query to fetch all bills
query = {
    "query": "SELECT * FROM Bill"
}

# Ensure your access token is valid
headers_report = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/text",
    "Accept": "application/json"
}

# Make the request
response_report = requests.post(url_report, headers=headers_report, data=query["query"])

if response_report.status_code == 200:
    report_data = response_report.json()
    
    # Extract relevant data from the response
    bills = report_data.get("QueryResponse", {}).get("Bill", [])
    
    if bills:
        # Normalize the JSON to flatten nested data and convert to DataFrame
        df = pd.json_normalize(bills)

        # Print the DataFrame columns before filtering
        print("Original DataFrame columns:")
        print(df.columns)

        # Define the columns you want to keep (updated to match the actual column names)
        selected_columns = [
            "DueDate",
            "Balance",
            "Id",
            "SyncToken",
            "DocNumber",
            "TxnDate",
            "PrivateNote",
            "Line",
            "VendorRef.value",
            "VendorRef.name",
            "APAccountRef.value",
            "APAccountRef.name",
            "LinkedTxn"
        ]
        
        # Filter the DataFrame to include only the selected columns
        df = df[selected_columns]

        # Rename columns to snake_case
        df.columns = ["".join(["_" + char.lower() if char.isupper() else char for char in col]).lstrip("_") for col in df.columns]

        # Print the filtered DataFrame columns
        print("Filtered DataFrame columns:")
        print(df.columns)

        # Define data types
        data_types = {
            "due_date": "string",
            "balance": "float64",
            "id": "int32",
            "sync_token": "int32",
            "doc_number": "string",
            "txn_date": "string",
            "private_note": "string",
            "line": "string",
            "vendor_ref_value": "string",
            "vendor_ref_name": "string",
            "ap_account_ref_value": "string",
            "ap_account_ref_name": "string",
            "linked_txn": "string"
        }

        # Apply data types where applicable
        for col, dtype in data_types.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)

        # Check data types before saving to Parquet
        print("DataFrame data types:")
        print(df.dtypes)

        # Save DataFrame to Parquet file
        s3_url = 's3://datalake-medusadistribution/datalake/to_redshift/qb/qb_bills.parquet'
        print(f"Saving DataFrame to Parquet file at {s3_url}")
        df.to_parquet(s3_url, index=False, engine='pyarrow')

        # Define SQL statements
        sql_statements = [
            """CREATE TABLE finance.temp_qb_bills(
                due_date VARCHAR(255),          
                balance DOUBLE PRECISION,         
                id INT,            
                sync_token INT,            
                doc_number VARCHAR(50),   
                txn_date VARCHAR(255),           
                private_note VARCHAR(255),   
                line VARCHAR(MAX),   
                vendor_ref_value VARCHAR(255),            
                vendor_ref_name VARCHAR(255),   
                ap_account_ref_value VARCHAR(255),            
                ap_account_ref_name VARCHAR(255),   
                linked_txn VARCHAR(MAX)
            );""",
            f"COPY finance.temp_qb_bills FROM '{s3_url}' IAM_ROLE '{os.getenv('REDSHIFT_IAM_ROLE')}' FORMAT AS PARQUET;",
            "TRUNCATE TABLE finance.qb_bills;",
            """INSERT INTO finance.qb_bills
                SELECT 
                    TO_DATE(due_date, 'YYYY-MM-DD') AS due_date,
                    balance,
                    id, 
                    sync_token,
                    doc_number,
                    TO_DATE(txn_date, 'YYYY-MM-DD') AS txn_date,
                    private_note,
                    line,
                    vendor_ref_value,
                    vendor_ref_name,
                    ap_account_ref_value,
                    ap_account_ref_name,
                    linked_txn
                FROM finance.temp_qb_bills;""",
            """DROP TABLE finance.temp_qb_bills;"""
        ]

        for sql in sql_statements:
            print(f"Executing SQL statement: {sql}")
            execute_sql(sql)

    else:
        print("No bills data found in the response.")
else:
    print(f"Error: {response_report.status_code}, {response_report.text}")
