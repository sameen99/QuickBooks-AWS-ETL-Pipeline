#!/usr/bin/env python

import requests
import base64
import pandas as pd
import os
from dotenv import load_dotenv
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from io import BytesIO
import psycopg2
import datetime

def debug_message(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def error_message(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[ERROR] [{timestamp}] {message}")

def execute_sql(sql_query):
    try:
        debug_message("Executing SQL query...")
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
        debug_message("SQL query executed successfully.")
    except Exception as e:
        error_message(f"An error occurred while executing SQL query: {str(e)}")

def fetch_quickbooks_data():
    try:
        debug_message("Fetching QuickBooks data...")
        load_dotenv("/home/sameen/qb_scripts/.env")
        load_dotenv("/home/sameen/qb_scripts/.env_access")
        client_id = os.getenv("CLIENT_ID")
        client_secret = os.getenv("CLIENT_SECRET")
        refresh_token = os.getenv("REFRESH_TOKEN")
        realm_id = os.getenv("REALM_ID")
        access_token = os.getenv("CURR_AUTH_TOKEN")
        if not all([client_id, client_secret, refresh_token, realm_id, access_token]):
            error_message("Missing required credentials. Check .env and .env_access files.")
            return None
        
        url_query = f"https://quickbooks.api.intuit.com/v3/company/{realm_id}/query"
        select_statement = "select * from Deposit"
        headers_query = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "text/plain",
            "Accept": "application/json",
        }
        all_data = []  
        has_more = True
        start_position = 1
        while has_more:
            params_query = {
                "query": f"{select_statement} STARTPOSITION {start_position}",
            }
            response_query = requests.get(url_query, headers=headers_query, params=params_query)
            if response_query.status_code != 200:
                error_message(f"Failed to fetch data from QuickBooks API. Status code: {response_query.status_code}")
                return None
            response_json = response_query.json()
            query_response = response_json.get("QueryResponse", {})
            deposits = query_response.get("Deposit", [])
            all_data.extend(deposits)  
            start_position += len(deposits)
            has_more = query_response.get("maxResults") == 100
        df_selected = pd.json_normalize(all_data)  
        return df_selected
    except Exception as e:
        error_message(f"An error occurred while fetching QuickBooks data: {str(e)}")
        return None


try:
    debug_message("Script started.")
    
    df_selected = fetch_quickbooks_data()
    if df_selected is not None:
        debug_message("QuickBooks data fetched.")
        
        selected_columns = ['TotalAmt', 'Id', 'TxnDate', 'PrivateNote', 'Line',
                            'DepositToAccountRef.value', 'DepositToAccountRef.name',
                            'CurrencyRef.value', 'CurrencyRef.name', 'DocNumber']

        df_selected = df_selected[selected_columns]

        df_selected.columns = ["".join(["_" + char.lower() if char.isupper() else char for char in col]).lstrip("_") for col in selected_columns]

        df_selected.columns = df_selected.columns.str.replace('.', '_')
        df_selected.columns = df_selected.columns.str.replace('__', '_')
        data_types = {
            'total_amt' : 'double',  
            'id' : 'int32',          
            'txn_date': 'string', 
            'private_note' : 'string',
            'line' : 'string', 
            'deposit_to_account_ref_value' : 'int32',
            'deposit_to_account_ref_name' : 'string',
            'currency_ref_value' : 'string',
            'currency_ref_name' : 'string',
            'doc_number' : 'string'
        }
        df_selected = df_selected.astype(data_types)

        s3_url = 's3://datalake-medusadistribution/datalake/to_redshift/qb/qb_deposit.parquet'
        # Write DataFrame to Parquet with the specified column names
        df_selected.to_parquet(s3_url)

        # Define SQL statements
        sql_statements = [
            """CREATE TABLE finance.temp_qb_deposit (
                total_amt DOUBLE PRECISION,
                id INT,  
                txn_date VARCHAR(255),
                private_note VARCHAR(514),
                line VARCHAR(65535), 
                deposit_to_account_ref_value INT,
                deposit_to_account_ref_name VARCHAR(255),
                currency_ref_value VARCHAR(3),
                currency_ref_name VARCHAR(50),
                doc_number VARCHAR(255)
            );""",
            f"COPY finance.temp_qb_deposit FROM '{s3_url}' IAM_ROLE '{os.getenv('REDSHIFT_IAM_ROLE')}' FORMAT AS PARQUET;",
            "TRUNCATE TABLE finance.qb_deposit;",
            """INSERT INTO finance.qb_deposit
               SELECT 
                   total_amt,
                   id,
                   TO_TIMESTAMP(txn_date, 'YYYY-MM-DD HH24:MI:SS'), -- Cast txn_date to TIMESTAMP
                   private_note,
                   line,
                   deposit_to_account_ref_value,
                   deposit_to_account_ref_name,
                   currency_ref_value,
                   currency_ref_name,
                   doc_number
               FROM finance.temp_qb_deposit;""",
            "DROP TABLE finance.temp_qb_deposit;"
        ]

        # Execute SQL statements
        for sql_statement in sql_statements:
            execute_sql(sql_statement)
            
    else:
        error_message("Failed to fetch QuickBooks data. Exiting script.")
except Exception as e:
    error_message(f"An unexpected error occurred: {str(e)}")
