#!/usr/bin/env python

import requests
import base64
import pandas as pd
import os
from dotenv import load_dotenv
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import psycopg2
import datetime
import json

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
        #load_dotenv("D:\Medusa\Quickbooks\intuit_keys.env")
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
        select_statement = "select * from Journalentry"
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
            journalentries = query_response.get("JournalEntry", [])
            all_data.extend(journalentries)  
            start_position += len(journalentries)
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
        
        selected_columns = ['Adjustment', 'Id', 'DocNumber', 'TxnDate', 'Line','PrivateNote']

        df_selected = df_selected[selected_columns].copy()

        df_selected.columns = ["".join(["_" + char.lower() if char.isupper() else char for char in col]).lstrip("_") for col in selected_columns]
        df_selected.columns = df_selected.columns.str.replace('.', '_')
        df_selected.columns = df_selected.columns.str.replace('__', '_')

        # Extract 'amount' from 'line' column
        df_selected['line'] = df_selected['line'].apply(lambda x: json.dumps(x))
        df_selected['line'] = df_selected['line'].apply(json.loads)
        #print(df_selected['line'])
        # Explode the 'line' column
        df_exploded = df_selected.explode('line')

        df_exploded.reset_index(drop=True, inplace=True)
        #print(df_exploded)

       # Normalize the nested JSON data within the 'line' column
        df_normalized = pd.json_normalize(df_exploded['line'])
        #print(df_normalized)
       # Combine the normalized data with the original DataFrame
        df_result = pd.concat([df_exploded.drop(columns=['line']), df_normalized], axis=1)

        df_result.rename(columns={
           'Id': 'line_id',
           'Description': 'line_description',
           'Amount': 'line_amount',
           'DetailType': 'line_detail_type',
           'JournalEntryLineDetail.PostingType': 'line_posting_type',
           'JournalEntryLineDetail.Entity.Type': 'line_entity_type',
           'JournalEntryLineDetail.Entity.EntityRef.value': 'line_entity_value',
           'JournalEntryLineDetail.Entity.EntityRef.name': 'line_entity_name',
           'JournalEntryLineDetail.AccountRef.value': 'line_account_value',
           'JournalEntryLineDetail.AccountRef.name': 'line_account_name',
           'JournalEntryLineDetail.ClassRef.value': 'line_class_value',
           'JournalEntryLineDetail.ClassRef.name': 'line_class_name',
           'JournalEntryLineDetail.DepartmentRef.value': 'line_department_value',
           'JournalEntryLineDetail.DepartmentRef.name': 'line_department_name'
        }, inplace=True)

        df_result.drop(columns=['line_detail_type'], inplace=True)

        df_result['line_entity_value'].fillna(0, inplace=True)  # Replace NaN with 0
        df_result['line_entity_value'] = df_result['line_entity_value'].astype(int)  # Convert to integer

        df_result['line_entity_type'] = df_result['line_entity_type'].astype(str)
        df_result['line_account_value'] = df_result['line_account_value'].astype('float64')

       # Print the resulting DataFrame
        print(df_result)

        # Define the correct column order as per the Redshift table
        correct_column_order = [
            'adjustment', 
            'id', 
            'doc_number', 
            'txn_date', 
            'private_note', 
            'line_id', 
            'line_description', 
            'line_amount', 
            'line_posting_type', 
            'line_entity_type', 
            'line_entity_value', 
            'line_entity_name', 
            'line_account_value', 
            'line_account_name', 
            'line_class_value', 
            'line_class_name', 
            'line_department_value', 
            'line_department_name'
        ]

        # Reorder the DataFrame columns to match the Redshift schema
        df_result = df_result[correct_column_order]
        
        data_types = {
            'adjustment' : 'boolean',  
            'id' : 'int32',          
            'doc_number' : 'string',
            'txn_date': 'string', 
            'private_note' : 'string',
            'line_id': 'int32',
            'line_description': 'string',
            'line_amount': 'float64',
            'line_posting_type': 'string',
            'line_entity_type': 'string',
            'line_entity_value': 'float64',
            'line_entity_name': 'string',
            'line_account_value': 'float64',
            'line_account_name': 'string',
            'line_class_value': 'float64',
            'line_class_name': 'string',
            'line_department_value': 'float64',
            'line_department_name': 'string'
        }
        df_result = df_result.astype(data_types)
        # Check data types before saving to Parquet
        print(df_result.dtypes)

        s3_url = 's3://datalake-medusadistribution/datalake/to_redshift/qb/qb_journalentry.parquet'
        df_result.to_parquet(s3_url)

        # Define SQL statements
        sql_statements = [
            """CREATE TABLE finance.temp_qb_journal_entry(
                adjustment BOOLEAN,
                id INT,
                doc_number VARCHAR(255),
                txn_date VARCHAR(255),
                private_note VARCHAR(514),
                line_id INT,
                line_description VARCHAR(max),
                line_amount DOUBLE PRECISION,
                line_posting_type VARCHAR(255),
                line_entity_type VARCHAR(max),
                line_entity_value DOUBLE PRECISION,
                line_entity_name VARCHAR(255),
                line_account_value DOUBLE PRECISION,
                line_account_name VARCHAR(255),
                line_class_value DOUBLE PRECISION,
                line_class_name VARCHAR(255),
                line_department_value DOUBLE PRECISION,
                line_department_name VARCHAR(255)
            );""",
            f"COPY finance.temp_qb_journal_entry FROM '{s3_url}' IAM_ROLE '{os.getenv('REDSHIFT_IAM_ROLE')}' FORMAT AS PARQUET;",
            "TRUNCATE TABLE finance.qb_journal_entry;",
            """INSERT INTO finance.qb_journal_entry
               SELECT 
                   adjustment,
                   id,
                   doc_number,
                   TO_TIMESTAMP(txn_date, 'YYYY-MM-DD HH24:MI:SS'),
                   private_note,
                   line_id,
                   line_description,
                   line_amount,
                   line_posting_type,
                   line_entity_type,
                   line_entity_value,
                   line_entity_name,
                   line_account_value,
                   line_account_name,
                   line_class_value,
                   line_class_name,
                   line_department_value,
                   line_department_name
               FROM finance.temp_qb_journal_entry;""",
            "DROP TABLE finance.temp_qb_journal_entry;"
        ]

        # Execute SQL statements
        for sql_statement in sql_statements:
            execute_sql(sql_statement)
    
    else:
        error_message("Failed to fetch QuickBooks data. Exiting script.")
except Exception as e:
    error_message(f"An unexpected error occurred: {str(e)}")