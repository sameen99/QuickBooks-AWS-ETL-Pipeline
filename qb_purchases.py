#!/usr/bin/env python

import requests
import pandas as pd
import os
from dotenv import load_dotenv
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
        select_statement = "SELECT * FROM Purchase"
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
            purchases = query_response.get("Purchase", [])
            all_data.extend(purchases)  
            start_position += len(purchases)
            has_more = len(purchases) == 100
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
        
        selected_columns = ['PaymentType','Credit','TotalAmt', 'Id','TxnDate', 'PrivateNote','Line','AccountRef.value', 'EntityRef.value','EntityRef.name']

        df_selected = df_selected[selected_columns].copy()

        df_selected.columns = ["".join(["_" + char.lower() if char.isupper() else char for char in col]).lstrip("_") for col in selected_columns]
        df_selected.columns = df_selected.columns.str.replace('.', '_')
        df_selected.columns = df_selected.columns.str.replace('__', '_')

        # Extract 'line' from 'line' column and handle potential errors
        df_selected['line'] = df_selected['line'].apply(lambda x: json.dumps(x) if isinstance(x, list) else json.dumps([]))
        df_selected['line'] = df_selected['line'].apply(json.loads)
        
        # Explode the 'line' column
        df_exploded = df_selected.explode('line')

        df_exploded.reset_index(drop=True, inplace=True)

        # Normalize the nested JSON data within the 'line' column
        df_normalized = pd.json_normalize(df_exploded['line'])
        
        # Combine the normalized data with the original DataFrame
        df_result = pd.concat([df_exploded.drop(columns=['line']), df_normalized], axis=1)

        df_result.rename(columns={
           'Id': 'line_id',
           'Description': 'line_description',
           'Amount': 'line_amount',
           'DetailType': 'line_detail_type',
           'AccountBasedExpenseLineDetail.AccountRef.value':'line_account_value',
           'AccountBasedExpenseLineDetail.AccountRef.name': 'line_account_name',
           'AccountBasedExpenseLineDetail.BillableStatus':'line_billable_status',
           'AccountBasedExpenseLineDetail.TaxCodeRef.value':'line_taxcode_value'
        }, inplace=True)

        # Handle NaNs and incompatible values
        df_result['id'] = pd.to_numeric(df_result['id'], errors='coerce').fillna(0).astype('Int32')
        df_result['account_ref_value'] = pd.to_numeric(df_result['account_ref_value'], errors='coerce').fillna(0).astype('Int32')
        df_result['entity_ref_value'] = pd.to_numeric(df_result['entity_ref_value'], errors='coerce').fillna(0).astype('Int32')
        df_result['line_id'] = pd.to_numeric(df_result['line_id'], errors='coerce').fillna(0).astype('Int32')
        df_result['line_account_value'] = pd.to_numeric(df_result['line_account_value'], errors='coerce').fillna(0).astype('Int32')



        # Define the correct column order as per the Redshift table
        correct_column_order = [
            'payment_type',
            'credit',
            'total_amt',
            'id',
            'txn_date',
            'private_note',
            'account_ref_value',
            'entity_ref_value',
            'entity_ref_name',
            'line_id', 
            'line_description', 
            'line_amount',  
            'line_account_value', 
            'line_account_name',
            'line_billable_status',
            'line_taxcode_value'
        ]

        # Reorder the DataFrame columns to match the Redshift schema
        df_result = df_result[correct_column_order]
        
        data_types = {
            'payment_type':'string',
            'credit':'string',
            'total_amt': 'float64',
            'id':'Int32',
            'txn_date':'string',
            'private_note':'string',
            'account_ref_value':'Int32',
            'entity_ref_value':'Int32',
            'entity_ref_name':'string',
            'line_id':'Int32', 
            'line_description':'string', 
            'line_amount': 'float64',  
            'line_account_value':'Int32', 
            'line_account_name': 'string',
            'line_billable_status':'string',
            'line_taxcode_value': 'string'
        }
        df_result = df_result.astype(data_types)
        
        # Check data types before saving to Parquet
        print(df_result.dtypes)

        # Save DataFrame to Parquet file
        s3_url = 's3://datalake-medusadistribution/datalake/to_redshift/qb/qb_purchase.parquet'
        df_result.to_parquet(s3_url, index=False)

        # Define SQL statements
        sql_statements = [
            """CREATE TABLE finance.temp_qb_purchase(
                   payment_type VARCHAR(255),
                   credit VARCHAR(255),
                   total_amt DOUBLE PRECISION,
                   id INT,
                   txn_date VARCHAR(255),
                   private_note VARCHAR(1024),
                   account_ref_value INT,
                   entity_ref_value INT,
                   entity_ref_name VARCHAR(255),
                   line_id INT,
                   line_description VARCHAR(1024),
                   line_amount DOUBLE PRECISION,
                   line_account_value INT,
                   line_account_name VARCHAR(255),
                   line_billable_status VARCHAR(255),
                   line_taxcode_value VARCHAR(255)
            );""",
            f"COPY finance.temp_qb_purchase FROM '{s3_url}' IAM_ROLE '{os.getenv('REDSHIFT_IAM_ROLE')}' FORMAT AS PARQUET;",
            "TRUNCATE TABLE finance.qb_purchase;",
            """INSERT INTO finance.qb_purchase
               SELECT 
                      payment_type,
                      credit,
                      total_amt,
                      id,
                      TO_DATE(txn_date, 'YYYY-MM-DD') AS txn_date,
                      private_note,
                      account_ref_value,
                      entity_ref_value,
                      entity_ref_name,
                      line_id,
                      line_description,
                      line_amount,
                      line_account_value,
                      line_account_name,
                      line_billable_status,
                      line_taxcode_value
                 FROM finance.temp_qb_purchase;"""
        ]

        for sql in sql_statements:
            execute_sql(sql)
        
        debug_message("Data processed and loaded into Redshift successfully.")
    else:
        error_message("Failed to fetch or process QuickBooks data.")
except Exception as e:
    error_message(f"An unexpected error occurred: {str(e)}")
