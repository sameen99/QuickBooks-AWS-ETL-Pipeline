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
        select_statement = "select * from billpayment"
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
            billpayments = query_response.get("BillPayment", [])
            all_data.extend(billpayments)  
            start_position += len(billpayments)
            has_more = query_response.get("maxResults") == 100
        df_selected = pd.json_normalize(all_data)  
        print("Columns after normalization:", df_selected.columns)
        return df_selected
    except Exception as e:
        error_message(f"An error occurred while fetching QuickBooks data: {str(e)}")
        return None


try:
    debug_message("Script started.")
    
    df_selected = fetch_quickbooks_data()
    if df_selected is not None:
        debug_message("QuickBooks data fetched.")
        
        selected_columns = ['PayType', 'TotalAmt', 'Id', 'TxnDate', 'VendorRef.value','VendorRef.name', 'CheckPayment.BankAccountRef.value','CheckPayment.BankAccountRef.name',
                    'DocNumber', 'CreditCardPayment.CCAccountRef.value', 'CreditCardPayment.CCAccountRef.name']

        df_selected = df_selected[selected_columns]

        df_selected.columns = ["".join(["_" + char.lower() if char.isupper() else char for char in col]).lstrip("_") for col in selected_columns]

        df_selected.columns = df_selected.columns.str.replace('.', '_')
        df_selected.columns = df_selected.columns.str.replace('__', '_')
 
        # Rename the column
        df_selected.rename(columns={'credit_card_payment_c_c_account_ref_value': 'credit_card_payment_cc_account_ref_value'}, inplace=True)
        df_selected.rename(columns={'credit_card_payment_c_c_account_ref_name': 'credit_card_payment_cc_account_ref_name'}, inplace=True)


        # Fill NaN values in the 'check_payment_bank_account_ref_value' column
        df_selected['check_payment_bank_account_ref_value'] = df_selected['check_payment_bank_account_ref_value'].fillna(0).astype('int32')
        df_selected['credit_card_payment_cc_account_ref_value'] = df_selected['credit_card_payment_cc_account_ref_value'].fillna(0).astype('int32')
      
        data_types = {
            'pay_type': 'string',
            'total_amt':'float64',
            'id': 'int32',
            'txn_date' : 'string',
            'vendor_ref_value' : 'int32',
            'vendor_ref_name' : 'string',
            'check_payment_bank_account_ref_value': 'int32',
            'check_payment_bank_account_ref_name' : 'string',
            'doc_number' :'string', 
            'credit_card_payment_cc_account_ref_value' : 'int32', 
            'credit_card_payment_cc_account_ref_name' : 'string'
            
        }
        print("Columns before type casting:", df_selected.columns)
        df_selected = df_selected.astype(data_types)
        

        s3_url = 's3://datalake-medusadistribution/datalake/to_redshift/qb/qb_billpayment.parquet'
        # Write DataFrame to Parquet with the specified column names
        df_selected.to_parquet(s3_url)

        # Define SQL statements
        sql_statements = [
            """CREATE TABLE finance.temp_qb_billpayment (
                pay_type VARCHAR(255),
                total_amt DOUBLE PRECISION,
                id INT,
                txn_date VARCHAR(255),
                vendor_ref_value INT,
                vendor_ref_name VARCHAR(255),
                check_payment_bank_account_ref_value INT,
                check_payment_bank_account_ref_name VARCHAR(255),
                doc_number VARCHAR(255),
                credit_card_payment_cc_account_ref_value INT,
                credit_card_payment_cc_account_ref_name VARCHAR(255)
            );""",
            f"COPY finance.temp_qb_billpayment FROM '{s3_url}' IAM_ROLE '{os.getenv('REDSHIFT_IAM_ROLE')}' FORMAT AS PARQUET;",
            "TRUNCATE TABLE finance.qb_billpayment;",
            """INSERT INTO finance.qb_billpayment
               SELECT 
                   pay_type,
                   total_amt,
                   id,
                   TO_TIMESTAMP(txn_date, 'YYYY-MM-DD HH24:MI:SS'),
                   vendor_ref_value,
                   vendor_ref_name,
                   check_payment_bank_account_ref_value,
                   check_payment_bank_account_ref_name,
                   doc_number,
                   credit_card_payment_cc_account_ref_value,
                   credit_card_payment_cc_account_ref_name
               FROM finance.temp_qb_billpayment;""",
            "DROP TABLE finance.temp_qb_billpayment;"
        ]

        # Execute SQL statements
        for sql_statement in sql_statements:
            execute_sql(sql_statement)
            
    else:
        error_message("Failed to fetch QuickBooks data. Exiting script.")
except Exception as e:
    error_message(f"An unexpected error occurred: {str(e)}")
