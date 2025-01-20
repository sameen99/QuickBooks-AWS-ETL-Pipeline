#!/usr/bin/env python

import requests
import pandas as pd
import os
import psycopg2
from datetime import datetime
from dateutil.relativedelta import relativedelta
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

# API URL and headers
url_report = f"https://quickbooks.api.intuit.com/v3/company/{realm_id}/reports/ProfitAndLoss"
headers_report = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# Define start and end month range
start_date = datetime(2024, 1, 1)  # Adjust the starting month/year as needed
end_date = datetime.now()  # Adjust the ending month/year as needed

# Loop through each month
current_date = start_date
while current_date <= end_date:
    month_start = current_date.strftime('%Y-%m-%d')
    month_end = (current_date + relativedelta(day=31)).strftime('%Y-%m-%d')
    month_str = current_date.strftime('%Y-%m')  # YYYY-MM format for the month column

    # Query parameters for the month
    params = {
        "start_date": month_start,
        "end_date": month_end
    }

    # Make the request to fetch report data
    response_report = requests.get(url_report, headers=headers_report, params=params)

    if response_report.status_code == 200:
        print(f"API request successful for {month_str}. Status code: {response_report.status_code}")
        report_data = response_report.json()

        def process_json(json_data):
            data = []

            def process_row(row, account_path):
                if 'Header' in row:
                    header = row['Header']['ColData']
                    account = header[0]['value'] if len(header) > 0 else ''
                    total = header[1]['value'] if len(header) > 1 else ''
                    data.append([account_path, account, total])

                if 'Rows' in row:
                    for sub_row in row['Rows']['Row']:
                        sub_account_path = account_path + ' -> ' + row['Header']['ColData'][0]['value']
                        process_row(sub_row, sub_account_path)

                if 'ColData' in row:
                    col_data = row['ColData']
                    account = col_data[0]['value'] if len(col_data) > 0 else ''
                    total = col_data[1]['value'] if len(col_data) > 1 else ''
                    data.append([account_path, account, total])

                if 'Summary' in row:
                    summary = row['Summary']['ColData']
                    account = summary[0]['value'] if len(summary) > 0 else ''
                    total = summary[1]['value'] if len(summary) > 1 else ''
                    data.append([account_path + ' (Summary)', account, total])

            for row in json_data['Rows']['Row']:
                process_row(row, '')

            return data

        # Convert JSON data to DataFrame
        data = process_json(report_data)
        df = pd.DataFrame(data, columns=['Path', 'Account', 'Total'])

        # Clean up the DataFrame
        df['Total'] = pd.to_numeric(df['Total'], errors='coerce').fillna(0)  # Ensure numeric amounts in Total 
        df['Account'] = df['Account'].replace('', pd.NA)  # Replace empty strings with NaN 
        df.fillna(0, inplace=True)  # Replace NaN with 0 for saving to Parquet
        df=df.drop(columns=['Path'])

        # Rename columns to match Redshift table
        df = df.rename(columns={'Account': 'category', 'Total': 'total_amount'})

        # Add month column (since it's missing in the data)
        df['month'] = month_str

        # Ensure 'Total' is of type float for Parquet
        df['total_amount'] = df['total_amount'].astype(float)

        # Save to CSV file
        df.to_csv('p&lnewest.csv', index=False)
        print(df)

        # Save DataFrame to Parquet file
        s3_url = f's3://datalake-medusadistribution/datalake/to_redshift/qb/profit_and_loss_{month_str}.parquet'
        try:
            df.to_parquet(s3_url, index=False, engine='pyarrow')
            print(f"DataFrame for {month_str} successfully saved to Parquet file: {s3_url}")
        except Exception as e:
            print(f"An error occurred while saving DataFrame for {month_str} to Parquet file: {str(e)}")

        # Define SQL statements
        sql_statements = [
            """CREATE TABLE IF NOT EXISTS finance.temp_qb_profit_and_loss(
                  category          VARCHAR(255),
                  total_amount      DOUBLE PRECISION,
                  month             VARCHAR(255)
               );""",
            f"COPY finance.temp_qb_profit_and_loss FROM '{s3_url}' IAM_ROLE '{os.getenv('REDSHIFT_IAM_ROLE')}' FORMAT AS PARQUET;",
            """INSERT INTO finance.qb_profit_and_loss
                       SELECT 
                            category,
                            total_amount,
                            TO_CHAR(TO_DATE(month, 'YYYY-MM'), 'Mon,YYYY') AS month
                       FROM finance.temp_qb_profit_and_loss;""",
            """DROP TABLE finance.temp_qb_profit_and_loss;"""
        ]

        for sql in sql_statements:
            print(f"Executing SQL statement: {sql}")
            execute_sql(sql)

    else:
        print(f"Error: {response_report.status_code}, {response_report.text}")

    # Move to the next month
    current_date += relativedelta(months=1)
