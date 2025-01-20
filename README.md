# QuickBooks-AWS-ETL-Pipeline
This repository contains the ETL pipeline for processing and integrating QuickBooks data into a scalable data architecture using AWS services.

---

## **Key Features**

- **API Integration**: Fetches tables and reports data directly from QuickBooks APIs.
- **Data Structuring**: Organizes the retrieved data into DataFrames (DF) for easy manipulation and processing.
- **Parquet Conversion**: Converts structured data into Parquet format for optimized storage and querying.
- **AWS S3 Storage**: Stores the Parquet files securely in AWS S3 buckets.
- **Redshift Integration**: Loads the processed data into Amazon Redshift for advanced analytics and reporting.

---

## **Technologies Used**

- **QuickBooks API**: For data extraction from QuickBooks.
- **Python**: For scripting and data processing.
- **Libraries**:
  - **Pandas**: For data manipulation and transformation.
  - **Boto3**: For interacting with AWS services.
  - **Pyarrow/Fastparquet**: For Parquet conversion.
- **AWS Services**:
  - **S3**: For data storage.
  - **Redshift**: For data warehousing and analytics.

---

## **Purpose**

This repository aims to simplify and automate the process of managing QuickBooks data by creating a robust and scalable ETL pipeline. It ensures that data is properly structured, stored, and available for business intelligence and reporting tasks.

---


