from google.cloud import bigquery
import os

PROJECT_DATASET = "bdm-project-446018.big_data_project"  # Centralized GCP project and dataset identifier

def create_raw_table():
    client = bigquery.Client()

    table_id = f"{PROJECT_DATASET}.raw_customer_data"

    schema = [
        bigquery.SchemaField("invoice_no", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("customer_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("gender", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("age", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("quantity", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("price", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("payment_method", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("invoice_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("shopping_mall", "STRING", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id, schema=schema)

    try:
        client.get_table(table_id)  # Check if table exists
        print(f"Table {table_id} already exists.")
    except Exception:
        table = client.create_table(table)  # API request
        print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

def load_data_from_csv(file_path):
    client = bigquery.Client()

    table_id = f"{PROJECT_DATASET}.raw_customer_data"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
    )

    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    try:
        job.result()  # Wait for the job to complete
        print(f"Loaded {job.output_rows} rows into {table_id}.")
    except Exception as e:
        print(f"An error occurred: {e}")

def create_cleaned_table():
    client = bigquery.Client()

    table_id = f"{PROJECT_DATASET}.cleaned_customer_data"

    schema = [
        bigquery.SchemaField("invoice_no", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gender", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("age", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("quantity", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("price", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("payment_method", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("invoice_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("shopping_mall", "STRING", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id, schema=schema)

    try:
        client.get_table(table_id)  # Check if table exists
        print(f"Table {table_id} already exists.")
    except Exception:
        table = client.create_table(table)  # API request
        print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

def clean_and_transform_data():
    client = bigquery.Client()

    raw_table = f"{PROJECT_DATASET}.raw_customer_data"
    cleaned_table = f"{PROJECT_DATASET}.cleaned_customer_data"

    query = f"""
        CREATE OR REPLACE TABLE `{cleaned_table}` AS
        SELECT
            invoice_no,
            customer_id,
            gender,
            age,
            category,
            quantity,
            price,
            payment_method,
            CAST(invoice_date AS DATE) AS invoice_date,
            shopping_mall
        FROM `{raw_table}`
        WHERE
            invoice_no IS NOT NULL
            AND customer_id IS NOT NULL
            AND age > 0
            AND quantity > 0
            AND price > 0;
    """

    query_job = client.query(query)

    try:
        query_job.result()  # Wait for the query to complete
        print(f"Cleaned data saved to {cleaned_table}.")
    except Exception as e:
        print(f"An error occurred during data cleaning: {e}")

if __name__ == "__main__":
    csv_file_path = "data/customer_shopping_data.csv"  # Replace with your CSV file path

    # Ensure raw table creation happens only once
    create_raw_table()

    # Load data from CSV to BigQuery
    load_data_from_csv(csv_file_path)

    # Create the cleaned table
    create_cleaned_table()

    # Clean and transform data
    clean_and_transform_data()
