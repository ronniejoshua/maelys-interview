import os
from google.cloud import bigquery
import logging
from maelys_etl import FORMAT
from maelys_etl.config import credentials

logging.basicConfig(level=logging.INFO, filename='extractor.log', format=FORMAT, datefmt='%d-%b-%y %H:%M:%S')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials.get('gcp_bigquery_credentials')

bq_schema = [
    bigquery.SchemaField('order_id', 'INTEGER'),
    bigquery.SchemaField('user_id', 'INTEGER'),
    bigquery.SchemaField('order_created', 'TIMESTAMP'),
    bigquery.SchemaField('sku', 'INTEGER'),
    bigquery.SchemaField('product_name', 'STRING'),
    bigquery.SchemaField('quantity', 'INTEGER'),
    bigquery.SchemaField('price_per_unit', 'FLOAT'),
    bigquery.SchemaField('state', 'STRING')]


def get_bigquery_table_schema(bq_project_id, bq_dataset_id, bq_table_id):
    """Get BigQuery Table Schema."""
    bigquery_client = bigquery.Client()
    bg_tableref = f"{bq_project_id}.{bq_dataset_id}.{bq_table_id}"
    bg_table = bigquery_client.get_table(bg_tableref)
    logging.info("Fetching Table Schema")
    return bg_table.schema


def upload_data_from_csv(bq_project_id, bq_dataset_id, bq_table_id, bq_table_schema, csv_file_path):
    """

    :return:
    """
    bigquery_client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        schema=bq_table_schema,
        # We can also append and query the latest data
        write_disposition="WRITE_TRUNCATE"
    )
    bq_table_id_ref = f"{bq_project_id}.{bq_dataset_id}.{bq_table_id}"
    with open(csv_file_path, "rb") as source_file:
        load_job = bigquery_client.load_table_from_file(source_file, bq_table_id_ref, job_config=job_config)

    logging.info(f"Starting job {load_job.job_id}")
    load_job.result()
    logging.info("Job finished.")
    req_table = bigquery_client.get_table(bq_table_id_ref)
    logging.info(f"Loaded {req_table.num_rows} rows & {len(req_table.schema)} to {bq_table_id_ref}.")
