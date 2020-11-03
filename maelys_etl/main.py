from maelys_etl import bq_data_upload as bq
from maelys_etl.config import orders_csv_file_path

if __name__ == "__main__":
    # import the data to table_id = "maelys-interview.maelys.orders6"
    bq_project_id = "maelys-interview"
    bq_dataset_id = "maelys"
    bq_table_id = "orders6"

    table_schema = bq.get_bigquery_table_schema(bq_project_id, bq_dataset_id, bq_table_id)
    print(table_schema)
    bq.upload_data_from_csv(bq_project_id, bq_dataset_id, bq_table_id, bq.bq_schema, orders_csv_file_path)
