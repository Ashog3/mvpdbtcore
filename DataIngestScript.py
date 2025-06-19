from google.cloud import bigquery
from google.oauth2 import service_account

def load_csv_from_gcs_to_bq(
    bucket_name: str,
    source_blob_name: str,
    dataset_id: str,
    table_id: str,
    project_id: str,
    service_account_file: str,
    location: str = "EU",
    autodetect_schema: bool = True,
    write_disposition: str = "WRITE_APPEND"
):
    """
    Load a CSV file from GCS into a BigQuery table using explicit service account credentials.

    Args:
        bucket_name (str): Name of the GCS bucket.
        source_blob_name (str): Path to the CSV file in the bucket.
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery table ID.
        project_id (str): Google Cloud project ID.
        service_account_file (str): Path to the service account JSON key file.
        location (str): BigQuery location.
        autodetect_schema (bool): Auto-detect schema from file.
        write_disposition (str): Write mode.
    """
    # Load credentials explicitly from JSON file
    credentials = service_account.Credentials.from_service_account_file(service_account_file)

    # Create a BigQuery client with the credentials
    client = bigquery.Client(project=project_id, credentials=credentials)

    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = dataset_ref.table(table_id)

    uri = f"gs://{bucket_name}/{source_blob_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=autodetect_schema,
        write_disposition=write_disposition,
    )

    load_job = client.load_table_from_uri(
        uri,
        table_ref,
        location=location,
        job_config=job_config,
    )

    print(f"Starting job {load_job.job_id}...")
    load_job.result()  # Waits for the job to complete

    destination_table = client.get_table(table_ref)
    print(f"Loaded {destination_table.num_rows} rows into {dataset_id}.{table_id}.")


# Example usage:
if __name__ == "__main__":
    load_csv_from_gcs_to_bq(
        bucket_name="dbt_lend",
        source_blob_name="sample1.csv",
        dataset_id="dbt_lend",
        table_id="sample1",
        project_id="i-ier1-6j336sl3-h9urmye1jqo7ms",
        service_account_file="C:/Users/ashokkumar_selvaraj/Documents/key_file.json"
    )
