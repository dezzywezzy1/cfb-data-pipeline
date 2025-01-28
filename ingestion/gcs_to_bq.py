from google.cloud import bigquery, storage
import os
# Initialize BigQuery and GCS clients
bq_client = bigquery.Client()
gcs_client = storage.Client()

# GCS bucket and folder
bucket_name = f"{os.getenv("BUCKET_NAME")}"
folder_path = f"{os.getenv("FOLDER_PATH")}"

# Get all JSON files in the GCS folder
bucket = gcs_client.bucket(bucket_name)
blobs = bucket.list_blobs(prefix=folder_path)

# Loop through all JSON files
for blob in blobs:
    if blob.name.endswith(".json"):  # Only process JSON files
        # Extract dataset and table names from the file name
        file_name = blob.name.split("/")[-1]  # Get the file name (e.g., rankings.json)
        dataset_name = file_name.replace(".json", "")  # Dataset name (e.g., rankings)
        table_name = "staging_table"  # Use a consistent table name for all datasets

        # Full dataset and table IDs
        dataset_id = f"{bq_client.project}.{dataset_name}"  # e.g., cfb-pipeline-project.rankings
        table_id = f"{dataset_id}.{table_name}"  # e.g., cfb-pipeline-project.rankings.staging_table

        # Step 1: Create the dataset if it doesn't exist
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"  # Set the location
        try:
            bq_client.get_dataset(dataset_id)  # Check if the dataset exists
            print(f"Dataset {dataset_id} already exists.")
        except Exception:
            bq_client.create_dataset(dataset, timeout=30)
            print(f"Created dataset {dataset_id}.")

        # Step 2: Load the JSON file into the corresponding table
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite table if it exists
        )

        uri = f"gs://{bucket_name}/{blob.name}"  # GCS URI of the JSON file
        print(f"Loading {uri} into {table_id}...")

        try:
            # Load the JSON file into BigQuery
            load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
            load_job.result()  # Wait for the job to complete
            print(f"Loaded {load_job.output_rows} rows into {table_id}.")
        except Exception as e:
            print(f"Failed to load {uri} into {table_id}: {e}")

print("All JSON files have been processed.")
