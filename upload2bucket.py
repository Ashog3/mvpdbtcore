import os
from google.cloud import storage
from google.oauth2 import service_account

# Google Cloud Storage configuration
GCS_BUCKET_NAME = ' europe-central2-dbt-lend-62482020-bucket'
GCS_FOLDER_NAME = 'test01'  # Optional folder name in GCS

# Google Cloud Secret Manager details
SECRET_NAME = 'git2bucket'  # Name of the secret in Secret Manager
PROJECT_ID = 'i-ier1-6j336sl3-h9urmye1jqo7ms'  # Your Google Cloud Project ID

# Function to retrieve service account credentials from Secret Manager
def get_credentials_from_secret_manager(secret_name, project_id):
    from google.cloud import secretmanager
    import json

    client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{project_id}/secrets/{secret_name}"
    response = client.access_secret_version(name=secret_name)
    secret_payload = response.payload.data.decode("UTF-8")
    
    keyfile_dict = json.loads(secret_payload)
    credentials = service_account.Credentials.from_service_account_info(keyfile_dict)
    return credentials

# Function to upload files to GCS
def upload_to_gcs(file_name, file_content, bucket_name, folder_name=''):
    credentials = get_credentials_from_secret_manager(SECRET_NAME, PROJECT_ID)
    storage_client = storage.Client(credentials=credentials, project=credentials.project_id)
    bucket = storage_client.get_bucket(bucket_name)

    # Create the GCS destination path
    gcs_file_path = f"{folder_name}/{file_name}" if folder_name else file_name
    
    # Upload file to GCS
    blob = bucket.blob(gcs_file_path)
    blob.upload_from_string(file_content)
    print(f"Uploaded {file_name} to gs://{bucket_name}/{gcs_file_path}")

# Function to upload all files from the local directory to GCS
def upload_files_from_directory(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            local_file_path = os.path.join(root, file)
            with open(local_file_path, 'rb') as f:
                file_content = f.read()
            upload_to_gcs(file, file_content, GCS_BUCKET_NAME, GCS_FOLDER_NAME)

# Main function to trigger the upload
if __name__ == "__main__":
    # Use the current directory (repo contents after `checkout`)
    upload_files_from_directory(os.getcwd())
