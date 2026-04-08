import time
import os
import logging
import boto3
import pandas as pd
from dotenv import load_dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from azure.storage.blob import BlobServiceClient
from google.cloud import storage as gcs

# SETUP
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)

WATCH_FOLDER = os.path.join(os.path.dirname(__file__), "watch-folder")


# FILE CLEANER
def clean_file(file_path, folder):
    log.info(f"Cleaning file: {os.path.basename(file_path)}")

    try:
        if file_path.endswith(".xlsx") or file_path.endswith(".xls"):
            df = pd.read_excel(file_path)
        elif file_path.endswith(".csv"):
            df = pd.read_csv(file_path)
        else:
            log.warning(f"Unsupported file type: {file_path}")
            return False

        original_count = len(df)
        log.info(f"Total rows found: {original_count}")

        # CLEANING RULES
        df.columns = df.columns.str.strip()
        df.dropna(how="all", inplace=True)
        df.drop_duplicates(inplace=True)
        str_cols = df.select_dtypes(include="object").columns
        for col in str_cols:
            df[col] = df[col].str.strip()

        if folder == "aws":
            df.dropna(subset=["acc_id", "acc_name"], inplace=True)
            df = df[df["cash_balance"] >= 0]
            df = df[df["AUM"] >= 0]

        elif folder == "azure":
            df.dropna(subset=["acc_id", "txn_code"], inplace=True)
            df = df[df["total_amount"] >= 0]
            df = df[df["quantity"] > 0]
            df = df[df["share_price"] > 0]

        elif folder == "gcp":
            df.dropna(subset=["share_id", "share_name"], inplace=True)
            df = df[df["today_price"] > 0]
            df = df[df["market_cap"] > 0]

        clean_count = len(df)
        removed = original_count - clean_count
        log.info(f"Removed {removed} unwanted rows")
        log.info(f"Clean rows remaining: {clean_count}")

        clean_path = file_path.rsplit(".", 1)[0] + "_clean.csv"
        df.to_csv(clean_path, index=False)
        log.info(f"Clean file saved: {os.path.basename(clean_path)}")

        return clean_path

    except Exception as e:
        log.error(f"Cleaning failed: {e}")
        return False


# UPLOAD FUNCTIONS
def upload_to_aws(file_path, file_name):
    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION")
    aws_bucket = os.getenv("AWS_BUCKET_NAME")

    log.info(f"Uploading to AWS S3 bucket: {aws_bucket}")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        region_name=aws_region
    )
    s3.upload_file(file_path, aws_bucket, file_name)
    log.info(f"AWS S3 upload successful: {file_name}")


def upload_to_azure(file_path, file_name):
    client = BlobServiceClient.from_connection_string(
        os.getenv("AZURE_CONNECTION_STRING")
    )
    container = client.get_container_client(
        os.getenv("AZURE_CONTAINER_NAME")
    )
    with open(file_path, "rb") as data:
        container.upload_blob(name=file_name, data=data, overwrite=True)
    log.info(f"Azure Blob upload successful: {file_name}")


def upload_to_gcp(file_path, file_name):
    client = gcs.Client.from_service_account_json(
        os.getenv("GCP_KEY_FILE")
    )
    bucket_name = os.getenv("GCP_BUCKET_NAME")
    log.info(f"Uploading to GCP bucket: {bucket_name}")
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_filename(file_path)
    log.info(f"GCP upload successful: {file_name}")


# FILE EVENT HANDLER
class FileHandler(FileSystemEventHandler):

    def on_created(self, event):
        if event.is_directory:
            return

        file_path = event.src_path
        file_name = os.path.basename(file_path)
        folder = os.path.basename(os.path.dirname(file_path))

        if "_clean" in file_name:
            return

        if not file_name.endswith((".csv", ".xlsx", ".xls")):
            log.warning(f"Skipping unsupported file: {file_name}")
            return

        log.info(f"New file detected: {file_name} in folder: {folder}")

        time.sleep(2)

        clean_path = clean_file(file_path, folder)
        if not clean_path:
            log.error("Cleaning failed - upload cancelled")
            return

        clean_name = os.path.basename(clean_path)

        try:
            if folder == "aws":
                upload_to_aws(clean_path, clean_name)
            elif folder == "azure":
                upload_to_azure(clean_path, clean_name)
            elif folder == "gcp":
                upload_to_gcp(clean_path, clean_name)
            else:
                log.warning(f"Unknown folder: {folder} - skipping")
                return

            log.info(f"Pipeline triggered successfully for: {clean_name}")

        except Exception as e:
            log.error(f"Upload failed: {e}")


# MAIN
if __name__ == "__main__":
    log.info(f"Watcher started. Monitoring: {WATCH_FOLDER}")
    log.info("Waiting for files in aws/ azure/ or gcp/ folder...")
    log.info("Press Ctrl+C to stop")

    observer = Observer()
    observer.schedule(
        FileHandler(),
        path=WATCH_FOLDER,
        recursive=True
    )
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        log.info("Watcher stopped")

    observer.join()