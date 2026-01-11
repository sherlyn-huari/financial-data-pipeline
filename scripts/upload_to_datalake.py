import os
from pathlib import Path
from typing import Optional
import requests
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv

load_dotenv()

STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
DATA_DIR = Path("data")


def get_blob_service_client():
    try:
        credential = DefaultAzureCredential()
        account_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
        return BlobServiceClient(account_url, credential=credential)
    except Exception as e:
        print(f"DefaultAzureCredential failed: {e}")
        print("Trying connection string")
        conn_str = build_connection_string_from_azure()
        if not conn_str:
            raise RuntimeError("All authentication methods failed")
        return BlobServiceClient.from_connection_string(conn_str)


def build_connection_string_from_azure() -> Optional[str]:
    subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
    resource_group = os.getenv("RESOURCE_GROUP_NAME") or (
        f"rg-{os.getenv('PROJECT_NAME')}-{os.getenv('ENVIRONMENT')}"
        if os.getenv("PROJECT_NAME") and os.getenv("ENVIRONMENT")
        else None
    )

    if not all([subscription_id, resource_group, STORAGE_ACCOUNT_NAME]):
        print(" Missing settings to build connection string automatically.")
        return None

    try:
        credential = DefaultAzureCredential()
        token = credential.get_token("https://management.azure.com/.default").token
        url = (
            "https://management.azure.com/"
            f"subscriptions/{subscription_id}/resourceGroups/{resource_group}"
            f"/providers/Microsoft.Storage/storageAccounts/{STORAGE_ACCOUNT_NAME}"
            "/listKeys?api-version=2023-01-01"
        )
        resp = requests.post(url, headers={"Authorization": f"Bearer {token}"})
        resp.raise_for_status()
        keys = resp.json().get("keys", [])
        if not keys:
            print("No storage keys returned from ARM")
            return None
        account_key = keys[0]["value"]
        conn_str = (
            "DefaultEndpointsProtocol=https;"
            f"AccountName={STORAGE_ACCOUNT_NAME};"
            f"AccountKey={account_key};"
            "EndpointSuffix=core.windows.net"
        )
        print("Derived connection string automatically from ARM")
        return conn_str
    except Exception as e:
        print(f"Failed to derive connection string via ARM: {e}")
        return None


def upload_file(blob_service_client, container_name, local_path, blob_path):
    try:
        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_path
        )

        with open(local_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        file_size_mb = local_path.stat().st_size / (1024 * 1024)
        print(f" Uploaded {blob_path} ({file_size_mb:.2f} MB)")
        return True
    except Exception as e:
        print(f" Failed to upload {blob_path}: {e}")
        return False
   

def main():
    print("Azure Data Lake Uploader")

    if not STORAGE_ACCOUNT_NAME:
        print("STORAGE_ACCOUNT_NAME not set in environment")
        return None
    
    blob_service_client = get_blob_service_client()
    print("Authentication successful")

    print("\n Uploading customers")
    customers_file = DATA_DIR / "customers.json"
    if customers_file.exists():
        upload_file(
            blob_service_client,
            "bronze",
            customers_file,
            "raw/customers/customers.json"
        )

    print("\n Uploading merchants")
    merchants_file = DATA_DIR / "merchants.json"
    if merchants_file.exists():
        upload_file(
            blob_service_client,
            "bronze",
            merchants_file,
            "raw/merchants/merchants.json"
        )

    print(" Uploading transactions")
    transactions_dir = DATA_DIR / "transactions"
    if transactions_dir.exists():
        transaction_files = sorted(transactions_dir.glob("transactions_*.csv"))
        for trans_file in transaction_files:
            upload_file(
                blob_service_client,
                "bronze",
                trans_file,
                f"raw/transactions/{trans_file.name}"
            )
    print("Upload complete")


if __name__ == "__main__":
    main()
