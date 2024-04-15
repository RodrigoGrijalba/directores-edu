from sqlalchemy import create_engine
from Constants import *
from azure.storage.blob import BlobServiceClient
import os
from zipfile import ZipFile
import shutil
from Secrets import *

def unzip(zip_path, extract_path):
        with ZipFile(zip_path) as zip_file:
                zip_file.extractall(extract_path)
        return

def get_blob_service_client():
        connection_string = CONNECTION_STRING_TEMPLATE.format(
                account_name = BLOB_ACCOUNT_NAME,
                account_key = BLOB_ACCOUNT_KEY
        )
        blob_service_client = BlobServiceClient.from_connection_string(
                connection_string
        )
        return blob_service_client

def get_blob_client(blob_name, blob_service_client: BlobServiceClient):
        blob_client = blob_service_client.get_blob_client(
                container=CONTAINER_NAME,
                blob=blob_name
        )
        return blob_client

def get_sql_engine():
        sql_connection_url = SQL_CONNECTION_TEMPLATE.format(
                username=SQL_USERNAME,
                password=SQL_PASSWORD,
                server_name=SQL_SERVER_NAME,
                database_name=SQL_DATABASE_NAME
        )
        engine = create_engine(
                sql_connection_url,
                fast_executemany=True
        )
        return engine

def cleanup():
        for filename in os.listdir(TEMP_OUTPUT_DIR):
                file_path = os.path.join(TEMP_OUTPUT_DIR, filename)
                try:
                        if os.path.isfile(file_path) or os.path.islink(file_path):
                                os.unlink(file_path)
                        elif os.path.isdir(file_path):
                                shutil.rmtree(file_path)
                except Exception as e:
                        print('Failed to delete %s. Reason: %s' % (file_path, e))
        return