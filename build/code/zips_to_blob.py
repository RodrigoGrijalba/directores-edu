from Constants import *
import os
from wget import download
import shutil
from helper_functions import *

def main():
        raw_data_dir = os.path.join(TEMP_OUTPUT_DIR, RAW_DATA_ARCHIVE_NAME)
        os.makedirs(raw_data_dir)
        all_data = {}
        for year, download_path in LINKS.items():
                download_name = os.path.join(raw_data_dir, year) + ".zip"
                download(MAIN_PAGE+download_path, out=download_name)
        shutil.make_archive(raw_data_dir, "zip", raw_data_dir)
        blob_client = get_blob_client(f"{raw_data_dir}.zip", get_blob_service_client())
        with open(f"{raw_data_dir}.zip", "rb") as f:
                blob_client.upload_blob(f, overwrite=True)
        cleanup()
        return

if __name__ == "__main__":
        main()