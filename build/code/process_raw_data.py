from simpledbf import Dbf5
from Constants import *
from azure.storage.blob import (
        BlobServiceClient,
        generate_blob_sas,
        BlobSasPermissions
)
import os
from helper_functions import *
import pandas as pd
import shutil
from sqlalchemy import create_engine
from Secrets import *

def assess_treated(value):
        if value in ["01", "02"]:
                return 0
        return 1

def main():
        blob_download_name = f"{RAW_DATA_ARCHIVE_NAME}.zip"
        zip_download_path = os.path.join(TEMP_OUTPUT_DIR, blob_download_name)
        blob_service_client = get_blob_service_client()
        blob_client = get_blob_client(blob_download_name, blob_service_client)
        with open(zip_download_path, "wb") as f:
                download_stream = blob_client.download_blob()
                f.write(download_stream.readall())
        unzip(zip_download_path, TEMP_OUTPUT_DIR)
        for index, year in enumerate(LINKS.keys()):
                year_data_dir = os.path.join(TEMP_OUTPUT_DIR, year)
                if os.path.exists(year_data_dir): shutil.rmtree(year_data_dir)
                os.makedirs(year_data_dir)
                unzip(f"{year_data_dir}.zip", year_data_dir)
                year_data_filename = os.path.join(year_data_dir, os.listdir(year_data_dir)[0])
                year_data = Dbf5(year_data_filename).to_dataframe().query(LOAD_FILTER)[RELEVANT_VARIABLES]
                year_data["COD_LOCAL"] = year_data["COD_MOD"] + "-" + year_data["ANEXO"]
                year_data.to_csv(f"{year_data_dir}.csv", index=False)
        sample_det_data = pd.read_csv(f"{os.path.join(TEMP_OUTPUT_DIR, SAMPLE_DET_YEAR)}.csv", dtype=DTYPES)
        sample_det_data = sample_det_data.query("(D01 > 0 | D02 > 0) & CUADRO == 'C308' & TIPDATO in ['01', '02']").drop(columns=["CUADRO"])
        split_det_data = pd.read_csv(f"{os.path.join(TEMP_OUTPUT_DIR, SPLIT_DET_YEAR)}.csv", dtype=DTYPES).query("CUADRO == 'C309'").drop(columns=["CUADRO"])
        merged_data = pd.merge(
                sample_det_data, 
                split_det_data, how="inner", 
                on="COD_LOCAL", 
                suffixes=[f"_{SAMPLE_DET_YEAR}", f"_{SPLIT_DET_YEAR}"]
        )
        merged_data["TREATED"] = merged_data["TIPDATO_2019"].apply(assess_treated)
        merged_data.to_csv
        sql_engine = get_sql_engine()
        merged_data.to_sql(
                "school_split",
                con=sql_engine,
                if_exists="replace",
                index=False,
                chunksize=1000
        )


        cleanup()
        return

if __name__ == "__main__":
        main()
