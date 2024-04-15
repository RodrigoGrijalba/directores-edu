from helper_functions import *
from wget import download
from Constants import *
from shutil import rmtree
from simpledbf import Dbf5
import pandas as pd

def main():
        for year, page in LINKS.items():
                download_path = os.path.join(TEMP_OUTPUT_DIR, year)
                download_link = f"{MAIN_PAGE}{page}"
                download(
                        download_link,
                        f"{download_path}.zip"
                )
                if os.path.exists(download_path): rmtree(download_path)
                os.makedirs(download_path)
                unzip(f"{download_path}.zip", download_path)
                data_file_path = os.path.join(download_path, os.listdir(download_path)[0])
                cuadro = "C309"
                if year == "2018":
                        cuadro = "C308"
                data = Dbf5(data_file_path).to_dataframe().query(f"CUADRO == '{cuadro}' & NIV_MOD in ['F0', 'B0']")[RELEVANT_VARIABLES]
                data["COD_LOCAL"] =  data["COD_MOD"] + "-" + data["ANEXO"]
                sql_engine = get_sql_engine()
                data.to_sql(
                        name=f"{year}_data",
                        con=sql_engine,
                        if_exists="replace",
                        index=False,
                        chunksize=1000
                )
        cleanup()

if __name__ == "__main__":
        main()


