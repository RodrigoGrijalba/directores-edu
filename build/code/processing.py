from numpy import extract
import pandas as pd
from zipfile import ZipFile
from simpledbf import Dbf5
import json
import os
import shutil
from wget import download

INPUT_DIR = "../input/"
TEMP_OUTPUT_DIR = "../temp/"
RELEVANT_VARIABLES = ["COD_MOD", "ANEXO", "TIPDATO", "D01", "D02", "CODGEO", "CODOOII"]
OUTPUT_DIR = "../output/"
# DTYPES = dict(zip(RELEVANT_VARIABLES, [str, str, str, int, int, str, str]))
MAIN_PAGE = "https://escale.minedu.gob.pe/documents/10156/"
LINKS = {
        "2018": "4594303/04+Docentes_02.zip",
        "2019": "5336482/07+Docentes_02.zip"
}
SAMPLE_DET_YEAR = "2018"
SPLIT_DET_YEAR = "2019"

def unzip(zip_path, extract_path):
        with ZipFile(zip_path) as zip_file:
                zip_file.extractall(extract_path)
        return

def get_sample_df(extract_path):
        data = Dbf5(extract_path).to_dataframe()
        sample_data = data.query("CUADRO == 'C308' & TIPDATO in ['01', '02'] & (NIV_MOD == 'F0' | NIV_MOD == 'B0') & (D01 > 0 | D02 > 0)")[RELEVANT_VARIABLES]
        # ratified_data.to_csv(TEMP_OUTPUT_DIR+"sample_schools.csv", index=False)
        return sample_data

def get_split_df(extract_path, sample_data):
        sample_data["COD_LOCAL"] = sample_data["COD_MOD"] + "-" + sample_data["ANEXO"]
        sample_ids = sample_data["COD_LOCAL"]
        post_treatment_data = Dbf5(extract_path).to_dataframe().query("CUADRO == 'C309' & (NIV_MOD == 'F0' | NIV_MOD == 'B0')")
        post_treatment_sample = post_treatment_data[RELEVANT_VARIABLES]
        post_treatment_sample["COD_LOCAL"] = post_treatment_sample["COD_MOD"] + "-" + post_treatment_sample["ANEXO"]
        post_treatment_sample = post_treatment_sample[post_treatment_sample["COD_LOCAL"].isin(sample_ids)]
        post_treatment_sample["TREATED"] = (~post_treatment_sample["TIPDATO"].isin(["01", "02"])).apply(int)
        split = dict(zip(post_treatment_sample["COD_LOCAL"], post_treatment_sample["TREATED"]))
        return split

def write_json(split):
        with open(OUTPUT_DIR+"groups.json", "w") as f:
                json.dump(split, f)

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

def main():
        os.makedirs(TEMP_OUTPUT_DIR+"zips/")
        for year, download_path in LINKS.items():
                zip_path = TEMP_OUTPUT_DIR + "zips/" + year + ".zip"
                extract_path = TEMP_OUTPUT_DIR + year
                os.makedirs(extract_path)
                download(MAIN_PAGE+download_path, out=zip_path)
                unzip(zip_path, extract_path)
        sample_det_dir = TEMP_OUTPUT_DIR + SAMPLE_DET_YEAR
        sample_det_path = os.path.join(sample_det_dir, os.listdir(sample_det_dir)[0])
        split_det_dir = TEMP_OUTPUT_DIR + SPLIT_DET_YEAR
        split_det_path = os.path.join(split_det_dir, os.listdir(split_det_dir)[0])
        sample_data = get_sample_df(sample_det_path)
        split = get_split_df(split_det_path, sample_data)
        write_json(split)
        cleanup()

if __name__ == "__main__":
        main()
