INPUT_DIR = "../input/"
TEMP_OUTPUT_DIR = "../temp/"
RELEVANT_VARIABLES = ["COD_MOD", "ANEXO", "CUADRO", "TIPDATO", "D01", "D02", "CODGEO", "CODOOII"]
OUTPUT_DIR = "../output/"
LOAD_FILTER = "NIV_MOD in ['B0', 'F0']"
DTYPES = dict(zip(RELEVANT_VARIABLES, [str, str, str, str, int, int, str, str]))
MAIN_PAGE = "https://escale.minedu.gob.pe/documents/10156/"
LINKS = {
        # "2015": "2400783/04+Doc3000.dbf.zip",
        # "2016": "2979785/03+Docentes.dbf.zip",
        # "2017": "4028089/02+Docentes_02.zip",
        "2018": "4594303/04+Docentes_02.zip",
        "2019": "5336482/07+Docentes_02.zip",
        "2020": "6226837/16+Docentes_02.zip"
}
SAMPLE_DET_YEAR = "2018"
SPLIT_DET_YEAR = "2019"
CONTAINER_NAME = "thesisdatacontainer"
RAW_DATA_ARCHIVE_NAME = "raw_data"
CONNECTION_STRING_TEMPLATE = 'DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net'
SAS_URL_TEMPLATE = 'https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas}'
SQL_SERVER_NAME = "thesisdb.database.windows.net"
SQL_DATABASE_NAME = "thesisdb"
SQL_CONNECTION_TEMPLATE = "mssql+pyodbc://{username}:{password}@{server_name}/{database_name}?driver=ODBC+Driver+18+for+SQL+Server"
ODBC_CONNECTION_TEMPLATE = "DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=tcp:{server};DATABASE={database};UID={username};PWD={password}"
