import pyodbc
from Constants import *
from Secrets import *

query = """
SELECT a.COD_MOD, a.ANEXO, a.D01, a.D02, a.CODGEO, a.CODOOII, a.TIPDATO AS STATUS_2018, b.TIPDATO AS STATUS_2019, c.TIPDATO AS STATUS_2020
INTO [dbo].[MasterTable]
FROM [dbo].[2018_data] AS a
INNER JOIN [dbo].[2019_data] AS b ON (b.COD_LOCAL = a.COD_LOCAL)
INNER JOIN [dbo].[2020_data] AS c ON (b.COD_LOCAL = c.COD_LOCAL)
ORDER BY a.COD_MOD ASC
"""

def main():
        connection_string = ODBC_CONNECTION_TEMPLATE.format(
                server = SQL_SERVER_NAME,
                database = SQL_DATABASE_NAME,
                username = SQL_USERNAME,
                password = SQL_PASSWORD
        )
        with pyodbc.connect(connection_string) as connection:
                with connection.cursor() as cursor:
                        cursor.execute(query)
        return

if __name__ == "__main__":
        main()