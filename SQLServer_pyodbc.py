from Connection import *
import pyodbc
import pandas as pd


def connect_to_caisisprod(cnxdict):
    con = ''
    constring = "DRIVER={0};" \
                "SERVER={1};" \
                "DATABASE={2};" \
                "TRUSTED_CONNECTION=yes".format(
                cnxdict['driver'],
                cnxdict['server'],
                cnxdict['database']
    )
    try:
        con = pyodbc.connect(constring)
    except Exception as ErrVal:
        print ('Connection Failed')
        print (ErrVal)
    return con


def test1_config_connect():
    print(pyodbc.drivers())
    cnxdict = read_db_config('caisisprod')
    cnxdict = connect_to_caisisprod(cnxdict)
    cnxdict.close()


def test2_get_table():
    sql = """
        SELECT TOP (10) [vDatasetPatients].[PatientId]
            , [PtMRN]
            , [PtLastName]
            , [PtFirstName]
            , [PtMiddleName]
            , [PtBirthDate]
            , [PtBirthDateText]
            , [PtDeathDate]
            , [PtDeathDateText]
            , [PtDeathType]
            , [PtDeathCause]
            , [CategoryId]
            , [Category]
        FROM [dbo].[vDatasetPatients]
            LEFT JOIN [dbo].[vDatasetCategories] on [vDatasetPatients].[PatientId] = [vDatasetCategories].[PatientId]
        WHERE [Category] LIKE '%AML%';
    """
    cnxdict = read_db_config('caisisprod')
    cnxdict = connect_to_caisisprod(cnxdict)
    df = pd.read_sql(sql,cnxdict)
    print(df)


# test1_config_connect()
# test2_get_table()
