from Connection import *
import pyodbc

import pandas as pd
from MySQLdbUtils import *
import pandas.io.sql as sql

def connect_to_caisisprod(cnxdict):
    con = ''
    constring = """
        DRIVER={0};
        SERVER={1};
        DATABASE={2};
        TDS_Version=8.0;
        unicode_results=True;
        CHARSET=UTF8;
        TRUSTED_CONNECTION=yes
    """.format(
                cnxdict['driver'],
                cnxdict['server'],
                cnxdict['database']
    )

    try:
        print ('About to Connect')
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
    tempsql = """
        SELECT TOP (10) a.[PatientId]
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
        FROM [dbo].[vDatasetPatients] a
            LEFT JOIN [dbo].[vDatasetCategories] c on a.[PatientId] = c.[PatientId]
        WHERE c.[Category] LIKE '%AML%';
    """
    cnxdict = read_db_config('caisisprod')
    cnxdict = connect_to_caisisprod(cnxdict)
    df = pd.read_sql(tempsql,cnxdict)
    print(df)

# test1_config_connect()
# test2_get_table()
