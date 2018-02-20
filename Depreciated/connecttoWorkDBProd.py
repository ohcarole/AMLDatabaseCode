from Utilities.SQLServer_pyodbc import *

cnxdict = read_db_config('caisiswork')
cnxdict = connect_to_caisisprod(cnxdict)

def create_temp_population_table(cnxdict):
    tempsql = """
        SELECT * FROM [WorkDBProd]..[vDatasetPatients];
    """
    df = dosqlread(tempsql, cnxdict)
    pass

create_temp_population_table(cnxdict)