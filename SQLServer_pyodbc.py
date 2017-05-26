import Connection
import pyodbc


def connect_to_caisisprod():
    try:
        con = pyodbc.connect(r'driver={SQL Server};'
                             r'SERVER=CONGO-H\H;'
                             r'DATABASE=CaisisProd;'
                             r'TRUSTED_CONNECTION=yes'
                             )
    except Exception as ErrVal:
        print ('Connection Failed')
        print (ErrVal)
    return con


def get_cnxdict_(sect):
    cnxdict = {
        'desc': 'connection and cursor information'
        , 'ini_section': sect
        , 'ini_file': 'J:\Estey_AML\AML Programming\Python\sharedUtils\Config.ini'
        , 'out_filedir': ''
        , 'out_filename': ''
        , 'out_fileext': ''
        , 'out_filepath': ''  # this is built from the other fields
        , 'schema': ''
        , 'tablelist': []
        , 'currtable': ''
        , 'myconfig': ''
        , 'itemnum': 0
        , 'cnx': {}
        , 'crs': {}
        , 'sql': ''
        , 'df': {}
        , 'multi': False
    }
    return cnxdict

cnxdict = get_cnxdict('caisisprod')
cnxdict = read_db_config(cnxdict)
print(pyodbc.drivers())
con = connect_to_caisisprod()
con.close()