from Connection import *
import pyodbc


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
    con = connect_to_caisisprod(cnxdict)
    con.close()


def get_table():
    sql = """
        SELECT * FROM Patients
    """


test1_config_connect()
