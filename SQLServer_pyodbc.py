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

print(pyodbc.drivers())
con = connect_to_caisisprod()
cnxdict = get_cnxdict(sect)
con.close()