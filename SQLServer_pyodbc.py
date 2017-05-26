import pyodbc


def connect_to_caisisprod():
    try:
        # con = pyodbc.connect(r'DRIVER={SQL Server};'
        con = pyodbc.connect(r'DRIVER={ODBC Driver 13 for SQL Server};'
           r'SERVER=CONGO-H\H;'
           r'DATABASE=CaisisProd;'
           r'UID=fhcrc/cmshaw;'
           r'PWD=Aaron!23'
           )
        con.close()
    except Exception as ErrVal:
        print ('Connection Failed')
        print (ErrVal)

print(pyodbc.drivers())
connect_to_caisisprod()
