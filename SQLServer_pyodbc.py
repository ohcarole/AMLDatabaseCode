import pyodbc


def connect_to_caisisprod():
    try:
        con = pyodbc.connect(r'Driver = {ODBC Driver 11 for SQL Server};'
           r'Server=CONGO-H\H;'
           r'Database=CaisisProd;'
           r'uid=fhcrc\cmshaw;pwd=Aaron!23'
           )
    except Exception as ErrVal:
        print ('Connection Failed')
        print (ErrVal)

connect_to_caisisprod()
