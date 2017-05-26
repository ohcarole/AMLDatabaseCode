import pyodbc


def connect_to_caisisprod():
    try:
        con = pyodbc.connect('Driver = (SQL Server);'
                               'Server=CONGO-H\H;'
                               'Database=CaisisProd;'
                               'uid=fhcrc\cmshaw;pwd=Aaron!23'
                               )
    except Exception as ErrVal:
        print ('Connection Failed')
        print (ErrVal)

connect_to_caisisprod()
