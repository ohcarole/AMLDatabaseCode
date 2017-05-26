import pypyodbc
print(pypyodbc.drivers())

def connect_to_caisisro():
    try:

        con = pypyodbc.connect('Driver = (SQL Server);'
            'Server=CONGO-H\H;'
            'Database=CaisisProd;'
            'uid=fhcrc\cmshaw;pwd=password'
        )
        con = pypyodbc.connect('Driver = {SQL Server};'
                               'Server=CONGO-H\H;'
                               'Database=CaisisProd;'
                               'Trusted_Connection=yes'
                               'uid=fhcrc\cmshaw;pwd=password'
                               )
        con = pypyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};'
                               'SERVER=CONGO-H\H;'
                               'DATABASE=CaisisProd;'
                               'UID=fhcrc\cmshaw;'
                               'PWD=Aaron!23'
                               )
        print ('Connected')
        print ('Connected')
        con.close()
    except Exception as ErrVal:
        print ('\nConnection Failed')
        print (ErrVal)
    return None

connect_to_caisisro()