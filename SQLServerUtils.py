import pypyodbc

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
        con = pypyodbc.connect('Driver = (SQL Server);'
                               'Server=CONGO-H\H;'
                               'Database=CaisisProd;'
                               'Trusted_Connection=yes'
                               'uid=fhcrc\cmshaw;pwd=Aaron!23'
                               )
        print ('Connected')
        print ('Connected')
        con.close()
    except Exception as ErrVal:
        print ('Connection Failed')
        print (ErrVal)
    return None

connect_to_caisisro()