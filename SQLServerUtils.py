import pypyodbc

def connecttocaisisro():
    try:
        con = pypyodbc.connect('Driver = (SQL Server);'
            'Server=CONGO-H\H;'
            'Database=CaisisProdRO;'
            'uid=fhcrc\cmshaw;pwd=password'
        )
        print ('Connected')
        con.close()
    except Exception as ErrVal:
        print ('Connection Failed')
        print (ErrVal)
    return None

connecttocaisisro()