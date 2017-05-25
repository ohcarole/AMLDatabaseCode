import sys
reload(sys)
import pypyodbc

def connecttocaisisro():
    try:
        con = pypyodbc.connect('Driver = (SQL Server);'
            'Server=CONGO-H\H;'
            'Database=CaisisProdRO;'
            'uid=fhcrc\cmshaw;pwd=password'
        )
        print ('Connected')
    except:
        print ('Connection Failed')
    con.close()
    return None

connecttocaisisro()