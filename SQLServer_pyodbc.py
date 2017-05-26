import pyodbc


def connect_to_caisisprod():
    print('\nfirst connection attempt')
    try:
        con = pyodbc.connect(r'DRIVER={SQL Server Native Client 11.0};'
           r'SERVER=CONGO-H\H;'
           r'DATABASE=CaisisProd;'
           r'UID=fhcrc/cmshaw;'
           r'PWD=Aaron!23'
           )
        con.close()
    except Exception as ErrVal:
        print ('\nConnection Failed')
        print (ErrVal)
    try:
        print('\nsecond connection attempt')
        con = pyodbc.connect(r'DRIVER={SQL Server};'
                             r'SERVER=CONGO-H\H;'
                             r'DATABASE=CaisisProd;'
                             r'TRUSTED_CONNECTIONS=yes'
                             )
        con.close()
    except Exception as ErrVal:
        print ('\nConnection Failed')
        print (ErrVal)

print(pyodbc.drivers())
connect_to_caisisprod()
