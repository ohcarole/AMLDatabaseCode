import pyodbc
# import Connection.py


def connect_to_caisisprod(cnxdict):
    con = ''
    constring = """
		DRIVER={0};
		SERVER={1};
		DATABASE={2};
		TDS_Version=8.0;
		unicode_results=True;
		CHARSET=UTF8;
		TRUSTED_CONNECTION=yes
	""".format(
        cnxdict['driver'],
        cnxdict['server'],
        cnxdict['database']
    )

    try:
        print ('-- About to Connect')
        con = pyodbc.connect(constring)
        print ('-- Connected to {}'.format(cnxdict['database']))
    except Exception as ErrVal:
        print ('-- Connection Failed')
        print (ErrVal)
    # con.autocommit = False
    return con


# def connect_to_caisisro():
#     try:
#
#         con = pypyodbc.connect('Driver = (SQL Server);'
#             'Server=CONGO-H\H;'
#             'Database=CaisisProd;'
#             'uid=fhcrc\cmshaw;pwd=password'
#         )
#         con = pypyodbc.connect('Driver = {SQL Server};'
#                                'Server=CONGO-H\H;'
#                                'Database=CaisisProd;'
#                                'Trusted_Connection=yes'
#                                'uid=fhcrc\cmshaw;pwd=password'
#                                )
#         con = pypyodbc.connect('DRIVER={SQL Server};'
#                                'SERVER=CONGO-H\H;'
#                                'DATABASE=CaisisProd;'
#                                'Trusted_Connection=yes'
#                                'UID=fhcrc\cmshaw;'
#                                'PWD=Aaron!23'
#                                )
#         print ('Connected')
#         print ('Connected')
#         con.close()
#     except Exception as ErrVal:
#         print ('\nConnection Failed')
#         print (ErrVal)
#     return None
#
# connect_to_caisisro()