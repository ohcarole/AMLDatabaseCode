from Utilities.MySQLdbUtils import *

reload(sys)

sys.setdefaultencoding('utf8')

cnxdict = connect_to_mysql_db_prod('temp')
filedescription = 'CR patients'
cnxdict['out_filepath'] = buildfilepath(cnxdict, filename='{} Workbook'.format(filedescription))
writer = pd.ExcelWriter(cnxdict['out_filepath'], datetime_format='mm/dd/yyyy')  # datetime_format='mmm d yyyy hh:mm:ss'

# Dependencies
cnxdict['sql'] = """
"""
dosqlexecute(cnxdict)

# Query to port to excel
sqlcmd = """
        SELECT * FROM temp.allarrivals ;
"""
df = pd.read_sql(sqlcmd, cnxdict['cnx'])
df.to_excel(writer, sheet_name='{} Worksheet'.format(filedescription)[0:29], index=False)
dowritersave(writer, cnxdict)

# for row in df.itertuples():
#     print(row)