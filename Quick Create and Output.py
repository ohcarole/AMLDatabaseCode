from Utilities.MySQLdbUtils import *

reload(sys)

sys.setdefaultencoding('utf8')

cnxdict = connect_to_mysql_db_prod('temp')
filedescription = 'Example'
cnxdict['sql'] = """
SELECT
    a.OriginalProtocol,
    a.protocol,
    a.mapto,
    a.noninduction,
    a.singleregimen,
    a.multiregimen,
    a.druglist,
    a.wildcard,
    b.Intensity,
    a.totaluse
FROM
    protocollist.protocollist a
        LEFT JOIN
    protocollist.intensitymapping b ON a.OriginalProtocol = b.Protocol
GROUP BY OriginalProtocol
ORDER BY a.mapto;
"""
dosqlexecute(cnxdict)

sqlcmd = """
SELECT
    a.OriginalProtocol,
    a.protocol,
    a.mapto,
    a.noninduction,
    a.singleregimen,
    a.multiregimen,
    a.druglist,
    a.wildcard,
    b.Intensity,
    a.totaluse
FROM
    protocollist.protocollist a
        LEFT JOIN
    protocollist.intensitymapping b ON a.OriginalProtocol = b.Protocol
GROUP BY OriginalProtocol
ORDER BY a.mapto;
"""
cnxdict['out_filepath'] = buildfilepath(cnxdict, filename='{} Workbook'.format(filedescription))
print(cnxdict['out_filepath'])

writer = pd.ExcelWriter(cnxdict['out_filepath'], datetime_format='mm/dd/yyyy')  # datetime_format='mmm d yyyy hh:mm:ss'

print(sqlcmd)
df = pd.read_sql(sqlcmd, cnxdict['cnx'])
df.to_excel(writer, sheet_name='{} Worksheet'.format(filedescription), index=False)
dowritersave(writer, cnxdict)
print(cnxdict['out_filepath'])

# for row in df.itertuples():
#     print(row)