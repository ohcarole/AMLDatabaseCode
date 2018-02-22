from Utilities.MySQLdbUtils import *

reload(sys)

sys.setdefaultencoding('utf8')

cnxdict = connect_to_mysql_db_prod('temp')
filedescription = 'multiple patient treatments'
filename='{} Workbook'.format(filedescription)[0:28]
print len(filename)
sqlcmd = """
SELECT * FROM temp.Result2 a
    join (SELECT count(*), PtMRN, ArrivalDate FROM temp.result2 
        GROUP BY PtMRN, ArrivalDate Having count(*) > 1) b 
        ON a.PtMRN = b.PtMRN and a.ArrivalDate = b.ArrivalDate;
"""


sqlcmd = """
SELECT * FROM PlaygroundDatabase.EEPatientList 
    WHERE arrival_id_ee IS NULL 
        and arrivaldx_ee not like '%apl%'
        and treatment_ee not like '%pall%'

"""
cnxdict['out_filepath'] = buildfilepath(cnxdict, filename='{} Workbook'.format(filedescription))
print(cnxdict['out_filepath'])

writer = pd.ExcelWriter(cnxdict['out_filepath'], datetime_format='mm/dd/yyyy')  # datetime_format='mmm d yyyy hh:mm:ss'

print(sqlcmd)
df = pd.read_sql(sqlcmd, cnxdict['cnx'])
df.to_excel(writer, sheet_name='{} Worksheet'.format(filedescription)[0:29], index=False)
dowritersave(writer, cnxdict)
print(cnxdict['out_filepath'])

# for row in df.itertuples():
#     print(row)