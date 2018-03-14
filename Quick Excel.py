from Utilities.MySQLdbUtils import *

reload(sys)

sys.setdefaultencoding('utf8')

cnxdict = connect_to_mysql_db_prod('temp')
filedescription = 'RedCap ECOG'
filename='{} Workbook'.format(filedescription)[0:28]

sqlcmd = """
SELECT  a.arrival_id, a.patientid, a.arrivaldate
    , group_concat(c.ProcName
        , IF(ProcCellSource IS NULL, "", concat(" from ",ProcCellSource))
        , IF(ProcDonMatch   IS NULL, "", concat(" (",ProcDonMatch,")"))
        , IF(ProcDate       IS NULL, "", concat(" on ",date_format(procdate,'%m/%d/%Y'))) SEPARATOR '\n\r')
    AS HCTProcedure
    from playgrounddatabase.playground a
    LEFT JOIN caisis.vdatasethctproc b
    ON a.PatientId = b.PatientId
    LEFT JOIN (
        SELECT * FROM caisis.vdatasetprocedures 
        WHERE ProcName = 'HCT') c
    ON a.PatientId = c.PatientId and b.ProcedureId = c.ProcedureId
    GROUP BY a.arrival_id
    ORDER BY a.arrival_id, ProcDate; 
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