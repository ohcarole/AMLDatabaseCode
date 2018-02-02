from MySQLdbUtils import *

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
SELECT a.PtMRN
    , a.PatientId
    , a.ArrivalDx
    , a.ArrivalDate
    , a.ArrivalYear
    , a.TreatmentStartDate
    , a.MedTxIntent
    , a.OriginalMedTxAgent AS MedTxAgent
    , a.BackboneName
    , a.BackboneAddOn
    , a.NextArrivalDate
    , a.TargetDate
    , a.FirstRangeDate
    , a.lastrangedate
    , a.DaysFromArrival
    , a.FirstTreatment
    FROM temp.Result2 a
    join (SELECT count(*), PtMRN, ArrivalDate FROM temp.result2 
        GROUP BY PtMRN, ArrivalDate Having count(*) > 1) b 
        ON a.PtMRN = b.PtMRN and a.ArrivalDate = b.ArrivalDate
        WHERE ArrivalYear > 2008;

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