from Utilities.MySQLdbUtils import *

reload(sys)

sys.setdefaultencoding('utf8')

cnxdict = connect_to_mysql_db_prod('temp')
writer = pd.ExcelWriter(cnxdict['out_filepath'], datetime_format='mm/dd/yyyy')  # datetime_format='mmm d yyyy hh:mm:ss'

filedescription = 'Patient Status '
sqlcmd = """
SELECT PtMRN, MIN(arrivalDate) AS FirstArrivalDate
        , PtBirthdate
        , PtLastName
        , PtDeathDate
        , PtDeathType
        , LastStatusDate
        , LastStatusType
        , LastInformationDate
        FROM temp.FindLastContactDate
        GROUP BY 1;
"""

print(sqlcmd)
df = pd.read_sql(sqlcmd, cnxdict['cnx'])
df.to_excel(writer, sheet_name='{} Worksheet'.format(filedescription), index=False)
dowritersave(writer, cnxdict)
print(cnxdict['out_filepath'])

# for row in df.itertuples():
#     print(row)