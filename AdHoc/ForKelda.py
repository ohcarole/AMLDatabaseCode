from Utilities.MySQLdbUtils import *

reload(sys)

sys.setdefaultencoding('utf8')

"""
Kelda was experimenting with putting a table for the GO expanded access data into RedCap

GO expanded access outcomes LIST FOR CAROLE.xlsx
"""


cnxdict = connect_to_mysql_db_prod('temp')
cnxdict['EchoSQL'] = 1
cnxdict['sql'] = """
DROP TABLE IF EXISTS temp.temp1 ;
CREATE TABLE temp.temp1
    SELECT b.arrival_id
        , a.MRN as PtMRN
        , b.PtMRN AS PlaygroundMRN
        , b.PatientId
        , a.`Date Consent Signed`
        , a.`Date of first dose`
        , b.arrivaldate
        , b.treatmentstartdate
        , abs(timestampdiff(day,a.`Date of first dose`,  b.treatmentstartdate)) as dosetotreatment
        , abs(timestampdiff(day,a.`Date Consent Signed`, b.treatmentstartdate)) as consenttotreatment
        , CASE
            WHEN   abs(timestampdiff(day,a.`Date of first dose`,  b.treatmentstartdate)) IS NULL
            THEN   abs(timestampdiff(day,a.`Date Consent Signed`, b.treatmentstartdate))

            WHEN   abs(timestampdiff(day,a.`Date Consent Signed`, b.treatmentstartdate)) IS NULL
            THEN   abs(timestampdiff(day,a.`Date of first dose`,  b.treatmentstartdate))

            WHEN   abs(timestampdiff(day,a.`Date of first dose`,  b.treatmentstartdate))
                <= abs(timestampdiff(day,a.`Date Consent Signed`, b.treatmentstartdate))
            THEN   abs(timestampdiff(day,a.`Date of first dose`,  b.treatmentstartdate))
            
            WHEN   abs(timestampdiff(day,a.`Date of first dose`,  b.treatmentstartdate)) 
                >  abs(timestampdiff(day,a.`Date Consent Signed`, b.treatmentstartdate))
            THEN   abs(timestampdiff(day,a.`Date Consent Signed`, b.treatmentstartdate))
            ELSE NULL
        END AS best
    FROM playgrounddatabase.gopats a
            LEFT JOIN
        playgrounddatabase.playground b ON a.mrn = b.ptmrn ;
        
DROP TABLE IF EXISTS temp.temp2 ;
CREATE TABLE temp.temp2
    SELECT a.* FROM temp.temp1 a
    JOIN (SELECT PatientId, min(best) as best FROM temp.temp1 GROUP BY PatientId) b
    ON a.PatientId = b.PatientId and a.best = b.best ;
  
"""
dosqlexecute(cnxdict)


filedescription = 'ForKelda'
filename='{} Workbook'.format(filedescription)[0:28]
print len(filename)
sqlcmd = """
    SELECT 'Match Found    ', a.* FROM temp.temp2 a
        UNION
    SELECT CASE
            WHEN PlaygroundMRN IS NULL THEN 'No Match Found'
            ELSE 'Match Found'
        END, a.* FROM temp.temp1 a
        WHERE a.PtMRN NOT IN (SELECT ptMRN FROM temp.temp2);    
"""
cnxdict['out_filepath'] = buildfilepath(cnxdict, filename='{} Workbook'.format(filedescription))
writer = pd.ExcelWriter(cnxdict['out_filepath'], datetime_format='mm/dd/yyyy')  # datetime_format='mmm d yyyy hh:mm:ss'

print(sqlcmd)
df = pd.read_sql(sqlcmd, cnxdict['cnx'])
df.to_excel(writer, sheet_name='{} Worksheet'.format(filedescription)[0:29], index=False)
dowritersave(writer, cnxdict)
print(cnxdict['out_filepath'])

# for row in df.itertuples():
#     print(row)