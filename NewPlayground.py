from MySQLdbUtils import *


cnxdict = connect_to_mysql_db_prod('newplayground')
cnxdict['EchoSQL']=1
sqlfileexecute("NewPlayground_2.sql", cnxdict, 'newplayground')


filedescription = 'Playground'
filename='{} Workbook'.format(filedescription)[0:28]
cnxdict['out_filepath'] = buildfilepath(cnxdict, filename='{} Workbook'.format(filedescription))
writer = pd.ExcelWriter(cnxdict['out_filepath'], datetime_format='mm/dd/yyyy')  # datetime_format='mmm d yyyy hh:mm:ss'

sqlcmd = """
    SELECT a.ptmrn
         , a.patientid
         , a.ptbirthdate
         , a.ptlastname
         , a.returnpatient
         , a.arrivaldx
         , a.arrivaltype
         , timestampdiff(YEAR, a.ptbirthdate, a.arrivaldate) AS age
         , a.arrivaldate
         , a.arrivalyear
         , a.treatmentstartdate
         , a.treatment
         , a.treatmentintent
         , a.intensity
         , a.backbonetype
         , a.backbonename
         ,   substring(a.backboneaddon,2,length(a.backboneaddon)) as backboneaddon
         ,   length(a.backbonename) as `len`
         , a.response
         , a.responsedate
         , a.crnumber
         , a.responseflowdate
         , a.responseflowsource
         , a.responseflowblasts
         , a.responseflowblaststext
         , a.relapse
         , a.relapsedate
         , a.relapsedisease
         , a.relapsetype
         , a.earliestrelapsedate
         , a.followupdays
         , a.followupmonths
         , a.followupyears
         , a.lastinformationdate
         , a.laststatusdate
         , a.laststatustype
         , a.ptdeathdate
         , a.ptdeathtype
         , a.nextarrivaldate
    FROM caisis.playground a ;
"""

df = pd.read_sql(sqlcmd, cnxdict['cnx'])
df.to_excel(writer, sheet_name=filename, index=False)
dowritersave(writer, cnxdict)
print(cnxdict['out_filepath'])

