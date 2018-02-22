from Utilities.MySQLdbUtils import *

reload(sys)

sys.setdefaultencoding('utf8')

cnxdict = connect_to_mysql_db_prod('temp')
filedescription = 'Example'
cnxdict['sql'] = """
CREATE TABLE PlaygroundDatabase.EEPatientList
    SELECT c.arrival_id
            , b.patientid
            , ptmrn_ee
            , ptlastname_ee
            , arrivaldate_ee
            , arrivalage_ee
            , performancestatus_ee
            , arrivaldx_ee
            , treatment_ee
            , various_ee
            , cyto_ee
            , cr1dur_ee
            , response_ee
            , comment_ee
    FROM
        temp.eepatientlist_20180221 a
            LEFT JOIN caisis.vdatasetpatients b ON b.ptmrn = a.ptmrn_ee
            LEFT JOIN caisis.arrivalidmapping c ON b.patientid = c.patientid
                AND a.arrivaldate_ee = c.arrivaldate
    ORDER BY a.Order; 
"""
dosqlexecute(cnxdict)

sqlcmd = """
SELECT * FROM PlaygroundDatabase.EEPatientList 
    WHERE arrival_id_ee IS NULL 
        and arrivaldx_ee not like '%apl%'
        and treatment_ee not like '%pall%'"""
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