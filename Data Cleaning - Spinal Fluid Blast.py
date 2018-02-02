from MySQLdbUtils import *

"""Data cleaning

These records are either from morphologic review of spinal fluid or 
flow so the path test should be Blast (MORPF) or Blast (FLOW), not 'BLASTS'

"""



reload(sys)

sys.setdefaultencoding('utf8')

cnxdict = connect_to_mysql_db_prod('temp')
writer = pd.ExcelWriter(cnxdict['out_filepath'], datetime_format='mm/dd/yyyy')  # datetime_format='mmm d yyyy hh:mm:ss'



filedescription = 'Spinal Blasts'
sqlcmd = """
    /*
    Data cleaning
    
    These records are either from morphologic review of spinal fluid or 
    flow so the path test should be Blast (MORPF) or Blast (FLOW), not 'BLASTS'
    
    */
    SELECT a.PathSpecimenType, b.PtMRN, b.DateObtained, b.PathTest, b.PathResult 
        FROM (SELECT PatientId, PathologyId, PathSpecimenType 
            FROM caisis.vdatasetpathology 
            WHERE PathSpecimenType LIKE '%spinal f%') a 
        JOIN caisis.vdatasetpathtest b
        ON a.PatientId = b.PatientId and a.PathologyId = b.PathologyId 
        WHERE UPPER(PathTest) = 'BLASTS' AND PathResult <> '0';
"""

print(sqlcmd)
df = pd.read_sql(sqlcmd, cnxdict['cnx'])
df.to_excel(writer, sheet_name='{} Worksheet'.format(filedescription), index=False)
dowritersave(writer, cnxdict)
print(cnxdict['out_filepath'])

# for row in df.itertuples():
#     print(row)