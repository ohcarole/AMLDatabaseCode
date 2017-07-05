from MySQLdbUtils import *
import pandas as pd

"""
Crosstab and excel writer examples
"""

 # get a connection to the hma section for an example
cnxdict = connect_to_mysql_db_prod('sabine')  # get a connection to the hma section for an example
writer = pd.ExcelWriter(cnxdict['out_filepath'])
print cnxdict['out_filepath']

df = pd.read_sql("""
         SELECT * FROM temp.pathtestresult;
     """,cnxdict['db'])
tbl = pd.crosstab([df.ptmrn,df.pathdate],[df.pathdate,df.pathresult], margins=True, dropna = False)
tbl.to_excel(writer, sheet_name='protarr')
writer.save()
#
# df = pd.read_sql("""
#          SELECT * FROM hma_201703.hmasummary;
#      """,cnxdict['cnx'])
# tbl = pd.crosstab([df.CALC_Protocol,df.ArrivalType],[df.ArrivalYear], margins=True, dropna = False)
# tbl.to_excel(writer, sheet_name='Arrival Year')
# tbl = pd.crosstab([df.CALC_Protocol,df.ArrivalType],[df.Intensity], margins=True, dropna = False)
# tbl.to_excel(writer, sheet_name='Arrival Intensity')
# tbl = pd.crosstab([df.CALC_Protocol, df.ArrivalType], [df.Intensity,df.HMA], margins=True, dropna=False)
# tbl.to_excel(writer, sheet_name='Arrival Intensity HMA')
#
# df = pd.read_sql("""
#          SELECT * FROM hma_201703.temp2 WHERE ArrivalYear > 2008;
#      """,cnxdict['cnx'])
# tbl = pd.crosstab([df.Intensity,df.CALC_Protocol,df.ArrivalType],[df.ArrivalYear,df.HMA], margins=True, dropna = False)
# tbl.to_excel(writer, sheet_name='Sheet5')
#
# writer.save()