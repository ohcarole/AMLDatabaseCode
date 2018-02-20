from Utilities.MySQLdbUtils import *
import pandas as pd
from openpyxl import load_workbook

"""
Crosstab and excel writer examples
"""

 # get a connection to the hma section for an example
cnxdict = connect_to_mysql_db_prod('junk')  # get a connection to the hma section for an example
book = load_workbook(cnxdict['out_filepath'])
writer = pd.ExcelWriter(cnxdict['out_filepath'], engine='openpyxl')
writer.book = book
writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
print cnxdict['out_filepath']

df = pd.read_sql("""
         SELECT * FROM temp.pathtestresult;
     """,cnxdict['db'])
df.to_excel(writer, sheet_name="New1")
writer.save

tbl = pd.crosstab([df.ptmrn,df.pathdate],[df.pathdate,df.pathresult], margins=True, dropna = False)
tbl.to_excel(writer, sheet_name='protarr')
dowritersave(writer,cnxdict)
# writer.save()
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