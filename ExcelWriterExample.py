from MySQLdbUtils import *
import pandas as pd

"""
Crosstab and excel writer examples
"""

cnxdict = connect_to_mysql_db_prod('hma')  # get a connection to the hma section for an example

writer = pd.ExcelWriter(cnxdict['out_filepath'])
print cnxdict['out_filepath']

df = pd.read_sql("""
         SELECT * FROM hmasummary;
     """,cnxdict['db'])
tbl = pd.crosstab([df.protocol,df.arrivaldx],df.ArrivalYear, margins=True, dropna = False)
tbl.to_excel(writer, sheet_name='protarr')

df = pd.read_sql("""
         SELECT * FROM hma_201703.hmasummary;
     """,cnxdict['cnx'])
tbl = pd.crosstab([df.CALC_Protocol,df.ArrivalType],[df.ArrivalYear], margins=True, dropna = False)
tbl.to_excel(writer, sheet_name='Arrival Year')
tbl = pd.crosstab([df.CALC_Protocol,df.ArrivalType],[df.Intensity], margins=True, dropna = False)
tbl.to_excel(writer, sheet_name='Arrival Intensity')
tbl = pd.crosstab([df.CALC_Protocol, df.ArrivalType], [df.Intensity,df.HMA], margins=True, dropna=False)
tbl.to_excel(writer, sheet_name='Arrival Intensity HMA')

df = pd.read_sql("""
         SELECT * FROM hma_201703.temp2 WHERE ArrivalYear > 2008;
     """,cnxdict['cnx'])
tbl = pd.crosstab([df.Intensity,df.CALC_Protocol,df.ArrivalType],[df.ArrivalYear,df.HMA], margins=True, dropna = False)
tbl.to_excel(writer, sheet_name='Sheet5')

writer.save()