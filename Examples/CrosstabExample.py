from Utilities.MySQLdbUtils import *
import pandas as pd

"""
Crosstab and excel writer examples
"""

cnxdict = connect_to_mysql_db_prod('hma')  # get a connection to the hma section for an example

writer = pd.ExcelWriter(cnxdict['out_filepath'])
print cnxdict['out_filepath']

df = pd.read_sql("""
         SELECT * FROM hma_201703.hmasummary;
     """,cnxdict['cnx'])
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
tbl = pd.crosstab([df.Intensity,df.CALC_Protocol,df.ArrivalType],[df.ArrivalYear,df.HMA], margins=True, dropna = True)
tbl.to_excel(writer, sheet_name='Sheet5')

df = pd.read_sql("""
    SELECT UWID, ArrivalDate, ArrivalDx, year(arrivaldate) as ArrivalYear, a.*
        FROM hma_201703.protocollist a
        LEFT JOIN hma_201703.amldata b ON a.OriginalProtocol = b.protocol;
""",cnxdict['cnx'])
tbl = pd.crosstab([df.multiregimen,df.ArrivalDx],[df.ArrivalYear], margins=True, dropna = True)
tbl.to_excel(writer, sheet_name='RegimenArrivalDxArrivalYear')


writer.save()