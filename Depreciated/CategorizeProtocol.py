from Utilities.MySQLdbUtils import *

cnxdict = connect_to_mysql_db_prod('protocolcategory')


def test():
    printtext('stack')


def create_protocol_output(cnxdict,writer):
    printtext('stack')
    df = pd.read_sql("""
        SELECT Intensity, Categorized, MedTxAgent, Total
            FROM protocolcategory.protocolcategory
                    WHERE
                    CASE
                        WHEN FALSE THEN FALSE
                        ELSE TRUE
                    END
                ORDER BY Intensity, Categorized, OrigMedTxAgent;
    """, cnxdict['cnx'])
    df.to_excel(writer, sheet_name='ALL Protocols in AML Database', index=False)

    df = pd.read_sql("""
        SELECT Intensity
                , count(*)   as `Protocols in Category`
                , sum(Total) as `Patients in Category`
            FROM protocolcategory.protocolcategory
            GROUP BY Intensity;
    """, cnxdict['cnx'])
    df.to_excel(writer, sheet_name='All Intensities in AML Database', index=False)

    print('\nOutput in:\n' + cnxdict['out_filepath'])


def MainRoutine(cnxdict):
    printtext('stack')
    sqlfileexecute("create_protocolcategory_table",cnxdict=cnxdict,sect='protocolcategory')
    writer = pd.ExcelWriter(cnxdict['out_filepath'])
    create_protocol_output(cnxdict, writer)
    dowritersave(writer, cnxdict)


MainRoutine(cnxdict)