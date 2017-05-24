import MySQLdb as db
import sys
from warnings import filterwarnings, resetwarnings
filterwarnings('ignore', category = db.Warning)
reload(sys)

def removepad(str):
    """
    Removes CTRL, LF, Tab, and Space from front and back of string
    :param str:
    :return:
    """
    return str.strip(chr(10)).strip(chr(13)).strip(chr(9)).strip(' ')


def get_colnames(cnxdict, sch='', tbl=''):
    if tbl == '':
        tbl = cnxdict['currtable']
    if sch == '':
        sch = cnxdict['schema']
    cnxdict['sql'] = """
        SELECT
            COLUMN_NAME
        FROM
            INFORMATION_SCHEMA.COLUMNS
        WHERE
            TABLE_SCHEMA = '{0}'
                AND TABLE_NAME = '{1}';
    """.format(sch, tbl)
    temp = dosqlexecute(cnxdict)
    # scan cursor
    collist = ''
    for row in cnxdict['crs']:
        collist = collist + ', `' + row[0] + '`'
    return collist