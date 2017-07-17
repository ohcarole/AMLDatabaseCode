import sys
reload(sys)
sys.setdefaultencoding('utf8')
from GeneralUtils import *
from warnings import filterwarnings, resetwarnings
import MySQLdb as db
import pandas as pd
import pandas.io.sql as sql
from openpyxl import load_workbook
from Connection import *
import sys, re, inspect
filterwarnings('ignore', category = db.Warning)


def connect_to_mysql_db_prod(sect):
    # cnxdict = get_cnxdict(sect)
    cnxdict = read_db_config(sect)
    cnxdict['cnx'] = db.connect(host=cnxdict['host'], user=cnxdict['user'],
                                     passwd=cnxdict['password'])
    cnxdict['db'] =  db.connect(host=cnxdict['host'], user=cnxdict['user'],
                                     passwd=cnxdict['password'], db=cnxdict['schema'])
    sql.execute("USE {}".format(cnxdict['schema']), cnxdict['db'])
    sql.execute("USE {}".format(cnxdict['schema']), cnxdict['cnx'])
    cnxdict['crs'] = cnxdict['db'].cursor()
    cnxdict['out_filepath'] = buildfilepath(cnxdict)
    return cnxdict


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


def dosqlexecute(cnxdict, Single=False):
    # cnxdict = connect_to_mysql_db_prod('nadir')
    filterwarnings('ignore', category=db.Warning)
    reload(sys)
    rows_changed = 0
    if Single:  # not using default delimiter
        delimit = '<end-of-code>'
        if cnxdict['sql'].strip()[-1:] == ';':  # last char is a semicolon
            cnxdict['sql'] = cnxdict['sql'].strip()[:-1] + delimit
    else:
        delimit = ';'
    for cmd in cnxdict['sql'].split(delimit):
        recent_rows_changed = 0
        cnxdict['sql'] = cmd.strip() + delimit
        if cnxdict['sql'] == delimit:
            pass
        else:
            cnxdict['sql'] = cnxdict['sql'].replace('<semicolon>', ';')
            cnxdict['sql'] = cnxdict['sql'].replace('<end-of-code>', ';')
            if 'insert' in cnxdict['sql'].lower():
                try:
                    cnxdict['crs'].execute(cnxdict['sql'])
                    cnxdict['db'].commit()
                    recent_rows_changed = cnxdict['crs'].rowcount
                except:
                    print ('-- SQL Insert Failed:')
                    print(cnxdict['sql'])
                    cnxdict['crs'].close()
            elif 'update' in cnxdict['sql'].lower():
                try:
                    cnxdict['crs'].execute(cnxdict['sql'])
                    cnxdict['db'].commit()
                    recent_rows_changed = cnxdict['crs'].rowcount
                except:
                    print ('-- SQL Update Failed:')
                    print(cnxdict['sql'])
                    cnxdict['crs'].close()
            elif 'create' in cnxdict['sql'].lower():
                try:
                    sql.execute(cnxdict['sql'], cnxdict['db'])
                    cnxdict['cnx'].commit()
                    """ Should be able to get number of rows from the INFORMATION_SCHEMA table as in this example:
                        SELECT    TABLE_ROWS
                            FROM  INFORMATION_SCHEMA.PARTITIONS
                            WHERE TABLE_SCHEMA = 'fungal'
                            AND   TABLE_NAME   = 'patientlist';
                        recent_rows_changed = cnxdict['crs'].rowcount
                    """
                except:
                    print ('-- SQL Create Failed:')
                    print(cnxdict['sql'])
            else:
                try:
                    sql.execute(cnxdict['sql'], cnxdict['db'])
                    cnxdict['cnx'].commit()
                except:
                    print('-- SQL Command Failed:')
                    print(cnxdict['sql'])

            if recent_rows_changed > rows_changed:
                rows_changed = recent_rows_changed
    resetwarnings()
    return rows_changed


def dosqlexecute_old(cnxdict, Single=False):
    # cnxdict = connect_to_mysql_db_prod('nadir')
    filterwarnings('ignore', category=db.Warning)
    reload(sys)
    rows_changed = 0
    if Single:  # not using default delimiter
        delimit = '<end-of-code>'
        if cnxdict['sql'].strip()[-1:] == ';':  # last char is a semicolon
            cnxdict['sql'] = cnxdict['sql'].strip()[:-1] + delimit
    else:
        delimit = ';'
    for cmd in cnxdict['sql'].split(delimit):
        recent_rows_changed = 0
        cnxdict['sql'] = cmd.strip() + delimit
        if cnxdict['sql'] == delimit:
            pass
        else:
            cnxdict['sql'] = cnxdict['sql'].replace('<semicolon>', ';')
            cnxdict['sql'] = cnxdict['sql'].replace('<end-of-code>', ';')
            try:
                if 'insert' in cnxdict['sql'].lower():
                    cnxdict['crs'].execute(cnxdict['sql'])
                    cnxdict['db'].commit()
                    recent_rows_changed = cnxdict['crs'].rowcount
                elif 'update' in cnxdict['sql'].lower():
                    cnxdict['crs'].execute(cnxdict['sql'])
                    cnxdict['db'].commit()
                    recent_rows_changed = cnxdict['crs'].rowcount
                elif 'create' in cnxdict['sql'].lower():
                    sql.execute(cnxdict['sql'], cnxdict['db'])
                    cnxdict['cnx'].commit()
                    """ Should be able to get number of rows from the INFORMATION_SCHEMA table as in this example:
                        SELECT    TABLE_ROWS
                            FROM  INFORMATION_SCHEMA.PARTITIONS
                            WHERE TABLE_SCHEMA = 'fungal'
                            AND   TABLE_NAME   = 'patientlist';
                        recent_rows_changed = cnxdict['crs'].rowcount
                    """
                else:
                    sql.execute(cnxdict['sql'], cnxdict['db'])
                    cnxdict['cnx'].commit()
            except Exception:
                print ( 'SQL Execute Failed:\n', cnxdict['sql'] )

            if recent_rows_changed > rows_changed:
                rows_changed = recent_rows_changed
    resetwarnings()
    return rows_changed


def dosqlupdate(cnxdict,Single=False):
    filterwarnings('ignore', category=db.Warning)
    reload(sys)
    rows_changed = 0
    if Single: # not using default delimiter
        delimit = '<end-of-code>'
    else:
        delimit = ';'
    if cnxdict['sql'].strip()[-1:] == ';':  # last char is a semicolon
        cnxdict['sql'] = cnxdict['sql'].strip()[:-1] + delimit
    for cmd in cnxdict['sql'].split(delimit):
        recent_rows_changed = 0
        cnxdict['sql'] = cmd.strip() + delimit
        if cnxdict['sql'] == delimit:
            pass
        else:
            cnxdict['sql'] = cnxdict['sql'].replace('<semicolon>', ';')
            cnxdict['sql'] = cnxdict['sql'].replace('<end-of-code>', ';')
            try:
                cnxdict['crs'].execute(cnxdict['sql'])
                #sql.execute(cnxdict['sql'], cnxdict['db'])
                cnxdict['db'].commit()
                recent_rows_changed = cnxdict['crs'].rowcount
            except Exception:
                print ('SQL Execute Failed:', cnxdict['sql'])
            if recent_rows_changed > rows_changed:
                rows_changed = recent_rows_changed
    resetwarnings()
    return rows_changed


def dosqlread(cmd,con):
    filterwarnings('ignore', category=db.Warning)
    try:
        df = pd.read_sql(cmd,con,)
    except Exception:
        df = ''
    return df


def addexceltable(sqlstm, writerobj, newsheet, con ):
    df = dosqlread(sqlstm, con)
    df.to_excel(writerobj, sheet_name=newsheet, index=False)


def sqlfileexecute(fullpathfilename, cnxdict=None, sect=None):
    """
    Takes a path and filename creates a cursor if needed and executes the sql file code
    :param fullpathfilename:
    :param cnxdict: MySQLdb Connection
    :param sect: Section in Config.ini
    :return: Pass back results of dosqlexecute()
    """
    if sect is None:
        sect = 'temp'
    if cnxdict is None:
        cnxdict = connect_to_mysql_db_prod(sect)
    cnxdict['sql'] = open(fullpathfilename).read()
    return dosqlexecute(cnxdict)
