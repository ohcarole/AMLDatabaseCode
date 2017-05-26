from GeneralUtils import *
import pandas.io.sql as sql
from Connection import *
import MySQLdb as db
import pandas as pd
import sys
from warnings import filterwarnings, resetwarnings
# filterwarnings('ignore', category = db.Warning)
reload(sys)
import pypyodbc

def connecttosqlserver():
    con = pypyodbc.connect('Driver = {SQL Server};'
        'Server=CONGO-H\H;'
        'Database=CaisisProdRO;'
        'uid=fhcrc\cmshaw;pwd=password'
    )
    con.close()
    return None


def buildfilepath(cnxdict, filename=''):
    if filename == '':
        filename = cnxdict['out_filename']
    return cnxdict['out_filedir'] + '\\' + filename + '.' + cnxdict['out_fileext']


def getconnection(sect):
    cnxdict = {
        'desc': 'connection and cursor information'
        , 'ini_section': sect
        , 'ini_file': 'J:\Estey_AML\AML Programming\Python\sharedUtils\Config.ini'
        , 'out_filedir': ''
        , 'out_filename': ''
        , 'out_fileext': ''
        , 'out_filepath': '' # this is built from the other fields
        , 'schema': ''
        , 'tablelist': []
        , 'currtable': ''
        , 'myconfig': ''
        , 'itemnum': 0
        , 'cnx': {}
        , 'crs': {}
        , 'sql': ''
        , 'df': {}
        , 'multi': False
    }
    cnxdict = read_db_config(cnxdict)
    cnxdict['cnx'] = db.connect(host=cnxdict['host'], user=cnxdict['user'],
                                     passwd=cnxdict['password'])
    cnxdict['db'] =  db.connect(host=cnxdict['host'], user=cnxdict['user'],
                                     passwd=cnxdict['password'], db=cnxdict['schema'])
    cnxdict['crs'] = cnxdict['db'].cursor()
    cnxdict['out_filepath'] = buildfilepath(cnxdict)
    return cnxdict


def dosqlexecute(cnxdict, Single=False):
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
        cnxdict['sql'] = removepad(cmd) + delimit
        if cnxdict['sql'] == delimit:
            pass
        else:
            cnxdict['sql'] = cnxdict['sql'].replace('<semicolon>', ';')
            cnxdict['sql'] = cnxdict['sql'].replace('<end-of-code>', ';')
            try:
                # cnxdict['crs'].execute(cnxdict['sql'])
                sql.execute(cnxdict['sql'], cnxdict['db'])
                cnxdict['db'].commit()
                recent_rows_changed = cnxdict['crs'].rowcount
            except Exception:
                print 'SQL Execute Failed:', cnxdict['sql']
            if recent_rows_changed > rows_changed:
                rows_changed = recent_rows_changed
    resetwarnings()
    return rows_changed


def dosqlreplace(cnxdict,Single=False):
    filterwarnings('ignore', category=db.Warning)
    reload(sys)
    rows_changed = 0
    if Single: # not using default delimiter
        delimit = '<end-of-code>'
        if cnxdict['sql'].strip()[-1:] == ';': # last char is a semicolon
            cnxdict['sql'] = cnxdict['sql'].strip()[:-1] + delimit
    else:
        delimit = ';'
    for cmd in cnxdict['sql'].split(delimit):
        recent_rows_changed = 0
        cnxdict['sql'] = removepad(cmd) + delimit
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
                print 'SQL Execute Failed:', cnxdict['sql']
            if recent_rows_changed > rows_changed:
                rows_changed = recent_rows_changed
    resetwarnings()
    return rows_changed


def dosqlread(cmd,db):
    filterwarnings('ignore', category=db.Warning)
    try:
        df = pd.read_sql(cmd,db)
        # print 'Data frame from sql command:' + cmd
        # print df
    except Exception:
        df = ''
        print 'Data Frame Failed:' + cmd
    print '-'*20
    return df
