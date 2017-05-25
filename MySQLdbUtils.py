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
    con = pypyodbc.connect('Driver = (SQL Server);'
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
    cnxdict['out_filepath'] = buildfilepath(cnxdict)
    return cnxdict


def dosqlexecute(cnxdict):
    filterwarnings('ignore', category=db.Warning)
    reload(sys)
    for cmd in cnxdict['sql'].split(';'):
        cnxdict['sql'] = removepad(cmd) + ';'
        if cnxdict['sql'] == ';':
            pass
        else:
            try:
                sql.execute(cnxdict['sql'], cnxdict['db'])
                cnxdict['db'].commit()
            except Exception:
                print 'SQL Execute Failed:', cnxdict['sql']
            # print '-'*20
            # print cnxdict['sql']
    resetwarnings()
    return None


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
