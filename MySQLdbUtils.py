from GeneralUtils import *
import pandas.io.sql as sql
from Connection import *
import MySQLdb as db
import pandas as pd
import sys
from warnings import filterwarnings, resetwarnings
filterwarnings('ignore', category = db.Warning)
reload(sys)


def connect_to_mysql_db_prod(sect):
    # cnxdict = get_cnxdict(sect)
    cnxdict = read_db_config(sect)
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
                if 'index' not in cnxdict['sql'].lower():
                    cnxdict['cnx'].commit()
                # recent_rows_changed = cnxdict['crs'].rowcount
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
    else:
        delimit = ';'
    if cnxdict['sql'].strip()[-1:] == ';':  # last char is a semicolon
        cnxdict['sql'] = cnxdict['sql'].strip()[:-1] + delimit
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


def dosqlread(cmd,con):
    filterwarnings('ignore', category=db.Warning)
    try:
        df = pd.read_sql(cmd,con)
    except Exception:
        df = ''
    return df
