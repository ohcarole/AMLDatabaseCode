from __future__ import print_function
import sys
reload(sys)
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
sys.setdefaultencoding('utf8')
from warnings import filterwarnings, resetwarnings
import MySQLdb as db
import pandas as pd
import pandas.io.sql as sql
import numpy as np
import sqlparse
from sqlparse import tokens
from Connection import *
import openpyxl
import sys, inspect, time, os, copy, datetime, pyexcel, pyexcel_xlsx
filterwarnings('ignore', category=db.Warning)
# db.autocommit(True)

showstackinfo = True
debugmode = False

IGNORE = set(['CREATE FUNCTION',])  # extend this, this goes with the sqlparse function _filter


def test():
    printtext('stack')


def getrowdictionary(row, fieldlist):
    rowdict = {}
    for field in fieldlist:
        rowdict[field] = row[fieldlist.index(field)]
    return rowdict


def cleandataframe(df, colname):  # 0xb5

    # Coding for all sorts of non-available characters
    # Reference Website: http://www.alanwood.net/demos/ansi.html
    """
        ANSI    Unicode Hex UnicodeHex  HTML4.0 Unicode Name    Unicode Range
        160	0xA0	U+00A0	&nbsp;	no-break space	Latin-1 Supplement
        161	0xA1	U+00A1	&iexcl;	inverted exclamation mark	Latin-1 Supplement
        162	0xA2	U+00A2	&cent;	cent sign	Latin-1 Supplement
        163	0xA3	U+00A3	&pound;	pound sign	Latin-1 Supplement
        164	0xA4	U+00A4	&curren;	currency sign	Latin-1 Supplement
        165	0xA5	U+00A5	&yen;	yen sign	Latin-1 Supplement
        166	0xA6	U+00A6	&brvbar;	broken bar	Latin-1 Supplement
        167	0xA7	U+00A7	&sect;	section sign	Latin-1 Supplement
        168	0xA8	U+00A8	&uml;	diaeresis	Latin-1 Supplement
        169	0xA9	U+00A9	&copy;	copyright sign	Latin-1 Supplement
        170	0xAA	U+00AA	&ordf;	feminine ordinal indicator	Latin-1 Supplement
        171	0xAB	U+00AB	&laquo;	left-pointing double angle quotation mark	Latin-1 Supplement
        172	0xAC	U+00AC	&not;	not sign	Latin-1 Supplement
        173	0xAD	U+00AD	&shy;	soft hyphen	Latin-1 Supplement
        174	0xAE	U+00AE	&reg;	registered sign	Latin-1 Supplement
        175	0xAF	U+00AF	&macr;	macron	Latin-1 Supplement
        176	0xB0	U+00B0	&deg;	degree sign	Latin-1 Supplement
        177	0xB1	U+00B1	&plusmn;	plus-minus sign	Latin-1 Supplement
        178	0xB2	U+00B2	&sup2;	superscript two	Latin-1 Supplement
        179	0xB3	U+00B3	&sup3;	superscript three	Latin-1 Supplement
        180	0xB4	U+00B4	&acute;	acute accent	Latin-1 Supplement
        181	0xB5	U+00B5	&micro;	micro sign	Latin-1 Supplement
        182	0xB6	U+00B6	&para;	pilcrow sign	Latin-1 Supplement
        183	0xB7	U+00B7	&middot;	middle dot	Latin-1 Supplement
        184	0xB8	U+00B8	&cedil;	cedilla	Latin-1 Supplement
        185	0xB9	U+00B9	&sup1;	superscript one	Latin-1 Supplement
        186	0xBA	U+00BA	&ordm;	masculine ordinal indicator	Latin-1 Supplement
        187	0xBB	U+00BB	&raquo;	right-pointing double angle quotation mark	Latin-1 Supplement
        188	0xBC	U+00BC	&frac14;	vulgar fraction one quarter	Latin-1 Supplement
        189	0xBD	U+00BD	&frac12;	vulgar fraction one half	Latin-1 Supplement
        190	0xBE	U+00BE	&frac34;	vulgar fraction three quarters	Latin-1 Supplement
        191	0xBF	U+00BF	&iquest;	inverted question mark	Latin-1 Supplement
        192	0xC0	U+00C0	&Agrave;	Latin capital letter A with grave	Latin-1 Supplement
        193	0xC1	U+00C1	&Aacute;	Latin capital letter A with acute	Latin-1 Supplement
        194	0xC2	U+00C2	&Acirc;	Latin capital letter A with circumflex	Latin-1 Supplement
        195	0xC3	U+00C3	&Atilde;	Latin capital letter A with tilde	Latin-1 Supplement
        196	0xC4	U+00C4	&Auml;	Latin capital letter A with diaeresis	Latin-1 Supplement
        197	0xC5	U+00C5	&Aring;	Latin capital letter A with ring above	Latin-1 Supplement
        198	0xC6	U+00C6	&AElig;	Latin capital letter AE	Latin-1 Supplement
        199	0xC7	U+00C7	&Ccedil;	Latin capital letter C with cedilla	Latin-1 Supplement
        200	0xC8	U+00C8	&Egrave;	Latin capital letter E with grave	Latin-1 Supplement
        201	0xC9	U+00C9	&Eacute;	Latin capital letter E with acute	Latin-1 Supplement
        202	0xCA	U+00CA	&Ecirc;	Latin capital letter E with circumflex	Latin-1 Supplement
        203	0xCB	U+00CB	&Euml;	Latin capital letter E with diaeresis	Latin-1 Supplement
        204	0xCC	U+00CC	&Igrave;	Latin capital letter I with grave	Latin-1 Supplement
        205	0xCD	U+00CD	&Iacute;	Latin capital letter I with acute	Latin-1 Supplement
        206	0xCE	U+00CE	&Icirc;	Latin capital letter I with circumflex	Latin-1 Supplement
        207	0xCF	U+00CF	&Iuml;	Latin capital letter I with diaeresis	Latin-1 Supplement
        208	0xD0	U+00D0	&ETH;	Latin capital letter Eth	Latin-1 Supplement
        209	0xD1	U+00D1	&Ntilde;	Latin capital letter N with tilde	Latin-1 Supplement
        210	0xD2	U+00D2	&Ograve;	Latin capital letter O with grave	Latin-1 Supplement
        211	0xD3	U+00D3	&Oacute;	Latin capital letter O with acute	Latin-1 Supplement
        212	0xD4	U+00D4	&Ocirc;	Latin capital letter O with circumflex	Latin-1 Supplement
        213	0xD5	U+00D5	&Otilde;	Latin capital letter O with tilde	Latin-1 Supplement
        214	0xD6	U+00D6	&Ouml;	Latin capital letter O with diaeresis	Latin-1 Supplement
        215	0xD7	U+00D7	&times;	multiplication sign	Latin-1 Supplement
        216	0xD8	U+00D8	&Oslash;	Latin capital letter O with stroke	Latin-1 Supplement
        217	0xD9	U+00D9	&Ugrave;	Latin capital letter U with grave	Latin-1 Supplement
        218	0xDA	U+00DA	&Uacute;	Latin capital letter U with acute	Latin-1 Supplement
        219	0xDB	U+00DB	&Ucirc;	Latin capital letter U with circumflex	Latin-1 Supplement
        220	0xDC	U+00DC	&Uuml;	Latin capital letter U with diaeresis	Latin-1 Supplement
        221	0xDD	U+00DD	&Yacute;	Latin capital letter Y with acute	Latin-1 Supplement
        222	0xDE	U+00DE	&THORN;	Latin capital letter Thorn	Latin-1 Supplement
        223	0xDF	U+00DF	&szlig;	Latin small letter sharp s	Latin-1 Supplement
        224	0xE0	U+00E0	&agrave;	Latin small letter a with grave	Latin-1 Supplement
        225	0xE1	U+00E1	&aacute;	Latin small letter a with acute	Latin-1 Supplement
        226	0xE2	U+00E2	&acirc;	Latin small letter a with circumflex	Latin-1 Supplement
        227	0xE3	U+00E3	&atilde;	Latin small letter a with tilde	Latin-1 Supplement
        228	0xE4	U+00E4	&auml;	Latin small letter a with diaeresis	Latin-1 Supplement
        229	0xE5	U+00E5	&aring;	Latin small letter a with ring above	Latin-1 Supplement
        230	0xE6	U+00E6	&aelig;	Latin small letter ae	Latin-1 Supplement
        231	0xE7	U+00E7	&ccedil;	Latin small letter c with cedilla	Latin-1 Supplement
        232	0xE8	U+00E8	&egrave;	Latin small letter e with grave	Latin-1 Supplement
        233	0xE9	U+00E9	&eacute;	Latin small letter e with acute	Latin-1 Supplement
        234	0xEA	U+00EA	&ecirc;	Latin small letter e with circumflex	Latin-1 Supplement
        235	0xEB	U+00EB	&euml;	Latin small letter e with diaeresis	Latin-1 Supplement
        236	0xEC	U+00EC	&igrave;	Latin small letter i with grave	Latin-1 Supplement
        237	0xED	U+00ED	&iacute;	Latin small letter i with acute	Latin-1 Supplement
        238	0xEE	U+00EE	&icirc;	Latin small letter i with circumflex	Latin-1 Supplement
        239	0xEF	U+00EF	&iuml;	Latin small letter i with diaeresis	Latin-1 Supplement
        240	0xF0	U+00F0	&eth;	Latin small letter eth	Latin-1 Supplement
        241	0xF1	U+00F1	&ntilde;	Latin small letter n with tilde	Latin-1 Supplement
        242	0xF2	U+00F2	&ograve;	Latin small letter o with grave	Latin-1 Supplement
        243	0xF3	U+00F3	&oacute;	Latin small letter o with acute	Latin-1 Supplement
        244	0xF4	U+00F4	&ocirc;	Latin small letter o with circumflex	Latin-1 Supplement
        245	0xF5	U+00F5	&otilde;	Latin small letter o with tilde	Latin-1 Supplement
        246	0xF6	U+00F6	&ouml;	Latin small letter o with diaeresis	Latin-1 Supplement
        247	0xF7	U+00F7	&divide;	division sign	Latin-1 Supplement
        248	0xF8	U+00F8	&oslash;	Latin small letter o with stroke	Latin-1 Supplement
        249	0xF9	U+00F9	&ugrave;	Latin small letter u with grave	Latin-1 Supplement
        250	0xFA	U+00FA	&uacute;	Latin small letter u with acute	Latin-1 Supplement
        251	0xFB	U+00FB	&ucirc;	Latin small letter with circumflex	Latin-1 Supplement
        252	0xFC	U+00FC	&uuml;	Latin small letter u with diaeresis	Latin-1 Supplement
        253	0xFD	U+00FD	&yacute;	Latin small letter y with acute	Latin-1 Supplement
        254	0xFE	U+00FE	&thorn;	Latin small letter thorn	Latin-1 Supplement
        255	0xFF	U+00FF	&yuml;	Latin small letter y with diaeresis	Latin-1 Supplement
    """

    for i in range(160,256):  # 0xa0 through 0xff
        df[colname].replace(unichr(i), " ", regex=True, inplace=True)

    # covered by above coding
    # df[colname].replace([u'\u00B5'], "u",   regex=True, inplace=True) # micro liter sign "micro"
    # df[colname].replace([u'\u00A0'], " ",   regex=True, inplace=True) # non-breaking space
    # df[colname].replace([u'\u00A9'], " ",   regex=True, inplace=True) # copyright sign
    # df[colname].replace([u'\u00AE'], " ",   regex=True, inplace=True) # registered sign
    # df[colname].replace([u'\u00B7'], ".",   regex=True, inplace=True) # middle dot
    # df[colname].replace([u'\u00BD'], "1/2", regex=True, inplace=True) # one half
    # df[colname].replace([u'\u00D2'], "O",   regex=True, inplace=True) # O grave
    # df[colname].replace([u'\u00D4'], "D",   regex=True, inplace=True) # D circumflex
    # df[colname].replace([u'\u00DF'], "s",   regex=True, inplace=True) # s sharp
    # df[colname].replace([u'\u00E9'], "e",   regex=True, inplace=True) # e acute
    # df[colname].replace([u'\u00EF'], "i",   regex=True, inplace=True) # i with diaeresis
    # df[colname].replace([u'\u00F6'], "o",   regex=True, inplace=True) # o with diaeresis

    # I called these out seperately because
    # 1) they were not in a continuous range and
    # 2) they came up as errors at some point
    df[colname].replace([u'\u0153'], "oe",  regex=True, inplace=True) # small ligature oe
    df[colname].replace([u'\u0161'], "s",   regex=True, inplace=True) # s with caron
    df[colname].replace([u'\u017e'], "z",   regex=True, inplace=True) # z with caron
    df[colname].replace([u'\u0178'], "Y",   regex=True, inplace=True) # Y with diaeresis
    df[colname].replace([u'\u02dc'], "~",   regex=True, inplace=True) # tilde
    df[colname].replace([u'\u2013'], "--",  regex=True, inplace=True) # double dash
    df[colname].replace([u'\u2014'], "--",  regex=True, inplace=True) # emphatic dash
    df[colname].replace([u'\u2018'], "'",   regex=True, inplace=True) # left single quote
    df[colname].replace([u'\u2019'], "'",   regex=True, inplace=True) # right single quote
    df[colname].replace([u'\u201c'], '"',   regex=True, inplace=True) # left double quote
    df[colname].replace([u'\u201d'], '"',   regex=True, inplace=True) # right double quote
    df[colname].replace([u'\u2022'], "-",   regex=True, inplace=True) # bullet
    df[colname].replace([u'\u2026'], "...", regex=True, inplace=True) # horizontal ellipsis
    df[colname].replace([u'\u203a'], "?",   regex=True, inplace=True) # single right-pointing angle quotation mark
    df[colname].replace([u'\u2122'], " ",   regex=True, inplace=True) # trademark sign

    return df


def clean_common_df_error_columns(df, collist=('PathResult','PathNotes','PathKaryotype','MedTxNotes','StatusNotes'), table='columns'):
    # print('Cleaning {0}: {1}'.format(table,collist))
    # open('{0}.xlsx'.format(table), 'a').close() # make sure file exists
    # tempwriter = pd.ExcelWriter('{0}.xlsx'.format(table), engine='openpyxl', datetime_format='mm/dd/yyyy')
    for colname in df.columns:
        if colname in collist:
            df = cleandataframe(df,colname)
    #         try:
    #             tf = df[colname]
    #             tf.to_frame(name=colname).to_excel(tempwriter, sheet_name=colname, index=False)
    #         except:
    #             print("Failed to save data from column: {0}".format(colname))
    # tempwriter.save()
    return df


def printtext(txt='stack', debugmode = False, showstackinfo = True):
    if debugmode == True:
        print('{0}'.format(txt)),
    elif showstackinfo == True and txt.find('stack') > -1:
        # prints the parent routine's name
        print(    '\n# Program:\t'
                + inspect.stack()[1][1]
                + ' \n# Function:\t'
                + inspect.stack()[1][3])
    return


def add_to_dict(target, itemname="", item=""):
    """
        given a dictionary and a parameter add the parameter to the dictionary
    :return: dictionary
    """
    target[itemname]=item
    return target


def connect_to_mysql_db_prod(sect, parameter_dict={}, DisplayPath=False, EchoSQL=False ):
    # cnxdict = get_cnxdict(sect)
    cnxdict = read_db_config(sect)
    cnxdict['cnx'] = db.connect(host=cnxdict['host'], user=cnxdict['user'],
                                     passwd=cnxdict['password'])
    cnxdict['db'] =  db.connect(host=cnxdict['host'], user=cnxdict['user'],
                                     passwd=cnxdict['password'], db=cnxdict['schema'])
    sql.execute("USE {}".format(cnxdict['schema']), cnxdict['db'])
    sql.execute("USE {}".format(cnxdict['schema']), cnxdict['cnx'])
    cnxdict['crs'] = cnxdict['db'].cursor()
    cnxdict['out_filepath'] = buildfilepath(cnxdict, DisplayPath=DisplayPath)
    cnxdict['out_csvpath']  = buildfilepath(cnxdict, DisplayPath=DisplayPath, fileext='csv')
    """
        Currently I am adding this parameter to the root, but I think it would make sense to make this more dynamic and
        have a parameter dictionary that contains these individual parameters.
    """
    for itemname in parameter_dict.keys():
        cnxdict[itemname] = parameter_dict[itemname]
    return cnxdict


def buildfilepath(cnxdict, filename='', DisplayPath=False, fileext=''):
    if filename == '':
        filename = cnxdict['out_filename']
    if fileext == '':
        fileext = cnxdict['out_fileext']
    outputfile = cnxdict['out_filedir'] + '\\' + filename + '_' + time.strftime('%Y%m%d_%H%M%S') + '.' + fileext
    if 'xls' in fileext:
        createexcelstub(cnxdict['cnx'],outputfile)
    if DisplayPath:
        print(outputfile)
    return outputfile


def createexcelstub(cnx,outputfile):
    writer = pd.ExcelWriter(outputfile, engine='openpyxl', datetime_format='mm/dd/yyyy')
    df = pd.read_sql("""
        SELECT '' ;
    """, cnx)
    df.to_excel(writer, sheet_name='Stub', index=False)
    return None


def dowritersave(writer,cnxdict):
    df = pd.read_sql("""
        SELECT 'Program Name:  {0}' as `Program Property List` union
        SELECT 'Developer:  Carole Shaw' as `Program Property List` union
        SELECT 'Created:  {1}' as `Program Property List` ;
    """.format( os.path.sys.argv[0]
               ,time.strftime("%c")), cnxdict['cnx'])
    df.to_excel(writer, sheet_name='Program information', index=False)
    print(cnxdict['out_filepath'])
    writer.save()
    writer.close()


def get_colnames(cnxdict, sch='', tbl=''):
    if tbl == '':
        tbl = cnxdict['currtable']
    if sch == '':
        sch = cnxdict['schema']
    cmd = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = "{0}" AND TABLE_NAME = "{1}";
    """.format(sch, tbl)
    readdata = pd.read_sql(cmd,cnxdict['cnx'])
    junk =  np.array(readdata.values).tolist()
    # scan cursor
    collist = list()
    for row in readdata.values:
        collist.append(row[0])
    return collist


def dosqlexecute_(cnxdict, Single=False, EchoSQL=None):
    # wraps dosqlexecute
    # removes comments from the sql statement, they can be problematic if they contain things like semicolons
    cnxdict['sql'] = removecommentstring(cnxdict['sql'])
    # completesqlexecute(cnxdict, Single=False, EchoSQL=None)


def dosqlexecute(cnxdict, Single=False, EchoSQL=None):
    cnxdict['sql'] = removecommentstring(cnxdict['sql'])
    # def dosqlexecute(cnxdict, Single=False, EchoSQL=None, ExecuteLoop=0):
    #     if ExecuteLoop == 0:
    #         cnxdict['sql'] = removecommentstring(cnxdict['sql'])
    #         ExecuteLoop = ExecuteLoop + 1
    if EchoSQL is None:
        EchoSQL = cnxdict['EchoSQL']
    filterwarnings('ignore', category=db.Warning)
    reload(sys)
    rows_changed = 0
    recent_rows_changed = 0
    if Single:  # not using default delimiter
        delimit = '<end-of-code>'
        if cnxdict['sql'].strip()[-1:] == ';':  # last char is a semicolon
            cnxdict['sql'] = cnxdict['sql'].strip()[:-1] + delimit
    elif '<end-of-code>' in cnxdict['sql']:
        delimit = '<end-of-code>'
    else:
        cnxdict['sql'] = cnxdict['sql'].replace(';\n', "<semicolon>")
        delimit = '<semicolon>'

    for cmd in cnxdict['sql'].split(delimit):
        cnxdict['sql'] = cmd.strip() + delimit
        if cnxdict['sql'] == delimit:
            pass
        else:
            cnxdict['sql'] = cnxdict['sql'].replace('<semicolon>', ';')
            cnxdict['sql'] = cnxdict['sql'].replace('<end-of-code>', ';')
            if EchoSQL == 1:
                print(cnxdict['sql'], '\n')
            if 'insert' in cnxdict['sql'].lower():
                try:
                    cnxdict['crs'].execute(cnxdict['sql'])
                    cnxdict['db'].commit()
                    recent_rows_changed = cnxdict['crs'].rowcount
                except:
                    print ('-- SQL Insert Failed:')
                    print(cnxdict['sql'])
                    cnxdict['crs'].close()
                    # reset crs, is there a need to reset db?
                    cnxdict['crs'] = cnxdict['db'].cursor()
                    return -1
                # if 'insert' in cnxdict['sql'].lower():
                #     try:
                #         cnxdict['crs'].execute(cnxdict['sql'])
                #         cnxdict['db'].commit()
                #         recent_rows_changed = cnxdict['crs'].rowcount
                #     except:
                #         print('-- SQL Insert Failed:')
                #         print(cnxdict['sql'])
                #         cnxdict['crs'].close()
            elif 'update' in cnxdict['sql'].lower():
                try:
                    cnxdict['crs'].execute(cnxdict['sql'])
                    cnxdict['db'].commit()
                    recent_rows_changed = cnxdict['crs'].rowcount
                except:
                    print('-- SQL Update Failed:')
                    print(cnxdict['sql'])
                    cnxdict['crs'].close()
                    # reset crs, is there a need to reset db?
                    cnxdict['crs'] = cnxdict['db'].cursor()

            elif 'delete from ' in cnxdict['sql'].lower():
                try:
                    cnxdict['crs'].execute(cnxdict['sql'])
                    cnxdict['db'].commit()
                    recent_rows_changed = cnxdict['crs'].rowcount
                except:
                    print ('-- SQL Delete Failed:')
                    print(cnxdict['sql'])
                    cnxdict['crs'].close()
                    # reset crs, is there a need to reset db?
                    cnxdict['crs'] = cnxdict['db'].cursor()

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
            elif 'drop' in cnxdict['sql'].lower() \
                    or 'index' in cnxdict['sql'].lower() \
                    or 'alter' in cnxdict['sql'].lower() :
                # alter a table
                try:
                    sql.execute(cnxdict['sql'], cnxdict['db'])
                    cnxdict['cnx'].commit()
                except:
                    print('-- SQL Command Failed:')
                    print(cnxdict['sql'])
            else:
                """
                Sometimes there is code within a set of SQL commands that expects a
                cursor to be returned.  This function does not facilitate the
                return of a cursor, therefore any commands in the form:
                SELECT <fieldlist> FROM <table> ;
                are skipped 
                """
                if EchoSQL:
                    print('-- SQL Select Not Done:\n{}\n'.format(cnxdict['sql']))

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


def dosqlread(cmd,con,curchunksize=1000):
    filterwarnings('ignore', category=db.Warning)
    # chunk = pd.read_sql(cmd, con, chunksize=curchunksize)
    df = pd.read_sql(cmd, con)
    try:
        df = clean_common_df_error_columns(df,df.columns)
    except Exception:
        print('SQL Failed:\n{}'.format(cmd))
        df = ''
    return df


def addexceltable(sqlstm, writerobj, newsheet, con ):
    df = dosqlread(sqlstm, con)
    df.to_excel(writerobj, sheet_name=newsheet[0:29], index=False)


def _filter(stmt, allow=0):
    ddl = [t for t in stmt.tokens if t.ttype in (tokens.DDL, tokens.Keyword)]
    start = ' '.join(d.value for d in ddl[:2])
    if ddl and start in IGNORE:
        allow = 1
    for tok in stmt.tokens:
        if allow or not isinstance(tok, sqlparse.sql.Comment):
            yield tok


def removecommentstring(sqlstring):
    """
    Parses the sql passed in to remove any comments, but reserves comments embedded
    in a fundtion code
    :param sqlstring: the text of a sql file
    newstring: the text of the sql file with comments removed
    :return: newstring
    """
    newstring = ''
    sqlstring = sqlstring + '<end of file remove later>'
    for stmt in sqlparse.split(sqlstring):
        if stmt == '' or stmt is None:
            pass
        else:
            sql = sqlparse.parse(stmt)[0]
            newstring = '{}\n{}\n'.format(newstring,sqlparse.sql.TokenList([t for t in _filter(sql)]))
            # print '\n{}'.format(sqlparse.sql.TokenList([t for t in _filter(sql)]))
    if '<end of file remove later>' in newstring:
        pass
    return newstring.replace("<end of file remove later>","",-1)
    # return newstring


def sqlfileexecute(fullpathfilename, cnxdict=None, sect=None):
    """
    Takes a path and filename creates a cursor if needed and executes the sql file code
    :param fullpathfilename:
    :param cnxdict: MySQLdb Connection
    :param sect: Section in Config_.ini
    :return: Pass back results of dosqlexecute()
    """
    if sect is None:
        sect = 'temp'
    if cnxdict is None:
        cnxdict = connect_to_mysql_db_prod(sect)
    # cnxdict['sql'] = open(fullpathfilename).read()
    cnxdict['sql'] = removecommentstring(open(fullpathfilename).read())
    # print(cnxdict['sql'])

    # Clean SQL imported from file to remove comments
    return dosqlexecute(cnxdict)