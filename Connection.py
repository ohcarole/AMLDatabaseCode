# from mysql.connector import MySQLConnection, Error, errorcode
import ConfigParser

def read_db_config( sect, config='Config.ini' ):
    """ Read database configuration file and return a dictionary object
    :param ini_filename: name of the configuration file, defaults to config.ini
    :param ini_section: configuration section of interest, defaults to mysql
    :return: a dictionary of database parameters
    """

    # create parser and read ini configuration file
    parser = ConfigParser.ConfigParser()

    # parser.read(cnxdict['ini_file'])
    parser.read(config)

    # get section, default to mysql
    db = {'db':''
        , 'cnx': {}
        , 'crs': {}
        , 'sql': ''
        , 'df': {}
        , 'multi': False
    }
    if parser.has_section(sect):
        items = parser.items(sect)
        for item in items:
            db[item[0]] = item[1]
    else:
        raise Exception('{0} not found '
                        'in the {1} file'.format(sect, config))
    return db


def buildfilepath(cnxdict, filename=''):
    if filename == '':
        filename = cnxdict['out_filename']
    return cnxdict['out_filedir'] + '\\' + filename + '.' + cnxdict['out_fileext']


def get_cnxdict(sect):
    cnxdict = {
        'desc': 'connection and cursor information'
        , 'ini_section': sect
        , 'ini_file': 'J:\Estey_AML\AML Programming\Python\sharedUtils\Config.ini'
        , 'out_filedir': ''
        , 'out_filename': ''
        , 'out_fileext': ''
        , 'out_filepath': ''  # this is built from the other fields
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
    return cnxdict