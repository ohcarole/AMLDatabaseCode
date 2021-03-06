# from mysql.connector import MySQLConnection, Error, errorcode
import ConfigParser
import time

def read_db_config( sect, config='J:\Estey_AML\AML Programming\Python\sharedUtils\ExcludedFromGitHub\Config_.ini'):
    """ Read database configuration file and return a dictionary object
    :param ini_filename: name of the configuration file, defaults to config_.ini
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
        , 'EchoSQL': False
          }
    if parser.has_section(sect):
        items = parser.items(sect)
        for item in items:
            db[item[0]] = item[1]
    else:
        raise Exception('{0} not found '
                        'in the {1} file'.format(sect, config))
    return db




