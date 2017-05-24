# from mysql.connector import MySQLConnection, Error, errorcode
import ConfigParser

def read_db_config( cnxdict ):
    """ Read database configuration file and return a dictionary object
    :param ini_filename: name of the configuration file, defaults to config.ini
    :param ini_section: configuration section of interest, defaults to mysql
    :return: a dictionary of database parameters
    """

    # create parser and read ini configuration file
    parser = ConfigParser.ConfigParser()

    parser.read(cnxdict['ini_file'])

    # get section, default to mysql
    db = {}
    if parser.has_section(cnxdict['ini_section']):
        items = parser.items(cnxdict['ini_section'])
        for item in items:
            db[item[0]] = item[1]
    else:
        raise Exception('{0} not found '
                        'in the {1} file'.format(cnxdict['ini_section'], cnxdict['ini_section']))
    return db

