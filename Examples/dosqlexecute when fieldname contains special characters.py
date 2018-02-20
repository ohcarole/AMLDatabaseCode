from Utilities.MySQLdbUtils import *

reload(sys)

sys.setdefaultencoding('utf8')

"""
The purpose of this example is to show how to utilize the 'dosqlexecute' procedure for field names
containing semi-colons or quotes.
"""

# set up the connection dictionary with the, at this point empty, sql property.
cnxdict = connect_to_mysql_db_prod('compositemodel')

# sample text representing odd types of values that can be found in fields from tables referenced in the
# seelct statment.  This example has a semi-colon, and both types of quotes.
FieldName0 = """
"weird 'field'; name"!
"""

## defining the sql statement with a place holder for the field name.
sqlcmd = """
        DROP TABLE IF EXISTS temp.junk;
        CREATE TABLE temp.junk
            SELECT rand() AS Id, `{0}` FROM `mrcrisk`.`karyo_variation`;
    """
FieldName0 = re.sub(r"(\f|\t|\r|\n|\v)*", '', FieldName0)  # remove: form-feeds, tabs, carriage-returm, new-line, vertical tab
sqlcmd = sqlcmd.replace(';','<end-of-code>').format(FieldName0)
print(sqlcmd)
cnxdict['sql'] = sqlcmd
dosqlexecute(cnxdict)