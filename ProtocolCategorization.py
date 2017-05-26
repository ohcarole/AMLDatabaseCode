from MySQLdbUtils import *

cnxdict = connect_to_mysql_db_prod('hma')  # get a connection to the hma section for an example

def make_protocolmap(cnxdict):
    cnxdict['sql'] = """
        SET SQL_SAFE_UPDATES = 0;

        DROP TABLE IF EXISTS protocolmapping;

        SET @rownum = 0;

        CREATE TABLE protocolmapping
            SELECT @rownum := @rownum+1             as pk
                , space(40)                         as UpdateItem
                , protocol                          as OriginalProtocol
                , CONCAT(' ',UCASE(a.protocol),' ') as protocol
                , space(50)                         as regimen
                , space(200)                        as druglist
                , space(10)                         as wildcard
                , space(50)                         as intensity
                , `total use`
            FROM (select protocol, count(*) as `total use` from amldata group by protocol) a
            WHERE
                a.protocol IS NOT NULL
                AND a.protocol > '';

        ALTER TABLE `protocolmapping`
            CHANGE COLUMN `pk` `pk` INT NOT NULL ,
            ADD PRIMARY KEY (`pk`);

        UPDATE `protocolmapping`
            SET `UpdateItem` = ''
            , `regimen`      = ''
            , `druglist`     = ''
            , `wildcard`     = ''
            , `intensity`    = '';


    """
    dosqlexecute(cnxdict)

def get_updateitem(cnxdict):
    cnxdict['itemnum'] = int(cnxdict['itemnum']) + 1
    return cnxdict['itemnum']

def create_mapping_metadata(cnxdict):
    cnxdict['sql'] = """
        UPDATE

    """
    dosqlexecute(cnxdict)

