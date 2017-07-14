from MySQLdbUtils import *

cnxdict = connect_to_mysql_db_prod('relevantlab')

def recreate_patientlist(cnxdict):
    """

    :param cnxdict:
    :return:
    """
    cnxdict['sql'] = """
        DROP TABLE IF EXISTS temp.patientlist;
        CREATE TABLE temp.patientlist
            SELECT b.*, min(arrivaldate) as arrivaldate
                FROM amldatabase2.pattreatment
                LEFT JOIN caisis.vdatasetpatients b ON pattreatment.UWID = b.PtMRN
                GROUP BY b.PtMRN;
        ALTER TABLE `temp`.`patientlist`
            ADD INDEX `PatientId`   (`PatientId`   ASC),
            ADD INDEX `PtMRN`       (`PtMRN`(10)   ASC),
            ADD INDEX `ArrivalDate` (`ArrivalDate` ASC);
    """
    dosqlexecute(cnxdict)
    return


def create_karyolist(cnxdict,writer,allpatients=1):
    """
    This program assumes that the temp.patientlist needs to have associated karyotypes
    :param cnxdict:
    :param writer:
    :return:
    """

    if allpatients == 1:
        recreate_patientlist(cnxdict)

    # For working on karyotype
    cnxdict['sql'] = """
        drop table if exists temp.t1;
        create table temp.t1
            select a.PatientId, a.PtMRN, a.ArrivalDate
                    , b.type
                    , b.DateObtained
                    , b.PathDate
                    , a.ArrivalDate as TempDateObtained
                    , CASE
                            WHEN b.`Karyo` = '' THEN b.PathResult
                            WHEN b.PathResult = '' THEN b.Karyo
                            ELSE NULL
                        END AS karyo
                    from temp.patientlist a
                left join caisis.allkaryo  b on a.ptmrn = b.ptmrn
                where ( b.karyo <> '' or pathresult <> '' )
                    and left(b.karyo,3)      <> 'nuc'
                    and left(b.pathresult,3) <> 'nuc';

        update temp.t1 SET TempDateObtained = str_to_Date(DateObtained,'%Y-%m-%d');
        update temp.t1 SET TempDateObtained = str_to_Date(PathDate,'%Y-%m-%d') WHERE TempDateObtained IS NULL;
        ALTER TABLE `temp`.`t1`
            ADD `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY
            , DROP COLUMN PathDate
            , DROP COLUMN DateObtained
            , CHANGE COLUMN `TempDateObtained` `DateObtained` DATETIME NULL DEFAULT NULL ;

        drop table if exists temp.t2;
        create table temp.t2
            select 'A'
                    , PatientId
                    , PtMRN
                    , ArrivalDate
                    , max(dateobtained) as DateObtained
                    , Type
                    , Karyo
                from temp.t1
                where dateobtained <= arrivaldate
                group by patientid, arrivaldate
            union
            select 'B'
                    , PatientId
                    , PtMRN
                    , ArrivalDate
                    , min(dateobtained) as DateObtained
                    , Type
                    , Karyo
                from temp.t1
                where dateobtained > arrivaldate
                group by patientid, arrivaldate
            ORDER BY PatientId, arrivaldate, dateobtained ;
        alter table temp.t2 add id int primary key auto_increment;

        drop table if exists temp.t3;
        create table temp.t3
        Select a.PatientId
                    , a.PtMRN
                    , a.ArrivalDate
                    , a.DateObtained
                    , b.karyo
            FROM (select min(id) as TempId
                        , PatientId
                        , PtMRN
                        , ArrivalDate
                        , min(DateObtained) as DateObtained
                    from temp.t2
                    group by PatientId, PtMRN, ArrivalDate ) a
            LEFT JOIN temp.t2 b
                on  a.PatientId = b.PatientId
                and a.DateObtained = b.DateObtained ;
    """
    dosqlexecute(cnxdict)

    cmd = "select * from temp.t1;"
    df  = dosqlread(cmd,cnxdict['cnx'])
    df.to_excel(writer, sheet_name='karyo history detail', index=False)

    cmd = "select * from temp.t3;"
    df  = dosqlread(cmd,cnxdict['cnx'])
    df.to_excel(writer, sheet_name='karyosummary', index=False)


    # cmd = "select patientid, ptmrn, arrivaldate, dateobtained, karyo from temp.t3 limit 4;"
    # cmd = "select patientid, ptmrn, karyo from temp.t3 where PtMRN = 'U0108449';"
    # cmd = """
    #     SELECT `allkaryo`.`index`,
    #         `allkaryo`.`ALLKaryoId`,
    #         `allkaryo`.`Type`,
    #         `allkaryo`.`PatientId`,
    #         `allkaryo`.`PtMRN`,
    #         `allkaryo`.`PathologyId`,
    #         `allkaryo`.`DateObtained`,
    #         `allkaryo`.`PathDate`,
    #         `allkaryo`.`Karyo`,
    #         `allkaryo`.`PathNum`,
    #         `allkaryo`.`PathNotes`,
    #         `allkaryo`.`PathTestId`,
    #         `allkaryo`.`PathTest`,
    #         `allkaryo`.`PathResult`
    #     FROM `caisis`.`allkaryo`
    #     LIMIT 200;
    # """
    # df  = dosqlread(cmd,cnxdict['cnx'])
    # """
    #     Experiment with
    #
    #     To delimit by a tab you can use the sep argument of to_csv:
    #
    #     df.to_csv(file_name, sep='\t')
    #     To use a specific encoding (e.g. 'utf-8') use the encoding argument:
    #
    #     df.to_csv(file_name, sep='\t', encoding='utf-8')
    #
    # """
    # df.to_excel(writer, sheet_name='karyosummary', index=False)
    return None


book = load_workbook(cnxdict['out_filepath'])
writer = pd.ExcelWriter(cnxdict['out_filepath'], engine='openpyxl')
writer.book = book
writer.sheets = dict((ws.title, ws) for ws in book.worksheets)

create_karyolist(cnxdict,writer,1)
writer.save()
writer.close()

