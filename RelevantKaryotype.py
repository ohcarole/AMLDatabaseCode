from Utilities.MySQLdbUtils import *

cnxdict = connect_to_mysql_db_prod('relevantkaryo')

def create_patientlist(cnxdict):
    """

    :param cnxdict:
    :return:
    """
    cnxdict['sql'] = """
        DROP TABLE IF EXISTS relevantkaryo.patientlist;
        CREATE TABLE relevantkaryo.patientlist
            SELECT b.*, min(arrivaldate) as arrivaldate
                FROM amldatabase2.pattreatment
                LEFT JOIN caisis.vdatasetpatients b ON pattreatment.UWID = b.PtMRN
                GROUP BY b.PtMRN;
        ALTER TABLE `relevantkaryo`.`patientlist`
            ADD INDEX `PatientId`   (`PatientId`   ASC),
            ADD INDEX `PtMRN`       (`PtMRN`(10)   ASC),
            ADD INDEX `ArrivalDate` (`ArrivalDate` ASC);
    """
    dosqlexecute(cnxdict)
    return


def create_karyolist(cnxdict,allpatients=1):
    """
    This program assumes that the temp.patientlist needs to have associated karyotypes
    :param cnxdict:
    :param writer:
    :return:
    """

    if allpatients == 1:
        create_patientlist(cnxdict)

    # For working on karyotype
    cnxdict['sql'] = """

        # creates a table associated with the patient list of all karyo results (after a few steps)
        # Note that DateObtained comes from the PathNote, whereas PathDate is the date pathology
        # report was electronically signed.  We default to the date obtained where available
        # Also sometimes the path result is in the field 'karyo' when coming from SCCACYTO, and in the
        # field 'PathResult' when coming from migrated or electronically extracted from pathnotes.
        # the fields are colapsed to the one field 'karyo' in this first query
        DROP TABLE IF EXISTS relevantkaryo.allkaryo;
        CREATE TABLE relevantkaryo.allkaryo
            SELECT a.PatientId
                    , a.PtMRN
                    , c.ArrivalDx
                    , c.Protocol
                    , c.ResponseDescription
                    , a.ArrivalDate
                    , c.TreatmentStartDate
                    , c.ResponseDate
                    , b.type
                    , CASE
                        WHEN YEAR(b.DateObtained) = 1900 THEN STR_TO_DATE(b.PathDate, '%Y-%m-%d')
                        WHEN b.DateObtained IS NULL      THEN STR_TO_DATE(b.PathDate, '%Y-%m-%d')
                        ELSE STR_TO_DATE(b.DateObtained, '%Y-%m-%d')
                    END AS DateObtained
                    , b.PathDate
                    , CASE
                            WHEN b.`Karyo`    = '' THEN LTRIM(b.PathResult)
                            WHEN b.PathResult = '' THEN LTRIM(b.Karyo)
                            ELSE NULL
                        END AS karyo
                    FROM relevantkaryo.patientlist a
                LEFT JOIN caisis.allkaryo  b ON a.ptmrn = b.ptmrn
                LEFT JOIN amldatabase2.`pattreatment with prev and next arrival` c on a.PtMRN = c.uwid and a.ArrivalDate = c.ArrivalDate
                WHERE ( b.karyo <> '' OR pathresult <> '' )
                    AND LEFT(b.karyo,3)      <> 'nuc'
                    AND LEFT(b.pathresult,3) <> 'nuc';

        ALTER TABLE `relevantkaryo`.`allkaryo`
            ADD `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY
            , CHANGE COLUMN `DateObtained` `DateObtained` DATETIME NULL DEFAULT NULL ;

        # Removes records that are not karyo results
        DELETE FROM relevantkaryo.allkaryo WHERE
            NOT (
            karyo rlike '^[0-9]{2}'
            or karyo rlike '^//[0-9]{2}'
            or karyo rlike '^[0-9]{2}(p|q)'
            or karyo = 'normal cytogenetics'
            or karyo = 'normal male karyotype'
            or karyo = 'normal female karyotype' );

        # Removes records that are not karyo results
        DELETE FROM relevantkaryo.allkaryo WHERE
            karyo rlike '^[0-9]{2}(p|q)';

        # find the first and last pathology report date for each patient
        DROP TABLE IF EXISTS relevantkaryo.relevantkaryo;
        CREATE TABLE relevantkaryo.relevantkaryo
        SELECT a.PatientId
            , a.PtMRN
            , a.ArrivalDx
            , a.Protocol
            , a.ResponseDescription
            , a.ArrivalDate
            , a.TreatmentStartDate
            , a.ResponseDate
            , a.Type as ArrivalPathRecordType
            , a.KaryoAtArrivalDate
            , a.KaryoAtArrival
            , b.Type as EarliestPathRecordType
            , b.EarliestKaryoDate
            , b.EarliestKaryo
            , c.Type as ResponsePathRecordType
            , c.ResponseKaryoDate
            , c.ResponseKaryo
            FROM (SELECT 'Arrival Karyo'
                            , PatientId
                            , PtMRN
                            , ArrivalDx
                            , Protocol
                            , ResponseDescription
                            , ArrivalDate
                            , TreatmentStartDate
                            , ResponseDate
                            , Type
                            , Karyo  AS KaryoAtArrival
                            , MAX(dateobtained) AS KaryoAtArrivalDate
                        FROM relevantkaryo.allkaryo
                        WHERE dateobtained <= treatmentstartdate
                        GROUP BY patientid, arrivaldate) a
                    LEFT JOIN ( SELECT 'Earliest Karyo'
                            , PatientId
                            , PtMRN
                            , ArrivalDate
                            , TreatmentStartDate
                            , Type
                            , Karyo  AS EarliestKaryo
                            , MIN(dateobtained) AS EarliestKaryoDate
                        FROM relevantkaryo.allkaryo
                        GROUP BY patientid, arrivaldate ) b
                        ON a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
                    LEFT JOIN ( SELECT 'Response Karyo'
                            , PatientId
                            , PtMRN
                            , ArrivalDate
                            , TreatmentStartDate
                            , Type
                            , Karyo  AS ResponseKaryo
                            , MAX(dateobtained) AS ResponseKaryoDate
                        FROM relevantkaryo.allkaryo
                        WHERE dateobtained BETWEEN DATE_ADD(treatmentstartdate,INTERVAL +7 DAY)
                            AND DATE_ADD(responsedate, INTERVAL +14 DAY)
                        GROUP BY patientid, arrivaldate ) c
                        ON a.PatientId = c.PatientId AND a.ArrivalDate = c.ArrivalDate
                    ORDER BY PatientId, arrivaldate ;

    """
    dosqlexecute(cnxdict)
    return None


def create_output(cnxdict,writer):
    cmd = "select * from relevantkaryo.allkaryo;"
    df = dosqlread(cmd, cnxdict['cnx'])
    df.to_excel(writer, sheet_name='karyo  detail', index=False)

    cmd = "select * from relevantkaryo.relevantkaryo;"
    df = dosqlread(cmd, cnxdict['cnx'])
    df.to_excel(writer, sheet_name='most relevant karyo per patient', index=False)


book = load_workbook(cnxdict['out_filepath'])
writer = pd.ExcelWriter(cnxdict['out_filepath'], engine='openpyxl')
writer.book = book
writer.sheets = dict((ws.title, ws) for ws in book.worksheets)

create_karyolist(cnxdict,1)
create_output(cnxdict,writer)




dowritersave(writer,cnxdict)


