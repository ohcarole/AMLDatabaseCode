import os

print(os.path.dirname(os.path.realpath(__file__)))
from TimepointUtility import *
import sys

reload(sys)



sys.setdefaultencoding('utf8')
cnxdict = connect_to_mysql_db_prod('nadir')


def get_population(cnxdict):
    """
    Default is to select all patients from the amldatabase2 schema, overwrite this
    function if another population is targetted
    :param cnxdict:
    :return:
    """
    cnxdict['sql'] = """
        DROP TABLE IF EXISTS temp.population;
        CREATE TABLE temp.population
            SELECT UWID AS PtMRN
                , ArrivalDate
                , ArrivalDx
                , TreatmentStartDate
                , Protocol
                , ResponseDate
                , ResponseDescription
            FROM amldatabase2.pattreatment;

        ALTER TABLE temp.population
            ADD INDEX `PtMRN`       (`PtMRN`(10) ASC),
            ADD INDEX `ArrivalDate` (`ArrivalDate` ASC);
    """
    dosqlexecute(cnxdict)


def get_supporting_tables(cnxdict):
    """
    Some of these tables are from caisis and some are from the amldatabase2 schema
    :return:
    """
    cnxdict['sql'] = """
        DROP TABLE IF EXISTS temp.patient ;
        CREATE TABLE temp.patient
            SELECT DISTINCT a.*
                FROM caisis.vdatasetpatients a
                LEFT JOIN temp.population
                    ON a.PtMRN = population.PtMRN
                WHERE population.PtMRN IS NOT NULL ;
        ALTER TABLE `temp`.`patient`
            ADD INDEX `PtMRN` (`PtMRN`(10) ASC);

        DROP TABLE IF EXISTS temp.therapy ;
        CREATE TABLE temp.therapy
            SELECT a.* FROM caisis.vdatasetmedicaltherapy a
                LEFT JOIN temp.population
                    ON a.PtMRN = population.PtMRN
                WHERE population.PtMRN IS NOT NULL ;
        ALTER TABLE `temp`.`therapy`
            ADD INDEX `PtMRN`     (`PtMRN`(10) ASC),
            ADD INDEX `MedTxDate` (`MedTxDate` ASC);

        DROP TABLE IF EXISTS temp.platelet ;
        CREATE TABLE temp.platelet
            SELECT a.PtMRN
                    , a.PatientId
                    , a.LabTestId
                    , a.LabDate
                    , a.LabTest
                    , CASE
                        WHEN a.LabResult REGEXP '^-?[0-9]\.*[0-9]*$' THEN CONVERT(LTRIM(RTRIM(a.LabResult)),DECIMAL(10,3))
                        ELSE NULL
                        END AS LabResult
                    , a.LabUnits
            FROM caisis.platelet  a
                LEFT JOIN temp.population
                    ON a.PtMRN = population.PtMRN
                WHERE population.PtMRN IS NOT NULL;
        ALTER TABLE `temp`.`platelet`
            ADD INDEX `PtMRN`   (`PtMRN`(10) ASC),
            ADD INDEX `LabDate` (`LabDate` ASC);

        DROP TABLE IF EXISTS temp.neutrophil ;
        CREATE TABLE temp.neutrophil
            SELECT a.PtMRN
                    , a.PatientId
                    , a.LabTestId
                    , a.LabDate
                    , a.LabTest
                    , CASE
                        WHEN a.LabResult REGEXP '^-?[0-9]\.*[0-9]*$' THEN CONVERT(LTRIM(RTRIM(a.LabResult)),DECIMAL(10,3))
                        ELSE NULL
                        END AS LabResult
                    , a.LabUnits
                    FROM caisis.neutrophil  a
                LEFT JOIN temp.population
                    ON a.PtMRN = population.PtMRN
                WHERE population.PtMRN IS NOT NULL;
        ALTER TABLE `temp`.`neutrophil`
            ADD INDEX `PtMRN`   (`PtMRN`(10) ASC),
            ADD INDEX `LabDate` (`LabDate` ASC);

    """
    dosqlexecute(cnxdict)


def create_range(cnxdict):
    get_population(cnxdict)
    get_supporting_tables(cnxdict)
    cnxdict['sql'] = """
        DROP TABLE IF EXISTS temp.range ;
        CREATE TABLE temp.range
            SELECT `pt`.`PtMRN`
                , `pt`.`ArrivalDx`
                , `pt`.`Protocol`
                , `pt`.`ResponseDescription`
                , `pt`.`ArrivalDate`
                , CASE
                    WHEN pt.TreatmentStartDate IS NULL THEN mt.MedTxDate
                    WHEN mt.MedTxDate IS NULL THEN pt.TreatmentStartDate
                    ELSE pt.TreatmentStartDate
                END TreatmentStartDate
                , `pt`.`ResponseDate`
                , CASE
                        WHEN    pt.TreatmentStartDate IS NOT NULL THEN DATE_ADD(pt.TreatmentStartDate, INTERVAL +7 DAY)
                        WHEN    pt.TreatmentStartDate IS NULL
                            AND mt.MedTxDate          IS NOT NULL THEN DATE_ADD(mt.MedTxDate, INTERVAL +7 DAY)
                        ELSE NULL
                END AS StartDateRange
                , CASE
                        WHEN pt.ResponseDate IS NOT NULL THEN pt.ResponseDate
                        ELSE DATE_ADD(pt.TreatmentStartDate, INTERVAL +35 DAY)
                END AS EndDateRange
                , CONVERT(NULL, DATETIME) AS PlateletNadirDate
                , -999.999 AS PlateletNadir
                , CONVERT(NULL, DATETIME) AS NeutrophilNadirDate
                , -999.999 AS NeutrophilNadir
                FROM temp.population pt
                LEFT JOIN temp.therapy    mt ON pt.PtMRN = mt.PtMRN
                WHERE ( upper(pt.ArrivalDx) RLIKE '(AML).*(ND)'
                        OR  upper(pt.ArrivalDx) RLIKE '(AML).*(RE)' )
                AND   ( upper(pt.ResponseDescription) LIKE '%CR%'
                        AND upper(pt.ResponseDescription) NOT RLIKE 'UNK'
                        AND pt.ResponseDescription IS NOT NULL )
                AND   ( upper(pt.Protocol) NOT RLIKE '(HU|CONSULT|PALL|HOSP|NO TREATMENT|UNK|OUTSIDE)'
                        AND upper(pt.Protocol) <> '' )
                GROUP BY pt.PtMRN
            ORDER BY pt.PtMRN, pt.ArrivalDate;
            ALTER TABLE `temp`.`range`
                ADD INDEX `PtMRN`       (`PtMRN`(10) ASC),
                ADD INDEX `ArrivalDate` (`ArrivalDate` ASC);

            DROP TABLE IF EXISTS temp.lastrange ;
            CREATE TABLE temp.lastrange SELECT * FROM temp.range;

            ALTER TABLE `temp`.`lastrange`
                ADD INDEX `PtMRN`       (`PtMRN`(10) ASC),
                ADD INDEX `ArrivalDate` (`ArrivalDate` ASC);

            DROP TABLE IF EXISTS temp.therapy;

    """
    dosqlexecute(cnxdict)


def find_Nadir(cnxdict, lablist=('platelet', 'neutrophil')):
    """
    Creates two tables, one with Nadir for platelets and one with Nadir for neutrophils
    :param cnxdict:
    :param lablist:
    :param writer:
    :return:
    """
    for labtest in lablist:
        cnxdict['sql'] = """
            DROP TABLE IF EXISTS temp.{0}_in_range;
            CREATE TABLE temp.{0}_in_range
                SELECT t1.PtMRN
                    , t1.ArrivalDate
                    , lab.LabDate
                    , lab.LabResult
                FROM temp.range t1
                    LEFT JOIN temp.{0} lab ON t1.PtMRN = lab.PtMRN
                    AND lab.LabDate BETWEEN t1.StartDateRange AND t1.EndDateRange
                ORDER BY t1.PtMRN, t1.ArrivalDate, lab.LabDate ;

            ALTER TABLE `temp`.`{0}_in_range`
                ADD INDEX `PtMRN` (`PtMRN`(10) ASC);

            DROP TABLE IF EXISTS temp.minimum_lab_in_range;
            CREATE TABLE temp.minimum_lab_in_range
                SELECT t1.PtMRN
                    , t1.ArrivalDate
                    , MIN(lt.LabResult) AS FirstLabResult
                FROM temp.range t1
                    LEFT JOIN temp.{0}_in_range lt ON t1.PtMRN = lt.PtMRN
                    AND lt.LabDate BETWEEN t1.StartDateRange AND t1.EndDateRange
                    GROUP BY t1.PtMRN, t1.ArrivalDate;

            ALTER TABLE `temp`.`minimum_lab_in_range`
                ADD INDEX `PtMRN` (`PtMRN`(10) ASC);

            DROP TABLE IF EXISTS temp.Nadir{0};
            CREATE TABLE temp.Nadir{0}
                SELECT lt.PtMRN
                    , lt.ArrivalDate
                    , MIN(lt.LabDate) AS LabDate
                    , lt.LabResult
                    FROM temp.minimum_lab_in_range llt
                    LEFT JOIN temp.range t1 ON t1.PtMRN = llt.PtMRN
                        AND t1.ArrivalDate = llt.ArrivalDate
                    LEFT JOIN temp.{0}_in_range lt ON t1.PtMRN = lt.PtMRN
                        AND t1.ArrivalDate = lt.ArrivalDate
                        AND llt.FirstLabResult = lt.LabResult
                    WHERE lt.PtMRN IS NOT NULL
                    GROUP BY lt.PtMRN, t1.ArrivalDate
                    ORDER BY t1.PtMRN, t1.ArrivalDate, lt.LabDate;

            ALTER TABLE `temp`.`Nadir{0}`
                ADD INDEX `PtMRN` (`PtMRN`(10) ASC);

            DROP TABLE IF EXISTS temp.minimum_lab_in_range;
        """.format(labtest)
        dosqlexecute(cnxdict)


def create_Nadir(cnxdict):
    """
        Nadir information is stored in the tables NadirPlatelet and NadirNeutrophil
        and here it is just copied into the Nadir fields for platelets and neutrophils in the Nadir<tablename> tables.
        :param cnxdict:
        :param writer:
        :return:
    """
    find_Nadir(cnxdict)
    cnxdict['sql'] = """
        DROP TABLE IF EXISTS temp.Nadir;
        CREATE TABLE temp.Nadir
            SELECT t1.`PtMRN`
                , t1.`ArrivalDx`
                , t1.`Protocol`
                , t1.`ResponseDescription`
                , t1.`ArrivalDate`
                , t1.`TreatmentStartDate`
                , t1.`ResponseDate`
                , CASE
                    WHEN NadirNeutrophil.LabResult IS NOT NULL THEN NadirNeutrophil.LabResult
                    ELSE NULL
                END AS NeutrophilNadir
                , CASE
                    WHEN NadirNeutrophil.LabDate   IS NOT NULL THEN NadirNeutrophil.LabDate
                    ELSE NULL
                END AS NeutrophilNadirDate
                , CASE
                    WHEN NadirPlatelet.LabResult   IS NOT NULL THEN NadirPlatelet.LabResult
                    ELSE NULL
                END AS PlateletNadir
                , CASE
                    WHEN NadirPlatelet.LabDate     IS NOT NULL THEN NadirPlatelet.LabDate
                    ELSE NULL
                END AS PlateletNadirDate
                FROM temp.range t1
                LEFT JOIN temp.NadirPlatelet
                    ON t1.PtMRN = NadirPlatelet.PtMRN   AND t1.ArrivalDate = NadirPlatelet.ArrivalDate
                LEFT JOIN temp.NadirNeutrophil
                    ON t1.PtMRN = NadirNeutrophil.PtMRN AND t1.ArrivalDate = NadirNeutrophil.ArrivalDate
            ORDER BY t1.`PtMRN`, t1.`ArrivalDate`;

        ALTER TABLE `temp`.`Nadir`
            ADD INDEX `PtMRN`       (`PtMRN`(10) ASC),
            ADD INDEX `ArrivalDate` (`ArrivalDate` ASC);

        DROP TABLE IF EXISTS temp.NadirPlatelet;
        DROP TABLE IF EXISTS temp.NadirNeutrophil;
        DROP TABLE IF EXISTS temp.range;


    """
    dosqlexecute(cnxdict)
    return


def create_recovery_range(cnxdict):
    cnxdict['sql'] = """
    """
    pass


def MainRoutine( cnxdict ):
    create_range(cnxdict)
    create_Nadir(cnxdict)
    create_recovery_range(cnxdict)
    return None


MainRoutineResult = MainRoutine(cnxdict)