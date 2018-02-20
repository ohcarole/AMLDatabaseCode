from Utilities.MySQLdbUtils import *
# from xlsxwriter.workbook import Workbook
# import os
# from MessageBox import *
# from TimepointUtility import *

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
    printtext('stack')
    cnxdict['sql'] = """
        DROP TABLE IF EXISTS recovery.population;
        CREATE TABLE recovery.population
            SELECT UWID AS PtMRN
                , ArrivalDate
                , ArrivalDx
                , TreatmentStartDate
                , Protocol
                , ResponseDate
                , ResponseDescription
                , NextArrivalDate
                , timestampdiff(day,TreatmentStartDate,NextArrivalDate) as DaysTreatmenttoNextArrival
            FROM amldatabase2.`pattreatment with prev and next arrival`;

        ALTER TABLE recovery.population
            ADD INDEX `PtMRN`       (`PtMRN`(10) ASC),
            ADD INDEX `ArrivalDate` (`ArrivalDate` ASC);
    """
    dosqlexecute(cnxdict)


def get_labs(cnxdict):
    printtext('stack')
    cnxdict['sql'] = """
        DROP TABLE IF EXISTS recovery.platelet ;
            CREATE TABLE recovery.platelet
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
                LEFT JOIN recovery.population
                    ON a.PtMRN = population.PtMRN
                WHERE population.PtMRN IS NOT NULL;
        ALTER TABLE `recovery`.`platelet`
            ADD INDEX `PtMRN`   (`PtMRN`(10) ASC),
            ADD INDEX `LabDate` (`LabDate` ASC);

        DROP TABLE IF EXISTS recovery.neutrophil ;
        CREATE TABLE recovery.neutrophil
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
                LEFT JOIN recovery.population
                    ON a.PtMRN = population.PtMRN
                WHERE population.PtMRN IS NOT NULL;
        ALTER TABLE `recovery`.`neutrophil`
            ADD INDEX `PtMRN`   (`PtMRN`(10) ASC),
            ADD INDEX `LabDate` (`LabDate` ASC);
    """


def get_supporting_tables(cnxdict):
    """
    Some of these tables are from caisis and some are from the amldatabase2 schema
    :return:
    """
    printtext('stack')
    # get_labs(cnxdict)
    cnxdict['sql'] = """
        DROP TABLE IF EXISTS recovery.patient ;
        CREATE TABLE recovery.patient
            SELECT DISTINCT a.*
                FROM caisis.vdatasetpatients a
                LEFT JOIN recovery.population
                    ON a.PtMRN = population.PtMRN
                WHERE population.PtMRN IS NOT NULL ;
        ALTER TABLE `recovery`.`patient`
            ADD INDEX `PtMRN` (`PtMRN`(10) ASC);

        DROP TABLE IF EXISTS recovery.relapse ;
        CREATE TABLE recovery.relapse
            SELECT a.* FROM caisis.vdatasetstatus a
                LEFT JOIN recovery.population b
                    ON a.PtMRN = b.PtMRN
                WHERE b.PtMRN IS NOT NULL
                AND a.Status LIKE '%relapse%';

        DROP TABLE IF EXISTS recovery.therapy ;
        CREATE TABLE recovery.therapy
            SELECT a.* FROM caisis.vdatasetmedicaltherapy a
                LEFT JOIN recovery.population
                    ON a.PtMRN = population.PtMRN
                WHERE population.PtMRN IS NOT NULL ;
        ALTER TABLE `recovery`.`therapy`
            ADD INDEX `PtMRN`     (`PtMRN`(10) ASC),
            ADD INDEX `MedTxDate` (`MedTxDate` ASC);

        # Get all karyotypes for all patients
        DROP TABLE IF EXISTS recovery.allkaryo ;
        CREATE TABLE recovery.allkaryo
            SELECT PtMRN
                , PathologyId
                , DateObtained
                , PathDate
                , CASE
                        WHEN `Karyo` = '' THEN PathResult
                        WHEN PathResult = '' THEN Karyo
                        ELSE NULL
                    END AS karyo
            FROM caisis.allkaryo
            WHERE type NOT IN ('FISH','SCCACGAT')
            and karyo <> '' OR pathresult <> ''
            ORDER BY PtMRN, DateObtained ;

        # remove cyto that are not karyotypes
        DELETE FROM recovery.allkaryo
                    WHERE karyo LIKE '%n/a%'
                    OR UPPER(LEFT(karyo,12)) = 'INSUFFICIENT'
                    OR UPPER(LEFT(karyo,9)) = 'NO GROWTH'
                    OR LEFT(karyo,12) LIKE '%ish%'
                    OR LEFT(karyo,12) LIKE '%nuc%'
                    OR LEFT(karyo,12) LIKE '%probe%'
                    OR LEFT(karyo,12) LIKE '%meta%'
                    OR LEFT(karyo,12) LIKE '%bcr%'
                    OR LEFT(karyo,12) LIKE '%egr%'
                    OR LEFT(karyo,4)  LIKE '%q%'
                    OR LEFT(karyo,20) LIKE '%cancelled%'
                    OR LEFT(karyo,12) RLIKE '\\%'
                    OR LEFT(LTRIM(karyo),1) NOT BETWEEN '0' AND '9';

        DROP TABLE IF EXISTS recovery.karyotype ;
        CREATE TABLE recovery.karyotype
            SELECT PtMRN
                    , MIN(DateObtained) AS KaryoDate
                    , karyo
                FROM recovery.allkaryo
                GROUP BY PtMRN;

        DROP TABLE IF EXISTS recovery.allkaryo ;
    """
    dosqlexecute(cnxdict)


def create_range(cnxdict):
    printtext('stack')
    # get_population(cnxdict)
    get_supporting_tables(cnxdict)
    cnxdict['sql'] = """
        DROP TABLE IF EXISTS recovery.range ;
        CREATE TABLE recovery.range
            SELECT `pt`.`PtMRN`
                , `pt`.`ArrivalDx`
                , `pt`.`Protocol`
                , `pt`.`ResponseDescription`
                , `pt`.`ArrivalDate`
                , CASE
                    WHEN `pt`.`NextArrivalDate` IS NULL THEN NOW()
                    ELSE `pt`.`NextArrivalDate`
                END AS `NextArrivalDate`
                , CASE
                    WHEN pt.TreatmentStartDate IS NULL THEN mt.MedTxDate
                    WHEN mt.MedTxDate IS NULL THEN pt.TreatmentStartDate
                    ELSE pt.TreatmentStartDate
                END TreatmentStartDate
                , `pt`.`ResponseDate`
                , CASE
                    WHEN min(b.StatusDate) between pt.ArrivalDate and pt.Nextarrivaldate
                        THEN min(b.StatusDate)
                    ELSE CONVERT(NULL, DATETIME)
                END AS RelapseDate
                , CASE
                    WHEN min(b.StatusDate) between pt.ArrivalDate and pt.Nextarrivaldate
                        THEN timestampdiff(DAY,pt.treatmentstartdate,min(b.StatusDate))
                    ELSE NULL
                END AS DaysTreatmentToRelapse
                , `pt`.DaysTreatmenttoNextArrival
                , CASE
                        WHEN    pt.TreatmentStartDate IS NOT NULL THEN DATE_ADD(pt.TreatmentStartDate, INTERVAL +7 DAY)
                        WHEN    pt.TreatmentStartDate IS NULL
                            AND mt.MedTxDate          IS NOT NULL THEN DATE_ADD(mt.MedTxDate, INTERVAL +7 DAY)
                        ELSE CONVERT(NULL, DATETIME)
                END AS StartDateRange
                , CASE
                        WHEN pt.ResponseDate IS NOT NULL THEN pt.ResponseDate
                        ELSE DATE_ADD(pt.TreatmentStartDate, INTERVAL +35 DAY)
                END AS EndDateRange
                , `kt`.KaryoDate
                , `kt`.Karyo
                FROM recovery.population pt
                LEFT JOIN recovery.relapse b ON pt.PtMRN = b.PtMRN
                LEFT JOIN recovery.therapy mt ON pt.PtMRN = mt.PtMRN
                LEFT JOIN recovery.karyotype kt on pt.PtMRN = kt.PtMRN
                GROUP BY pt.PtMRN, pt.ArrivalDate
            ORDER BY pt.PtMRN, pt.ArrivalDate;

    ALTER TABLE `recovery`.`range`
        ADD INDEX `PtMRN`       (`PtMRN`(10) ASC),
        ADD INDEX `ArrivalDate` (`ArrivalDate` ASC);

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
    printtext('stack')

    for labtest in lablist:
        cnxdict['sql'] = """
            DROP TABLE IF EXISTS recovery.{0}_in_range;
            CREATE TABLE recovery.{0}_in_range
                SELECT a.PtMRN
                    , a.ArrivalDate
                    , lab.LabDate
                    , lab.LabResult
                FROM recovery.range a
                    LEFT JOIN recovery.{0} lab ON a.PtMRN = lab.PtMRN
                    AND lab.LabDate BETWEEN a.StartDateRange AND a.EndDateRange
                ORDER BY a.PtMRN, a.ArrivalDate, lab.LabDate ;

            ALTER TABLE `recovery`.`{0}_in_range`
                ADD INDEX `PtMRN` (`PtMRN`(10) ASC);

            DROP TABLE IF EXISTS recovery.minimum_lab_in_range;
            CREATE TABLE recovery.minimum_lab_in_range
                SELECT a.PtMRN
                    , a.ArrivalDate
                    , MIN(lt.LabResult) AS FirstLabResult
                FROM recovery.range a
                    LEFT JOIN recovery.{0}_in_range lt ON a.PtMRN = lt.PtMRN
                    AND lt.LabDate BETWEEN a.StartDateRange AND a.EndDateRange
                    GROUP BY a.PtMRN, a.ArrivalDate;

            ALTER TABLE `recovery`.`minimum_lab_in_range`
                ADD INDEX `PtMRN` (`PtMRN`(10) ASC);

            DROP TABLE IF EXISTS recovery.Nadir{0};
            CREATE TABLE recovery.Nadir{0}
                SELECT c.PtMRN
                    , c.ArrivalDate
                    , MIN(c.LabDate) AS LabDate
                    , c.LabResult
                    FROM recovery.minimum_lab_in_range a
                    LEFT JOIN recovery.range b ON b.PtMRN = a.PtMRN
                        AND b.ArrivalDate = a.ArrivalDate
                    LEFT JOIN recovery.{0}_in_range c ON b.PtMRN = c.PtMRN
                        AND b.ArrivalDate = c.ArrivalDate
                        AND a.FirstLabResult = c.LabResult
                    WHERE c.PtMRN IS NOT NULL
                    GROUP BY c.PtMRN, b.ArrivalDate
                    ORDER BY b.PtMRN, b.ArrivalDate, c.LabDate;

            ALTER TABLE `recovery`.`Nadir{0}`
                ADD INDEX `PtMRN` (`PtMRN`(10) ASC);

            DROP TABLE IF EXISTS recovery.minimum_lab_in_range;
            DROP TABLE IF EXISTS recovery.{0}_in_range;

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
    printtext('stack')
    find_Nadir(cnxdict)
    cnxdict['sql'] = """
        DROP TABLE IF EXISTS recovery.Nadir;
        CREATE TABLE recovery.Nadir
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
                END AS ANCNadir
                , CASE
                    WHEN NadirNeutrophil.LabDate   IS NOT NULL THEN NadirNeutrophil.LabDate
                    ELSE NULL
                END AS ANCNadirDate
                , CASE
                    WHEN NadirPlatelet.LabResult   IS NOT NULL THEN NadirPlatelet.LabResult
                    ELSE NULL
                END AS PLTNadir
                , CASE
                    WHEN NadirPlatelet.LabDate     IS NOT NULL THEN NadirPlatelet.LabDate
                    ELSE NULL
                END AS PLTNadirDate
                FROM recovery.range t1
                LEFT JOIN recovery.NadirPlatelet
                    ON t1.PtMRN = NadirPlatelet.PtMRN   AND t1.ArrivalDate = NadirPlatelet.ArrivalDate
                LEFT JOIN recovery.NadirNeutrophil
                    ON t1.PtMRN = NadirNeutrophil.PtMRN AND t1.ArrivalDate = NadirNeutrophil.ArrivalDate
            ORDER BY t1.`PtMRN`, t1.`ArrivalDate`;

        ALTER TABLE `recovery`.`Nadir`
            ADD INDEX `PtMRN`       (`PtMRN`(10) ASC),
            ADD INDEX `ArrivalDate` (`ArrivalDate` ASC);

        DROP TABLE IF EXISTS recovery.NadirPlatelet;
        DROP TABLE IF EXISTS recovery.NadirNeutrophil;

    """
    dosqlexecute(cnxdict)
    return


def create_recovery_range(cnxdict, lablist=('platelet','neutrophil')):
    printtext('stack')
    for labtest in lablist:
        if labtest == 'platelet':
            abbreviation = 'PLT'
            levelname1 = '20'
            levelname2 = '50'
            levelname3 = '100'
            levelvalue1 = 20
            levelvalue2 = 50
            levelvalue3 = 100
        elif labtest == 'neutrophil':
            abbreviation = 'ANC'
            levelname1 = '500'
            levelname2 = '1000'
            levelname3 = 'None'
            levelvalue1 = 0.5
            levelvalue2 = 1.0
            levelvalue3 = -999

        cnxdict['sql'] = """
            DROP TABLE IF EXISTS recovery.{0}_in_range;
            CREATE TABLE recovery.{0}_in_range
                SELECT a.`PtMRN`,
                    b.ArrivalDate,
                    a.`PatientId`,
                    a.`LabTestId`,
                    a.`LabDate`,
                    a.`LabTest`,
                    a.`LabResult`,
                    a.`LabUnits`,
                    b.StartDateRange,
                    b.NextArrivalDate
                FROM recovery.{0} a
                    LEFT JOIN recovery.range b ON a.PtMRN = b.PtMRN
                WHERE
                    a.LabDate between b.StartDateRange AND b.NextArrivalDate
                    and b.ArrivalDate < b.StartDateRange
                GROUP BY a.PtMRN, b.ArrivalDate, a.LabDate
                ORDER BY a.PtMRN, b.ArrivalDate, a.LabDate;

            ALTER TABLE `recovery`.`{0}_in_range`
                ADD COLUMN `SeqId` INT NOT NULL AUTO_INCREMENT FIRST,
                ADD PRIMARY KEY (`SeqId`);

            ALTER TABLE `recovery`.`{0}_in_range`
                ADD INDEX `PtMRN`       (`PtMRN`(10) ASC),
                ADD INDEX `SeqId`       (`SeqId` ASC),
                ADD INDEX `ArrivalDate` (`ArrivalDate` ASC);

            DROP TABLE IF EXISTS recovery.{0}Recovery;
            CREATE TABLE recovery.{0}Recovery
                SELECT nadir.PtMRN
                    , nadir.ArrivalDx
                    , nadir.Protocol
                    , nadir.ResponseDescription
                    , nadir.ArrivalDate
                    , nadir.TreatmentStartDate
                    , nadir.ResponseDate
                    , a.seqid

                    , CASE
                        WHEN a.LabDate < nadir.{1}NadirDate THEN "Falling"
                        WHEN c.LabResult IS NULL AND b.LabResult >= a.LabResult THEN "Increasing"
                        WHEN c.LabResult >= b.LabResult AND b.LabResult >= a.LabResult THEN "Increasing"
                        ELSE ""
                    END AS {1}CountTrend

                    , CASE
                        WHEN nadir.{1}Nadir = a.LabResult AND nadir.{1}NadirDate = a.LabDate THEN "Yes"
                        ELSE ""
                    END AS IsNadir{1}

                    , CASE
                        WHEN a.LabResult > {5}
                            AND c.LabResult IS NULL AND b.LabResult >= a.LabResult THEN "Yes"
                        WHEN a.LabResult > {5}
                            AND c.LabResult >= b.LabResult
                            AND b.LabResult >= a.LabResult THEN "Yes"
                        ELSE ""
                    END AS {1}Recover{2}

                    , CASE
                        WHEN a.LabResult > {6}
                            AND c.LabResult IS NULL AND b.LabResult >= a.LabResult THEN "Yes"
                        WHEN a.LabResult > {6}
                            AND c.LabResult >= b.LabResult
                            AND b.LabResult >= a.LabResult THEN "Yes"
                        ELSE ""
                    END AS {1}Recover{3}

                    , CASE
                        WHEN {7} = -999 THEN ""
                        WHEN a.LabResult > {7}
                            AND c.LabResult IS NULL AND b.LabResult >= a.LabResult THEN "Yes"
                        WHEN a.LabResult > {7}
                            AND c.LabResult >= b.LabResult
                            AND b.LabResult >= a.LabResult THEN "Yes"
                        ELSE ""
                    END AS {1}Recover{4}

                    , TIMESTAMPDIFF(DAY,nadir.treatmentstartdate,a.labdate) AS DaysFromTreatment

                    , CASE
                        WHEN nadir.{1}Nadir = a.LabResult AND nadir.{1}NadirDate = a.LabDate THEN nadir.{1}Nadir
                        ELSE ""
                    END AS {1}Nadir

                    , CASE
                        WHEN nadir.{1}Nadir = a.LabResult AND nadir.{1}NadirDate = a.LabDate THEN nadir.{1}NadirDate
                        ELSE ""
                    END AS {1}NadirDate

                    , a.labdate AS {1}date1
                    , a.labresult AS {1}result1
                    , b.labdate AS {1}date2
                    , b.labresult AS {1}result2
                    , c.labdate AS {1}date3
                    , c.labresult AS {1}result3

                FROM recovery.nadir
                        LEFT JOIN
                            recovery.{0}_in_range a
                            ON nadir.PtMRN = a.PtMRN AND nadir.ArrivalDate = a.ArrivalDate
                        LEFT JOIN recovery.{0}_in_range b
                            ON nadir.PtMRN = b.PtMRN AND nadir.ArrivalDate = b.ArrivalDate AND b.seqid = a.seqid +1
                        LEFT JOIN recovery.{0}_in_range c
                            ON nadir.PtMRN = c.PtMRN AND nadir.ArrivalDate = c.ArrivalDate AND c.seqid = a.seqid +2;

            DROP TABLE IF EXISTS recovery.{0}_in_range;

        """.format(labtest
                   ,abbreviation
                   ,levelname1
                   ,levelname2
                   ,levelname3
                   ,levelvalue1
                   ,levelvalue2
                   ,levelvalue3)
        printtext(cnxdict['sql'])
        dosqlexecute(cnxdict)


def create_all_recovery(cnxdict, lablist=('platelet','neutrophil')):
    printtext('stack')

    headerstmt = """
        -- Put it all together
        DROP TABLE IF EXISTS recovery.AllCounts;
        CREATE TABLE recovery.AllCounts
            SELECT a.PtMRN
                , a.ArrivalDx
                , a.Protocol
                , a.ResponseDescription
                , a.ArrivalDate
                , a.TreatmentStartDate
                , a.ResponseDate
                , a.RelapseDate
                , a.DaysTreatmentToRelapse
                , a.NextArrivalDate
                , a.DaysTreatmenttoNextArrival
                , timestampdiff(DAY,a.TreatmentStartDate, b.PLTNadirDate) AS DaystoPLTNadir
                , b.PLTNadir
                , b.PLTNadirDate
                , timestampdiff(DAY,a.TreatmentStartDate, b.ANCNadirDate) AS DaystoANCNadir
                , b.ANCNadir
                , b.ANCNadirDate
        """
    fieldlist = ''
    mainjoin  = """
            FROM recovery.range a
                LEFT JOIN recovery.Nadir b
                    ON a.PtMRN = b.PtMRN AND a.ArrivalDate = b.ArrivalDate
    """
    joinstmt  = ''
    orderstmt = """
            ORDER BY a.PtMRN, a.ArrivalDate ;
    """
    dropstmt  = ''
    tblnum    = 0

    for labtest in lablist:
        if labtest == 'platelet':
            abbreviation = 'PLT'
            levellist = ('20','50','100')
        elif labtest == 'neutrophil':
            abbreviation = 'ANC'
            levellist = ('500', '1000')

        for level in levellist:
            tblnum = tblnum + 1
            cnxdict['sql'] = """
                -- First {1} Recovery at level {2}
                DROP TABLE IF EXISTS recovery.First{1}{2}Recovery ;
                CREATE TABLE recovery.First{1}{2}Recovery
                    SELECT a.* FROM recovery.{0}recovery a
                            LEFT JOIN ( SELECT
                                          PtMRN
                                        , ArrivalDate
                                        , {1}Result1
                                        , min({1}Date1) AS {1}Date1
                                    FROM recovery.{0}recovery
                                        WHERE {1}Recover{2} = 'yes'
                                        GROUP BY ptmrn, arrivaldate) b
                                ON a.PtMRN = b.PtMRN
                                    AND a.ArrivalDate = b.ArrivalDate
                                    AND a.{1}Date1    = b.{1}Date1
                                    AND a.{1}Result1  = b.{1}Result1
                        WHERE b.PtMRN IS NOT NULL AND a.{1}Recover{2} = 'yes'
                        ORDER BY PtMRN, ArrivalDate, {1}Date1;
            """.format(
                labtest,
                abbreviation,
                level )
            printtext(cnxdict['sql'])
            dosqlexecute(cnxdict)

            fieldlist = fieldlist + """
                , {2}.DaysFromTreatment as DaysTreatmentTo{0}{1}Recovery
                , {2}.{0}date1 as {0}{1}Date1
                , {2}.{0}result1 as {0}{1}Result1
                , {2}.{0}date2 as {0}{1}Date2
                , {2}.{0}result2 as {0}{1}Result2
                , {2}.{0}date3 as {0}{1}Date3
                , {2}.{0}result3 as {0}{1}Result3
            """.format(
                abbreviation,
                level,
                't' + str(tblnum))

            joinstmt = joinstmt + """
                    LEFT JOIN recovery.First{0}{1}Recovery {2}
                            ON a.PtMRN = {2}.PtMRN AND a.ArrivalDate = {2}.ArrivalDate
                """.format(
                    abbreviation,
                    level,
                    't' + str(tblnum))

            dropstmt = dropstmt + """
                DROP TABLE IF EXISTS recovery.First{0}{1}Recovery;
            """.format(
                abbreviation,
                level )

    cnxdict['sql'] = headerstmt + fieldlist + mainjoin + joinstmt + orderstmt
    printtext(cnxdict['sql'])
    dosqlexecute(cnxdict)
    cnxdict['sql'] = dropstmt
    printtext(cnxdict['sql'])
    dosqlexecute(cnxdict)


def MainRoutine( cnxdict ):
    printtext('stack')
    create_range(cnxdict)
    create_Nadir(cnxdict)
    create_recovery_range(cnxdict)
    create_all_recovery(cnxdict)
    return None


def OutputRoutine(cnxdict):
    writer = pd.ExcelWriter(cnxdict['out_filepath'], engine='xlsxwriter')
    df = pd.read_sql("""
        SELECT * FROM recovery.allcounts ;
    """, cnxdict['cnx'])
    df.to_excel(writer, sheet_name='All Count Recovery', index=False)
    df = pd.read_sql("""
        SELECT a.*, b.karyodate, karyo FROM recovery.allcounts a
            LEFT JOIN recovery.range b on a.ptmrn = b.ptmrn and a.arrivaldate = b.arrivaldate;
    """, cnxdict['cnx'])
    df.to_excel(writer, sheet_name='All Patient Detail', index=False)
    dowritersave(writer, cnxdict)


# MainRoutineResult = MainRoutine(cnxdict)
OutputRoutine(cnxdict)

"""
I used the following update statements to add nadir into the platelet recovery table
UPDATE recovery.plateletrecovery
    SET PLTNadir = '',
        PLTNadirDate = '' ;

UPDATE recovery.plateletrecovery a, recovery.nadir b
    SET a.IsNadirPLT = 'Yes',
        a.PLTNadir = b.PLTNadir,
        a.PLTNadirDate = b.PLTNadirDate
    WHERE a.PtMRN = b.PtMRN and a.ArrivalDate = b.ArrivalDate and a.PLTdate1 = b.PLTNadirDate ;

"""
get_labs(cnxdict)