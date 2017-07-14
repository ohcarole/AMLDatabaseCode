from xlsxwriter.workbook import Workbook
import os
from MessageBox import *
print(os.path.dirname(os.path.realpath(__file__)))
from MySQLdbUtils import *
import sys, re
reload(sys)



"""
Loop through labs imported into the caisis database and find those most relevant for the amldatabase2 arrivals.
"""

sys.setdefaultencoding('utf8')


def get_standard_lab_list():
    return (
            {  'lab':    'albumin'
             , 'outtbl': 'albumin'
             , 'colnm':  'albumin'},

            {  'lab':    'creatinine'
             , 'outtbl': 'creatinine'
             , 'colnm':  'creatinine'},

            {  'lab':    'hematocrit'
             , 'outtbl': 'hematocrit'
             , 'colnm':  'hematocrit'},

            {  'lab':    'hemoglobin'
             , 'outtbl': 'hemoglobin'
             , 'colnm':  'hemoglobin'},

            {  'lab':    'neutrophil'
             , 'outtbl': 'neutrophil'
             , 'colnm':  'neutrophil'},

            {  'lab':    'rbc'
             , 'outtbl': 'rbc'
             , 'colnm':  'rbc'},

            {  'lab':    'wbc'
             , 'outtbl': 'wbc'
             , 'colnm':  'wbc'},

            {  'lab':    'platelet'
             , 'outtbl': 'platelet'
             , 'colnm':  'platelet'},

            {  'lab':    'v_blast'
             , 'outtbl': 'circ_blast'
             , 'colnm':  'circulating_blast'},

            {  'lab':    'v_unclassified'
             , 'outtbl': 'circ_unclassified'
             , 'colnm':  'circulating_unclassified'}
         )


def create_rangetable():
    """
    creates a table of AML patient demographics merging what we know
    from the AML database and the Caisis demographic data
    :param cnxdict:
    :param tbl:
    :return:
    """
    cnxdict = connect_to_mysql_db_prod('utility')

    MsgResp = tkMessageBox.showinfo(title="Range Data"
                                    , message="Use existing range data?"
                                    , type="yesnocancel")
    window.wm_withdraw()
    if MsgResp == 'yes': # YES -- USE RANGE CREATED LAST TIME
        cnxdict['sql'] = """
            DROP TABLE IF EXISTS temp.t1;
            CREATE TABLE temp.t1
                SELECT * FROM temp.rangetable;
            ALTER TABLE `temp`.`t1`
                ADD INDEX `UWID`        (`UWID`(10) ASC),
                ADD INDEX `ArrivalDate` (`ArrivalDate` ASC);
        """
        dosqlexecute(cnxdict)
        return MsgResp
    elif MsgResp == 'cancel': # CANCEL
        return MsgResp
    else: # NO -- RECREATE RANGE
        pass  # let the program below run!

    cnxdict['sql'] = """
        -- Create range table
        DROP TABLE IF EXISTS temp.t1 ;
        CREATE TABLE temp.t1
            SELECT t0.PtMRN, t0.PatientId, UWID, ArrivalDx, Protocol, Regimen, Wildcard, ResponseDescription
                , AMLDxDate
                , ArrivalDate
                , TreatmentStartDate
                , ResponseDate
                -- Arrival
                , 'ARRIVAL' AS Type
                , CASE
                    WHEN ArrivalDate IS NULL THEN NULL
                    WHEN AMLDxDate > date_add(ArrivalDate, INTERVAL -35 DAY) THEN date_add(AMLDxDate, INTERVAL -5 DAY) -- Since the patient was diagnosed not that long ago, look at dx values as well as arrival values
                    ELSE date_add(ArrivalDate, INTERVAL -35 DAY)
                END AS StartDateRange
                , ArrivalDate AS TargetDate
                , CASE
                    WHEN TreatmentStartDate IS NULL THEN NULL
                    ELSE TreatmentStartDate
                END AS EndDateRange
                FROM amldatabase2.pattreatment
                LEFT JOIN temp.t0 on t0.PtMRN = pattreatment.UWID
                LEFT JOIN (SELECT WildCard
                    , CASE
                        WHEN multiregimen  <> '' THEN concat(multiregimen, druglist)
                        WHEN singleregimen <> '' THEN concat(singleregimen,druglist)
                        WHEN noninduction  <> '' THEN concat(noninduction, druglist)
                        WHEN mapto LIKE '%HCT%'  THEN 'HCT'
                        ELSE druglist
                    END AS Regimen
                    , OriginalProtocol
                    FROM temp.protocollist ) as pl on pattreatment.Protocol = pl.OriginalProtocol
                WHERE UWID IS NOT NULL
                GROUP BY UWID, ArrivalDate
            UNION
            SELECT t0.PtMRN, t0.PatientId, UWID, ArrivalDx, Protocol, Regimen, Wildcard, ResponseDescription
                , AMLDxDate
                , ArrivalDate
                , TreatmentStartDate
                , ResponseDate
                -- Arrival
                , 'TREATMENT' AS Type
                , ArrivalDate AS StartDateRange
                , CASE
                    WHEN TreatmentStartDate IS NULL THEN NULL
                    ELSE date_add(TreatmentStartDate, INTERVAL -1 DAY)
                END as TargetDate -- try to get labs from the day before treatment starts
                , CASE
                    WHEN TreatmentStartDate IS NULL THEN NULL
                    ELSE date_add(TreatmentStartDate, INTERVAL +2 DAY)
                END AS EndDateRange
                FROM amldatabase2.pattreatment
                LEFT JOIN temp.t0 on t0.PtMRN = pattreatment.UWID
                LEFT JOIN (SELECT WildCard
                    , CASE
                        WHEN multiregimen  <> '' THEN concat(multiregimen, druglist)
                        WHEN singleregimen <> '' THEN concat(singleregimen,druglist)
                        WHEN noninduction  <> '' THEN concat(noninduction, druglist)
                        WHEN mapto LIKE '%HCT%'  THEN 'HCT'
                        ELSE druglist
                    END AS Regimen
                    , OriginalProtocol
                    FROM temp.protocollist ) as pl on pattreatment.Protocol = pl.OriginalProtocol
                WHERE UWID IS NOT NULL
                GROUP BY UWID, ArrivalDate
            UNION
            SELECT t0.PtMRN, t0.PatientId, UWID, ArrivalDx, Protocol, Regimen, Wildcard, ResponseDescription
                , AMLDxDate
                , ArrivalDate
                , TreatmentStartDate
                , ResponseDate
                -- Arrival
                , 'RESPONSE' AS Type
                , CASE
                    WHEN ResponseDate IS NULL THEN NULL
                    ELSE date_add(ResponseDate, INTERVAL -14 DAY)
                END AS StartDateRange
                , ResponseDate AS TargetDate
                , CASE
                    WHEN ResponseDate IS NULL THEN NULL
                    ELSE date_add(ResponseDate, INTERVAL +14 DAY)
                END AS EndDateRange
                FROM amldatabase2.pattreatment
                LEFT JOIN temp.t0 on t0.PtMRN = pattreatment.UWID
                LEFT JOIN (SELECT WildCard
                    , CASE
                        WHEN multiregimen  <> '' THEN concat(multiregimen, druglist)
                        WHEN singleregimen <> '' THEN concat(singleregimen,druglist)
                        WHEN noninduction  <> '' THEN concat(noninduction, druglist)
                        WHEN mapto LIKE '%HCT%'  THEN 'HCT'
                        ELSE druglist
                    END AS Regimen
                    , OriginalProtocol
                    FROM temp.protocollist ) as pl on pattreatment.Protocol = pl.OriginalProtocol
                WHERE UWID IS NOT NULL
                GROUP BY UWID, ArrivalDate
            ORDER BY UWID, ArrivalDate, TYPE;
        ALTER TABLE `temp`.`t1`
            ADD INDEX `UWID`        (`UWID`(10) ASC),
            ADD INDEX `ArrivalDate` (`ArrivalDate` ASC);
        DROP TABLE IF EXISTS temp.rangetable;
        CREATE TABLE temp.rangetable
            SELECT * FROM temp.t1;
        ALTER TABLE `temp`.`rangetable`
            ADD INDEX `UWID`        (`UWID`(10) ASC),
            ADD INDEX `ArrivalDate` (`ArrivalDate` ASC);    """
    dosqlexecute(cnxdict)
    return None


def create_temp_lab_table(labtest):

    cnxdict = connect_to_mysql_db_prod('utility')
    cnxdict['sql'] = """
        DROP TABLE IF EXISTS temp.t3 ;
        CREATE TABLE temp.t3
            SELECT t2.PtMRN
                , t2.PatientId
                , t1.ArrivalDx
                , t1.Protocol
                , t1.ResponseDescription
                , t1.AMLDxDate
                , t1.ArrivalDate
                , t1.TreatmentStartDate
                , t1.ResponseDate
                , concat(t1.Type,'_','{0}') AS Type
                , t1.StartDateRange
                , t1.TargetDate
                , t1.EndDateRange
                , abs(timestampdiff(DAY,t1.TargetDate,t2.LabDate)) AS DaysFromTarget
                , t2.LabTestId
                , t2.LabDate
                , t2.LabTest
                , t2.LabResult
                , t2.LabUnits
            FROM temp.t1
                LEFT JOIN caisis.{1} as t2 on t1.UWID = t2.PtMRN
                WHERE t2.LabDate between t1.StartDateRange and t1.EndDateRange
                AND   LEFT(LTRIM(t2.LabResult),1) IN ('.','0','1','2','3','4','5','6','7','8','9','<','>','.')
                ORDER BY t1.UWID, t1.TargetDate, DaysFromTarget;

        DROP TABLE IF EXISTS temp.{0} ;
        CREATE TABLE temp.{0}
            SELECT UWID, PtMRN, PatientId
                    , ArrivalDate
                    , TargetDate
                    , min(DaysFromTarget) as DaysFromTarget
                    , Type
                    , LabDate
                    , LabResult
                    , LabUnits
                FROM temp.t3
                GROUP BY UWID, TargetDate, type ;
        ALTER TABLE `temp`.`{0}`
            ADD INDEX `PatientId`   (`PatientId`   ASC),
            ADD INDEX `PtMRN`       (`PtMRN`(10)   ASC),
            ADD INDEX `UWID`        (`UWID`(10)    ASC),
            ADD INDEX `LabDate`     (`LabDate`     ASC),
            ADD INDEX `ArrivalDate` (`ArrivalDate` ASC);
    """.format(labtest['outtbl'],labtest['lab'])
    dosqlexecute(cnxdict)
    return


def create_lab_summary_table(cmd,joincmd):
    cnxdict = connect_to_mysql_db_prod('utility')
    cnxdict['sql'] = """
        DROP TABLE IF EXISTS temp.labsummary;
        CREATE TABLE temp.labsummary
            SELECT t0.UWID, t0.PtMRN, t0.PatientId
                , t0.ArrivalDx
                , t0.Protocol
                , t0.Regimen
                , t0.WildCard
                , t0.ResponseDescription
                , t0.ArrivalDate
                , t0.TreatmentStartDate
                , t0.ResponseDate
                {0}
            FROM temp.rangetable t0
                {1}
            GROUP BY t0.UWID, t0.ArrivalDate;
    """.format(cmd,joincmd)
    dosqlexecute(cnxdict)
    return


def create_lab_summary(lablist, timepointlist):
    cmd = ''
    joincmd = ''
    tblnum = 0
    for timepoint in timepointlist:
        for labtest in lablist:
            tblnum = tblnum + 1
            cmd = cmd + """
                , {0}.LabDate   AS `{1}_{2}_date`
                , {0}.LabResult AS `{1}_{2}`
                , {0}.LabUnits  AS `{1}_{2}_units`
            """.format('tbl_' + str(tblnum), timepoint, labtest['colnm'])

            joincmd = joincmd + """LEFT JOIN temp.{3} {0}
                    ON t0.UWID = {0}.UWID AND t0.ArrivalDate = {0}.ArrivalDate AND left({0}.type,{2}) = '{1}'
            """.format('tbl_' + str(tblnum), timepoint.upper(), len(timepoint), labtest['outtbl'])
    create_lab_summary_table(cmd, joincmd)



def create_lab_tables(lablist='',timepointlist=''):
    cnxdict = connect_to_mysql_db_prod('utility')

    for labtest in lablist:
        print('Creating temp table for {0}'.format(labtest['lab']))
        create_temp_lab_table(labtest)
    create_lab_summary(lablist, timepointlist)
    return

def MainRoutine():

    if create_rangetable() <> 'cancel':
        # get section, default to mysql
        lablist = get_standard_lab_list()
        timepointlist = (
            'arrival'
            , 'treatment'
            , 'response' )
        create_lab_tables(lablist, timepointlist)
    return None


# MainRoutineResult = MainRoutine()
#
# create_rangetable()
# lablist = get_standard_lab_list()
# timepointlist = (
#     'arrival'
#     , 'treatment'
#     , 'response')
# create_lab_tables(lablist='',timepointlist='')
