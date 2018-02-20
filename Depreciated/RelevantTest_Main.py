import os

print(os.path.dirname(os.path.realpath(__file__)))
from Utilities.MySQLdbUtils import *
import sys

reload(sys)



"""
Loop through labs imported into the caisis database and find those most relevant for the amldatabase2 arrivals.
"""

sys.setdefaultencoding('utf8')
cnxdict = connect_to_mysql_db_prod('relevantlab')


def create_rangetable(cnxdict, writer):
    """
    creates a table of AML patient demographics merging what we know
    from the AML database and the Caisis demographic data
    :param cnxdict:
    :param tbl:
    :return:
    """
    cnxdict['sql'] = """
        -- Create range table
        DROP TABLE IF EXISTS temp.t0 ;
        CREATE TABLE temp.t0
            SELECT PatientId, PtMRN FROM caisis.vdatasetpatients;

        DROP TABLE IF EXISTS temp.t1 ;
        CREATE TABLE temp.t1
            SELECT t0.PtMRN, t0.PatientId, UWID, ArrivalDx, Protocol, ResponseDescription
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
                WHERE UWID IS NOT NULL
                GROUP BY UWID, ArrivalDate
            UNION
            SELECT t0.PtMRN, t0.PatientId, UWID, ArrivalDx, Protocol, ResponseDescription
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
                WHERE UWID IS NOT NULL
                GROUP BY UWID, ArrivalDate
            UNION
            SELECT t0.PtMRN, t0.PatientId, UWID, ArrivalDx, Protocol, ResponseDescription
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
            ADD INDEX `ArrivalDate` (`ArrivalDate` ASC);
    """
    print (cnxdict['sql'])
    dosqlexecute(cnxdict)

    cmd = 'SELECT * FROM temp.t1;'
    df = dosqlread(cmd,cnxdict['cnx'])
    df.to_excel(writer, sheet_name='Range Table', index=False)
    return


def create_temp_lab_table(cnxdict,labtest,writer):
    cnxdict['sql'] = """
        DROP TABLE IF EXISTS temp.t3 ;
        CREATE TABLE temp.t3
            SELECT t1.UWID
                , t2.PtMRN
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
                LEFT JOIN caisis.{0} as t2 on t1.UWID = t2.PtMRN
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
    """.format(labtest)
    dosqlexecute(cnxdict)

    cmd = 'SELECT * FROM temp.{0};'.format(labtest)
    df = dosqlread(cmd,cnxdict['cnx'])
    df.to_excel(writer, sheet_name=labtest, index=False)
    return


def create_arrival_lab_summary(cnxdict,cmd,joincmd,writer):
    cnxdict['sql'] = """
        DROP TABLE IF EXISTS temp.t4;
        CREATE TABLE temp.t4
            SELECT t0.UWID, t0.PtMRN, t0.PatientId
                , t0.ArrivalDx
                , t0.Protocol
                , t0.ResponseDescription
                , t0.ArrivalDate
                , t0.TreatmentStartDate
                , t0.ResponseDate
                {0}
            FROM temp.rangetable t0
                {1}
            GROUP BY t0.UWID, t0.ArrivalDate;
    """.format(cmd,joincmd)
    print(cnxdict['sql'])
    dosqlexecute(cnxdict)

    cmd = 'SELECT * FROM temp.t4;'
    df = dosqlread(cmd,cnxdict['cnx'])
    df.to_excel(writer, sheet_name='summary', index=False)
    return


def MainRoutine( cnxdict ):
    book = load_workbook(cnxdict['out_filepath'])
    writer = pd.ExcelWriter(cnxdict['out_filepath'], engine='openpyxl')
    writer.book = book
    writer.sheets = dict((ws.title, ws) for ws in book.worksheets)

    # Only need to run this if we are looking for other ranges besides arrival, treatment, and response from amldatabase2
    # This will need some thinking to switch to using Caisis exclusively

    keepongoing = 'yes'
    MsgResp = tkMessageBox.showinfo(title="Range Data"
                                    , message="Use existing range data?"
                                    , type="yesnocancel")
    window.wm_withdraw()
    if MsgResp == 'no':
        try:
             create_rangetable(cnxdict, writer)
        except:
             pass
    elif MsgResp == 'cancel':
        keepongoing = 'no'

    if keepongoing == 'yes':
        lablist = ('albumin','creatinine','hematocrit','hemoglobin','neutrophil','rbc','wbc','platelet','v_blast', 'v_unclassified')
        for labtest in lablist:
            print('Creating temp table for {}'.format(labtest))
            create_temp_lab_table(cnxdict,labtest,writer)
        lablist = ('albumin','creatinine','hematocrit','hemoglobin','neutrophil','rbc','wbc','platelet','v_blast', 'v_unclassified')
        cmd     = ''
        joincmd = ''
        tblnum=0
        timepointlist = ('arrival','treatment','response')
        for timepoint in timepointlist:
            for labtest in lablist:
                tblnum = tblnum+1
                cmd = cmd + """
                    , {0}.LabDate   AS `{1}_{2}_date`
                    , {0}.LabResult AS `{1}_{2}`
                    , {0}.LabUnits  AS `{1}_{2}_units`
                """.format('tbl_' + str(tblnum),timepoint,labtest)

                joincmd = joincmd + """LEFT JOIN temp.{3} {0}
                        ON t0.UWID = {0}.UWID AND t0.ArrivalDate = {0}.ArrivalDate AND left({0}.type,{2}) = '{1}'
                """.format('tbl_' + str(tblnum),timepoint.upper(), len(timepoint), labtest)

        create_arrival_lab_summary(cnxdict,cmd,joincmd,writer)

        outputfile = cnxdict['out_filedir'] + '\\' + cnxdict['out_filename'] + '.' + cnxdict['out_fileext']
        print(outputfile)

    dowritersave(writer, cnxdict)

    return None


MainRoutineResult = MainRoutine(cnxdict)