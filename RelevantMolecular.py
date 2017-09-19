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
cnxdict = connect_to_mysql_db_prod('relevantlab')


def create_rangetable(cnxdict):
    """
    creates a table of AML patient demographics merging what we know
    from the AML database and the Caisis demographic data
    :param cnxdict:
    :param tbl:
    :return:
    """
    cnxdict['sql'] = """
        -- Create range table
        DROP TABLE IF EXISTS molecular.cp ;
        CREATE TABLE molecular.cp
            SELECT PatientId, PtMRN FROM caisis.vdatasetpatients;

        DROP TABLE IF EXISTS molecular.range ;
        CREATE TABLE molecular.range
            SELECT cp.PtMRN, cp.PatientId, UWID, ArrivalDx, Protocol, ResponseDescription
                , AMLDxDate
                , ArrivalDate
                , TreatmentStartDate
                , ResponseDate
                -- Arrival
                , 'DIAGNOSIS' AS Type
                , CASE
                    WHEN AMLDxDate IS NULL THEN NULL
                    ELSE date_add(AMLDxDate, INTERVAL -35 DAY)
                END AS StartDateRange
                , ArrivalDate AS TargetDate
                , CASE
                    WHEN TreatmentStartDate IS NULL THEN NULL
                    ELSE TreatmentStartDate
                END AS EndDateRange
                FROM amldatabase2.pattreatment
                LEFT JOIN molecular.cp on cp.PtMRN = pattreatment.UWID
                WHERE UWID IS NOT NULL
                GROUP BY UWID, ArrivalDate
            UNION
            SELECT cp.PtMRN, cp.PatientId, UWID, ArrivalDx, Protocol, ResponseDescription
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
                LEFT JOIN molecular.cp on cp.PtMRN = pattreatment.UWID
                WHERE UWID IS NOT NULL
                GROUP BY UWID, ArrivalDate
            UNION
            SELECT cp.PtMRN, cp.PatientId, UWID, ArrivalDx, Protocol, ResponseDescription
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
                LEFT JOIN molecular.cp on cp.PtMRN = pattreatment.UWID
                WHERE UWID IS NOT NULL
                GROUP BY UWID, ArrivalDate
            UNION
            SELECT cp.PtMRN, cp.PatientId, UWID, ArrivalDx, Protocol, ResponseDescription
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
                LEFT JOIN molecular.cp on cp.PtMRN = pattreatment.UWID
                WHERE UWID IS NOT NULL
                GROUP BY UWID, ArrivalDate
            ORDER BY UWID, ArrivalDate, TYPE;

        ALTER TABLE `molecular`.`range`
            ADD INDEX `UWID`        (`UWID`(10) ASC),
            ADD INDEX `ArrivalDate` (`ArrivalDate` ASC);

        DROP TABLE IF EXISTS molecular.cp ;
    """
    dosqlexecute(cnxdict)
    return


def create_temp_mutation_table(cnxdict, lablist = ('v_npm1mutation', 'v_cepbamutation', 'v_flt3mutation' )):
    for labtest in lablist:
        print('Creating molecular table for caisis table or view {}'.format(labtest))
        viewshortname = labtest.replace('v_', '')
        cnxdict['sql'] = """
            DROP TABLE IF EXISTS molecular.t3 ;
            CREATE TABLE molecular.t3
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
                    , concat(t1.Type,'_','{1}') AS Type
                    , t1.StartDateRange
                    , t1.TargetDate
                    , t1.EndDateRange
                    , abs(timestampdiff(DAY,t1.TargetDate,t2.LabDate)) AS DaysFromTarget
                    , t2.LabTestId
                    , t2.LabDate
                    , t2.LabTest
                    , t2.LabResult
                    , t2.LabUnits
                    , t2.BaseLengthTest
                    , t2.BaseLength
                    , t2.RatioTest
                    , t2.Ratio
                FROM molecular.range t1
                    LEFT JOIN caisis.{0} as t2 on t1.UWID = t2.PtMRN
                    WHERE t2.LabDate between t1.StartDateRange and t1.EndDateRange
                        AND   UPPER(LabResult) NOT RLIKE 'ORDER'
                        AND   UPPER(LabResult) NOT RLIKE 'NOT NEEDED'
                        AND   UPPER(LabResult) NOT RLIKE 'WRONG'
                        AND   UPPER(LabResult) NOT RLIKE 'CORRECTION'
                        AND   UPPER(LabResult) NOT RLIKE 'SEE INTERPRETATION'
                        AND   UPPER(LabResult) NOT RLIKE 'SPECIMEN RECEIVED'
                    ORDER BY t1.UWID, t1.TargetDate, DaysFromTarget;

            DROP TABLE IF EXISTS molecular.{1} ;
            CREATE TABLE molecular.{1}
                SELECT UWID, PtMRN, PatientId
                        , ArrivalDate
                        , TargetDate
                        , min(DaysFromTarget) as DaysFromTarget
                        , Type
                        , LabDate
                        , LabResult
                        , LabUnits
                        , BaseLengthTest
                        , BaseLength
                        , RatioTest
                        , Ratio
                    FROM molecular.t3
                    GROUP BY UWID, TargetDate, type ;
            ALTER TABLE `molecular`.`{1}`
                ADD INDEX `PatientId`   (`PatientId`   ASC),
                ADD INDEX `PtMRN`       (`PtMRN`(10)   ASC),
                ADD INDEX `UWID`        (`UWID`(10)    ASC),
                ADD INDEX `LabDate`     (`LabDate`     ASC),
                ADD INDEX `ArrivalDate` (`ArrivalDate` ASC);

            DROP TABLE IF EXISTS molecular.t3 ;

        """.format(labtest,viewshortname)
        print (cnxdict['sql'])
        dosqlexecute(cnxdict)
    return
#
#
# lablist = ('v_npm1mutation', 'v_cepbamutation')
# for labtest in lablist:
#     print('Creating temp table for {}'.format(labtest))
#     create_temp_mutation_table(cnxdict, labtest)


def create_mutation_summary(cnxdict,lablist = ('v_npm1mutation', 'v_cepbamutation')):
    cmd = ''
    joincmd = ''
    tblnum = 0
    timepointlist = ('diagnosis','arrival', 'treatment', 'response')
    for timepoint in timepointlist:
        for labtest in lablist:
            tblnum = tblnum + 1
            viewshortname = labtest.replace('v_', '')
            cmd = cmd + """
                , {0}.LabDate   AS `{1}_{2}_date`
                , {0}.LabResult AS `{1}_{2}`
                , {0}.BaseLengthTest AS `{1}_{2}_BaseLengthTest`
                , {0}.BaseLength AS `{1}_{2}_BaseLength`
                , {0}.RatioTest AS `{1}_{2}_RatioTest`
                , {0}.Ratio AS `{1}_{2}_Ratio`""".format('tbl_' + str(tblnum), timepoint, labtest.replace('v_',''))
            joincmd = joincmd + """LEFT JOIN molecular.{3} {0}
                    ON t0.UWID = {0}.UWID AND t0.ArrivalDate = {0}.ArrivalDate AND left({0}.type,{2}) = '{1}'
            """.format('tbl_' + str(tblnum), timepoint.upper(), len(timepoint), viewshortname)

    cnxdict['sql'] = """
        DROP TABLE IF EXISTS molecular.arrivalmutations;
        CREATE TABLE molecular.arrivalmutations
            SELECT t0.UWID, t0.PtMRN, t0.PatientId
                , t0.ArrivalDx
                , t0.Protocol
                , t0.ResponseDescription
                , t0.ArrivalDate
                , t0.TreatmentStartDate
                , t0.ResponseDate
                {0}
            FROM molecular.range t0
                {1}
            GROUP BY t0.UWID, t0.ArrivalDate;

            DROP TABLE IF EXISTS molecular.cepbamutation;
            DROP TABLE IF EXISTS molecular.npm1mutation;
    """.format(cmd, joincmd)
    print(cnxdict['sql'])
    dosqlexecute(cnxdict)
    return




def get_writer(cnxdict):
    outfile = cnxdict['out_filepath']
    MsgResp = tkMessageBox.showinfo(title="Output Data"
        , message="""
        Add to existing workbook 'relevantlab.xlsx'?
        Note that if adding to the lab workbook,
        the program to produce the lab workbook, 'RelevantTest.py'
        must be run first.
        """
        , type="yesnocancel")
    writer = pd.ExcelWriter(cnxdict['out_filepath'], engine='xlsxwriter', datetime_format='mm/dd/yyyy')
    window.wm_withdraw()
    if MsgResp == 'yes':
        try:
            book = load_workbook(cnxdict['out_filepath'])
            writer = pd.ExcelWriter(cnxdict['out_filepath'], engine='openpyxl', datetime_format='mm/dd/yyyy')
            writer.book = book
            writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
            try:
                book.remove('Arrival Mutation Summary')
            except:
                pass
        except:
            outfile = 'h:\\temp\\export\\molecular'
            writer = pd.ExcelWriter(outfile, engine='xlsxwriter', datetime_format='mm/dd/yyyy')
    print (outfile)
    return writer




def MainRoutine(cnxdict):
    # Only need to run this if we are looking for other ranges besides arrival, treatment, and response from amldatabase2
    # This will need some thinking to switch to using Caisis exclusively

    keepongoing = 'yes'
    MsgResp = tkMessageBox.showinfo(title="Range Data"
                                    , message="Use existing range data?"
                                    , type="yesnocancel")
    window.wm_withdraw()
    if MsgResp == 'no':
        try:
            create_rangetable(cnxdict)
        except:
            pass
    elif MsgResp == 'cancel':
        keepongoing = 'no'

    if keepongoing == 'yes':
        lablist = ('v_npm1mutation','v_cepbamutation','v_flt3mutation')
        create_temp_mutation_table(cnxdict, lablist)
        create_mutation_summary(cnxdict,lablist)
        writer = get_writer(cnxdict)
        cmd = 'SELECT * FROM molecular.arrivalmutations;'
        df = dosqlread(cmd, cnxdict['cnx'])
        df.to_excel(writer, sheet_name='Arrival Mutation Summary', index=False)

    dowritersave(writer, cnxdict)

    return None


MainRoutineResult = MainRoutine(cnxdict)