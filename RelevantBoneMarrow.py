from MySQLdbUtils import *
reload(sys)



"""
Loop through labs imported into the caisis database and find those most relevant for the amldatabase2 arrivals.
"""

sys.setdefaultencoding('utf8')
cnxdict = connect_to_mysql_db_prod('marrow')


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
        DROP TABLE IF EXISTS relevantmarrow.cp ;
        CREATE TABLE relevantmarrow.cp
            SELECT PatientId, PtMRN  FROM caisis.vdatasetpatients;

        DROP TABLE IF EXISTS relevantmarrow.range ;
        CREATE TABLE relevantmarrow.range
            SELECT cp.PtMRN
                , cp.PatientId
                , ArrivalDx
                , Protocol
                , ResponseDescription
                , AMLDxDate
                , ArrivalDate
                , TreatmentStartDate
                , ResponseDate
                , NextArrivalDate
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
                FROM amldatabase2.`pattreatment with prev and next arrival` pt
                LEFT JOIN relevantmarrow.cp on cp.PtMRN = pt.UWID
                GROUP BY UWID, ArrivalDate
            UNION
            SELECT cp.PtMRN
                , cp.PatientId
                , ArrivalDx
                , Protocol
                , ResponseDescription
                , AMLDxDate
                , ArrivalDate
                , TreatmentStartDate
                , ResponseDate
                , NextArrivalDate
                , 'RESPONSE' AS Type
                , CASE
                    WHEN TreatmentStartDate IS NULL THEN NULL
                    ELSE TreatmentStartDate
                END AS StartDateRange
                , ResponseDate AS TargetDate
                , CASE
                    WHEN ResponseDate IS NULL THEN NULL
                    ELSE ResponseDate
                END AS EndDateRange
                FROM amldatabase2.`pattreatment with prev and next arrival` pt
                LEFT JOIN relevantmarrow.cp on cp.PtMRN = pt.UWID
                GROUP BY UWID, ArrivalDate
        ORDER BY PtMRN, ArrivalDate, TYPE;

        ALTER TABLE `relevantmarrow`.`range`
            ADD INDEX `PtMRN`        (`PtMRN`(10) ASC),
            ADD INDEX `ArrivalDate` (`ArrivalDate` ASC);    """
    print (cnxdict['sql'])
    dosqlexecute(cnxdict)

    cmd = 'SELECT * FROM temp.t1;'
    df = dosqlread(cmd,cnxdict['cnx'])
    df.to_excel(writer, sheet_name='Range Table', index=False)
    return


def create_marrow_table(cnxdict,marrowtest,writer):
    cnxdict['sql'] = """
        DROP TABLE IF EXISTS relevantmarrow.t3 ;
        CREATE TABLE relevantmarrow.t3
            SELECT
                t1.PtMRN
                , t1.PatientId
                , ArrivalDx
                , Protocol
                , ResponseDescription
                , AMLDxDate
                , ArrivalDate
                , TreatmentStartDate
                , ResponseDate
                , NextArrivalDate
                , Type
                , StartDateRange
                , TargetDate
                , EndDateRange
                , abs(timestampdiff(DAY,t1.TargetDate,t2.DateObtained)) AS DaysFromTarget
                , PathologyId
                , DateObtained
                , PathTest
                , PathResult
                FROM relevantmarrow.range t1
                    LEFT JOIN caisis.vdatasetpathtest as t2 on t1.PtMRN = t2.PtMRN
                WHERE lcase(PathTest) = '{0}'
                AND   (PathResult RLIKE '^[0-9]+'
                       OR LEFT(PathResult,1) IN ('<','>')
                       OR PathResult LIKE '%neg%'
                       OR PathResult LIKE '%pos%'
                       )
                ORDER BY t1.PtMRN, t1.ArrivalDate;


        DROP TABLE IF EXISTS relevantmarrow.`{0}` ;
        CREATE TABLE relevantmarrow.`{0}`
            SELECT PtMRN, PatientId
                    , ArrivalDate
                    , TargetDate
                    , min(DaysFromTarget) as DaysFromTarget
                    , Type
                    , PathTest
                    , DateObtained
                    , PathResult
                FROM relevantmarrow.t3
                GROUP BY PtMRN, TargetDate, type ;
        ALTER TABLE `relevantmarrow`.`{0}`
            ADD INDEX `PatientId`    (`PatientId`    ASC),
            ADD INDEX `PtMRN`        (`PtMRN`(10)    ASC),
            ADD INDEX `DateObtained` (`DateObtained` ASC),
            ADD INDEX `ArrivalDate`  (`ArrivalDate`  ASC);
    """.format(marrowtest)
    dosqlexecute(cnxdict)
    return


def create_bonemarrow_summary(cnxdict,cmd,joincmd,writer):
    cnxdict['sql'] = """
        DROP TABLE IF EXISTS relevantmarrow.t4;
        CREATE TABLE relevantmarrow.t4
            SELECT t0.PtMRN
                , t0.PatientId
                , t0.ArrivalDx
                , t0.Protocol
                , t0.ResponseDescription
                , t0.ArrivalDate
                , t0.TreatmentStartDate
                , t0.ResponseDate
                {0}
            FROM relevantmarrow.range t0
                {1}
            GROUP BY t0.PtMRN, t0.ArrivalDate;
    """.format(cmd,joincmd)
    dosqlexecute(cnxdict)
    return


def MainRoutine( cnxdict ):
    book = load_workbook(cnxdict['out_filepath'])
    writer = pd.ExcelWriter(cnxdict['out_filepath'], engine='openpyxl')
    writer.book = book
    writer.sheets = dict((ws.title, ws) for ws in book.worksheets)

    # Only need to run this if we are looking for other ranges besides arrival, treatment, and response from amldatabase2
    # This will need some thinking to switch to using Caisis exclusively

    marrowtestlist = """
        BLASTS
        Blasts (FLOW POS/NEG)
        Blasts (FLOW)
        Blasts (Morph)
        Blasts (PB BM RPT)
        Blasts (PB FLOW)
        Blasts (PB MORPH)
        Blasts (PBFLOW POS/NEG)
        Blasts (PBFLOW)
        Blasts (PBMorph)
        """

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
        MsgResp = tkMessageBox.showinfo(title="Marrow Data"
                                        , message="Use existing marrow data?"
                                        , type="yesnocancel")
        window.wm_withdraw()
        if MsgResp <> 'yes':
            for marrowtest in marrowtestlist.lower().split('\n'):
                marrowtest = marrowtest.strip()
                if marrowtest <> '':
                    print('Creating temp table for {}'.format(marrowtest))
                    create_marrow_table(cnxdict,marrowtest,writer)

        cmd     = ''
        joincmd = ''
        tblnum=0
        timepointlist = ('arrival','response')
        for timepoint in timepointlist:
            for marrowtest in marrowtestlist.lower().split('\n'):
                marrowtest = marrowtest.strip()
                if marrowtest <> '':
                    tblnum = tblnum+1
                    cmd = cmd + """
                        , {0}.DateObtained   AS `{1}_{2}_date`
                        , {0}.PathTest   AS `{1}_{2}_test`
                        , {0}.PathResult AS `{1}_{2}_result`
                    """.format('tbl_' + str(tblnum),timepoint,marrowtest)

                    joincmd = joincmd + """LEFT JOIN relevantmarrow.`{3}` {0}
                            ON t0.PtMRN = {0}.PtMRN AND t0.ArrivalDate = {0}.ArrivalDate AND left({0}.type,{2}) = '{1}'
                    """.format('tbl_' + str(tblnum),timepoint.upper(), len(timepoint), marrowtest)
        create_bonemarrow_summary(cnxdict,cmd,joincmd,writer)

        cmd = 'SELECT * FROM relevantmarrow.t4;'
        df = dosqlread(cmd, cnxdict['cnx'])
        df.to_excel(writer, sheet_name='summary', index=False)

        outputfile = cnxdict['out_filedir'] + '\\' + cnxdict['out_filename'] + '.' + cnxdict['out_fileext']
        print(outputfile)

    dowritersave(writer, cnxdict)

    return None


# MainRoutineResult = MainRoutine(cnxdict)
writer = pd.ExcelWriter(cnxdict['out_filepath'], engine='xlsxwriter')
cmd = 'SELECT * FROM relevantmarrow.t4;'
df = dosqlread(cmd, cnxdict['cnx'])
df.to_excel(writer, sheet_name='summary', index=False)