from MySQLdbUtils import *
reload(sys)



"""
Loop through labs imported into the caisis database and find those most relevant for the amldatabase2 arrivals.
"""

sys.setdefaultencoding('utf8')
cnxdict = connect_to_mysql_db_prod('relapse')


def create_rangetable(cnxdict, writer):
    """
    creates a table of AML patient demographics merging what we know
    from the AML database and the Caisis demographic data
    :param cnxdict:
    :param tbl:
    :return:
    """
    cnxdict['sql'] = """
        drop view if exists caisis.v_arrival ;
        create view caisis.v_arrival AS
                select *
                    from caisis.vdatasetstatus
                    where status like '%arrival%';

        drop view if exists caisis.v_arrival_with_next ;
        create view caisis.v_arrival_with_next AS
        select a.*, min(b.statusdate) as nextarrivaldate from caisis.v_arrival a
            left join caisis.v_arrival b
                on a.ptmrn = b.ptmrn and a.statusdate < b.statusdate
                group by a.ptmrn, a.statusdate ;


        drop view if exists caisis.v_response ;
        create view caisis.v_response AS
                select b1.* from caisis.v_responsetypes a1
                    left join caisis.vdatasetstatus b1
                        on a1.status like b1.status
                            and b1.status is not null;

        drop view if exists caisis.v_relapse ;
        create view caisis.v_relapse AS
            SELECT ptmrn
            , PatientId
            , status
            , statusdate as relapsedate
            , statusdisease as arrivaldx
            FROM
                caisis.vdatasetstatus
            WHERE
                status LIKE '%rel%'
                and (upper(statusdisease) rlike 'A(M|P)L' or upper(statusdisease) rlike 'MDS')
            order by ptmrn, statusdate;
    """
    # dosqlexecute(cnxdict) # normally do not need to recreate views

    cnxdict['sql'] = """
        # find all arrivals associated with any subsequent response
        drop table if exists temp.temp1 ;
        create table temp.temp1
        Select a.ptmrn
            , a.patientid
            , a.status as statustype
            , a.statusdate as arrivaldate
            , a.statusdisease as arrivaldx
            , b.statusdate as responsedate
            , b.status as responsedescription
            , timestampdiff(day,a.statusdate, b.statusdate) as daysarrivetoresponse
            , a.nextarrivaldate
            from caisis.v_arrival_with_next a
            left join caisis.v_response b
            ON a.ptmrn = b.ptmrn
                and a.statusdisease = b.statusdisease
                and b.statusdate >  a.statusdate
                and b.statusdate <= a.nextarrivaldate
            where b.ptmrn is not null;

        # find the earliest response associated with all arrivals
        drop table if exists temp.temp2 ;
        create table temp.temp2
            SELECT PtMRN
                , PatientId
                , statustype
                , arrivaldate
                , arrivaldx
                , min(responsedate) as responsedate
                FROM temp.temp1 b
                GROUP BY PtMRN, PatientId, statustype, arrivaldate, arrivaldx;

        drop table if exists relevantArrivalResponse.ArrivalResponse ;
        create table relevantArrivalResponse.ArrivalResponse
            SELECT b.* from temp.temp2 a
                left join temp.temp1 b
                    on a.ptmrn = b.ptmrn
                        and a.arrivaldate = b.arrivaldate
                        and a.responsedate = b.responsedate ;

        DROP TABLE IF EXISTS relevantrelapse.range ;
        CREATE TABLE relevantrelapse.range
            SELECT a.*
                , 'RELAPSE' AS Type
                , CASE
                    WHEN a.ResponseDate IS NULL THEN NULL
                    ELSE date_add(a.ResponseDate, INTERVAL -5 DAY)
                END AS StartDateRange
                , a.ResponseDate AS TargetDate
                , CASE
                    WHEN a.NextArrivalDate IS NULL THEN NULL
                    ELSE a.NextArrivalDate
                END AS EndDateRange
                FROM relevantArrivalResponse.ArrivalResponse a
                LEFT JOIN caisis.v_relapse b on a.PtMRN = b.PtMRN
                GROUP BY a.ptmrn, a.arrivaldate
        ORDER BY a.ptmrn, a.arrivaldate, TYPE;

        ALTER TABLE `relevantrelapse`.`range`
            ADD INDEX `PtMRN`        (`PtMRN`(10) ASC),
            ADD INDEX `ArrivalDate` (`ArrivalDate` ASC);
    """
    dosqlexecute(cnxdict)
    return


def create_relapse_summary(cnxdict,cmd,joincmd,writer):
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
    writer = pd.ExcelWriter(cnxdict['out_filepath'], engine='openpyxl')
    try:
        book = load_workbook(cnxdict['out_filepath'])
        writer.book = book
        writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
    except:
        pass

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
        cmd = ''
        joincmd = ''
        tblnum = 0
        timepointlist = ('relapse')

        cnxdict['sql'] = """
            DROP TABLE IF EXISTS relevantrelapse.arrivalrelapse;
            CREATE TABLE relevantrelapse.arrivalrelapse
                SELECT a.*, b.relapsedate FROM relevantrelapse.`range` a
                    left join caisis.v_relapse b
                        on a.ptmrn = b.ptmrn
                        and b.relapsedate between a.StartDateRange and a.EndDateRange;
        """
        dosqlexecute(cnxdict)

    dowritersave(writer, cnxdict)

    return None


MainRoutineResult = MainRoutine(cnxdict)