from Utilities.MySQLdbUtils import *
reload(sys)



"""
Loop through labs imported into the caisis database and find those most relevant for the amldatabase2 arrivals.
"""

sys.setdefaultencoding('utf8')
cnxdict = connect_to_mysql_db_prod('relapse')


"""
select * from caisis.vdatasetstatus ;

# ---  ARRIVAL  -------------------------------------------------------------------------------------------
set @idx := 0;
DROP TABLE IF EXISTS temp.status ;
CREATE TABLE temp.status
    SELECT @idx:=@idx + 1 AS recnum, a.*
        FROM caisis.vdatasetstatus a
        WHERE status like '%arrival%'
        ORDER BY ptmrn , statusdate;
ALTER TABLE `temp`.`status`
    CHANGE COLUMN `recnum` `recnum` BIGINT(21) NOT NULL ,
    ADD PRIMARY KEY (`recnum`);

drop table if exists caisis.arrival ;
create table caisis.arrival
select
    a.recnum
    , b.recnum as nextrecnum
    , a.ptmrn
    , a.patientid
    , a.statusdisease as arrivaldx
    , a.statusdate as arrivaldate
    , a.status
    , b.statusdisease as nextarrivaldx
    , b.statusdate as nextarrivaldate
    , b.status as nextstatus
    from temp.status a
    left join temp.status b
        on a.ptmrn = b.ptmrn and a.recnum+1 = b.recnum
union
select
    a.recnum
    , b.recnum as nextrecnum
    , a.ptmrn
    , a.patientid
    , a.statusdisease as arrivaldx
    , a.statusdate as arrivaldate
    , a.status
    , b.statusdisease as nextarrivaldx
    , b.statusdate as nextarrivaldate
    , b.status as nextstatus
    from temp.status a
    left join temp.status b
        on a.ptmrn = b.ptmrn and a.recnum+1 = b.recnum
    where b.ptmrn is null
    order by 1;

select * from caisis.arrival;

# ---  TREATMENT -------------------------------------------------------------------------------------------
set @idx := 0;

DROP TABLE IF EXISTS temp.treatment ;
create table temp.treatment
    select @idx:=@idx+1 as recnum, ptmrn, medtxdate, medtxdisease, medtxagent, MedTxCycle
        from caisis.vdatasetmedicaltherapy
        where medtxdisease rlike 'A(M|P)L'
        order by ptmrn, medtxdate;

DROP TABLE IF EXISTS caisis.treatment ;
create table caisis.treatment
select
    a.recnum
    , a.ptmrn
    , a.medtxdisease as arrivaldx
    , a.medtxdate as treatmentdate
    , a.medtxagent as protocol
    , a.MedTxCycle as cycles
    , b.medtxdisease as nextarrivaldx
    , b.medtxdate as nexttreatmentdate
    , b.medtxagent as nextprotocol
    , b.MedTxCycle as nextcycles
    from temp.treatment a
    left join temp.treatment b
        on a.ptmrn = b.ptmrn and a.recnum+1 = b.recnum
    union
select
    a.recnum
    , a.ptmrn
    , a.medtxdisease as arrivaldx
    , a.medtxdate as treatmentdate
    , a.medtxagent as protocol
    , a.MedTxCycle as cycles
    , b.medtxdisease as nextarrivaldx
    , b.medtxdate as nexttreatmentdate
    , b.medtxagent as nextprotocol
    , b.MedTxCycle as nextcycles
    from temp.treatment a
    left join temp.treatment b
        on a.ptmrn = b.ptmrn and a.recnum+1 = b.recnum
    where b.ptmrn is null
    order by recnum;

select * from caisis.treatment;

# ---  RESPONSE  -------------------------------------------------------------------------------------------
set @idx := 0;

drop table if exists temp.response ;
CREATE table temp.response AS
    SELECT @idx:=@idx + 1 AS recnum, a.*
    FROM caisis.v_responsetypes b
        LEFT JOIN caisis.vdatasetstatus a
            ON b.status LIKE a.Status
            AND a.Status IS NOT NULL ;
select * from temp.response;

ALTER TABLE temp.response
    CHANGE COLUMN `recnum` `recnum` BIGINT(21) NOT NULL ,
    ADD PRIMARY KEY (`recnum`);

drop table if exists caisis.response ;
create table caisis.response
select
    a.recnum
    , b.recnum as nextrecnum
    , a.ptmrn
    , a.patientid
    , a.statusdisease as arrivaldx
    , a.statusdate as responsedate
    , a.status as responsedescription
    , b.statusdisease as nextarrivaldx
    , b.statusdate as nextresponsedate
    , b.status as nextresponsedescription
    from temp.response a
    left join temp.response b
        on a.ptmrn = b.ptmrn and a.recnum+1 = b.recnum
union
select
    a.recnum
    , b.recnum as nextrecnum
    , a.ptmrn
    , a.patientid
    , a.statusdisease as arrivaldx
    , a.statusdate as responsedate
    , a.status as responsedescription
    , b.statusdisease as nextarrivaldx
    , b.statusdate as nextresponsedate
    , b.status as nextresponsedescription
    from temp.response a
    left join temp.response b
        on a.ptmrn = b.ptmrn and a.recnum+1 = b.recnum
    where b.ptmrn is null
    order by 1;

select * from caisis.response;


# ---  RELAPSE  -------------------------------------------------------------------------------------------
set @idx := 0;
DROP TABLE IF EXISTS temp.relapse ;
CREATE TABLE temp.relapse
    SELECT @idx:=@idx + 1 AS recnum, a.*
        FROM caisis.vdatasetstatus a
            WHERE
                status LIKE '%rel%'
                and (upper(statusdisease) rlike 'A(M|P)L' or upper(statusdisease) rlike 'MDS')
                ORDER BY ptmrn , statusdate ;

ALTER TABLE `temp`.`relapse`
    CHANGE COLUMN `recnum` `recnum` BIGINT(21) NOT NULL ,
    ADD PRIMARY KEY (`recnum`);

drop table if exists caisis.relapse ;
create table caisis.relapse
select
    a.recnum
    , b.recnum as nextrecnum
    , a.ptmrn
    , a.patientid
    , a.statusdisease as arrivaldx
    , a.statusdate as relapsedate
    , a.status as relapsedescription
    , b.statusdisease as nextarrivaldx
    , b.statusdate as nextrelapsedate
    , b.status as nextrelapsedescription
    from temp.relapse a
    left join temp.relapse b
        on a.ptmrn = b.ptmrn and a.recnum+1 = b.recnum
union
select
    a.recnum
    , b.recnum as nextrecnum
    , a.ptmrn
    , a.patientid
    , a.statusdisease as arrivaldx
    , a.statusdate as relapsedate
    , a.status as relapsedescription
    , b.statusdisease as nextarrivaldx
    , b.statusdate as nextrelapsedate
    , b.status as nextrelapsedescription
    from temp.relapse a
    left join temp.relapse b
        on a.ptmrn = b.ptmrn and a.recnum+1 = b.recnum
    where b.ptmrn is null
    order by 1;

select * from caisis.relapse;

# ---  DECEASED -------------------------------------------------------------------------------------------
set @idx := 0;

DROP TABLE IF EXISTS temp.deceased ;
create table temp.deceased
    select @idx:=@idx+1 as recnum, a.*
        from caisis.vdatasetpatients a
        where ptdeathdate is not null
        order by ptmrn;

DROP TABLE IF EXISTS caisis.deceased ;
create table caisis.deceased
select
    a.recnum
    , a.ptmrn
    , a.ptdeathdate
    , a.ptdeathtype
    , a.ptdeathcause
    from temp.deceased a
    left join temp.deceased b
        on a.ptmrn = b.ptmrn and a.recnum+1 = b.recnum
    union
select
    a.recnum
    , a.ptmrn
    , a.ptdeathdate
    , a.ptdeathtype
    , a.ptdeathcause
    from temp.deceased a
    left join temp.deceased b
        on a.ptmrn = b.ptmrn and a.recnum+1 = b.recnum
    where b.ptmrn is null
    order by recnum;

select * from caisis.deceased;

#-------------------------------------------------------------------------------------------------------
drop table if exists temp.tempdata ;
create table temp.tempdata
select a.*
    , b.recnum as treatmentrecnum, b.treatmentdate, b.protocol, b.cycles, b.nexttreatmentdate, b.nextprotocol, b.nextcycles
    , c.recnum as responserecnum, c.responsedate, c.responsedescription, c.nextresponsedate, c.nextresponsedescription
    , d.recnum as relapserecnum, d.relapsedate, d.relapsedescription, d.nextrelapsedate, d.nextrelapsedescription
    , e.recnum as deceasedrecnum, e.ptdeathdate, e.ptdeathtype, e.ptdeathcause
    from caisis.arrival a
    left join (select * from caisis.treatment where ptmrn is null) b on 1
    left join (select * from caisis.response where ptmrn is null) c on 1
    left join (select * from caisis.relapse where ptmrn is null) d on 1
    left join (select * from caisis.deceased where ptmrn is null) e on 1;

UPDATE temp.tempdata a, caisis.treatment b
    set a.treatmentdate = b.treatmentdate
    , a.treatmentrecnum = b.recnum
    , a.protocol = b.protocol
    , a.cycles = b.cycles
    , a.nexttreatmentdate = b.nexttreatmentdate
    , a.nextprotocol = b.nextprotocol
    , a.nextcycles = b.nextcycles
    where a.ptmrn = b.ptmrn and b.treatmentdate between a.arrivaldate and a.nextarrivaldate ;


update temp.tempdata a, caisis.response b
    set a.responserecnum = b.recnum
    , a.responsedate = b.responsedate
    , a.responsedescription = b.responsedescription
    , a.nextresponsedate = b.nextresponsedate
    , a.nextresponsedescription = b.nextresponsedescription
    where a.ptmrn = b.ptmrn and b.responsedate between a.treatmentdate and a.nextarrivaldate ;


update temp.tempdata a, caisis.relapse b
        set a.relapserecnum = b.recnum
        , a.relapsedate = b.relapsedate
        , a.relapsedescription = b.relapsedescription
        , a.nextrelapsedate = b.nextrelapsedate
        , a.nextrelapsedescription = b.nextrelapsedescription
        where a.ptmrn = b.ptmrn and b.relapsedate between a.responsedate and a.nextarrivaldate ;


update temp.tempdata a, caisis.deceased b
        set a.deceasedrecnum = b.recnum
        , a.ptdeathdate = b.ptdeathdate
        , a.ptdeathtype = b.ptdeathtype
        , a.ptdeathcause = b.ptdeathcause
        where a.ptmrn = b.ptmrn;


select * from temp.tempdata
    where responsedescription = 'CR';

select ptmrn, responsedescription, nextresponsedescription, ptdeathdate from temp.tempdata;
"""


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

    writer = pd.ExcelWriter(cnxdict['out_filepath'], datetime_format='mm/dd/yyyy')

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
            drop table if exists temp.temp3 ;
            create table temp.temp3
                select a.ptmrn, b.arrivaldate, min(a.medtxdate) as treatmentdate from caisis.vdatasetmedicaltherapy a
                    left join relevantrelapse.arrivalrelapse b
                        on a.ptmrn = b.ptmrn and a.medtxdate between b.arrivaldate and b.responsedate
                        group by a.ptmrn, b.arrivaldate;

        """
        dosqlexecute(cnxdict)

        df = pd.read_sql("""
            SELECT a.ptmrn
                , a.patientid
                , a.arrivaldate
                , a.arrivaldx
                , c.treatmentdate
                , a.responsedate
                , a.responsedescription
                , a.daysarrivetoresponse
                , a.relapsedate
                , timestampdiff(day,responsedate,relapsedate) as `days response to relapse`
                , a.nextarrivaldate
                , timestampdiff(day,relapsedate,nextarrivaldate) as `days relapse to next arrival`
                , d.treatmentdate as nexttreatmentdate
                , timestampdiff(day,relapsedate,d.treatmentdate) as `days relapse to next treatment`
                , b.ptdeathdate
                , timestampdiff(day,relapsedate,ptdeathdate) `days relapse to death`
                FROM relevantrelapse.arrivalrelapse a
                left join (
                    select ptmrn, PtDeathDate from caisis.vdatasetpatients
                        where ptdeathdate is not null) b
                on a.ptmrn = b.ptmrn
                left join temp.temp3 c on
                    a.ptmrn = c.ptmrn and a.arrivaldate = c.arrivaldate
                left join temp.temp3 d on
                    a.ptmrn = d.ptmrn and a.nextarrivaldate = d.arrivaldate
                where responsedescription = 'CR'
                and ptdeathdate is not null
                order by ptmrn, arrivaldate;
        """, cnxdict['cnx'])
        df.to_excel(writer, sheet_name='Deaths for CR patients', index=False)

    dowritersave(writer, cnxdict)

    return None


MainRoutineResult = MainRoutine(cnxdict)