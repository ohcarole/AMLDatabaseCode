from MySQLdbUtils import *
reload(sys)



"""
Loop through labs imported into the caisis database and find those most relevant for the amldatabase2 arrivals.
"""

sys.setdefaultencoding('utf8')
cnxdict = connect_to_mysql_db_prod('relapse')
writer = pd.ExcelWriter(cnxdict['out_filepath'], datetime_format='mm/dd/yyyy')

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

    drop table if exists temp.response ;
    CREATE table temp.response AS
        SELECT -999 as recnum, a.*
        FROM caisis.vdatasetstatus a
            join caisis.v_responsetypes b
                ON b.status LIKE a.Status
        ORDER BY a.ptmrn, a.statusdate ;

    set @idx := 0;
    UPDATE temp.response a SET recnum = @idx:=@idx + 1 ;

    # SELECT * from temp.response1 where ptmrn = 'U2531816';

    ALTER TABLE temp.response
        CHANGE COLUMN `recnum` `recnum` BIGINT(21) NOT NULL ,
        ADD PRIMARY KEY (`recnum`);

    drop table if exists caisis.response ;
    create table caisis.response
    select
        a.recnum
        , a.ptmrn
        , a.patientid

        , a.statusdisease as arrivaldx0
        , a.statusdate as responsedate0
        , a.status as responsedescription0

        , b.statusdisease as arrivaldx1
        , b.statusdate as responsedate1
        , b.status as responsedescription1

        , c.statusdisease as arrivaldx2
        , c.statusdate as responsedate2
        , c.status as responsedescription2

        , d.statusdisease as arrivaldx3
        , d.statusdate as responsedate3
        , d.status as responsedescription3

        , e.statusdisease as arrivaldx4
        , e.statusdate as responsedate4
        , e.status as responsedescription4

        , f.statusdisease as arrivaldx5
        , f.statusdate as responsedate5
        , f.status as responsedescription5

        , g.statusdisease as arrivaldx6
        , g.statusdate as responsedate6
        , g.status as responsedescription6

        , h.statusdisease as arrivaldx7
        , h.statusdate as responsedate7
        , h.status as responsedescription7

        from temp.response a
        left join temp.response b
            on a.ptmrn = b.ptmrn and a.recnum+1 = b.recnum
        left join temp.response c
            on a.ptmrn = c.ptmrn and a.recnum+2 = c.recnum
        left join temp.response d
            on a.ptmrn = d.ptmrn and a.recnum+3 = d.recnum
        left join temp.response e
            on a.ptmrn = e.ptmrn and a.recnum+4 = e.recnum
        left join temp.response f
            on a.ptmrn = f.ptmrn and a.recnum+5 = f.recnum
        left join temp.response g
            on a.ptmrn = g.ptmrn and a.recnum+6 = g.recnum
        left join temp.response h
            on a.ptmrn = h.ptmrn and a.recnum+7 = h.recnum
        order by ptmrn, responsedate0;

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
        , a.ptmrn
        , a.patientid
        , a.statusdisease as arrivaldx0
        , a.statusdate as relapsedate0
        , a.status as relapsedescription0
        , b.statusdisease as arrivaldx1
        , b.statusdate as relapsedate1
        , b.status as relapsedescription1
        , c.statusdisease as arrivaldx2
        , c.statusdate as relapsedate2
        , c.status as relapsedescription2
        , d.statusdisease as arrivaldx3
        , d.statusdate as relapsedate3
        , d.status as relapsedescription3
        , e.statusdisease as arrivaldx4
        , e.statusdate as relapsedate4
        , e.status as relapsedescription4
        , f.statusdisease as arrivaldx5
        , f.statusdate as relapsedate5
        , f.status as relapsedescription5
        , g.statusdisease as arrivaldx6
        , g.statusdate as relapsedate6
        , g.status as relapsedescription6
        , h.statusdisease as arrivaldx7
        , h.statusdate as relapsedate7
        , h.status as relapsedescription7
        from temp.relapse a
        left join temp.relapse b
            on a.ptmrn = b.ptmrn and a.recnum+1 = b.recnum
        left join temp.relapse c
            on a.ptmrn = c.ptmrn and a.recnum+2 = c.recnum
        left join temp.relapse d
            on a.ptmrn = d.ptmrn and a.recnum+3 = d.recnum
        left join temp.relapse e
            on a.ptmrn = e.ptmrn and a.recnum+4 = e.recnum
        left join temp.relapse f
            on a.ptmrn = f.ptmrn and a.recnum+5 = f.recnum
        left join temp.relapse g
            on a.ptmrn = g.ptmrn and a.recnum+6 = g.recnum
        left join temp.relapse h
            on a.ptmrn = h.ptmrn and a.recnum+7 = h.recnum
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
    # U2531816
    #-------------------------------------------------------------------------------------------------------
    select * from caisis.response;

    drop table if exists temp.tempdata ;
    create table temp.tempdata
    select a.*
        , b.recnum as treatmentrecnum, b.treatmentdate, b.protocol, b.cycles
        , b.nexttreatmentdate, b.nextprotocol, b.nextcycles
        , c.recnum as responserecnum
        , c.responsedate0, c.responsedescription0
        , c.responsedate1, c.responsedescription1
        , c.responsedate2, c.responsedescription2
        , c.responsedate3, c.responsedescription3
        , c.responsedate4, c.responsedescription4
        , c.responsedate5, c.responsedescription5
        , c.responsedate6, c.responsedescription6
        , c.responsedate7, c.responsedescription7
        , d.recnum as relapserecnum
        , d.relapsedate0, d.relapsedescription0
        , d.relapsedate1, d.relapsedescription1
        , d.relapsedate2, d.relapsedescription2
        , d.relapsedate3, d.relapsedescription3
        , d.relapsedate4, d.relapsedescription4
        , d.relapsedate5, d.relapsedescription5
        , d.relapsedate6, d.relapsedescription6
        , d.relapsedate7, d.relapsedescription7
        , e.recnum as deceasedrecnum, e.ptdeathdate, e.ptdeathtype, e.ptdeathcause
        from caisis.arrival a
        left join (select * from caisis.treatment where ptmrn is null) b on 1
        left join (select * from caisis.response  where ptmrn is null) c on 1
        left join (select * from caisis.relapse   where ptmrn is null) d on 1
        left join (select * from caisis.deceased  where ptmrn is null) e on 1;

    select * from temp.tempdata;

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
        , a.responsedate0 = b.responsedate0
        , a.responsedescription0 = b.responsedescription0
        , a.responsedate1 = b.responsedate1
        , a.responsedescription1 = b.responsedescription1
        , a.responsedate2 = b.responsedate2
        , a.responsedescription2 = b.responsedescription2
        , a.responsedate3 = b.responsedate3
        , a.responsedescription3 = b.responsedescription3
        , a.responsedate4 = b.responsedate4
        , a.responsedescription5 = b.responsedescription5
        , a.responsedate6 = b.responsedate6
        , a.responsedescription6 = b.responsedescription6
        , a.responsedate7 = b.responsedate7
        , a.responsedescription7 = b.responsedescription7
        where a.ptmrn = b.ptmrn and b.responsedate0 between a.treatmentdate and a.nextarrivaldate ;

    update temp.tempdata a, caisis.relapse b
            set a.relapserecnum = b.recnum
            , a.relapsedate0 = b.relapsedate0
            , a.relapsedescription0 = b.relapsedescription0
            , a.relapsedate1 = b.relapsedate1
            , a.relapsedescription1 = b.relapsedescription1
            , a.relapsedate2 = b.relapsedate2
            , a.relapsedescription2 = b.relapsedescription2
            , a.relapsedate3 = b.relapsedate3
            , a.relapsedescription3 = b.relapsedescription3
            , a.relapsedate4 = b.relapsedate4
            , a.relapsedescription4 = b.relapsedescription4
            , a.relapsedate5 = b.relapsedate5
            , a.relapsedescription5 = b.relapsedescription5
            , a.relapsedate6 = b.relapsedate6
            , a.relapsedescription6 = b.relapsedescription6
            , a.relapsedate7 = b.relapsedate7
            , a.relapsedescription7 = b.relapsedescription7
            where a.ptmrn = b.ptmrn
                and b.relapsedate0 between a.responsedate0 and a.nextarrivaldate ;


    update temp.tempdata a, caisis.deceased b
            set a.deceasedrecnum = b.recnum
            , a.ptdeathdate = b.ptdeathdate
            , a.ptdeathtype = b.ptdeathtype
            , a.ptdeathcause = b.ptdeathcause
            where a.ptmrn = b.ptmrn;


    drop table if exists temp.remissiontime;
    create table temp.remissiontime
        select recnum, ptmrn, arrivaldx, arrivaldate
            , treatmentdate, protocol, cycles
            , responserecnum
            , responsedate0, responsedescription0
            , relapsedate0, relapsedescription0
            , case
                when relapsedate0 is null and responsedescription1 like '%cr%'
                    then concat('Need relapse for ',DATE_FORMAT(responsedate0, '%m/%d/%Y'),' response 1.')
                when responsedescription0 like '%cr%'
                    then round(timestampdiff(day,responsedate0,relapsedate0)/30,1)
                else NULL
            end as  `Remission #1 Months`

            , responsedate1, responsedescription1
            , relapsedate1, relapsedescription1
            , case
                when relapsedate1 is null and responsedescription1 like '%cr%'
                    then concat('Need relapse for ',DATE_FORMAT(responsedate1, '%m/%d/%Y'),' response 2.')
                when responsedescription1 like '%cr%'
                    then round(timestampdiff(day,responsedate1,relapsedate1)/30,1)
                else NULL
            end as  `Remission #2 Months`

            , responsedate2, responsedescription2
            , relapsedate2, relapsedescription2
            , case
                when relapsedate2 is null and responsedescription2 like '%cr%'
                    then concat('Need relapse for ',DATE_FORMAT(responsedate2, '%m/%d/%Y'),' response 3.')
                when responsedescription2 like '%cr%'
                    then round(timestampdiff(day,responsedate2,relapsedate2)/30,1)
                else NULL
            end as  `Remission #3 Months`

            , responsedate3, responsedescription3
            , relapsedate3, relapsedescription3
            , case
                when relapsedate3 is null and responsedescription3 like '%cr%'
                    then concat('Need relapse for ',DATE_FORMAT(responsedate3, '%m/%d/%Y'),' response 4.')
                when responsedescription3 like '%cr%'
                    then round(timestampdiff(day,responsedate3,relapsedate3)/30,1)
                else NULL
            end as  `Remission #4 Months`

            , responsedate4, responsedescription4
            , relapsedate4, relapsedescription4

            , responsedate5, responsedescription5
            , relapsedate5, relapsedescription5

            , responsedate6, responsedescription6
            , relapsedate6, relapsedescription6

            , responsedate7, responsedescription7
            , relapsedate7, relapsedescription7

            , case
                when relapsedate0 is null and responsedescription1 like '%cr%'
                    then concat('Need relapse for ',DATE_FORMAT(responsedate0, '%m/%d/%Y'),' response 1.')
                when responsedescription0 like '%cr%'
                    then round(timestampdiff(day,responsedate0,relapsedate0)/30,1)
                else NULL
            end as  `Remission #1 Months`
            , case
                when relapsedate1 is null and responsedescription1 like '%cr%'
                    then concat('Need relapse for ',DATE_FORMAT(responsedate1, '%m/%d/%Y'),' response 2.')
                when responsedescription1 like '%cr%'
                    then round(timestampdiff(day,responsedate1,relapsedate1)/30,1)
                else NULL
            end as  `Remission #2 Months`
            , case
                when relapsedate2 is null and responsedescription2 like '%cr%'
                    then concat('Need relapse for ',DATE_FORMAT(responsedate2, '%m/%d/%Y'),' response 3.')
                when responsedescription2 like '%cr%'
                    then round(timestampdiff(day,responsedate2,relapsedate2)/30,1)
                else NULL
            end as  `Remission #3 Months`
            , case
                when relapsedate3 is null and responsedescription3 like '%cr%'
                    then concat('Need relapse for ',DATE_FORMAT(responsedate3, '%m/%d/%Y'),' response 4.')
                when responsedescription3 like '%cr%'
                    then round(timestampdiff(day,responsedate3,relapsedate3)/30,1)
                else NULL
            end as  `Remission #4 Months`
            , timestampdiff(day,arrivaldate,ptdeathdate) as `Time to Death`
            , IF(relapsedate0 IS NULL
                ,if(responsedescription0 like '%cr%','Need Relapse Date','')
                , timestampdiff(day,responsedate0,relapsedate0)) as `CR1 days`
            , IF(relapsedate1 IS NULL
                ,if(responsedescription1 like '%cr%','Need Relapse Date','')
                , timestampdiff(day,responsedate1,relapsedate1)) as `CR2 days`
            , IF(relapsedate2 IS NULL
                ,if(responsedescription2 like '%cr%','Need Relapse Date','')
                , timestampdiff(day,responsedate2,relapsedate2)) as `CR3 days`
            , IF(relapsedate3 IS NULL
                ,if(responsedescription3 like '%cr%','Need Relapse Date','')
                , timestampdiff(day,responsedate3,relapsedate3)) as `CR4 days`
            , IF(relapsedate0 IS NULL, 0, timestampdiff(day,responsedate0,relapsedate0)) +
                  IF(relapsedate1 IS NULL, 0, timestampdiff(day,responsedate1,relapsedate1)) +
                  IF(relapsedate2 IS NULL, 0, timestampdiff(day,responsedate2,relapsedate2)) +
                  IF(relapsedate3 IS NULL, 0, timestampdiff(day,responsedate3,relapsedate3)) AS `Days in Remission`


            , (timestampdiff(day,arrivaldate,ptdeathdate) -         case
                    when relapsedate0 is null then null
                    when timestampdiff(day,responsedate0,relapsedate0) > 0
                    then timestampdiff(day,responsedate0,relapsedate0)
                    else 0
                end
                + case
                    when timestampdiff(day,responsedate1,relapsedate1) > 0
                    then timestampdiff(day,responsedate1,relapsedate1)
                    else 0
                end
                + case
                    when timestampdiff(day,responsedate2,relapsedate2) > 0
                    then timestampdiff(day,responsedate2,relapsedate2)
                    else 0
                end
                + case
                    when timestampdiff(day,responsedate3,relapsedate3) > 0
                    then timestampdiff(day,responsedate3,relapsedate3)
                    else 0
                end
            ) as `Time out of remission`
            , round((
                case
                    when relapsedate0 is null then null
                    when timestampdiff(day,responsedate0,relapsedate0) > 0
                    then timestampdiff(day,responsedate0,relapsedate0)
                    else 0
                end
                + case
                    when timestampdiff(day,responsedate1,relapsedate1) > 0
                    then timestampdiff(day,responsedate1,relapsedate1)
                    else 0
                end
                + case
                    when timestampdiff(day,responsedate2,relapsedate2) > 0
                    then timestampdiff(day,responsedate2,relapsedate2)
                    else 0
                end
                + case
                    when timestampdiff(day,responsedate3,relapsedate3) > 0
                    then timestampdiff(day,responsedate3,relapsedate3)
                    else 0
                end
                )/30,1) as `Months in Remission`
            , round((
                timestampdiff(day,arrivaldate,ptdeathdate) -
                case
                    when relapsedate0 is null then null
                    when timestampdiff(day,responsedate0,relapsedate0) > 0
                    then timestampdiff(day,responsedate0,relapsedate0)
                    else 0
                end
                + case
                    when timestampdiff(day,responsedate1,relapsedate1) > 0
                    then timestampdiff(day,responsedate1,relapsedate1)
                    else 0
                end
                + case
                    when timestampdiff(day,responsedate2,relapsedate2) > 0
                    then timestampdiff(day,responsedate2,relapsedate2)
                    else 0
                end
                + case
                    when timestampdiff(day,responsedate3,relapsedate3) > 0
                    then timestampdiff(day,responsedate3,relapsedate3)
                    else 0
                end
                )/30,1) as `Months out of Remission`
                , ptdeathdate, ptdeathtype, ptdeathcause
        --     , nextarrivaldx, nextarrivaldate, nextstatus
        --     , nexttreatmentdate, nextprotocol, nextcycles
        --     , nextresponsedate, nextresponsedescription
        --     , nextrelapsedate, nextrelapsedescription
            from temp.tempdata
            where year(arrivaldate) > 2007
            and responsedescription0 = 'CR'
            and ptdeathdate is not null
            order by recnum;

"""

df = pd.read_sql("""
select recnum, ptmrn, arrivaldx, arrivaldate
    , treatmentdate, protocol, cycles
    , responserecnum
    , responsedate0, responsedescription0
    , relapsedate0, relapsedescription0
    , case
        when relapsedate0 is null and responsedescription1 like '%cr%'
            then concat('Need relapse for ',DATE_FORMAT(responsedate0, '%m/%d/%Y'),' response 1.')
        when responsedescription0 like '%cr%'
            then round(timestampdiff(day,responsedate0,relapsedate0)/30,1)
        else NULL
    end as  `Remission #1 Months`

    , responsedate1, responsedescription1
    , relapsedate1, relapsedescription1
    , case
        when relapsedate1 is null and responsedescription1 like '%cr%'
            then concat('Need relapse for ',DATE_FORMAT(responsedate1, '%m/%d/%Y'),' response 2.')
        when responsedescription1 like '%cr%'
            then round(timestampdiff(day,responsedate1,relapsedate1)/30,1)
        else NULL
    end as  `Remission #2 Months`

    , responsedate2, responsedescription2
    , relapsedate2, relapsedescription2
    , case
        when relapsedate2 is null and responsedescription2 like '%cr%'
            then concat('Need relapse for ',DATE_FORMAT(responsedate2, '%m/%d/%Y'),' response 3.')
        when responsedescription2 like '%cr%'
            then round(timestampdiff(day,responsedate2,relapsedate2)/30,1)
        else NULL
    end as  `Remission #3 Months`

    , responsedate3, responsedescription3
    , relapsedate3, relapsedescription3
    , case
        when relapsedate3 is null and responsedescription3 like '%cr%'
            then concat('Need relapse for ',DATE_FORMAT(responsedate3, '%m/%d/%Y'),' response 4.')
        when responsedescription3 like '%cr%'
            then round(timestampdiff(day,responsedate3,relapsedate3)/30,1)
        else NULL
    end as  `Remission #4 Months`

    , responsedate4, responsedescription4
    , relapsedate4, relapsedescription4

    , responsedate5, responsedescription5
    , relapsedate5, relapsedescription5

    , responsedate6, responsedescription6
    , relapsedate6, relapsedescription6

    , responsedate7, responsedescription7
    , relapsedate7, relapsedescription7

    , case
        when relapsedate0 is null and responsedescription1 like '%cr%'
            then concat('Need relapse for ',DATE_FORMAT(responsedate0, '%m/%d/%Y'),' response 1.')
        when responsedescription0 like '%cr%'
            then round(timestampdiff(day,responsedate0,relapsedate0)/30,1)
        else NULL
    end as  `Remission #1 Months`
    , case
        when relapsedate1 is null and responsedescription1 like '%cr%'
            then concat('Need relapse for ',DATE_FORMAT(responsedate1, '%m/%d/%Y'),' response 2.')
        when responsedescription1 like '%cr%'
            then round(timestampdiff(day,responsedate1,relapsedate1)/30,1)
        else NULL
    end as  `Remission #2 Months`
    , case
        when relapsedate2 is null and responsedescription2 like '%cr%'
            then concat('Need relapse for ',DATE_FORMAT(responsedate2, '%m/%d/%Y'),' response 3.')
        when responsedescription2 like '%cr%'
            then round(timestampdiff(day,responsedate2,relapsedate2)/30,1)
        else NULL
    end as  `Remission #3 Months`
    , case
        when relapsedate3 is null and responsedescription3 like '%cr%'
            then concat('Need relapse for ',DATE_FORMAT(responsedate3, '%m/%d/%Y'),' response 4.')
        when responsedescription3 like '%cr%'
            then round(timestampdiff(day,responsedate3,relapsedate3)/30,1)
        else NULL
    end as  `Remission #4 Months`
    , round((
        case
            when relapsedate0 is null then null
            when timestampdiff(day,responsedate0,relapsedate0) > 0
            then timestampdiff(day,responsedate0,relapsedate0)
            else 0
        end
        + case
            when timestampdiff(day,responsedate1,relapsedate1) > 0
            then timestampdiff(day,responsedate1,relapsedate1)
            else 0
        end
        + case
            when timestampdiff(day,responsedate2,relapsedate2) > 0
            then timestampdiff(day,responsedate2,relapsedate2)
            else 0
        end
        + case
            when timestampdiff(day,responsedate3,relapsedate3) > 0
            then timestampdiff(day,responsedate3,relapsedate3)
            else 0
        end
        )/30,1) as `Time in Remission`

    , ptdeathdate, ptdeathtype, ptdeathcause
--     , nextarrivaldx, nextarrivaldate, nextstatus
--     , nexttreatmentdate, nextprotocol, nextcycles
--     , nextresponsedate, nextresponsedescription
--     , nextrelapsedate, nextrelapsedescription
    from temp.tempdata
    where year(arrivaldate) > 2007
    and responsedescription0 = 'CR'
    and ptdeathdate is not null
    order by recnum;






""", cnxdict['cnx'])
# df.to_excel(writer, sheet_name='Deaths for CR patients', index=False)

df = pd.read_sql("""
select ptmrn
    , intensity

    , responsedescription0 as `CR1 Type`
    , responsedate0 as `CR1 Date`
    , if(responsedescription0 like '%cr%',relapsedate0,null) as Rel1
    , `CR1 Months`

    , if(responsedescription1 like '%cr%',responsedescription1,null) as `CR2 Type`
    , if(responsedescription1 like '%cr%',responsedate1,null) as `CR2 Date`
    , if(responsedescription1 like '%cr%',relapsedate1,null) as Rel2
    , `CR2 Months`

    , if(responsedescription2 like '%cr%',responsedescription2,null) as `CR3 Type`
    , if(responsedescription2 like '%cr%',responsedate2,null) as `CR3 Date`
    , if(responsedescription2 like '%cr%',relapsedate2,null) as Rel3
    , `CR3 Months`

    , if(responsedescription3 like '%cr%',responsedescription3,null) as `CR4 Type`
    , if(responsedescription3 like '%cr%',responsedate3,null) as `CR4 Date`
    , if(responsedescription3 like '%cr%',relapsedate3,null) as Rel4
    , `CR4 Months`

    , `Time to Death`
    , `CR1 days`
    , `CR2 days`
    , `CR3 days`
    , `CR4 days`
    , `Days in Remission`
    , `Days out of Remission`
    , `Months in Remission`
    , `Months out of Remission`
    , `ptdeathdate`
 from temp.remissiontime;
""", cnxdict['cnx'])
df.to_excel(writer, sheet_name='Deaths for CR patients', index=False)
