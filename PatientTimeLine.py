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
cnxdict = connect_to_mysql_db_prod('utility')


def producetimeline(cnxdict):
    cnxdict['sql'] = """
        -- select status, count(*) from caisis.vdatasetstatus group by status ;
        --
        -- select status, count(*) from caisis.vdatasetstatus where status like '%cr%' group by status ;
        --
        -- select * from caisis.vdatasetstatus  where status like '%cr%' ; -- n = 1629
        --
        -- select * from caisis.vdatasetstatus  where status like '%cr%' and year(statusdate) = 2015; -- n= 159
        --
        -- select * from caisis.vdatasetstatus  where status = 'CR' ; -- n = 1152
        --
        -- select * from caisis.vdatasetstatus  where ptMRN = 'U1962960';
        --
        -- select * from caisis.vdatasetstatus  where status = 'CR' and year(statusdate) = 2015; -- n= 94 ;
        --
        drop table if exists temp.hct;
        create table temp.hct
        SELECT a.`index`,
            a.`PtMRN`,
            a.`PatientId`,
            a.`ProcedureId`,
            a.`ProcDateText`,
            a.`ProcDate`,
            CASE
                WHEN ProcDonMatch IS NULL
                    THEN CONCAT(ProcCellSource," from ",ProcDonRelation)
                WHEN ProcDonMatch = 'AUTO'
                    THEN  CONCAT(ProcCellSource," from ",ProcDonMatch)
                WHEN ProcDonMatch = 'UNRELATED'
                    THEN  CONCAT(ProcCellSource," from ",ProcDonMatch)
                ELSE CONCAT(ProcCellSource," from ",ProcDonMatch," ",ProcDonRelation)
            END AS ProcType,
            a.`ProcInstitution`,
            a.`ProcName`,
            a.`ProcNotes`,
            b.`ProcedureHCTId`,
            b.`ProcCellID`,
            b.`ProcCellsInfused`,
            b.`ProcCellSource`,
            b.`ProcDonMatch`,
            b.`ProcDonRelation`,
            b.`ProcDonSex`,
            b.`ProcHCTNotes`
            from (
                select * from caisis.vdatasetprocedures
            ) a
            left join (
                select * from caisis.vdatasethctproc
            ) b
            on a.procedureid = b.procedureid
            WHERE b.procedureid is not null;

        drop table if exists temp.cyto;
        create table temp.cyto as
            select * from caisis.allkaryo
                where pathresult > ''
                and (pathtest like '%cyto%' or pathresult rlike '^(\/\/|[0-9])');

        delete from temp.cyto
            where lower(pathresult) IN ( 'not done'
                , 'no result'
                , 'no growth'
                , 'unk'
                , 'not reported'
                , 'n/a'
                , 'insufficient'
                , 'insufficient sample'
                , 'insufficient growth'
                , 'cancelled'
                , 'inadequate for diagnosis'
                , 'no growth or insufficient growth' ) ;

        drop table if exists temp.fish;
        create table temp.fish as
            select * from caisis.allkaryo
                where pathresult > '' ;

        delete from temp.fish
            where Type like '%CGAT%'
            OR (pathtest like '%cyto%' AND lower(pathresult) NOT rlike '(nuc|ish){1})')
            OR (pathresult rlike '^(\/\/|[0-9])' AND lower(pathresult) NOT rlike '(nuc|ish)')
            OR pathresult = ''
            OR pathresult = '?'
            OR pathresult like '%insufficient growth%'
            OR lower(pathresult) IN ( 'not done'
                , 'no result'
                , 'no growth'
                , 'unk'
                , 'not reported'
                , 'n/a'
                , 'insufficient'
                , 'insufficient sample'
                , 'insufficient growth'
                , 'insufficient growth.'
                , 'cancelled'
                , 'inadequate for diagnosis'
                , 'no growth or insufficient growth'
                , 'inadequate for fish analysis'
                , '') ;

        drop table if exists temp.cgat;
        create table temp.cgat as
            select * from caisis.allkaryo
                where Type like '%CGAT%';

        delete from temp.cgat
            where type like '%FISH%'
            OR type like '%CYTO%'
            OR pathresult rlike '^(\/\/)?[0-9])'
            OR lower(pathresult) rlike '(nuc|ish)'
            OR pathresult = '?'
            OR pathresult like '%insufficient growth%'
            OR lower(pathresult) IN ( 'not done'
                , 'no result'
                , 'no growth'
                , 'unk'
                , 'not reported'
                , 'n/a'
                , 'insufficient'
                , 'insufficient sample'
                , 'insufficient growth'
                , 'insufficient growth.'
                , 'cancelled'
                , 'inadequate for diagnosis'
                , 'no growth or insufficient growth'
                , 'inadequate for fish analysis'
                , '') ;

        drop table if exists temp.output ;
        create table temp.output
        select PtMRN
                        , 'DIAGNOSIS             ' as eventcategory
                        , statusdate as eventdate
                        , statusdisease as event
                        , status as eventresult
                        from caisis.vdatasetstatus  where
                            statusdisease like '%AML%'
                            AND status like '%diag%'
        union
        select PtMRN
                        , 'ARRIVAL' as eventcategory
                        , statusdate as eventdate
                        , statusdisease as event
                        , status as eventresult
                        from caisis.vdatasetstatus  where
                            statusdisease like '%AML%'
                            AND status like '%arrival%'
        union
        select PtMRN
                        , 'TREATMENT'as eventcategory
                        , MedTxDate as eventdate
                        , concat(MedTxIntent," ",MedTxType,' with ',MedTxAgent) as event
                        , '' as eventresult
                        from caisis.vdatasetmedicaltherapy
        union
        select PtMRN
                        , 'RESPONSE' as eventcategory
                        , statusdate as eventdate
                        , concat(statusdisease,' Treatment Response') as event
                        , status as eventresult
                        from caisis.vdatasetstatus  where
                            statusdisease like '%AML%'
                            AND (status like '%CR%'
                            or status like '%death%'
                            or status = 'PR'
                            or status like '%resist%')
        union
        select PtMRN
                        , 'RELAPSE' as eventcategory
                        , statusdate as eventdate
                        , concat(statusdisease,' Relapse') as event
                        , '' as eventresult
                        from caisis.vdatasetstatus  where
                            statusdisease like '%AML%'
                            AND status LIKE '%rel%'
        union
        select PtMRN
                        , 'MORPH' as eventcategory
                        , dateobtained as eventdate
                        , pathtest as event
                        , pathresult as eventresult
                        from caisis.`vdatasetpathtest` where
                            pathtest like '%blast%'
                            and pathtest not like '%flow%'
        union
        select PtMRN
                        , 'FLOW' as eventcategory
                        , dateobtained as eventdate
                        , pathtest as event
                        , pathresult as eventresult
                        from caisis.`vdatasetpathtest` where pathtest like '%flow%'
        union
        select PtMRN
                        , 'CYTO' as eventcategory
                        , dateobtained as eventdate
                        , pathtest as event
                        , pathresult as eventresult
                        from temp.cyto
        union
        select PtMRN
                        , 'FISH' as eventcategory
                        , dateobtained as eventdate
                        , pathtest as event
                        , pathresult as eventresult
                        from temp.fish
        union
        select PtMRN
                        , 'HCT' as eventcategory
                        , ProcDate as eventdate
                        , ProcType as event
                        , concat(ProcCellsInfused, ' cells infused') as eventresult
                        from temp.hct
        union
        select PtMRN
                        , 'HOTSPOT' as eventcategory
                        , SpecimenReceivedDtTm as eventdate
                        , PositiveTestList as event
                        ,'Positive' as eventresult
                        from caisis.hotspot
                        where PositiveTestList > ''
        union
        select PtMRN
                        , 'HOTSPOT' as eventcategory
                        , SpecimenReceivedDtTm as eventdate
                        , NegativeTestList as event
                        ,'Negative' as eventresult
                        from caisis.hotspot
                        where NegativeTestList > ''
        union
        select PtMRN
                        , 'HOTSPOT' as eventcategory
                        , SpecimenReceivedDtTm as eventdate
                        , UncertainTestList as event
                        ,'Uncertain' as eventresult
                        from caisis.hotspot
                        where UncertainTestList > ''
        union
        select PtMRN
                        , 'CGAT' as eventcategory
                        , dateobtained as eventdate
                        , pathtest as event
                        , pathresult as eventresult
                        from temp.cgat
        union
        select PtMRN
                        , 'MOLECULAR' as eventcategory
                        , labdate as eventdate
                        , labtest as event
                        , labresult as eventresult
                        from caisis.molecular
        ORDER BY 1, eventdate ;

        --
        -- SELECT  a.PtMRN
        --         , Disease
        --         , Treatment
        --         , Response
        --         , TreatmentDate
        --         , ResponseDate
        --         , timestampdiff(DAY,TreatmentDate,ResponseDate) AS DaysPassed
        -- FROM    (SELECT
        --             PtMRN,
        --                 'RESPONSE' AS ResponseCategory,
        --                 StatusDate AS ResponseDate,
        --                 StatusDisease AS Disease,
        --                 Status AS Response
        --         FROM
        --             caisis.vdatasetstatus
        --         WHERE
        --             status = 'CR'
        --                 AND (statusdisease LIKE '%AML%'
        --                 OR statusdisease LIKE '%APL%'
        --                 OR statusdisease LIKE '%MDS%'
        --                 OR statusdisease LIKE '%RAEB%'
        --                 OR statusdisease LIKE '%CYTO%')) a
        -- LEFT JOIN (SELECT
        --             PtMRN,
        --                 MedicalTherapyId,
        --                 'PRIOR TREATMENT' AS TreatmentCategory,
        --                 MedTxDate AS TreatmentDate,
        --                 MedTxAgent AS Treatment
        --         FROM
        --             caisis.vdatasetmedicaltherapy ) b
        -- ON a.PtMRN = b.PtMRN
        -- WHERE  timestampdiff(DAY,TreatmentDate,ResponseDate) between 0 and 365
        -- -- AND YEAR(TreatmentDate) >= 2015
        -- ;

        drop table if exists temp.treatment ;
        create table temp.treatment
        SELECT  a.PtMRN
                , Disease
                , Treatment
                , Response
                , MAX(TreatmentDate) AS TreatmentDate
                , ResponseDate
                , timestampdiff(DAY,TreatmentDate,ResponseDate) AS DaysPassed
        FROM    (SELECT
                    PtMRN,
                        'RESPONSE' AS ResponseCategory,
                        StatusDate AS ResponseDate,
                        StatusDisease AS Disease,
                        Status AS Response
                FROM
                    caisis.vdatasetstatus
                WHERE
                    status = 'CR'
                        AND (statusdisease LIKE '%AML%'
                        OR statusdisease LIKE '%APL%'
                        OR statusdisease LIKE '%MDS%'
                        OR statusdisease LIKE '%RAEB%'
                        OR statusdisease LIKE '%CYTO%')) a
        LEFT JOIN (SELECT
                    PtMRN,
                        MedicalTherapyId,
                        'PRIOR TREATMENT' AS TreatmentCategory,
                        MedTxDate AS TreatmentDate,
                        MedTxAgent AS Treatment
                FROM
                    caisis.vdatasetmedicaltherapy ) b
        ON a.PtMRN = b.PtMRN
        WHERE  timestampdiff(DAY,TreatmentDate,ResponseDate) between 0 and 365
        -- AND YEAR(TreatmentDate) >= 2015
        GROUP BY a.PtMRN, ResponseDate;


        -- select * from output where ptmrn in (select ptmrn from treatment


        UPDATE temp.output a, temp.treatment b
            set a.eventcategory = CASE
                WHEN eventcategory = 'TREATMENT' AND a.eventdate = b.treatmentdate THEN 'CR TREATMENT'
            ELSE eventcategory
            END
            WHERE a.PtMRN = b.PtMRN ;

        UPDATE temp.output a, temp.hct b
            set a.eventcategory = CASE
                WHEN a.eventcategory = 'ARRIVAL' AND a.eventdate <= b.ProcDate THEN 'ARRIVAL (BEFORE HCT)'
            ELSE a.eventcategory
            END
            WHERE a.PtMRN = b.PtMRN ;


        SELECT a.* FROM temp.output a
            join (SELECT *
                FROM temp.output
                where eventcategory = 'CR TREATMENT'
                and year(eventdate) >= 2015) b
            on a.PtMRN = b.PtMRN;

        SELECT * from temp.hct;
        SELECT distinct a.* FROM temp.output a
            join (SELECT *
                FROM temp.output
                where eventcategory LIKE '%ARRIVAL%'
                and eventdate between '2014-01-01' and '2016-06-30') b
            on a.PtMRN = b.PtMRN;
        --
        --     SELECT b.*, a.*
        --     FROM temp.output a
        --     left join (SELECT *
        --         FROM temp.output
        --         where eventcategory LIKE '%RESPONSE%'
        --         and event like '%CR%') b
        --     on a.PtMRN = b.PtMRN and a.eventdate > b.eventdate and year(a.eventdate) = 2015;
    """
    print (cnxdict['sql'])
    dosqlexecute(cnxdict)
    return


producetimeline(cnxdict)