from Utilities.MySQLdbUtils import *
reload(sys)

def create_all_views(cnxdict):
    """
    creates a table of AML patient demographics merging what we know
    from the AML database and the Caisis demographic data
    :param cnxdict:
    :param tbl:
    :return:
    """
    print('creating views')
    cnxdict['sql'] = """
        DROP VIEW IF EXISTS  caisis.v_diagnosis;
        CREATE VIEW caisis.v_diagnosis AS
        SELECT
            PtMRN
            , PatientId
            , '1 DIAGNOSIS' AS Event
            , MIN(statusdate) AS EventDate
            , concat(statusdisease, ': ', status) as EventDescription
            , statusdisease as EventResult
            , MIN(statusdate) AS AMLDxDate
            , statusdisease as AMLDx
        FROM
            caisis.vdatasetstatus
        WHERE status LIKE '%diag%'
        AND status NOT LIKE '%non-heme%'
        GROUP BY PtMRN ;

        DROP TABLE IF EXISTS  temp.v_diagnosis;
        CREATE TABLE temp.v_diagnosis AS SELECT * FROM caisis.v_diagnosis;
        ALTER TABLE temp.v_diagnosis
            ADD INDEX `PtMRN` (`PtMRN`(10) ASC);

        DROP VIEW IF EXISTS caisis.v_arrival;
        CREATE VIEW caisis.v_arrival AS
            SELECT
                PtMRN
                , PatientId
                , PtName
                , PtBirthDate
                , PtGender
                , PtRace
                , PtEthnicity
                , PtDeathDate
                , PtDeathType
                , PtDeathCause
                , statusdate AS  ArrivalDate
                , statusdisease AS  ArrivalDx
                , timestampdiff(YEAR,PtBirthDate,statusdate) AS ArrivalAge
                , '2 ARRIVAL' AS  Event
                , statusdisease AS EventResult
                , statusdate AS  EventDate
                , concat(statusdisease, ': ', status) as EventDescription
            FROM
                caisis.vdatasetstatus a
                LEFT JOIN (
                    SELECT PtMRN as JoinedPtMRN, MAX(YEAR(StatusDate)) AS MostRecentStatus
                        FROM caisis.vdatasetstatus
                        WHERE YEAR(StatusDate) >= 2008
                        GROUP BY PtMRN) b
                ON a.PtMRN = b.JoinedPtMRN
                LEFT JOIN (
                    SELECT PtMRN AS PtMRN_
                        , concat(
                            CASE
                                WHEN PtLastName IS NULL THEN ''
                                ELSE concat(UPPER(PtLastName),', ')
                            END,
                            CASE
                                WHEN PtFirstName IS NULL THEN ''
                                ELSE concat(UPPER(PtFirstName),' ')
                            END,
                            CASE
                                WHEN PtMiddleName IS NULL THEN ''
                                ELSE concat(LEFT(UPPER(PtMiddleName),1),'.')
                            END) AS PtName
                        ,  PtBirthdate
                        ,  PtGender
                        ,  PtRace
                        ,  PtEthnicity
                        ,  PtDeathDate
                        ,  PtDeathType
                        ,  PtDeathCause
                    FROM caisis.vdatasetpatients) c
                ON a.PtMRN = c.PtMRN_
            WHERE b.JoinedPtMRN IS NOT NULL
                and `status` LIKE '%arrival%'
            GROUP BY PtMRN, statusdate;

        DROP TABLE IF EXISTS temp.v_arrival;
        CREATE TABLE temp.v_arrival SELECT * FROM Caisis.v_arrival;
        ALTER TABLE temp.v_arrival ADD RowNum INT PRIMARY KEY AUTO_INCREMENT FIRST;
        ALTER TABLE temp.v_arrival
            ADD INDEX `PtMRN` (`PtMRN`(10) ASC),
            ADD INDEX `ArrivalDate` (`ArrivalDate` ASC),
            ADD INDEX `ArrivalDx` (`ArrivalDx` (30) ASC);

        DROP VIEW IF EXISTS caisis.v_arrival_with_prev_next;
        CREATE VIEW caisis.v_arrival_with_prev_next AS
        SELECT a.*,
                b.ArrivalDate AS  PrevArrivalDate,
                b.ArrivalDx AS  PrevArrivalDx
        FROM (
            SELECT a.RowNum
                ,  a.PtMRN
                ,  a.PatientId
                ,  a.PtName
                ,  a.PtBirthdate
                ,  a.PtGender
                ,  a.PtRace
                ,  a.PtEthnicity
                ,  a.PtDeathDate
                ,  a.PtDeathType
                ,  a.PtDeathCause
                ,  a.ArrivalAge
                ,  a.ArrivalDate
                ,  a.ArrivalDx
                ,  a.Event
                ,  a.EventDate
                ,  a.EventDescription
                ,  a.EventResult
                ,  b.ArrivalDate AS  NextArrivalDate
                ,  b.ArrivalDx AS  NextArrivalDx
            FROM
                temp.v_arrival a
                    LEFT JOIN
                temp.v_arrival b ON a.PtMRN = b.PtMRN AND a.RowNum = b.RowNum - 1
            UNION
            SELECT a.RowNum
                ,  a.PtMRN
                ,  a.PatientId
                ,  a.PtName
                ,  a.PtBirthdate
                ,  a.PtGender
                ,  a.PtRace
                ,  a.PtEthnicity
                ,  a.PtDeathDate
                ,  a.PtDeathType
                ,  a.PtDeathCause
                ,  a.ArrivalAge
                ,  a.ArrivalDate
                ,  a.ArrivalDx
                ,  a.Event
                ,  a.EventDate
                ,  a.EventDescription
                ,  a.EventResult
                ,  b.ArrivalDate AS  NextArrivalDate
                ,  b.ArrivalDx AS  NextArrivalDx
            FROM
                temp.v_arrival a
                    LEFT JOIN
                temp.v_arrival b ON a.PtMRN = b.PtMRN AND a.RowNum = b.RowNum - 1
                WHERE b.RowNum IS NULL ) a
        LEFT JOIN
            temp.v_arrival b ON a.PtMRN = b.PtMRN AND a.RowNum = b.RowNum + 1
        ORDER BY PtMRN, ArrivalDate;

        DROP TABLE IF EXISTS temp.v_arrival_with_prev_next;
        CREATE TABLE temp.v_arrival_with_prev_next SELECT * FROM Caisis.v_arrival_with_prev_next;
        ALTER TABLE temp.v_arrival_with_prev_next
            ADD INDEX `PtMRN` (`PtMRN`(10) ASC),
            ADD INDEX `ArrivalDate` (`ArrivalDate` ASC),
            ADD INDEX `NextArrivalDate` (`ArrivalDate` ASC),
            ADD INDEX `PrevArrivalDate` (`ArrivalDate` ASC),
            ADD INDEX `ArrivalDx` (`ArrivalDx` (30) ASC);

        DROP VIEW IF EXISTS caisis.v_encounter;
        CREATE VIEW caisis.v_encounter AS
        SELECT
            b.RowNum
            , a.PtMRN
            , a.PatientId
            , b.ArrivalDate
            , b.ArrivalDx
            , '3 ENCOUNTER' as Event
            , a.EncDate AS EventDate
            , concat(b.ArrivalDx,': ',a.EncType) AS EventDescription
            , a.EncDate as EncounterDate
            , a.EncECOG_Score as EncounterECOG
            , concat(b.ArrivalDx,': ',a.EncType) as EncounterType
        FROM
            caisis.encounter a
                LEFT JOIN
            temp.v_arrival_with_prev_next b ON a.PtMRN = b.PtMRN
                AND a.EncDate = b.ArrivalDate
        ORDER BY a.PtMRN, a.EncDate;

        DROP TABLE IF EXISTS temp.v_encounter;
        CREATE TABLE temp.v_encounter SELECT * FROM Caisis.v_encounter;
        ALTER TABLE temp.v_encounter
            ADD INDEX `PtMRN` (`PtMRN`(10) ASC),
            ADD INDEX `ArrivalDate` (`ArrivalDate` ASC);


        /*
        INDUCTION AND CYCLES
        */

        DROP VIEW IF EXISTS Caisis.v_firsttreatment;
        CREATE VIEW Caisis.v_firsttreatment AS
            SELECT
                vdatasetmedtxadministration.MedicalTherapyId AS MedicalTherapyId
                , MIN(vdatasetmedtxadministration.MedTxAdminStartDate) AS InductionStartDate
                , COUNT(*) as CycleCount
                FROM caisis.vdatasetmedtxadministration
                GROUP BY MedicalTherapyId;

        DROP TABLE IF EXISTS temp.v_firsttreatment;
        CREATE TABLE temp.v_firsttreatment AS
        SELECT * FROM caisis.v_firsttreatment;
            ALTER TABLE `temp`.`v_firsttreatment`
                CHANGE COLUMN `MedicalTherapyId` `MedicalTherapyId` BIGINT(20) NOT NULL ,
                ADD PRIMARY KEY (`MedicalTherapyId`);

        DROP VIEW IF EXISTS caisis.v_treatment;
        CREATE VIEW caisis.v_treatment AS
            SELECT a.PtMRN
                , a.PatientId
                , CASE
                        WHEN c.MedTxAdminStartDate = d.InductionStartDate THEN '4 TREATMENT'
                        WHEN a.MedTxDate = c.MedTxAdminStartDate THEN '4 TREATMENT'
                        WHEN c.MedTxAdminStartDate IS NOT NULL THEN '4 TREATMENT CYCLE'
                        ELSE '4 TREATMENT'
                END AS Event
                , CASE
                        WHEN c.MedTxAdminStartDate = d.InductionStartDate THEN c.MedTxAdminStartDate
                        WHEN a.MedTxDate = c.MedTxAdminStartDate THEN a.MedTxDate
                        WHEN c.MedTxAdminStartDate IS NOT NULL THEN c.MedTxAdminStartDate
                        ELSE a.MedTxDate
                END AS EventDate
                , concat('Treatment/Cycle Administered: ',a.MedTxAgent) as EventDescription
                , CASE
                        WHEN a.MedTxCycle IS NOT NULL THEN concat(a.MedTxCycle,' (abstracted)')
                        WHEN d.CycleCount > 0 THEN concat(d.CycleCount,' (calculated/estimated)')
                        ELSE 'not available'
                END AS EventResult
                , CASE
                        WHEN c.MedTxAdminStartDate = d.InductionStartDate THEN c.MedTxAdminStartDate
                        WHEN a.MedTxDate = c.MedTxAdminStartDate THEN a.MedTxDate
                        WHEN c.MedTxAdminStartDate IS NOT NULL THEN c.MedTxAdminStartDate
                        ELSE a.MedTxDate
                END AS TreatmentStartDate
                , a.MedTxAgent as TreatmentProtocol
                , d.InductionStartDate
                , -9 AS CyclesToResponse
                , CASE
                        WHEN a.MedTxCycle IS NOT NULL THEN a.MedTxCycle
                        WHEN d.CycleCount > 0 THEN d.CycleCount
                        ELSE NULL
                END AS `CycleCount`
                , CASE
                        WHEN a.MedTxCycle IS NOT NULL THEN ' (abstracted)'
                        WHEN d.CycleCount > 0 THEN ' (calculated/estimated)'
                        ELSE 'not available'
                END AS `CycleCountNote`
                , CASE
                        WHEN c.MedTxAdminStartDate = d.InductionStartDate THEN 'Yes'
                        WHEN a.MedTxDate = c.MedTxAdminStartDate THEN 'Yes'
                        WHEN c.MedTxAdminStartDate IS NOT NULL THEN 'No'
                        ELSE ''
                END AS IsFirstCycle
                , c.MedTxAdminStartDate AS CycleStartDate
            FROM `caisis`.`vdatasetmedicaltherapy` a
                LEFT JOIN caisis.vdatasetmedtxadministration c
                    ON a.MedicalTherapyId = c.MedicalTherapyId
                LEFT JOIN temp.v_firsttreatment d
                    ON a.MedicalTherapyId = d.MedicalTherapyId
            ORDER BY a.PtMRN, a.MedTxDate, MedTxAdminStartDate;

        DROP TABLE IF EXISTS temp.v_treatment;
        CREATE TABLE temp.v_treatment SELECT * FROM caisis.v_treatment;
        ALTER TABLE `temp`.`v_treatment`
            ADD INDEX `PtMRN` (`PtMRN`(10) ASC);

        DROP VIEW IF EXISTS temp.min_treatment ;
        CREATE VIEW temp.min_treatment AS
            SELECT PtMRN, ArrivalDate, TreatmentStartDate AS FirstInduction, min(DaysToTreatment) AS DaysToTreatment
                FROM temp.potentialrx
                WHERE DaysToTreatment > 0 or DaysToTreatment IS NULL
                GROUP BY PtMRN, ArrivalDate;

        DROP TABLE IF EXISTS caisis.induction ;
        CREATE TABLE caisis.induction
            SELECT a.ptmrn
                , a.arrivaldate AS arrivaldate
                , a.nextarrivaldate
                , b.Treatmentstartdate
                , b.TreatmentProtocol
                , b.`Event`
                , b.`EventDate`
                , b.`EventDescription`
                , STR_TO_DATE(NULL,'%m%h') AS InductionStartDate
                , TIMESTAMPDIFF(DAY,
                    a.arrivaldate,
                    b.treatmentstartdate) AS DaysToTreatment
            FROM caisis.playground a
                 LEFT JOIN temp.v_treatment b ON a.PtMRN = b.PtMRN
            WHERE
                (b.TreatmentStartDate BETWEEN a.arrivaldate AND a.NextArrivalDate
                    OR b.TreatmentStartDate IS NULL)
                    OR (a.NextArrivalDate IS NULL
                    AND b.TreatmentStartDate BETWEEN a.arrivaldate
                    AND DATE_ADD(a.arrivaldate, INTERVAL + 180 DAY));

        UPDATE caisis.induction a
                , (SELECT PtMRN, ArrivalDate, TreatmentStartDate AS FirstInduction, min(DaysToTreatment) AS DaysToTreatment
                        FROM caisis.induction
                        WHERE DaysToTreatment >= 0 or DaysToTreatment IS NULL
                        GROUP BY PtMRN, ArrivalDate) b
            SET a.InductionStartDate = b.FirstInduction
            WHERE a.PtMRN = b.PtMRN and a.ArrivalDate = b.Arrivaldate;



        /*
        RESPONSE
        */

        DROP VIEW IF EXISTS caisis.v_response;
        CREATE VIEW caisis.v_response AS
            SELECT b.PtMRN
                , b.PatientId
                , '6 RESPONSE' AS Event
                , statusdate AS EventDate
                , CONCAT(statusdisease, ': Treatment Response (',`status`,')') AS EventDescription
                , status AS EventResult
                , statusDate AS ResponseDate
                , status AS ResponseDescription
            FROM caisis.vdatasetstatus a
            LEFT JOIN caisis.v_arrival_with_prev_next b ON a.PtMRN = b.PtMRN
            WHERE statusdisease LIKE '%AML%'
                    AND (`status` LIKE '%CR%'
                    OR `status` LIKE '%death%'
                    OR `status` = 'PR'
                    OR `status` LIKE '%resist%')
            GROUP BY a.PtMRN, a.statusdate;
        DROP TABLE IF EXISTS temp.v_response;
        CREATE TABLE temp.v_response SELECT * FROM caisis.v_response;
        ALTER TABLE `temp`.`v_response`
            ADD INDEX `PtMRN` (`PtMRN`(10) ASC);

        /*
        RELAPSE
        */
        DROP VIEW IF EXISTS caisis.v_relapse ;
        CREATE VIEW caisis.v_relapse AS
            SELECT ptmrn
            , PatientId
            , '7 RELAPSE' as Event
            , statusdate AS EventDate
            , concat(statusdisease,': ',status) AS EventDescription
            , status AS RelapseDescription
            , statusdate AS relapsedate
            , statusdisease AS arrivaldx
            FROM
                caisis.vdatasetstatus
            WHERE
                status LIKE '%rel%'
                AND (UPPER(statusdisease) RLIKE 'A(M|P)L' OR UPPER(statusdisease) RLIKE 'MDS')
            ORDER BY ptmrn, statusdate;
        DROP TABLE IF EXISTS temp.v_relapse ;
        CREATE TABLE temp.v_relapse AS
            SELECT * FROM caisis.v_relapse;
        ALTER TABLE `temp`.`v_relapse`
            ADD INDEX `PtMRN` (`PtMRN`(10) ASC),
            ADD INDEX `ArrivalDx` (`ArrivalDx` (30) ASC);
        SELECT * FROM temp.v_relapse;
        SELECT * FROM playground;

        DROP VIEW if exists caisis.v_hct;
        CREATE VIEW caisis.v_hct AS
            SELECT a.`index`,
                a.`PtMRN`,
                a.`PatientId`,
                '5 HCT' AS Event,
                a.ProcDate as EventDate,
                CASE
                    WHEN ProcDonMatch IS NULL
                        THEN CONCAT(ProcCellSource," from ",ProcDonRelation)
                    WHEN ProcDonMatch = 'AUTO'
                        THEN  CONCAT(ProcCellSource," from ",ProcDonMatch)
                    WHEN ProcDonMatch = 'UNRELATED'
                        THEN  CONCAT(ProcCellSource," from ",ProcDonMatch)
                    ELSE CONCAT(ProcCellSource," from ",ProcDonMatch," ",ProcDonRelation)
                END AS EventDescription,
                concat('cells infused: ',ProcCellsInfused) AS EventResult,
                a.`ProcedureId`,
                a.`ProcDateText`,
                a.`ProcDate`,
                a.ProcDate AS HCTDate,
                CASE
                    WHEN ProcDonMatch IS NULL
                        THEN CONCAT(ProcCellSource," from ",ProcDonRelation)
                    WHEN ProcDonMatch = 'AUTO'
                        THEN  CONCAT(ProcCellSource," from ",ProcDonMatch)
                    WHEN ProcDonMatch = 'UNRELATED'
                        THEN  CONCAT(ProcCellSource," from ",ProcDonMatch)
                    ELSE CONCAT(ProcCellSource," from ",ProcDonMatch," ",ProcDonRelation)
                END AS HCTDescription,
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
        DROP TABLE IF EXISTS temp.v_hct;
        CREATE TABLE temp.v_hct AS SELECT * FROM Caisis.v_hct;

        /*
        Count Recovery
        */
        /*
        Karyotype
        */
        /*
        MRC Risk
        */
        /*
        ELN 2017 Risk
        */
        /*
        Lab Results
        */
        /*
        Molecular (FLT3/CEBPA/NPM1)
        */

        /*
        FISH
        */
        drop view if exists caisis.v_fish;
        create view caisis.v_fish as
            select * from caisis.allkaryo
                where pathresult > ''
                and type like '%fish%';

        drop table if exists temp.v_fish;
        create table temp.v_fish as select * from caisis.v_fish;

        delete from temp.v_fish
            where Type like '%CGAT%'
            OR (pathtest like '%cyto%' AND lower(pathresult) NOT rlike '(nuc|ish){1})')
            OR (pathresult rlike '^(\/\/|[0-9])' AND lower(pathresult) NOT rlike '(nuc|ish)')
            OR pathresult = ''
            OR pathresult = '?'
            OR pathresult like '%insufficient growth%'
            OR lower(pathresult) IN ( 'not done'
                , 'failed to identify icsn diagnosis'
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

        ALTER TABLE `temp`.`v_fish`
        CHANGE COLUMN `DateObtained` `DateObtained` DATETIME NULL DEFAULT NULL ;

        ALTER TABLE `temp`.`v_fish`
            ADD INDEX `PtMRN` (`PtMRN`(10) ASC),
            ADD INDEX `dateobtained` (`dateobtained` ASC);

        /*
        CGAT
        */
        drop view if exists caisis.v_cgat;
        create view caisis.v_cgat as
            select * from caisis.allkaryo
                where pathresult > ''
                and type like '%cgat%';

        drop table if exists temp.v_cgat;
        create table temp.v_cgat as select * from caisis.v_cgat;

        delete from temp.v_cgat
            where type like '%FISH%'
            OR type like '%CYTO%'
            OR pathresult rlike '^(\/\/)?[0-9])'
            OR lower(pathresult) rlike '(nuc|ish)'
            OR pathresult = '?'
            OR pathresult like '%insufficient growth%'
            OR lower(pathresult) IN ( 'not done'
                , 'failed to identify icsn diagnosis'
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

        ALTER TABLE `temp`.`v_cgat`
        CHANGE COLUMN `DateObtained` `DateObtained` DATETIME NULL DEFAULT NULL ;

        ALTER TABLE `temp`.`v_cgat`
            ADD INDEX `PtMRN` (`PtMRN`(10) ASC),
            ADD INDEX `dateobtained` (`dateobtained` ASC);


        /*
        CYTO
        */
        DROP VIEW IF EXISTS caisis.v_cyto;
        CREATE VIEW Caisis.v_cyto AS
            select *, pathresult as pathkaryotype from caisis.allkaryo
                where pathresult > ''
                and (pathtest like '%cyto%' or pathresult rlike '^(\/\/|[0-9])');
        delete from Caisis.v_cyto
            where lower(pathresult) IN ( 'not done'
                , 'failed to identify icsn diagnosis'
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

        DROP TABLE IF EXISTS temp.v_cyto;
        CREATE TABLE temp.v_cyto AS SELECT * FROM Caisis.v_cyto;

        ALTER TABLE `temp`.`v_cyto`
        CHANGE COLUMN `DateObtained` `DateObtained` DATETIME NULL DEFAULT NULL ;

        ALTER TABLE `temp`.`v_cyto`
            ADD INDEX `PtMRN` (`PtMRN`(10) ASC),
            ADD INDEX `dateobtained` (`dateobtained` ASC);


        SELECT * FROM temp.v_cyto;

        /*
        ALIVE
        */

        DROP VIEW IF EXISTS caisis.v_alive;
        CREATE VIEW Caisis.v_alive AS
            SELECT
                PtMRN,
                PatientId,
                PtName,
                b.AliveDate,
                concat('Last Patient Contact:  ',LastContactType)  AS LastContactType,
                '98 ALIVE' AS Event,
                b.AliveDate  AS EventDate,
                concat('Last Patient Contact:  ',LastContactType)  AS EventDescription,
                EventDate AS OrderDate
            FROM
                (SELECT * FROM temp.v_arrival WHERE PtDeathDate IS NULL) a
                LEFT JOIN (SELECT PtMRN as PtMRN_
                        , statusdate as AliveDate
                        , status     as LastContactType
                    FROM caisis.vdatasetstatus
                    WHERE status in ('Alive'
                        , 'Antecedent Hematologic Disorder'
                        , 'Arrival Work-up'
                        , 'BM Relapse'
                        , 'CNS Relapse'
                        , 'CR'
                        , 'CR CYTO MRD'
                        , 'CR MRD'
                        , 'CR UNK MRD'
                        , 'CR-MRD'
                        , 'Cri'
                        , 'CRi CYTO MRD'
                        , 'Cri MRD'
                        , 'CRi-MRD'
                        , 'CRp'
                        , 'CRp EXD MRD'
                        , 'CRp MRD'
                        , 'CYTO Relapse'
                        , 'Diagnosis Date'
                        , 'FISH Relapse'
                        , 'FLOW Relapse'
                        , 'Initial AHD Date'
                        , 'Metastatic Disease'
                        , 'Newly Diagnosed'
                        , 'No Evidence of Disease'
                        , 'Non-Heme Cancer Diagnosis'
                        , 'Non-Heme Cancer Diagnosis/Diagnosis Date'
                        , 'Not Categorized'
                        , 'Persistent Disease'
                        , 'PR'
                        , 'Recovery of ANC 1000'
                        , 'Recovery of ANC 500'
                        , 'Recovery of Counts'
                        , 'Recovery of Plts 100k'
                        , 'Recovery of Plts 50k'
                        , 'Recurrence'
                        , 'Refractory'
                        , 'Relapse'
                        , 'Resistant')
                         GROUP BY PtMRN) b
                    ON a.PtMRN = b.PtMRN_;

        DROP TABLE IF EXISTS temp.v_alive;
        CREATE TABLE temp.v_alive AS SELECT * FROM Caisis.v_alive;

/*
DECEASED
*/

        DROP VIEW IF EXISTS caisis.v_deceased;
        CREATE VIEW Caisis.v_deceased AS
            SELECT
                PtMRN,
                PatientId,
                PtName,
                PtDeathDate,
                concat('Patient Deceased: ',PtDeathCause)  AS PtDeathCause,
                '99 DECEASED' AS Event,
                PtDeathDate   AS EventDate,
                concat('Patient Deceased: ',PtDeathCause)  AS EventDescription,
                PtDeathDate AS OrderDate
            FROM
                temp.v_arrival
            WHERE PtDeathDate IS NOT NULL
            GROUP BY PtMRN, PtDeathDate;

        DROP TABLE IF EXISTS temp.v_deceased;
        CREATE TABLE temp.v_deceased AS SELECT * FROM Caisis.v_deceased;



    """
    dosqlexecute(cnxdict) # normally do not need to recreate views

    return


# cnxdict = connect_to_mysql_db_prod('caisismysql')
# cnxdict['EchoSQL'] = 1
# create_all_views(cnxdict)