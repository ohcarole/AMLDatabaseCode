 DROP VIEW IF EXISTS  caisis.v_diagnosis;
CREATE VIEW caisis.v_diagnosis AS
SELECT
    PtMRN
    , PatientId
    , '1 DIAGNOSIS' AS Event
    , MIN(statusdate) AS EventDate
    , concat(statusdisease, ': ', status) as EventDescription
    , MIN(statusdate) AS AMLDxDate
FROM
    caisis.vdatasetstatus
WHERE status LIKE '%diag%' # OR status LIKE '%work%'
AND status NOT LIKE '%non-heme%'
GROUP BY PtMRN # , statusdate
;
SELECT * FROM caisis.v_diagnosis;


DROP TABLE IF EXISTS temp.t_arrival_;
CREATE TABLE temp.t_arrival_
SELECT
    PtMRN
    , PatientId
    , statusdate AS  ArrivalDate
    , statusdisease AS  ArrivalDx
    , '2 ARRIVAL' AS  Event
    , statusdate AS  EventDate
    , concat(statusdisease, ': ', status) as EventDescription
FROM
    caisis.vdatasetstatus a
    LEFT JOIN (
        SELECT PtMRN as JoinedPtMRN, MAX(YEAR(StatusDate)) AS MostRecentStatus
            FROM caisis.vdatasetstatus
            WHERE YEAR(StatusDate) >= 2008
            GROUP BY PtMRN) b
    on a.PtMRN = b.JoinedPtMRN
WHERE b.JoinedPtMRN IS NOT NULL
    and `status` LIKE '%arrival%'
GROUP BY PtMRN, statusdate;
ALTER TABLE temp.t_arrival_ ADD RowNum INT PRIMARY KEY AUTO_INCREMENT FIRST;

SELECT b.*, a.*
FROM
    caisis.playground a
        LEFT JOIN
    (SELECT
        ptmrn, max(year(arrivaldate))
    FROM
        caisis.playground
    WHERE
        YEAR(arrivaldate) >= 2008
    GROUP BY ptmrn) b ON a.ptmrn = b.ptmrn
    where b.ptmrn is not null;


DROP VIEW IF EXISTS caisis.v_arrival_with_prev_next;
CREATE VIEW caisis.v_arrival_with_prev_next AS
SELECT a.*,
        b.ArrivalDate AS  PrevArrivalDate,
        b.ArrivalDx AS  PrevArrivalDx
FROM (
    SELECT
        a.RowNum,
        a.PtMRN,
        a.PatientId,
        a.ArrivalDate,
        a.ArrivalDx,
        a.Event,
        a.EventDate,
        a.EventDescription,
        b.ArrivalDate AS  NextArrivalDate,
        b.ArrivalDx AS  NextArrivalDx
    FROM
        temp.t_arrival_ a
            LEFT JOIN
        temp.t_arrival_ b ON a.PtMRN = b.PtMRN AND a.RowNum = b.RowNum - 1
    UNION
    SELECT
        a.RowNum,
        a.PtMRN,
        a.PatientId,
        a.ArrivalDate,
        a.ArrivalDx,
        a.Event,
        a.EventDate,
        a.EventDescription,
        b.ArrivalDate AS  NextArrivalDate,
        b.ArrivalDx AS  NextArrivalDx
    FROM
        temp.t_arrival_ a
            LEFT JOIN
        temp.t_arrival_ b ON a.PtMRN = b.PtMRN AND a.RowNum = b.RowNum - 1
        WHERE b.RowNum IS NULL ) a
LEFT JOIN
    temp.t_arrival_ b ON a.PtMRN = b.PtMRN AND a.RowNum = b.RowNum + 1
ORDER BY PtMRN, ArrivalDate;

SELECT * FROM caisis.v_arrival_with_prev_next;

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
    , a.EncType AS EventDescription
    , a.EncDate as EncounterDate
    , a.EncECOG_Score as EncounterECOG
FROM
    caisis.encounter a
        LEFT JOIN
    caisis.v_arrival_with_prev_next b ON a.PtMRN = b.PtMRN
        AND a.EncDate = b.ArrivalDate
ORDER BY a.PtMRN, a.EncDate;
SELECT * FROM caisis.v_encounter;



/*
Building Playground Table
Up to this point we could build the playground information for diagnosis, arrival, and encounter through joins
but from here on out because of the nature of parent-child relationships the following must be updates
to place holders:
Induction
Response
Cycle

*/
DROP TABLE IF EXISTS caisis.playground ;
CREATE TABLE caisis.playground
SELECT a.PtMRN
    , a.PatientId
    , b.AMLDxDate
    , a.ArrivalDate
    , a.ArrivalDx
    , c.EncounterECOG AS ArrivalECOG
    , STR_TO_DATE(NULL,'%m%h') AS InductionStartDate
    , cast(' ' AS CHAR) AS TreatmentProtocol
    , STR_TO_DATE(NULL,'%m%h') AS ResponseDate
    , cast(' ' AS CHAR) AS ResponseDescription
    , STR_TO_DATE(NULL,'%m%h') AS RelapseDate
    , cast(' ' AS CHAR) AS RelapseDescription
    , a.NextArrivalDate, a.NextArrivalDx, a.PrevArrivalDate, a.PrevArrivalDx
    # Cycles before response
    , STR_TO_DATE(NULL,'%m%h') AS Cycle1Date
    , cast(' ' AS CHAR)        AS Cycle1Treatment
    , STR_TO_DATE(NULL,'%m%h') AS Cycle2Date
    , cast(' ' AS CHAR)        AS Cycle2Treatment
    , STR_TO_DATE(NULL,'%m%h') AS Cycle3Date
    , cast(' ' AS CHAR)        AS Cycle3Treatment
    , STR_TO_DATE(NULL,'%m%h') AS Cycle4Date
    , cast(' ' AS CHAR)        AS Cycle4Treatment
    # Cycles after response
    , STR_TO_DATE(NULL,'%m%h') AS PRT1Date
    , cast(' ' AS CHAR)        AS PRT1Treatment
    , STR_TO_DATE(NULL,'%m%h') AS PRT2Date
    , cast(' ' AS CHAR)        AS PRT2Treatment
    , STR_TO_DATE(NULL,'%m%h') AS PRT3Date
    , cast(' ' AS CHAR)        AS PRT3Treatment
    , STR_TO_DATE(NULL,'%m%h') AS PRT4Date
    , cast(' ' AS CHAR)        AS PRT4Treatment
    , -9 as CyclesToResponse
    , space(100) as CyclesCalculationMethod
    , -9 AS DaysToResponse
    FROM caisis.v_arrival_with_prev_next AS a
    LEFT JOIN caisis.v_diagnosis b ON a.ptmrn = b.ptmrn
    LEFT JOIN caisis.v_encounter c ON a.ptmrn = c.ptmrn AND a.ArrivalDate = c.ArrivalDate
    ORDER BY a.ptmrn, a.eventdate ;

ALTER TABLE `caisis`.`playground`
    CHANGE COLUMN `TreatmentProtocol`     `TreatmentProtocol`   MEDIUMTEXT
    , CHANGE COLUMN `ResponseDescription` `ResponseDescription` MEDIUMTEXT
    , CHANGE COLUMN `RelapseDescription`  `RelapseDescription`  MEDIUMTEXT
    , CHANGE COLUMN `Cycle1Treatment`     `Cycle1Treatment`     MEDIUMTEXT
    , CHANGE COLUMN `Cycle2Treatment`     `Cycle2Treatment`     MEDIUMTEXT
    , CHANGE COLUMN `Cycle3Treatment`     `Cycle3Treatment`     MEDIUMTEXT
    , CHANGE COLUMN `Cycle4Treatment`     `Cycle4Treatment`     MEDIUMTEXT
    , CHANGE COLUMN `PRT1Treatment`       `PRT1Treatment`       MEDIUMTEXT
    , CHANGE COLUMN `PRT2Treatment`       `PRT2Treatment`       MEDIUMTEXT
    , CHANGE COLUMN `PRT3Treatment`       `PRT3Treatment`       MEDIUMTEXT
    , CHANGE COLUMN `PRT4Treatment`       `PRT4Treatment`       MEDIUMTEXT
    , CHANGE COLUMN `PrevArrivalDx`       `PrevArrivalDx`       MEDIUMTEXT ;

ALTER TABLE `caisis`.`playground`
    ADD INDEX `PtMRN` (`PtMRN`(10) ASC),
    ADD INDEX `ArrivalDate` (`ArrivalDate` ASC),
    ADD INDEX `ArrivalDx` (`ArrivalDx` (30) ASC);

select * from caisis.playground;


/*
INDUCTION AND CYCLES
*/

DROP TABLE IF EXISTS temp.v_firsttreatment;
CREATE TABLE temp.v_firsttreatment
    SELECT
        vdatasetmedtxadministration.MedicalTherapyId AS MedicalTherapyId
        , MIN(vdatasetmedtxadministration.MedTxAdminStartDate) AS InductionStartDate
        , COUNT(*) as CycleCount
        FROM caisis.vdatasetmedtxadministration
        GROUP BY MedicalTherapyId;

ALTER TABLE `temp`.`v_firsttreatment`
CHANGE COLUMN `MedicalTherapyId` `MedicalTherapyId` BIGINT(20) NOT NULL ,
ADD PRIMARY KEY (`MedicalTherapyId`);

DROP VIEW IF EXISTS caisis.v_treatment;
CREATE VIEW caisis.v_treatment AS
SELECT b.RowNum
    , a.PtMRN
    , a.PatientId
    , b.ArrivalDate
    , b.ArrivalDx
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
            WHEN ISNULL(`a`.`MedTxCycle`) THEN -(9)
            ELSE `a`.`MedTxCycle`
    END AS `CycleCount`
    , CASE
            WHEN c.MedTxAdminStartDate = d.InductionStartDate THEN 'Yes'
            WHEN a.MedTxDate = c.MedTxAdminStartDate THEN 'Yes'
            WHEN c.MedTxAdminStartDate IS NOT NULL THEN 'No'
            ELSE ''
    END AS IsFirstCycle
    , c.MedTxAdminStartDate AS CycleStartDate
FROM `caisis`.`vdatasetmedicaltherapy` a
    LEFT JOIN caisis.v_arrival_with_prev_next b
        ON a.PtMRN = b.PtMRN AND a.MedTxDate BETWEEN b.ArrivalDate AND b.NextArrivalDate
    LEFT JOIN caisis.vdatasetmedtxadministration c
        ON a.MedicalTherapyId = c.MedicalTherapyId
    LEFT JOIN temp.v_firsttreatment d
        ON a.MedicalTherapyId = d.MedicalTherapyId
ORDER BY a.PtMRN, a.MedTxDate, MedTxAdminStartDate;
DROP TABLE IF EXISTS temp.v_treatment;
CREATE TABLE temp.v_treatment SELECT * FROM caisis.v_treatment;
ALTER TABLE `temp`.`v_treatment`
    ADD INDEX `PtMRN` (`PtMRN`(10) ASC),
    ADD INDEX `ArrivalDate` (`ArrivalDate` ASC);

DROP TABLE IF EXISTS temp.v_induction ;
CREATE TABLE temp.v_induction
    SELECT PtMRN
        , TreatmentProtocol
        , ArrivalDate
        , TreatmentStartDate
        , CASE
            WHEN InductionStartDate IS NULL THEN TreatmentStartDate
            ELSE InductionStartDate
        END AS InductionStartDate
    FROM
        caisis.v_treatment
    WHERE
        Event = '4 TREATMENT'
    GROUP BY PtMRN, ArrivalDate
    ORDER BY PtMRN, InductionStartDate;
ALTER TABLE `temp`.`v_induction`
    ADD INDEX `PtMRN` (`PtMRN`(10) ASC),
    ADD INDEX `ArrivalDate` (`ArrivalDate` ASC);

select * from temp.v_induction;

UPDATE caisis.playground a, temp.v_induction b
    SET a.InductionStartDate = b.InductionStartDate
    , a.TreatmentProtocol    = b.TreatmentProtocol
    , a.Cycle1Date           = b.InductionStartDate
    , a.Cycle1Treatment      = b.TreatmentProtocol
    WHERE a.PtMRN = b.PtMRN and a.ArrivalDate = b.ArrivalDate
    and   b.InductionStartDate BETWEEN a.ArrivalDate and a.NextArrivalDate;

SELECT * FROM caisis.playground;

UPDATE caisis.playground a, temp.v_induction b
    SET a.InductionStartDate = b.InductionStartDate
    , a.TreatmentProtocol    = b.TreatmentProtocol
    , a.Cycle1Date           = b.InductionStartDate
    , a.Cycle1Treatment      = b.TreatmentProtocol
    WHERE a.PtMRN = b.PtMRN and a.ArrivalDate = b.ArrivalDate
    and   b.InductionStartDate <  a.InductionStartDate
    and   b.InductionStartDate >= a.ArrivalDate ;

SELECT * FROM caisis.playground;

UPDATE caisis.playground a, temp.v_induction b
    SET a.InductionStartDate = b.InductionStartDate
    , a.TreatmentProtocol    = b.TreatmentProtocol
    , a.Cycle1Date           = b.InductionStartDate
    , a.Cycle1Treatment      = b.TreatmentProtocol
    WHERE a.PtMRN = b.PtMRN and a.InductionStartDate IS NULL
    and   b.InductionStartDate BETWEEN a.ArrivalDate AND a.NextArrivalDate ;

SELECT * FROM caisis.playground;

SELECT COUNT(*) FROM caisis.playground WHERE InductionStartDate IS NULL;

/*
RESPONSE
*/

DROP VIEW IF EXISTS caisis.v_response;
CREATE VIEW caisis.v_response AS
    SELECT b.PtMRN
        , '5 RESPONSE' AS Event
        , statusdate AS EventDate
        , CONCAT(statusdisease, ': Treatment Response (',`status`,')') AS EventDescription
        , statusDate AS ResponseDate
        , `status` AS ResponseDescription
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
SELECT * FROM caisis.v_response;

select * from caisis.playground;

UPDATE caisis.playground
    SET ResponseDescription = NULL
        , ResponseDate = NULL ;
UPDATE caisis.playground a, caisis.v_response b
    SET a.ResponseDescription = b.ResponseDescription
        , a.ResponseDate = b.ResponseDate
    WHERE a.PtMRN = b.PtMRN
    and b.ResponseDate BETWEEN a.InductionStartDate and a.NextArrivalDate;
UPDATE caisis.playground a, caisis.v_response b
    SET a.ResponseDescription = b.ResponseDescription
    , a.ResponseDate = b.ResponseDate
    WHERE a.PtMRN = b.PtMRN
    and b.ResponseDate BETWEEN date_add(a.ResponseDate, INTERVAL 7 DAY) and a.NextArrivalDate;


/*
CYCLES starting before response
*/

UPDATE caisis.playground
    SET Cycle2Date    = NULL
    , Cycle2Treatment = NULL
    , Cycle3Date    = NULL
    , Cycle3Treatment = NULL
    , Cycle4Date    = NULL
    , Cycle4Treatment = NULL;
# second cycle
UPDATE caisis.playground a, temp.v_treatment b
    SET a.Cycle2Date    = b.TreatmentStartDate
    , a.Cycle2Treatment = b.TreatmentProtocol
    WHERE a.PtMRN = b.PtMRN
        AND b.TreatmentStartDate BETWEEN date_add(a.Cycle1Date, INTERVAL 7 DAY) and date_add(a.ResponseDate, INTERVAL -1 DAY);
# third cycle
UPDATE caisis.playground a, temp.v_treatment b
    SET a.Cycle3Date    = b.TreatmentStartDate
    , a.Cycle3Treatment = b.TreatmentProtocol
    WHERE a.PtMRN = b.PtMRN
        AND b.TreatmentStartDate BETWEEN date_add(a.Cycle2Date, INTERVAL 7 DAY) and date_add(a.ResponseDate, INTERVAL -1 DAY);
# fourth cycle
UPDATE caisis.playground a, temp.v_treatment b
    SET a.Cycle4Date    = b.TreatmentStartDate
    , a.Cycle4Treatment = b.TreatmentProtocol
    WHERE a.PtMRN = b.PtMRN
        AND b.TreatmentStartDate BETWEEN date_add(a.Cycle3Date, INTERVAL 7 DAY) and date_add(a.ResponseDate, INTERVAL -1 DAY);


UPDATE caisis.playground a, caisis.v_response b
    SET a.CyclesToResponse =
        CASE
            WHEN a.Cycle4Date IS NOT NULL AND a.Cycle4Date <= a.ResponseDate   THEN 4
            WHEN a.Cycle3Date IS NOT NULL AND a.Cycle3Date <= a.ResponseDate   THEN 3
            WHEN a.Cycle2Date IS NOT NULL AND a.Cycle2Date <= a.ResponseDate   THEN 2
            WHEN timestampdiff(day,  a.InductionStartDate,a.ResponseDate) < 35 THEN 1
            WHEN timestampdiff(month,a.InductionStartDate,a.ResponseDate) <= 2 THEN 2
            ELSE -9
        END
        , a.CyclesCalculationMethod =
        CASE
            WHEN a.Cycle4Date IS NOT NULL AND a.Cycle4Date <= a.ResponseDate   THEN '4 Cycles Recorded'
            WHEN a.Cycle3Date IS NOT NULL AND a.Cycle3Date <= a.ResponseDate   THEN '3 Cycles Recorded'
            WHEN a.Cycle2Date IS NOT NULL AND a.Cycle2Date <= a.ResponseDate   THEN '2 Cycles Recorded'
            WHEN timestampdiff(day,  a.InductionStartDate,a.ResponseDate) < 35 THEN 'Less than 35 days to Response'
            WHEN timestampdiff(month,a.InductionStartDate,a.ResponseDate) <= 2 THEN 'Response two months after Induction Start'
            ELSE ''
        END
    WHERE a.PtMRN = b.PtMRN
    and b.ResponseDescription IS NOT NULL;

UPDATE caisis.playground SET DaysToResponse =
    CASE
        WHEN InductionStartDate IS NULL THEN -9
        WHEN ResponseDate IS NULL       THEN -9
        ELSE timestampdiff(day,InductionStartDate,ResponseDate)
    END;

SELECT PtMRN, PatientId, AMLDxDate
    , ArrivalDate, ArrivalDx, ArrivalECOG
    , TreatmentProtocol
    , InductionStartDate
    , CyclesToResponse
    , CyclesCalculationMethod
    , DaysToResponse
    , Cycle1Date
    , Cycle2Date
    , Cycle3Date
    , Cycle4Date
    , ResponseDate
    , RelapseDate
    , ResponseDescription
    , RelapseDescription
    , PRT1Date
    , PRT2Date
    , PRT3Date
    , PRT4Date
    , NextArrivalDate, NextArrivalDx, PrevArrivalDate, PrevArrivalDx
    FROM Caisis.Playground ;

/*
RELAPSE
*/
DROP VIEW IF EXISTS caisis.v_relapse ;
CREATE VIEW caisis.v_relapse AS
    SELECT ptmrn
    , PatientId
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

UPDATE playground a, temp.v_relapse b
    SET a.RelapseDate = b.RelapseDate,
    a.RelapseDescription = b.RelapseDescription
    WHERE a.PtMRN = b.PtMRN
    AND b.RelapseDate BETWEEN date_add(a.ResponseDate, INTERVAL 1 DAY) AND date_add(a.NextArrivalDate, INTERVAL 1 DAY);

#    and b.ResponseDate BETWEEN date_add(a.ResponseDate, INTERVAL 7 DAY) and a.NextArrivalDate;


SELECT ResponseDescription, count(*) FROM caisis.playground group by ResponseDescription;

/*
POST Remission Cycles
*/

UPDATE caisis.playground
    SET PRT1Date    = NULL
    , PRT1Treatment = NULL
    , PRT2Date    = NULL
    , PRT2Treatment = NULL
    , PRT3Date    = NULL
    , PRT3Treatment = NULL
    , PRT4Date    = NULL
    , PRT4Treatment = NULL;
# first PRT
UPDATE caisis.playground a, temp.v_treatment b
    SET a.PRT1Date    = b.TreatmentStartDate
    , a.PRT1Treatment = b.TreatmentProtocol
    WHERE a.PtMRN = b.PtMRN
        AND b.TreatmentStartDate BETWEEN date_add(a.ResponseDate, INTERVAL 7 DAY) and date_add(a.NextArrivalDate, INTERVAL -1 DAY);
# second PRT
UPDATE caisis.playground a, temp.v_treatment b
    SET a.PRT2Date    = b.TreatmentStartDate
    , a.PRT2Treatment = b.TreatmentProtocol
    WHERE a.PtMRN = b.PtMRN
        AND b.TreatmentStartDate BETWEEN date_add(a.PRT1Date, INTERVAL 7 DAY) and date_add(a.NextArrivalDate, INTERVAL -1 DAY);
# third PRT
UPDATE caisis.playground a, temp.v_treatment b
    SET a.PRT3Date    = b.TreatmentStartDate
    , a.PRT3Treatment = b.TreatmentProtocol
    WHERE a.PtMRN = b.PtMRN
        AND b.TreatmentStartDate BETWEEN date_add(a.PRT2Date, INTERVAL 7 DAY) and date_add(a.NextArrivalDate, INTERVAL -1 DAY);
# fourth PRT
UPDATE caisis.playground a, temp.v_treatment b
    SET a.PRT4Date    = b.TreatmentStartDate
    , a.PRT4Treatment = b.TreatmentProtocol
    WHERE a.PtMRN = b.PtMRN
        AND b.TreatmentStartDate BETWEEN date_add(a.PRT3Date, INTERVAL 7 DAY) and date_add(a.NextArrivalDate, INTERVAL -1 DAY);

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

# t(6;9) patients
SELECT * FROM caisis.playground a
    left join (
        select ptmrn from caisis.allkaryo
            WHERE pathresult like '%t(6;9)%' group by ptmrn
        ) b ON a.ptmrn = b.ptmrn
    WHERE b.ptmrn IS NOT NULL;