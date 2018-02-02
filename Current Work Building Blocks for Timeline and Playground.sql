DROP VIEW IF EXISTS  caisis.v_diagnosis;
CREATE VIEW caisis.v_diagnosis AS
SELECT
    PtMRN,
    PatientId,
    '1 DIAGNOSIS' AS Event,
    statusdate AS  EventDate,
    statusdate AS  AMLDxDate
FROM
    caisis.vdatasetstatus
WHERE status LIKE '%work%' OR status LIKE '%diag%'
GROUP BY PtMRN, statusdate;
SELECT * FROM caisis.v_diagnosis;

DROP TABLE IF EXISTS temp.t_arrival_;
CREATE TABLE temp.t_arrival_
SELECT
    PtMRN
    ,PatientId
    ,statusdate AS  ArrivalDate
    ,statusdisease AS  ArrivalDx
    ,'2 ARRIVAL' AS  Event
    ,statusdate AS  EventDate
FROM
    caisis.vdatasetstatus
WHERE
    status LIKE '%arrival%'
GROUP BY PtMRN, statusdate;
ALTER TABLE temp.t_arrival_ ADD RowNum INT PRIMARY KEY AUTO_INCREMENT FIRST;


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
    , a.EncDate as EncounterDate
    , a.EncECOG_Score as EncounterECOG
FROM
    caisis.encounter a
        LEFT JOIN
    caisis.v_arrival_with_prev_next b ON a.PtMRN = b.PtMRN
        AND a.EncDate = b.ArrivalDate
ORDER BY a.PtMRN, a.EncDate;
SELECT * FROM caisis.v_encounter;

DROP VIEW IF EXISTS caisis.v_firsttreatment;
CREATE VIEW caisis.v_firsttreatment AS
SELECT
    vdatasetmedtxadministration.MedicalTherapyId AS MedicalTherapyId
    , MIN(vdatasetmedtxadministration.MedTxAdminStartDate) AS InductionStartDate
    , COUNT(*) as CycleCount
    FROM caisis.vdatasetmedtxadministration
    GROUP BY MedicalTherapyId;

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
    LEFT JOIN caisis.v_firsttreatment d
        ON a.MedicalTherapyId = d.MedicalTherapyId
ORDER BY a.PtMRN, a.MedTxDate, MedTxAdminStartDate;
SELECT * FROM caisis.v_treatment;


--     SELECT
--         `a`.`PtMRN` AS `PtMRN`,
--         `a`.`PatientId` AS `PatientId`,
--         `a`.`MedicalTherapyId` AS `MedicalTherapyId`,
--         (CASE
--             WHEN ISNULL(`a`.`MedTxCycle`) THEN -(9)
--             ELSE `a`.`MedTxCycle`
--         END) AS `CycleCount`,
--         `d`.`InductionStartDate` AS `InductionStartDate`,
--         (CASE
--             WHEN (`b`.`MedTxAdminStartDate` = `d`.`InductionStartDate`) THEN 'Yes'
--             WHEN (`a`.`MedTxDate` = `b`.`MedTxAdminStartDate`) THEN 'Yes'
--             ELSE ''
--         END) AS `IsFirstCycle`,
--         `a`.`MedTxDate` AS `InductionDate`,
--         `b`.`MedTxAdminStartDate` AS `CycleDate`,
--         `a`.`MedTxAgent` AS `InductionAgent`,
--         `b`.`MedTxAdminAgent` AS `CycleAgent`,
--         `a`.`MedTxType` AS `InductionType`,
--         `b`.`MedTxAdminType` AS `CycleType`,
--         `a`.`MedTxDisease` AS `Disease`,
--         `a`.`MedTxIndication` AS `TreatmentIndication`,
--         `c`.`Categorized` AS `InductionCategory`,
--         `c`.`Intensity` AS `InductionIntensity`
--     FROM
--         (((`caisis`.`vdatasetmedicaltherapy` `a`
--         LEFT JOIN `caisis`.`vdatasetmedtxadministration` `b` ON ((`a`.`MedicalTherapyId` = `b`.`MedicalTherapyId`)))
--         LEFT JOIN `protocolcategory`.`cherrypicking` `c` ON ((`a`.`MedTxAgent` = `c`.`MedTxAgent`)))
--         LEFT JOIN (SELECT
--             `caisis`.`vdatasetmedtxadministration`.`MedicalTherapyId` AS `MedicalTherapyId`,
--                 MIN(`caisis`.`vdatasetmedtxadministration`.`MedTxAdminStartDate`) AS `InductionStartDate`
--         FROM
--             `caisis`.`vdatasetmedtxadministration`
--         GROUP BY `caisis`.`vdatasetmedtxadministration`.`MedicalTherapyId`) `d` ON ((`a`.`MedicalTherapyId` = `d`.`MedicalTherapyId`)))
