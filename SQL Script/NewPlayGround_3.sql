/************************************************************************************************
    NPM1:  Update to find the molecular information closest to arrival 
*/

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


ALTER TABLE `molecular`.`range` 
    ADD COLUMN `arrival_id` DOUBLE NULL DEFAULT NULL AFTER `EndDateRange`;
    
UPDATE `molecular`.`range` a, caisis.`arrivalidmapping` b
    SET a.arrival_id = b.arrival_id
    WHERE a.Patientid = b.Patientid and a.ArrivalDate = b.ArrivalDate ;

DROP TABLE IF EXISTS molecular.cp ;

SELECT * FROM molecular.range ;

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
        LEFT JOIN caisis.v_npm1mutation as t2 on t1.UWID = t2.PtMRN
        WHERE t2.LabDate between t1.StartDateRange and t1.EndDateRange
            AND   UPPER(LabResult) NOT RLIKE 'ORDER'
            AND   UPPER(LabResult) NOT RLIKE 'NOT NEEDED'
            AND   UPPER(LabResult) NOT RLIKE 'WRONG'
            AND   UPPER(LabResult) NOT RLIKE 'CORRECTION'
            AND   UPPER(LabResult) NOT RLIKE 'SEE INTERPRETATION'
            AND   UPPER(LabResult) NOT RLIKE 'SPECIMEN RECEIVED'
        ORDER BY t1.UWID, t1.TargetDate, DaysFromTarget;

DROP TABLE IF EXISTS molecular.v_npm1mutation ;
CREATE TABLE molecular.v_npm1mutation
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
ALTER TABLE 
    ADD INDEX `PatientId`   (`PatientId`   ASC),
    ADD INDEX `PtMRN`       (`PtMRN`(10)   ASC),
    ADD INDEX `UWID`        (`UWID`(10)    ASC),
    ADD INDEX `LabDate`     (`LabDate`     ASC),
    ADD INDEX `ArrivalDate` (`ArrivalDate` ASC);

DROP TABLE IF EXISTS molecular.t3 ;





/*
select * FROM caisis.playground ;
SELECT * FROM caisis.molecularlabs ;
SELECT * FROM molecular.range 
SELECT * FROM molecular.v_npm1mutation;
*/

/*
Have Vasta Global look for date of death for these patients
*/
DROP TABLE IF EXISTS temp.FindLastContactDate ;
CREATE TABLE temp.FindLastContactDate
    SELECT * FROM temp.allarrivals
        WHERE PtDeathDate IS NULL
        AND LastInformationDate < '2017-10-01'
        AND BackBoneName IS NOT NULL
        #  AND Response LIKE '%cr%'
        AND YEAR(ArrivalDate) > 2008;

SELECT PtMRN, MIN(arrivalDate) AS FirstArrivalDate
        , PtBirthdate
        , PtLastName
        , PtDeathDate
        , PtDeathType
        , LastStatusDate
        , LastStatusType
        , LastInformationDate
        FROM temp.FindLastContactDate
        GROUP BY 1;

