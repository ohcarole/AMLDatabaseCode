# Starting the effort to identify TRM for patients.  Issue is that we
# only want to count protocols that are actually induction or currative treatment.

/*
SELECT DISTINCT
    PtMRN
    , PatientId
    , MedTxDate
    , MedTxAgent
    , Categorized
    , Intensity
    , Wildcard
FROM ProtocolCategory.PatientProtocol ;

SELECT * FROM TEMP.FirstTxDate;

SELECT * FROM Caisis.`v_arrival_with_prev_next`;
*/

/*************************************************************************************
Patients with an arrival date joined to view arrival with next in
order to find a range between which all relevant dates must fall
*/
DROP TABLE IF EXISTS temp.Result1 ;
CREATE TABLE temp.Result1
    SELECT a.PtMRN
        , a.PatientId
        , StatusDisease AS ArrivalDx # test comment
        , StatusDate AS ArrivalDate
        , YEAR(StatusDate) AS ArrivalYear
        , CAST(NULL AS DATETIME) AS TreatmentStartDate
        , CAST(NULL AS DATETIME) AS ResponseDate
        , CAST(NULL AS CHAR(15)) AS Response
        , CAST(NULL AS CHAR(40)) AS FlowSource
        , CAST(NULL AS DECIMAl(7,2)) AS FlowBlasts
        , b.NextArrivalDate
        , StatusDate AS TargetDate
        , StatusDate AS FirstRangeDate # 3 days before arrival
        , CASE # up to 100 days after arrival
            WHEN b.NextArrivalDate BETWEEN a.StatusDate AND DATE_ADD(a.StatusDate, INTERVAL 100 DAY) THEN b.NextArrivalDate
            WHEN b.NextArrivalDate > DATE_ADD(a.StatusDate, INTERVAL 100 DAY)                        THEN NULL
            WHEN b.NextArrivalDate IS NULL                                                           THEN NULL
            ELSE NULL
        END AS LastRangeDate
    FROM caisis.vdatasetstatus a
    LEFT JOIN caisis.vdatasetarrivalwithprevnext b ON a.PatientId = b.PatientId and a.StatusDate = b.ArrivalDate
    WHERE a.status like '%work%' 
    ;

DELETE FROM temp.Result1 
    WHERE ArrivalDx NOT LIKE '%aml%' 
    AND   ArrivalDx NOT LIKE '%mds%';
    ;




/*
SELECT * FROM caisis.vdatasetstatus 
    WHERE status like '%work%' and patientid is not null 
    ORDER BY PtMRN, StatusDate ;
SELECT * FROM caisis.vdatasetarrivalwithprevnext 
    ORDER BY PtMRN, ArrivalDate;

SELECT * FROM caisis.vdatasetstatus a JOIN caisis.vdatasetarrivalwithprevnext b on a.PtMRN = b.PtMRN and a.StatusDate = b.ArrivalDate ;

SET @i:=0;
SET @j:=0;
SELECT * FROM 
(SELECT @i:=@i+1 as Id, a.* FROM caisis.vdatasetstatus a WHERE status like '%work%' and patientid is not null ORDER BY PtMRN, StatusDate ) a
    JOIN 
(SELECT @j:=@j+1 as Id, a.* FROM caisis.vdatasetarrivalwithprevnext a ORDER BY PtMRN, ArrivalDate ) b
on a.id = b.id and a.ptmrn = b.ptmrn and a.statusdate = b.arrivaldate ;


SELECT s.PtMRN, a.* FROM caisis.vdatasetstatus a 
    JOIN (SELECT a.PtMRN FROM (SELECT PtMRN, StatusDate, COUNT(*) FROM caisis.vdatasetstatus 
                WHERE status like '%work%' GROUP BY PtMRN, StatusDate ) a
            LEFT JOIN (SELECT PtMRN, ArrivalDate, COUNT(*) FROM caisis.vdatasetarrivalwithprevnext GROUP BY PtMRN, ArrivalDate) b
            ON a.PtMRN = b.PtMRN AND a.StatusDate = b.ArrivalDate
            WHERE b.PtMRN IS NULL) s 
        ON a.PtMRN = s.PtMRN
        WHERE a.status like '%work%' ;

SELECT s.PtMRN, a.* FROM caisis.vdatasetstatus a 
    JOIN (SELECT a.PtMRN FROM (SELECT PtMRN, StatusDate, COUNT(*) FROM caisis.vdatasetstatus 
                WHERE status like '%work%' GROUP BY PtMRN, StatusDate ) a
            RIGHT JOIN (SELECT PtMRN, ArrivalDate, COUNT(*) FROM caisis.vdatasetarrivalwithprevnext GROUP BY PtMRN, ArrivalDate) b
            ON a.PtMRN = b.PtMRN AND a.StatusDate = b.ArrivalDate
            WHERE a.PtMRN IS NULL) s 
        ON a.PtMRN = s.PtMRN
        WHERE a.status like '%work%' ;

SELECT * FROM temp.Result1;
*/

/*************************************************************************************
Find all treatments where a treatment was actually given
*/
DROP TABLE IF EXISTS Temp.Treatment ;
CREATE TABLE Temp.Treatment
    SELECT * FROM caisis.vdatasetmedicaltherapy
        where (medtxdisease like '%aml%' or medtxdisease like '%mds%')
        and upper(medtxagent) not in ('HU'
            , 'HU/PALLIATIVE'
            , 'PALLIATIVE CARE'
            , 'NO TREATMENT'
            , 'HOSPICE'
            , 'UNKNOWN'
        )
        AND MedTxIntent NOT IN ( 'Consolidation Regimen'
            , 'Immunosuppressive Regimen'
            , 'Maintenance Regimen'
            , 'Other Regimen'
            , 'Palliative Regimen'
            , 'Consolidation'
            , 'Immunosuppressive'
            );


DROP TABLE IF EXISTS Temp.NonTreatment ;
CREATE TABLE Temp.NonTreatment
    SELECT * FROM caisis.vdatasetmedicaltherapy
        where (medtxdisease like '%aml%' or medtxdisease like '%mds%')
        and upper(medtxagent) in ('HU'
            , 'HU/PALLIATIVE'
            , 'PALLIATIVE CARE'
            , 'NO TREATMENT'
            , 'HOSPICE'
            , 'UNKNOWN'
        
        OR MedTxIntent NOT IN ( 'Consolidation Regimen'
            , 'Immunosuppressive Regimen'
            , 'Maintenance Regimen'
            , 'Other Regimen'
            , 'Palliative Regimen'
            , 'Consolidation'
            , 'Immunosuppressive'
            ));

/*
SELECT * FROM temp.Treatment;
SELECT * FROM temp.NonTreatment;
*/


/*************************************************************************************
Create a table of all possible treatment dates that fall in the (arrival to
next arrival
Fill in treatment start date
*/
DROP TABLE IF EXISTS Temp.Result2 ;
CREATE TABLE Temp.Result2
    SELECT a.PtMRN
        , a.PatientId
        , a.ArrivalDx
        , CASE
            WHEN b.MedTxDate BETWEEN a.FirstRangeDate AND a.LastRangeDate
                 AND b.MedTxDate BETWEEN a.FirstRangeDate AND DATE_ADD(a.FirstRangeDate, INTERVAL 100 DAY)
                 THEN 'In range and w/i 100 days'
            WHEN b.MedTxDate BETWEEN a.FirstRangeDate AND a.LastRangeDate
                THEN 'In range'
            WHEN b.MedTxIntent LIKE '%induct%'
                AND a.ArrivalDx LIKE '%nd%'
                AND b.MedTxDate BETWEEN a.FirstRangeDate AND DATE_ADD(a.FirstRangeDate, INTERVAL 100 DAY)
                THEN 'Induction within 100 days of arrival'
            WHEN b.MedTxDate BETWEEN a.FirstRangeDate AND DATE_ADD(a.FirstRangeDate, INTERVAL 100 DAY)
                THEN 'Within 100 days of arrival'
            ELSE 'No relevant treatments w/i 100 days and before the next arrival'
        END AS TreatmentRelevance
        , a.ArrivalDate
        , a.ArrivalYear
        , b.MedTxDate AS TreatmentStartDate
        , b.MedTxIntent
        , b.MedTxType
        , b.MedTxAgent AS OriginalMedTxAgent
        , LTRIM(RTRIM(UPPER(b.MedTxAgent))) AS MedTxAgent
        , LTRIM(RTRIM(UPPER(b.MedTxAgent))) AS MedTxAgentNoParen
        , LOCATE('(',LTRIM(RTRIM(UPPER(b.MedTxAgent)))) AS FirstParen
        , LENGTH(LTRIM(RTRIM(UPPER(b.MedTxAgent)))) - LOCATE(')',LTRIM(RTRIM(REVERSE(UPPER(b.MedTxAgent)))))  AS LastParen
        , CAST(NULL AS DATETIME) AS ResponseDate
        , CAST(NULL AS CHAR(15)) AS Response
        , CAST(NULL AS DATETIME) AS FlowDate
        , CAST(NULL AS CHAR(40)) AS FlowSource
        , CAST(NULL AS DECIMAl(7,2)) AS FlowBlasts
        , CAST(NULL AS CHAR(45)) AS RelapseType
        , CAST(NULL AS DATETIME) AS RelapseDate
        , CAST(NULL AS CHAR(45)) AS RelapseDisease

/*
        , Status AS RelapseType
        , StatusDate AS RelapseDate
        , StatusDisease AS RelapseDisease
        , StatusNotes AS RelapseNotes
*/

        , a.NextArrivalDate
        , a.TargetDate
        , a.FirstRangeDate
        , a.LastRangeDate
        , TIMESTAMPDIFF(DAY,a.FirstRangeDate,b.MedTxDate) AS DaysFromArrivaltoTreatment
        , 'Not First Treatment' AS FirstTreatment
    FROM Temp.Result1 a
    LEFT JOIN Temp.Treatment b
    ON a.PatientId = b.PatientId
        AND CASE
                WHEN b.MedTxDate BETWEEN a.FirstRangeDate AND a.LastRangeDate THEN TRUE
                WHEN a.LastRangeDate IS NULL
                    AND b.MedTxDate BETWEEN a.FirstRangeDate AND DATE_ADD(a.FirstRangeDate, INTERVAL 100 DAY) THEN TRUE
                ELSE FALSE
            END
        ORDER BY PtMRN, ArrivalDate, TreatmentStartDate;

UPDATE TEMP.Result2 SET MedTxAgentNoParen = LTRIM(RTRIM(REPLACE(MedTxAgent, SUBSTRING(MedTxAgent,FirstParen,LastParen-FirstParen+2),''))) ;


/*
POTENTIAL DATA CLEANING AREA:
These don't make sense, if relapse or refractory, MedTxIntent is a salvage, not reinduction.
However the situation may be that these are actually second cycles of induction and should
have been recorded on the medical therapy administrations (cycles) sub-form
SELECT * FROM temp.Result2 where MedTxIntent LIKE '%re%' and MedTxIntent LIKE '%induction%';

*/

DROP TABLE IF EXISTS Temp.FirstTreatment ;
CREATE TABLE Temp.FirstTreatment
SELECT PtMRN
    , PatientId
    , ArrivalDx
    , ArrivalDate
    , TreatmentStartDate
    , MIN(TIMESTAMPDIFF(DAY,FirstRangeDate,TreatmentStartDate)) AS DaysFromArrivaltoTreatment
    , NextArrivalDate
FROM Temp.Result2
GROUP BY PtMRN, PatientId, ArrivalDx, ArrivalDate, TreatmentStartDate, NextArrivalDate
ORDER BY PtMRN, PatientId, ArrivalDate, TreatmentStartDate ;

UPDATE temp.Result2 SET FirstTreatment = '';

UPDATE temp.Result2 a, temp.FirstTreatment b
    SET FirstTreatment = 'Is First'
    WHERE a.PatientId = b.PatientId
    AND   a.ArrivalDate = b.ArrivalDate
    AND   a.ArrivalDx   = b.ArrivalDx
    AND   a.DaysFromArrivaltoTreatment = b.DaysFromArrivaltoTreatment ;

SELECT * FROM temp.Result2 ;
SELECT * FROM temp.FirstTreatment ;
SELECT * FROM temp.NonTreatment ;

UPDATE temp.Result2 a, temp.NonTreatment b
    SET a.TreatmentStartDate = b.MedTxDate
        , a.MedTxIntent = b.MedTxIntent
        , a.MedTxType = b.MedTxType
        , a.OriginalMedTxAgent = b.MedTxAgent
        , a.MedTxAgent = LTRIM(RTRIM(UPPER(a.MedTxAgent)))
        , a.MedTxAgentNoParen = LTRIM(RTRIM(UPPER(a.MedTxAgent)))
        # , a.FirstParen = LOCATE('(',LTRIM(RTRIM(UPPER(b.MedTxAgent))))
        # , a.LastParen = LENGTH(LTRIM(RTRIM(UPPER(b.MedTxAgent)))) - LOCATE(')',LTRIM(RTRIM(REVERSE(UPPER(b.MedTxAgent)))))
        WHERE a.PtMRN = b.PtMRN
            AND CASE
                WHEN b.MedTxDate BETWEEN a.ArrivalDate and date_add(a.ArrivalDate, INTERVAL 100 DAY) THEN True
                WHEN b.MedTxDate > ArrivalDate and a.NextArrivalDate IS NULL THEN True
                ELSE False
            END
            AND a.ArrivalDx = b.MedTxDisease
            AND a.TreatmentStartDate IS NULL
            AND a.MedTxAgent IS NULL ;


ALTER TABLE `temp`.`Result2` DROP COLUMN `LastParen`, DROP COLUMN `FirstParen`;


/*
SELECT * FROM temp.result2;
SELECT PtMRN
    , PatientId
    , ArrivalDx
    , TreatmentRelevance
    , ArrivalDate
    , ArrivalYear
    , TreatmentStartDate
    , NextArrivalDate
    , MedTxIntent
    , OriginalMedTxAgent
    FROM temp.result2
    ORDER BY PtMRN, ArrivalDate, TreatmentStartDate;
select * from temp.treatment;
*/


ALTER TABLE `temp`.`result2`
    ADD COLUMN `Intensity` mediumtext NULL AFTER `MedTxIntent`
    , ADD COLUMN `AnthracyclinDose` INTEGER NULL AFTER `MedTxIntent`
    , ADD COLUMN `Anthracyclin` VARCHAR(255) NULL AFTER `MedTxIntent`
    , ADD COLUMN `BackboneAddOn` VARCHAR(255) NULL AFTER `MedTxIntent`
    , ADD COLUMN `BackboneName` VARCHAR(225) NULL AFTER `MedTxIntent`
    , ADD COLUMN `BackboneType` VARCHAR(20) NULL AFTER `MedTxIntent`
    , CHANGE COLUMN `Response` `Response` TEXT NULL DEFAULT NULL
    , ADD COLUMN `FlowBlastsText` VARCHAR(45) NULL AFTER `FlowBlasts`
    , ADD COLUMN `RelapseNotes` TEXT NULL AFTER `RelapseDate`;


UPDATE temp.Result2 a, caisis.backbonemapping b
    SET a.BackBoneType = b.BackBoneType
        , a.BackBoneName = b.BackBoneName
        , a.BackBoneAddOn = b.BackBoneAddOn
    where a.OriginalMedTxAgent = b.OriginalMedTxAgent
        AND a.BackBoneName IS NULL ;

UPDATE temp.Result2 a, caisis.backbonemapping b
    SET a.BackBoneType = b.BackBoneType
        , a.BackBoneName = b.BackBoneName
        , a.BackBoneAddOn = b.BackBoneAddOn
    where a.MedTxAgent = b.OriginalMedTxAgent
        AND a.BackBoneName IS NULL ;

UPDATE temp.Result2 a, caisis.backbonemapping b
    SET a.BackBoneType = b.BackBoneType
        , a.BackBoneName = b.BackBoneName
        , a.BackBoneAddOn = b.BackBoneAddOn
    where a.MedTxAgent = b.MedTxAgent
        AND a.BackBoneName IS NULL ;

UPDATE temp.Result2 a, caisis.backbonemapping b
    SET a.BackBoneType = b.BackBoneType
        , a.BackBoneName = b.BackBoneName
        , a.BackBoneAddOn = b.BackBoneAddOn
    where a.MedTxAgentNoParen = b.MedTxAgent
        AND a.BackBoneName IS NULL ;

UPDATE temp.Result2 a, caisis.backbonemapping b
    SET a.BackBoneType = b.BackBoneType
        , a.BackBoneName = b.BackBoneName
        , a.BackBoneAddOn = b.BackBoneAddOn
    where a.OriginalMedTxAgent = b.OriginalProtocol
        AND a.BackBoneName IS NULL ;

UPDATE temp.Result2 a, caisis.backbonemapping b
    SET a.BackBoneType = b.BackBoneType
        , a.BackBoneName = b.BackBoneName
        , a.BackBoneAddOn = b.BackBoneAddOn
    where a.MedTxAgent = b.OriginalProtocol
        AND a.BackBoneName IS NULL ;

UPDATE temp.Result2 a, caisis.backbonemapping b
    SET a.BackBoneType = b.BackBoneType
        , a.BackBoneName = b.BackBoneName
        , a.BackBoneAddOn = b.BackBoneAddOn
    where a.MedTxAgentNoParen = b.OriginalProtocol
        AND a.BackBoneName IS NULL ;


/**********************************************************************************************************
In the code below I have currently been trying to make sure that regimens are associated correctly with arrival following these rules
    treatment should be after arrival
    treatment should not be after the next arrival if there is one
    treatment should not start more than 100 days after arrival
    there should only be one treatment after an arrival
    treatment should be for curative intent
*********************************************************************************************************/

/*
Use for data cleaning, trying to find multiple regimens of interest associated with an
arrival in order to have abstraction staff do data cleaning.
SELECT a.PtMRN
    , a.PatientId
    , a.ArrivalDx
    , a.ArrivalDate
    , a.ArrivalYear
    , a.TreatmentStartDate
    , a.MedTxIntent
    , a.MedTxType
    , a.OriginalMedTxAgent AS MedTxAgent
    , a.BackboneName
    , a.BackboneAddOn
    , a.NextArrivalDate
    , a.TargetDate
    , a.FirstRangeDate
    , a.lastrangedate
    , a.DaysFromArrivaltoTreatment
    , a.FirstTreatment
    FROM temp.Result2 a
    join (SELECT count(*), PtMRN, ArrivalDate FROM temp.result2
        GROUP BY PtMRN, ArrivalDate
        Having count(*) > 1) b
        ON a.PtMRN = b.PtMRN and a.ArrivalDate = b.ArrivalDate
        WHERE a.DaysFromArrivaltoTreatment <= 100 AND ArrivalYear > 2008;


SELECT a.PtMRN
    , a.PatientId
    , a.ArrivalDx
    , a.ArrivalDate
    , a.ArrivalYear
    , a.TreatmentStartDate
    , a.MedTxIntent
    , a.MedTxType
    , a.OriginalMedTxAgent AS MedTxAgent
    , a.BackboneName
    , a.BackboneAddOn
    , a.NextArrivalDate
    , a.TargetDate
    , a.FirstRangeDate
    , a.lastrangedate
    , a.DaysFromArrivaltoTreatment
    , a.FirstTreatment
    FROM temp.Result2 a
    LEFT join (SELECT count(*), PtMRN, ArrivalDate FROM temp.result2
        GROUP BY PtMRN, ArrivalDate Having count(*) = 0) b
        ON a.PtMRN = b.PtMRN and a.ArrivalDate = b.ArrivalDate
        WHERE ArrivalYear > 2008
        AND MedTxAgent IS NULL ;

*/

/*
SELECT count(*), * FROM temp.result2 GROUP BY PtMRN, ArrivalDate ;
SELECT * FROM caisis.backbonemapping ;
SELECT * FROM temp.Result2 where treatment is not null and backbonename is null;
# figure out how many are not mapped.  I think most of these should be newer data
SELECT Treatment, year(treatmentstartdate), count(*) FROM temp.Result2 WHERE treatment is not null and backbonename is null GROUP BY 1, 2;


DROP TABLE IF EXISTS Temp.FirstTreatment ;
CREATE TABLE Temp.FirstTreatment
SELECT PtMRN
    , PatientId
    , ArrivalDx
    , ArrivalDate
    , TreatmentStartDate
    , MIN(TIMESTAMPDIFF(DAY,FirstRangeDate,TreatmentStartDate)) AS DaysFromArrivaltoTreatment
    , NextArrivalDate
FROM Temp.Result2
GROUP BY PtMRN, PatientId, ArrivalDx, ArrivalDate, TreatmentStartDate, NextArrivalDate
ORDER BY PtMRN, PatientId, ArrivalDate, TreatmentStartDate ;

UPDATE temp.Result2 SET FirstTreatment = '';

UPDATE temp.Result2 a, temp.FirstTreatment b
    SET FirstTreatment = 'Is First'
    WHERE a.PatientId = b.PatientId
    AND   a.ArrivalDate = b.ArrivalDate
    AND   a.ArrivalDx   = b.ArrivalDx
    AND   a.DaysFromArrivaltoTreatment = b.DaysFromArrivaltoTreatment ;

SELECT * FROM temp.Result2 ;
SELECT * FROM temp.FirstTreatment ;
SELECT * FROM temp.NonTreatment ;

UPDATE temp.Result2 a, temp.NonTreatment b
    SET a.TreatmentStartDate = b.MedTxDate
        , a.MedTxIntent = b.MedTxIntent
        , a.MedTxType = b.MedTxType
        , a.OriginalMedTxAgent = b.MedTxAgent
        , a.MedTxAgent = LTRIM(RTRIM(UPPER(a.MedTxAgent)))
        , a.MedTxAgentNoParen = LTRIM(RTRIM(UPPER(a.MedTxAgent)))
        # , a.FirstParen = LOCATE('(',LTRIM(RTRIM(UPPER(b.MedTxAgent))))
        # , a.LastParen = LENGTH(LTRIM(RTRIM(UPPER(b.MedTxAgent)))) - LOCATE(')',LTRIM(RTRIM(REVERSE(UPPER(b.MedTxAgent)))))
        WHERE a.PtMRN = b.PtMRN
            AND CASE
                WHEN b.MedTxDate BETWEEN a.ArrivalDate and date_add(a.ArrivalDate, INTERVAL 100 DAY) THEN True
                WHEN b.MedTxDate > ArrivalDate and a.NextArrivalDate IS NULL THEN True
                ELSE False
            END
            AND a.ArrivalDx = b.MedTxDisease
            AND a.TreatmentDate IS NULL
            AND a.MedTxAgent IS NULL ;

SELECT a.PtMRN
    , a.PatientId
    , a.ArrivalDx
    , a.ArrivalDate
    , a.ArrivalYear
    , a.TreatmentStartDate
    , a.MedTxIntent
    , a.NextArrivalDate
    , b.MedTxDate
    , b.MedTxType
    , b.MedTxDisease
    , b.MedTxIndication
    , b.MedTxIntent
    , b.MedTxAgent
FROM temp.Result2 a
    JOIN temp.NonTreatment b
    ON a.PtMRN = b.PtMRN
    WHERE a.TreatmentStartDate IS NULL # = 'No relevant treatments w/i 100 days and before the next arrival'
    AND CASE
        WHEN b.MedTxDate BETWEEN a.ArrivalDate and date_add(a.ArrivalDate, INTERVAL 100 DAY) THEN True
        WHEN b.MedTxDate > ArrivalDate and a.NextArrivalDate IS NULL THEN True
        ELSE False
    END ;

SELECT * FROM temp.FirstTreatment GROUP BY PtMRN, ArrivalDate, ArrivalDx, TreatmentStartDate;


*/



SELECT PtMRN
    , PatientId
    , ArrivalDx
    , ArrivalDate
    , TreatmentStartDate
    , TIMESTAMPDIFF(DAY,FirstRangeDate,TreatmentStartDate) AS DaysFromArrivaltoTreatment
    , NextArrivalDate
FROM Temp.Result2
GROUP BY PtMRN, PatientId, ArrivalDx, ArrivalDate, TreatmentStartDate, NextArrivalDate
ORDER BY PtMRN, PatientId, ArrivalDate, TreatmentStartDate ;

# Multiple treatments
SELECT b.* FROM temp.result2 b JOIN
    (SELECT PtMRN
        , ArrivalDx
        , TreatmentStartDate
        FROM temp.Result2
        GROUP BY PtMRN, ArrivalDx, ArrivalDate, TreatmentStartDate
        HAVING count(*) > 1 ) a
    ON b.PtMRN = a.PtMRN and b.ArrivalDx = a.ArrivalDx and a.TreatmentStartDate = b.TreatmentStartDate;


SELECT * FROM temp.FirstTreatment ;

DROP TABLE IF EXISTS temp.Response ;
CREATE TABLE Temp.Response
    SELECT b.`PtMRN`,
        b.`PatientId`,
        b.`ArrivalDx`,
        b.`ArrivalDate`,
        b.`ArrivalYear`,
        b.`TreatmentStartDate`,
        b.`BackboneType`,
        b.`BackboneName`,
        b.`BackboneAddOn`,
        b.`Anthracyclin`,
        b.`AnthracyclinDose`,
        b.`Intensity`,
        b.`OriginalMedTxAgent`,
        b.`MedTxAgent`,
        b.`MedTxAgentNoParen`,
        b.`FlowSource`,
        b.`FlowBlasts`,
        b.`NextArrivalDate`,
        b.`TargetDate`,
        b.`FirstRangeDate`,
        b.`lastrangedate`,
        b.`DaysFromArrivaltoTreatment`
        , TIMESTAMPDIFF(DAY,b.`TreatmentStartDate`,a.`StatusDate`) AS `DaysFromTreatmenttoResponse`
        , b.`FirstTreatment`
        , a.StatusDisease AS ResponseDisease
        , a.Status AS ResponseInRange
        , a.StatusDate AS ResponseDate
        FROM caisis.vdatasetstatus a
            JOIN temp.result2 b
                ON a.PtMRN = b.PtMRN
                AND a.StatusDisease = ArrivalDx
                AND CASE

                    # when a treatment date is available
                    WHEN b.TreatmentStartDate IS NOT NULL THEN
                        CASE
                            WHEN a.StatusDate BETWEEN Date_add(b.TreatmentStartDate, INTERVAL 7 DAY) AND Date_add(b.TreatmentStartDate, INTERVAL 90 DAY)
                                AND a.StatusDate < b.NextArrivalDate
                                 THEN True  # Response 7-90 days after treatment and before next arrival
                            WHEN a.StatusDate BETWEEN Date_add(b.TreatmentStartDate, INTERVAL 7 DAY)  AND Date_add(b.TreatmentStartDate, INTERVAL 90 DAY)
                                AND b.NextArrivalDate IS NULL
                                 THEN True  # Response 7-90 days after treatment when no next arrival
                            WHEN a.StatusDate BETWEEN Date_add(b.TreatmentStartDate, INTERVAL 7 DAY) AND b.NextArrivalDate
                                 THEN True  # Response >90 days after treatment and before next arrival
                            ELSE False
                        END

                    WHEN b.TreatmentStartDate IS NULL THEN # When no treatment date
                        CASE
                            WHEN a.StatusDate BETWEEN Date_add(b.ArrivalDate, INTERVAL 7 DAY) AND Date_add(b.ArrivalDate, INTERVAL 100 DAY)
                                AND a.StatusDate < b.NextArrivalDate
                                THEN True  # Response 7-90 days after arrival and before next arrival
                            WHEN a.StatusDate BETWEEN Date_add(b.ArrivalDate, INTERVAL 7 DAY)  AND Date_add(b.ArrivalDate, INTERVAL 100 DAY)
                                AND b.NextArrivalDate IS NULL
                                 THEN True  # Response 7-90 days after arrival when no next arrival
                            WHEN a.StatusDate BETWEEN Date_add(b.ArrivalDate, INTERVAL 7 DAY) AND b.NextArrivalDate
                                 THEN True  # Response >90 days after arrival and before next arriva
                            ELSE False
                        END
                    ELSE NULL  # shouldn't happen
                END
                /*add in many conditions, next arrival is null, too many days later, too close to arrival date for response*/
        WHERE (StatusDisease LIKE '%aml%' OR StatusDisease like '%apl%')
            AND (Status like '%unk response%'
            OR Status In (
            '+CRp'
            , 'CR'
            , 'CR CYTO MRD'
            , 'CR MRD'
            , 'CR-MRD'
            , 'Cri'
            , 'CRi CYTO MRD'
            , 'Cri MRD'
            , 'CRi-MRD'
            , 'CRp'
            , 'CRp CYTO MRD'
            , 'CRp EXD MRD'
            , 'CRp MRD'
            , 'PR'
            , 'Resistant'
            , 'Refractory'
            , 'Death'
            , 'Response:  Death'
            , 'Response: Death'
            , 'Persistent Disease'
            , 'Dead of Disease'
            , 'Response Not Categorized'
        ));

UPDATE temp.result2 a, temp.response b
    SET a.Response = b.ResponseInRange,
        a.ResponseDate = b.ResponseDate
        WHERE a.PtMRN       = b.PtMRN
        AND   a.ArrivalDate = b.ArrivalDate
        AND   a.ArrivalDx   = b.ArrivalDx ;




/*
Data cleaning, patients with multiple responses to treatment
SELECT b.PtMRN
        , b.ArrivalDx
        , b.ArrivalDate
        , b.TreatmentStartDate
        , b.BackboneName
        , b.BackboneAddOn
        , b.OriginalMedTxAgent
        , b.NextArrivalDate
        , b.DaysFromArrivaltoTreatment
        , b.DaysFromTreatmenttoResponse
        , b.ResponseDisease
        , b.ResponseInRange
        , b.ResponseDate
    FROM temp.response b
    join (SELECT COUNT(*), PtMRN, ArrivalDx, ArrivalDate, max(DaysFromTreatmentToResponse) AS DaysFromTreatmentToResponse
        FROM temp.Response a GROUP BY 2,3,4 Having Count(*)>1 ) a
    ON  a.PtMRN = b.PtMRN
    AND a.ArrivalDx = b.ArrivalDx
    AND a.ArrivalDate = b.ArrivalDate
    WHERE a.DaysFromTreatmentToResponse <= 100 ;


SELECT DISTINCT b.PtMRN
        , b.ArrivalDx
        , b.ArrivalDate
        , b.NextArrivalDate
        , b.ResponseDisease
        , b.ResponseInRange
        , b.ResponseDate
    FROM temp.response b
    join (SELECT COUNT(*), PtMRN, ArrivalDx, ArrivalDate, max(DaysFromTreatmentToResponse) AS DaysFromTreatmentToResponse
        FROM temp.Response a GROUP BY 2,3,4 Having Count(*)>1 ) a
    ON  a.PtMRN = b.PtMRN
    AND a.ArrivalDx = b.ArrivalDx
    AND a.ArrivalDate = b.ArrivalDate
    WHERE a.DaysFromTreatmentToResponse <= 100 ;


SELECT PtMRN, ResponseDate, Count(*) FROM (SELECT DISTINCT b.PtMRN
        , b.ArrivalDx
        , b.ArrivalDate
        , b.NextArrivalDate
        , b.ResponseDisease
        , b.ResponseInRange
        , b.ResponseDate
    FROM temp.response b
    join (SELECT COUNT(*), PtMRN, ArrivalDx, ArrivalDate, max(DaysFromTreatmentToResponse) AS DaysFromTreatmentToResponse
        FROM temp.Response a GROUP BY 2,3,4 Having Count(*)>1 ) a
    ON  a.PtMRN = b.PtMRN
    AND a.ArrivalDx = b.ArrivalDx
    AND a.ArrivalDate = b.ArrivalDate
    WHERE a.DaysFromTreatmentToResponse <= 100) a GROUP BY PtMRN, ResponseDate;



Data cleaning, convert Not Categorized to Response Not Categorized
SELECT Status
        , CASE WHEN (Status like '%unk response%'
            OR Status In (
            '+CRp'
            , 'CR'
            , 'CR CYTO MRD'
            , 'CR MRD'
            , 'CR-MRD'
            , 'Cri'
            , 'CRi CYTO MRD'
            , 'Cri MRD'
            , 'CRi-MRD'
            , 'CRp'
            , 'CRp CYTO MRD'
            , 'CRp EXD MRD'
            , 'CRp MRD'
            , 'PR'
            , 'Resistant'
            , 'Refractory'
            , 'Death'
            , 'Response:  Death'
            , 'Response: Death'
            , 'Persistent Disease'
            , 'Dead of Disease'
            , 'Response Not Categorized'
        )) THEN StatusDisease ELSE '' END as StatusDisease
        , (Status like '%unk response%'
            OR Status In (
            '+CRp'
            , 'CR'
            , 'CR CYTO MRD'
            , 'CR MRD'
            , 'CR-MRD'
            , 'Cri'
            , 'CRi CYTO MRD'
            , 'Cri MRD'
            , 'CRi-MRD'
            , 'CRp'
            , 'CRp CYTO MRD'
            , 'CRp EXD MRD'
            , 'CRp MRD'
            , 'PR'
            , 'Resistant'
            , 'Refractory'
            , 'Death'
            , 'Response:  Death'
            , 'Response: Death'
            , 'Persistent Disease'
            , 'Dead of Disease'
            , 'Response Not Categorized'
        )) as Response
        , count(*)
        FROM caisis.vdatasetstatus
        WHERE (StatusDisease LIKE "%AML%" OR StatusDisease LIKE "%APL%")
        group by 1;


SELECT PtMRN, StatusDate, StatusDisease, Status, StatusNotes, a.* FROM caisis.vdatasetstatus a
    WHERE (StatusDisease LIKE "%AML%" OR StatusDisease LIKE "%APL%")
    AND status IN ('Not Categorized','Unknown');

Do some error checking here, date of response must be after treatment start !!!!
select * from temp.response where DaysFromTreatmenttoResponse <= DaysFromArrivaltoTreatment OR DaysFromTreatmenttoResponse < 8;
select * from temp.response where DaysFromTreatmenttoResponse < date_add(DaysFromArrivaltoTreatment, interval 7 day);

*/

/*
SELECT * FROM temp.Result2 where FirstTreatment = 'Is First' AND treatment is not null and backbonename is null;
SELECT * FROM temp.Result2 ;

SELECT PtMRN, Count(*) FROM temp.Result2 WHERE TreatmentStartDate IS NULL GROUP BY PtMRN ;
SELECT PtMRN, Count(*) FROM temp.Result2 WHERE YEAR(ArrivalDate) = 2017 AND month(ArrivalDate) < 7 AND TreatmentStartDate IS NULL GROUP BY PtMRN ;


SELECT * FROM caisis.backbonemapping;
SELECT PtMRN, ArrivalDate, OriginalMedTxAgent, treatmentstartdate
    FROM temp.Result2
    WHERE FirstTreatment = 'Is First'
    AND OriginalMedTxAgent is not null
    AND backbonename is null
    ORDER BY 3, 4;
SELECT OriginalMedTxAgent, COUNT(*)
    FROM temp.Result2
    WHERE FirstTreatment = 'Is First'
    AND OriginalMedTxAgent is not null
    AND backbonename is null
    group BY 1;

*/

/**************************************************************************
In this section we are looking for blasts by flow at time of response
**************************************************************************

SELECT * FROM temp.Result2 ;
SELECT * FROM temp.Response ;
SELECT * FROM temp.flowblasts ;

SELECT FlowBlastsText FROM temp.Result2 ;

SELECT FlowBlastsText FROM temp.Result2
    WHERE FlowBlastsText RLIKE '\d*[\.\d]?'
    AND FlowBlastsText NOT RLIKE '\-|~'
    AND FlowBlastsText NOT RLIKE '[a-zA-Z]';

SELECT FlowBlastsText FROM temp.Result2 WHERE FlowBlastsText NOT RLIKE '\d*[\.\d]?';
SELECT FlowBlastsText FROM temp.Result2 WHERE FlowBlastsText RLIKE '\w';

*/

DROP TABLE IF EXISTS temp.flowblasts ;
CREATE TABLE temp.flowblasts
    SELECT b.* FROM caisis.vdatasetpathtest b join temp.result2 a
        on a.PtMRN = b.PtMRN
        AND timestampdiff(day,a.ResponseDate,b.DateObtained) between -7 and 7
        WHERE b.PathResult IS NOT NULL
        AND PathTest IN ( 'Blasts (FLOW POS/NEG)'
            , 'Blasts (FLOW)'
            , 'Blasts (PB FLOW)'
            , 'Blasts (PBFLOW POS/NEG)'
            , 'Blasts (PBFLOW)'
            , 'Evidence of Disease (FLOW)'
            , 'Evidence of Disease (PB FLOW)'
            , 'PB Blast (FLOW)'
        );


/***********************************************************************************
Update the result table to contain the Flow blasts at response, when the flow date
exactly matches response date
***********************************************************************************/


SELECT PathResult FROM temp.flowblasts where PathResult RLIKE "~|-|[?]|<|>" ;

SELECT * FROM temp.flowblasts where          
        CASE
             WHEN PathResult IS NULL             THEN False  # Nothing to map
             WHEN PathResult = ''                THEN False  # Nothing to map
             WHEN PathResult RLIKE "~|-|[?]|<|>" THEN True  # a dash or approximate symbol
             WHEN PathResult RLIKE '[a-zA-Z]'    THEN False  # contains letters
             WHEN PathResult RLIKE '\d*[\.\d]?'  THEN False
             ELSE False
         END ;

SELECT PathResult FROM temp.flowblasts where PathResult RLIKE "~|-|[?]|<" ;


UPDATE temp.result2 a, temp.flowblasts b
    SET a.FlowDate     = b.DateObtained
    , a.FlowSource     = b.PathTestCategory
    , a.FlowBlasts =
         CASE
             WHEN b.PathResult IS NULL             THEN NULL  # Nothing to map
             WHEN b.PathResult = ''                THEN NULL  # Nothing to map
             WHEN b.PathResult RLIKE "~|-|[?]|<|>" THEN NULL  # a dash or approximate symbol
             WHEN b.PathResult RLIKE '[a-zA-Z]'    THEN NULL  # contains letters
             WHEN b.PathResult RLIKE '\d*[\.\d]?'  THEN CAST(b.PathResult AS DECIMAL(7,3))  
             ELSE CAST(b.PathResult AS DECIMAL(7,3))
         END
    , a.FlowBlastsText =
        CASE
            WHEN b.PathResult = '-' THEN 'negative'
            WHEN b.PathResult = '+' THEN 'positive'
            WHEN b.PathResult = '<' THEN 'decreased'
            WHEN b.PathResult = '>' THEN 'increased'
            WHEN b.PathResult = '~' THEN 'unchanged'
            WHEN b.PathResult = '?' THEN 'unknown'
            ELSE LEFT(b.PathResult,45)
        END
    WHERE b.PtMRN IS NOT NULL
        AND a.FlowBlastsText IS NULL
        AND a.PtMRN = b.PtMRN
        AND a.ResponseDate = b.DateObtained
        AND b.PathTest IN ('Blasts (FLOW)') ;

/***********************************************************************************
Update the result table to contain the Flow blasts at response, when the flow date
matches response date within 7 days
AND timestampdiff(day,a.ResponseDate,b.DateObtained) between -7 and 7

SELECT timestampdiff(day,a.ResponseDate,b.DateObtained),
    a.PtMRN, a.ResponseDate, b.DateObtained, b.PathResult
    From temp.result2 a join temp.flowblasts b
        on a.PtMRN = b.PtMRN
        WHERE abs(timestampdiff(day,a.ResponseDate,b.DateObtained)) between 1 and 7;
***********************************************************************************/



UPDATE temp.result2 a, temp.flowblasts b
    SET a.FlowDate     = b.DateObtained
    , a.FlowSource     = b.PathTestCategory
     , a.FlowBlasts =
         CASE
             WHEN b.PathResult IS NULL             THEN NULL  # Nothing to map
             WHEN b.PathResult = ''                THEN NULL  # Nothing to map
             WHEN b.PathResult RLIKE "~|-|[?]|<|>" THEN NULL  # a dash or approximate symbol
             WHEN b.PathResult RLIKE '[a-zA-Z]'    THEN NULL  # contains letters
             WHEN b.PathResult RLIKE '\d*[\.\d]?'  THEN CAST(b.PathResult AS DECIMAL(7,3))  
             ELSE CAST(b.PathResult AS DECIMAL(7,3))
         END
    , a.FlowBlastsText =
        CASE
            WHEN b.PathResult = '-' THEN 'negative'
            WHEN b.PathResult = '+' THEN 'positive'
            WHEN b.PathResult = '<' THEN 'decreased'
            WHEN b.PathResult = '>' THEN 'increased'
            WHEN b.PathResult = '~' THEN 'unchanged'
            WHEN b.PathResult = '?' THEN 'unknown'
            ELSE LEFT(b.PathResult,45)
        END
    WHERE b.PtMRN IS NOT NULL
        AND a.FlowBlastsText IS NULL
        AND a.PtMRN = b.PtMRN
        AND timestampdiff(day,a.ResponseDate,b.DateObtained) between -7 and 7
        AND b.PathTest IN ('Blasts (FLOW)') ;


/***********************************************************************************
Update the result table to contain the Flow blasts at response, when the flow date
matches response date exactly
***********************************************************************************/

UPDATE temp.result2 a, temp.flowblasts b
    SET a.FlowDate     = b.DateObtained
    , a.FlowSource     = b.PathTestCategory
     , a.FlowBlasts =
         CASE
             WHEN b.PathResult IS NULL             THEN NULL  # Nothing to map
             WHEN b.PathResult = ''                THEN NULL  # Nothing to map
             WHEN b.PathResult RLIKE "~|-|[?]|<|>" THEN NULL  # a dash or approximate symbol
             WHEN b.PathResult RLIKE '[a-zA-Z]'    THEN NULL  # contains letters
             WHEN b.PathResult RLIKE '\d*[\.\d]?'  THEN CAST(b.PathResult AS DECIMAL(7,3))  
             ELSE CAST(b.PathResult AS DECIMAL(7,3))
         END
    , a.FlowBlastsText =
        CASE
            WHEN b.PathResult = '-' THEN 'negative'
            WHEN b.PathResult = '+' THEN 'positive'
            WHEN b.PathResult = '<' THEN 'decreased'
            WHEN b.PathResult = '>' THEN 'increased'
            WHEN b.PathResult = '~' THEN 'unchanged'
            WHEN b.PathResult = '?' THEN 'unknown'
            ELSE LEFT(b.PathResult,45)
        END
    WHERE b.PtMRN IS NOT NULL
        AND a.FlowBlastsText IS NULL
        AND a.PtMRN = b.PtMRN
        AND a.ResponseDate = b.DateObtained
        AND b.PathTest IN ( 'Blasts (PB FLOW)'
            , 'Blasts (PBFLOW)'
            , 'PB Blast (FLOW)'
            ) ;

/***********************************************************************************
Update the result table to contain the Flow blasts at response, when the flow date
matches response date within 7 days
***********************************************************************************/

UPDATE temp.result2 a, temp.flowblasts b
    SET a.FlowDate     = b.DateObtained
    , a.FlowSource     = b.PathTestCategory
     , a.FlowBlasts =
         CASE
             WHEN b.PathResult IS NULL             THEN NULL  # Nothing to map
             WHEN b.PathResult = ''                THEN NULL  # Nothing to map
             WHEN b.PathResult RLIKE "~|-|[?]|<|>" THEN NULL  # a dash or approximate symbol
             WHEN b.PathResult RLIKE '[a-zA-Z]'    THEN NULL  # contains letters
             WHEN b.PathResult RLIKE '\d*[\.\d]?'  THEN CAST(b.PathResult AS DECIMAL(7,3))  
             ELSE CAST(b.PathResult AS DECIMAL(7,3))
         END
    , a.FlowBlastsText =
        CASE
            WHEN b.PathResult = '-' THEN 'negative'
            WHEN b.PathResult = '+' THEN 'positive'
            WHEN b.PathResult = '<' THEN 'decreased'
            WHEN b.PathResult = '>' THEN 'increased'
            WHEN b.PathResult = '~' THEN 'unchanged'
            WHEN b.PathResult = '?' THEN 'unknown'
            ELSE LEFT(b.PathResult,45)
        END
    WHERE b.PtMRN IS NOT NULL
        AND a.FlowBlastsText IS NULL
        AND a.PtMRN = b.PtMRN
        AND timestampdiff(day,a.ResponseDate,b.DateObtained) between -7 and 7
        AND b.PathTest IN ( 'Blasts (PB FLOW)'
            , 'Blasts (PBFLOW)'
            , 'PB Blast (FLOW)'
            ) ;

/***********************************************************************************
Update the result table to contain the Flow blasts at response, when the flow date
matches response date exactly
***********************************************************************************/

UPDATE temp.result2 a, temp.flowblasts b
    SET a.FlowDate     = b.DateObtained
    , a.FlowSource     = b.PathTestCategory
     , a.FlowBlasts =
         CASE
             WHEN b.PathResult IS NULL             THEN NULL  # Nothing to map
             WHEN b.PathResult = ''                THEN NULL  # Nothing to map
             WHEN b.PathResult RLIKE "~|-|[?]|<|>" THEN NULL  # a dash or approximate symbol
             WHEN b.PathResult RLIKE '[a-zA-Z]'    THEN NULL  # contains letters
             WHEN b.PathResult RLIKE '\d*[\.\d]?'  THEN CAST(b.PathResult AS DECIMAL(7,3))  
             ELSE CAST(b.PathResult AS DECIMAL(7,3))
         END
    , a.FlowBlastsText =
        CASE
            WHEN b.PathResult = '-' THEN 'negative'
            WHEN b.PathResult = '+' THEN 'positive'
            WHEN b.PathResult = '<' THEN 'decreased'
            WHEN b.PathResult = '>' THEN 'increased'
            WHEN b.PathResult = '~' THEN 'unchanged'
            WHEN b.PathResult = '?' THEN 'unknown'
            ELSE LEFT(b.PathResult,45)
        END
    WHERE b.PtMRN IS NOT NULL
        AND a.FlowBlastsText IS NULL
        AND a.PtMRN = b.PtMRN
        AND a.ResponseDate = b.DateObtained
        AND b.PathTest IN ( 'Blasts (FLOW POS/NEG)'
            , 'Blasts (PBFLOW POS/NEG)'
            , 'Evidence of Disease (FLOW)'
            , 'Evidence of Disease (PB FLOW)'
        );

/***********************************************************************************
Update the result table to contain the Flow blasts at response, when the flow date
matches response date within 7 days
AND timestampdiff(day,a.ResponseDate,b.DateObtained) between -7 and 7
***********************************************************************************/


UPDATE temp.result2 a, temp.flowblasts b
    SET a.FlowDate     = b.DateObtained
    , a.FlowSource     = b.PathTestCategory
     , a.FlowBlasts =
         CASE
             WHEN b.PathResult IS NULL             THEN NULL  # Nothing to map
             WHEN b.PathResult = ''                THEN NULL  # Nothing to map
             WHEN b.PathResult RLIKE "~|-|[?]|<|>" THEN NULL  # a dash or approximate symbol
             WHEN b.PathResult RLIKE '[a-zA-Z]'    THEN NULL  # contains letters
             WHEN b.PathResult RLIKE '\d*[\.\d]?'  THEN CAST(b.PathResult AS DECIMAL(7,3))  
             ELSE CAST(b.PathResult AS DECIMAL(7,3))
         END
    , a.FlowBlastsText =
        CASE
            WHEN b.PathResult = '-' THEN 'negative'
            WHEN b.PathResult = '+' THEN 'positive'
            WHEN b.PathResult = '<' THEN 'decreased'
            WHEN b.PathResult = '>' THEN 'increased'
            WHEN b.PathResult = '~' THEN 'unchanged'
            WHEN b.PathResult = '?' THEN 'unknown'
            ELSE LEFT(b.PathResult,45)
        END
    WHERE b.PtMRN IS NOT NULL
        AND a.FlowBlastsText IS NULL
        AND a.PtMRN = b.PtMRN
        AND timestampdiff(day,a.ResponseDate,b.DateObtained) between -7 and 7
        AND b.PathTest IN ( 'Blasts (FLOW POS/NEG)'
            , 'Blasts (PBFLOW POS/NEG)'
            , 'Evidence of Disease (FLOW)'
            , 'Evidence of Disease (PB FLOW)'
        );
SELECT count(*) FROM temp.Result2 WHERE NOT FlowBlasts IS NULL;


/***********************************************************************************

fIGURE OUT IF PATIENTS HAVE RELAPSED

***********************************************************************************/
SELECT * FROM temp.Result2 ;
SELECT Status, count(*) FROM caisis.vdatasetstatus WHERE status like '%relapse%' GROUP BY status ;

DROP TABLE IF EXISTS temp.RelapseStatus ;
CREATE TABLE temp.RelapseStatus
    SELECT PtMRN
        , Status AS RelapseType
        , StatusDate AS RelapseDate
        , StatusDisease AS RelapseDisease
        , StatusNotes AS RelapseNotes
        FROM caisis.vdatasetstatus a
        WHERE status like '%relapse%';

SELECT * FROM temp.RelapseStatus ;
SELECT PtMRN FROM temp.RelapseStatus GROUP BY 1;

UPDATE temp.Result2 a
    SET a.RelapseType  = NULL
    , a.RelapseDate    = NULL
    , a.RelapseDisease = NULL
    , a.RelapseNotes   = '' ;

UPDATE temp.Result2 a, temp.RelapseStatus b
    SET a.RelapseType  = b.RelapseType
    , a.RelapseDate    = b.RelapseDate
    , a.RelapseDisease = b.RelapseDisease
    , a.RelapseNotes   = b.RelapseNotes
    WHERE a.PtMRN = b.PtMRN
        AND b.RelapseDate BETWEEN a.ResponseDate and a.NextArrivalDate ;

UPDATE temp.Result2 a, temp.RelapseStatus b
    SET a.RelapseType  = b.RelapseType
    , a.RelapseDate    = b.RelapseDate
    , a.RelapseDisease = b.RelapseDisease
    , a.RelapseNotes   = b.RelapseNotes
    WHERE a.PtMRN = b.PtMRN
        AND a.RelapseType IS NULL
        AND a.NextArrivalDate IS NULL
        AND b.RelapseDate > a.ResponseDate ;

UPDATE temp.Result2 a, temp.RelapseStatus b
    SET a.RelapseType  = b.RelapseType
    , a.RelapseDate    = b.RelapseDate
    , a.RelapseDisease = b.RelapseDisease
    , a.RelapseNotes   = b.RelapseNotes
    WHERE a.PtMRN = b.PtMRN
        AND a.RelapseType IS NULL
        AND b.RelapseDate BETWEEN a.TreatmentStartDate and a.NextArrivalDate ;

UPDATE temp.Result2 a, temp.RelapseStatus b
    SET a.RelapseType  = b.RelapseType
    , a.RelapseDate    = b.RelapseDate
    , a.RelapseDisease = b.RelapseDisease
    , a.RelapseNotes   = b.RelapseNotes
    WHERE a.PtMRN = b.PtMRN
        AND a.RelapseType IS NULL
        AND a.NextArrivalDate IS NULL
        AND b.RelapseDate > a.TreatmentStartDate ;

ALTER TABLE temp.Result2
      ADD COLUMN DaysFromResponseToRelapse   INTEGER AFTER DaysFromArrivalToTreatment
    , ADD COLUMN DaysFromTreatmentToResponse INTEGER AFTER DaysFromArrivalToTreatment
    , ADD COLUMN FirstRelapseDate DATETIME           AFTER DaysFromArrivalToTreatment ;

UPDATE temp.Result2 SET DaysFromTreatmentToResponse = timestampdiff(DAY, TreatmentStartDate, ResponseDate);
UPDATE temp.Result2 SET DaysFromResponseToRelapse   = timestampdiff(DAY, ResponseDate, RelapseDate);

UPDATE temp.Result2 a,
    (SELECT PtMRN, MIN(RelapseDate) AS FirstRelapseDate
        FROM temp.RelapseStatus
        WHERE RelapseDate IS NOT NULL GROUP BY PtMRN) b
    SET a.FirstRelapseDate = b.FirstRelapseDate
    WHERE a.PtMRN = b.PtMRN ;

/*
SELECT * FROM temp.Result2 ;
SELECT ArrivalDx, RelapseDisease FROM temp.Result2 ;
*/

DROP TABLE IF EXISTS temp.AllArrivals ;
CREATE TABLE temp.AllArrivals
    SELECT a.PtMRN
        , PatientId
        , ArrivalDx
        , CASE
            WHEN ArrivalDx NOT LIKE '%AML%' THEN 'Other'
            WHEN ArrivalDx LIKE '%ND1%' THEN 'ND1/2'
            WHEN ArrivalDx LIKE '%ND2%' THEN 'ND1/2'
            WHEN ArrivalDx LIKE '%REL%' THEN 'REL'
            WHEN ArrivalDx LIKE '%REF%' THEN 'REF'
            ELSE 'Unknown'
        END AS ArrivalType
        , MedTxIntent
        , ArrivalDate
        , YEAR(ArrivalDate) AS ArrivalYear
        , BackboneName
        , TreatmentStartDate
        , ResponseDate
        , Response
         , CASE
            WHEN Response IN ( 'CR'
                , 'CR CYTO MRD'
                , 'CR MRD'
                , 'CR-MRD'
                , 'Cri'
                , 'CRi CYTO MRD'
                , 'CRi MRD'
                , 'CRp'
                , 'CRp MRD'
            ) THEN CASE
                     WHEN ArrivalDx LIKE '%ND1%' AND ArrivalDx LIKE '%AML%' THEN '1'
                     WHEN ArrivalDx LIKE '%ND2%' AND ArrivalDx LIKE '%AML%' THEN '1'
                     WHEN ArrivalDx LIKE '%REF%' AND ArrivalDx LIKE '%AML%' THEN '1'
                     WHEN ArrivalDx LIKE '%REL%' AND ArrivalDx LIKE '%AML%' AND MedTxIntent LIKE '%1%' THEN '2'
                     WHEN ArrivalDx LIKE '%REL%' AND ArrivalDx LIKE '%AML%' THEN '1+'
                     ELSE 'Unknown'
                 END
            ELSE ''
         END AS CRNumber
        , FlowBlasts
        , FlowBlastsText
        , CASE
            WHEN RelapseDate IS NULL THEN 'N'
            WHEN RelapseDate IS NOT NULL THEN 'Y'
            ELSE 'Unknown'
        END AS Relapse
        , RelapseDate
        , DaysFromResponseToRelapse AS CRLength
        , FirstRelapseDate
        , b.PtDeathDate
        , b.PtDeathType
        , CAST(NULL AS DATETIME) AS LastStatusDate
        , CAST(NULL AS CHAR(45)) AS LastStatusType
        , b.PtDeathDate AS LastInformationDate
        , NextArrivalDate
        FROM Temp.Result2 a
        JOIN (SELECT PtMRN, PtDeathDate, PtDeathType FROM caisis.vdatasetpatients) b on a.PtMRN = b.PtMRN ;

UPDATE temp.allarrivals a, caisis.vdatasetlastvisit b
    SET a.LastInformationDate =
        CASE
            WHEN a.LastInformationDate IS NULL THEN b.LastLabDate
            ELSE a.LastInformationDate
        END
    WHERE a.PtMRN = b.PtMRN
        AND a.PtDeathDate IS NULL ;


UPDATE temp.allarrivals a, (SELECT PtMRN
    , PatientId
    , max(statusdate) AS LastStatus
    , Status
    FROM vdatasetstatus
    WHERE Status IN ( 'Alive'
        , 'Arrival Work-up'
        , 'CR'
        , 'CR CYTO MRD'
        , 'CR MRD'
        , 'CR-MRD'
        , 'CRi'
        , 'CRi MRD'
        , 'CRp'
        , 'CRp MRD'
        , 'CYTO Relapse'
        , 'Diagnosis Date'
        , 'FLOW Relapse'
        , 'Initial AHD Date'
        , 'Newly Diagnosed'
        , 'Non-Heme Cancer Diagnosis'
        , 'PB Relapse'
        , 'Persistent Disease'
        , 'PR'
        , 'Recovery of ANC 1000'
        , 'Recovery of ANC 500'
        , 'Recovery of Counts'
        , 'Recovery of Plts 100k'
        , 'Recovery of Plts 50k'
        , 'Refractory'
        , 'Relapse'
        , 'Resistant'
    )
    group by PtMRN ) b
    SET a.LastStatusDate = b.LastStatus
    , a.LastStatusType = b.Status
    WHERE a.PtMRN = b.PtMRN  ;

UPDATE temp.allarrivals SET LastInformationDate = LastStatusDate
    WHERE LastStatusDate > LastInformationDate ;


UPDATE temp.allarrivals SET LastInformationDate = LastStatusDate
    WHERE  LastInformationDate IS NULL;

SELECT * FROM temp.result2 ;
SELECT * FROM temp.allarrivals ;

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
        , PtDeathDate
        , PtDeathType
        , LastStatusDate
        , LastStatusType
        , LastInformationDate
        FROM temp.FindLastContactDate
        GROUP BY 1;


SELECT * FROM temp.allarrivals WHERE LastStatusDate > LastInformationDate ;

