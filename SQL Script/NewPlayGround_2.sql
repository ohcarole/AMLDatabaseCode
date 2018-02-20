/*************************************************************************************
Recreates the table vdatasetarrivalwithprevnext
*/
call caisis.`Create Arrival with Prev and Next 20180125`();


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


/*************************************************************************************
Find all treatments where no treatment was given
*/
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
            ) 
            OR MedTxIntent IN ( 'Consolidation Regimen'
            , 'Immunosuppressive Regimen'
            , 'Maintenance Regimen'
            , 'Other Regimen'
            , 'Palliative Regimen'
            , 'Consolidation'
            , 'Immunosuppressive'
            );

/*************************************************************************************
Patients with an arrival date joined to view arrival with next in
order to find a range between which all relevant dates must fall
*/

DROP TABLE IF EXISTS temp.PlaygroundTemp ;
CREATE TABLE temp.PlaygroundTemp
    SELECT a.PtMRN
        , a.PatientId
        , StatusDisease AS ArrivalDx # test comment
        , StatusDate AS ArrivalDate
        , YEAR(StatusDate) AS ArrivalYear
        , b.NextArrivalDate
    FROM caisis.vdatasetstatus a
    JOIN caisis.vdatasetarrivalwithprevnext b ON a.PatientId = b.PatientId and a.StatusDate = b.ArrivalDate
    WHERE a.status like '%work%' 
    ;

ALTER TABLE `temp`.`PlaygroundTemp`
      ADD COLUMN `Intensity`                TEXT NULL FIRST
    , ADD COLUMN `Response`                 TEXT NULL FIRST
    , ADD COLUMN `ResponseFlowSource`       TEXT NULL FIRST
    , ADD COLUMN `RelapseType`              TEXT NULL FIRST
    , ADD COLUMN `RelapseDisease`           TEXT NULL FIRST
    , ADD COLUMN `Treatment`                TEXT NULL FIRST
    , ADD COLUMN `TreatmentIntent`          TEXT NULL FIRST
    , ADD COLUMN `MedTxIntent`              TEXT NULL FIRST
    , ADD COLUMN `OriginalMedTxAgent`       TEXT NULL FIRST
    , ADD COLUMN `MedTxAgent`               TEXT NULL FIRST
    , ADD COLUMN `MedTxAgentNoParen`        TEXT NULL FIRST
    , ADD COLUMN `RelapseNotes`             TEXT NULL FIRST
    , ADD COLUMN `OtherTreatment`           TEXT NULL FIRST
    , ADD COLUMN `FirstArrivalDx`           TEXT NULL FIRST
    , ADD COLUMN `ArrivalKaryotype`         TEXT NULL FIRST
    , ADD COLUMN `ArrivalFISH`              TEXT NULL FIRST
    , ADD COLUMN `ArrivalCGAT`              TEXT NULL FIRST
    
    , ADD COLUMN `TreatmentCount`           INTEGER  NULL DEFAULT 0 FIRST
    , ADD COLUMN `AnthracyclinDose`         INTEGER  NULL FIRST
    , ADD COLUMN `DaysToResponse`           INTEGER  NULL FIRST
    , ADD COLUMN `FollowUpDays`             INTEGER  NULL FIRST

    , ADD COLUMN `ResponseFlowBlasts`       DECIMAL(5,2) NULL FIRST
    , ADD COLUMN `FollowUpMonths`           DECIMAL(5,2) NULL FIRST 
    , ADD COLUMN `FollowUpYears`            DECIMAL(5,2) NULL FIRST 

    , ADD COLUMN `Anthracyclin`             VARCHAR(255) NULL FIRST
    , ADD COLUMN `BackboneAddOn`            VARCHAR(255) NULL FIRST
    , ADD COLUMN `BackboneName`             VARCHAR(225) NULL FIRST
    , ADD COLUMN `BackboneType`             VARCHAR(20)  NULL FIRST
    , ADD COLUMN `ResponseFlowBlastsText`   VARCHAR(45)  NULL FIRST
    , ADD COLUMN `ArrivalType`              VARCHAR(45)  NULL FIRST
    , ADD COLUMN `FirstArrivalType`         VARCHAR(45)  NULL FIRST
    , ADD COLUMN `CRNumber`                 VARCHAR(10)  NULL FIRST
    , ADD COLUMN `Relapse`                  VARCHAR(10)  NULL FIRST
    , ADD COLUMN `LastStatusType`           VARCHAR(50)  NULL FIRST
    , ADD COLUMN `PtDeathType`              VARCHAR(50)  NULL FIRST
    , ADD COLUMN `PtLastName`               VARCHAR(50)  NULL FIRST
    , ADD COLUMN `ReturnPatient`            VARCHAR(3)   NULL FIRST

    , ADD COLUMN `TreatmentStartDate`       DATETIME NULL FIRST
    , ADD COLUMN `ResponseDate`             DATETIME NULL FIRST
    , ADD COLUMN `ResponseFlowDate`         DATETIME NULL FIRST
    , ADD COLUMN `RelapseDate`              DATETIME NULL FIRST
    , ADD COLUMN `EarliestRelapseDate`      DATETIME NULL FIRST
    , ADD COLUMN `LastInformationDate`      DATETIME NULL FIRST
    , ADD COLUMN `LastStatusDate`           DATETIME NULL FIRST
    , ADD COLUMN `PtDeathDate`              DATETIME NULL FIRST
    , ADD COLUMN `PtBirthDate`              DATETIME NULL FIRST
    , ADD COLUMN `FirstArrivalDate`         DATETIME NULL FIRST
    , ADD COLUMN `ArrivalKaryotypeDate`     DATETIME NULL FIRST
    , ADD COLUMN `ArrivalFISHDate`          DATETIME NULL FIRST
    , ADD COLUMN `ArrivalCGATDate`          DATETIME NULL FIRST
;

/*
Records where the patient was not arriving for AML
ALTER TABLE `caisis`.`Playground`
   ADD COLUMN `FirstArrivalType`              VARCHAR(45)  NULL FIRST
;

*/
SELECT * FROM temp.PlaygroundTemp 
    WHERE ArrivalDx NOT LIKE '%aml%' 
    AND   ArrivalDx NOT LIKE '%mds%'
    ;

/*
Remove APL
*/

DELETE FROM temp.PlaygroundTemp 
WHERE
    ArrivalDx NOT LIKE '%aml%'
    AND ArrivalDx NOT LIKE '%mds%'
;


/*
The main purpose of this requerying is to re-order the fields
excluding APL
*/
DROP TABLE IF EXISTS caisis.`Playground` ;
CREATE TABLE caisis.`Playground`
    SELECT b.`arrival_id`
       , a.`PtMRN`
       , a.`PatientId`
       , a.`PtBirthDate`
       , a.`PtLastName`
       
       # Information about this arrival at UW/SCCA
       , a.`ReturnPatient`
       , a.`ArrivalDx`
       , a.`ArrivalType`
       , a.`ArrivalDate`
       , a.`ArrivalYear`
       , a.`ArrivalKaryotype`
       , a.`ArrivalKaryotypeDate`
       , a.`ArrivalFISH`
       , a.`ArrivalFISHDate`
       , a.`ArrivalCGAT`
       , a.`ArrivalCGATDate`
       
       # Treatment at this arrival
       , a.`TreatmentStartDate`
       , a.`Treatment`
       , a.`TreatmentIntent`
       , a.`TreatmentCount`
       , a.`OtherTreatment`

       , a.`BackboneType`
       , a.`BackboneName`
       , a.`BackboneAddOn`
       , a.`Anthracyclin`
       , a.`AnthracyclinDose`
       , a.`Intensity`
       
       # response for this arrival's treatment
       , a.`DaysToResponse`
       , a.`Response`
       , a.`ResponseDate`
       , a.`CRNumber`

       , a.`ResponseFlowDate`
       , a.`ResponseFlowSource`
       , a.`ResponseFlowBlasts`
       , a.`ResponseFlowBlastsText`


       # relapse from remission at this arrival
       , a.`Relapse`
       , a.`RelapseDate`
       , a.`RelapseDisease`
       , a.`RelapseType`
       , a.`RelapseNotes`


       # first date patient ever relapsed
       , a.`EarliestRelapseDate`

       # Information about the very first arrival at UW/SCCA
       , a.`FirstArrivalDx`
       , a.`FirstArrivalType`
       , a.`FirstArrivalDate`

       , a.`FollowUpDays`
       , a.`FollowUpMonths`
       , a.`FollowUpYears`
       , a.`LastInformationDate`
       , a.`LastStatusDate`
       , a.`LastStatusType`
       , a.`PtDeathDate`
       , a.`PtDeathType`

       , a.`NextArrivalDate`
       , a.`MedTxIntent`
       , a.`OriginalMedTxAgent`
       , a.`MedTxAgent`
       , a.`MedTxAgentNoParen`

    FROM `temp`.`PlaygroundTemp` a 
    JOIN arrivalidmapping b
    ON a.patientid = b.patientid and a.arrivaldate = b.arrivaldate
    WHERE
    a.`ArrivalDx` LIKE '%aml%'
    OR a.`ArrivalDx` LIKE '%mds%';

/*
QUICK ALTER
Alter table caisis.playground ADD COLUMN `FirstArrivalDx` TEXT NULL FIRST
    , ADD COLUMN `ArrivalKaryotype` TEXT NULL FIRST
    , Alter table caisis.playground ADD COLUMN `ArrivalKaryotypeDate`         DATETIME NULL FIRST

*/


UPDATE caisis.Playground
    SET ArrivalType = CASE
            WHEN ArrivalDx NOT LIKE '%AML%' THEN 'Other'
            WHEN ArrivalDx LIKE '%ND1%' THEN 'ND1/2'
            WHEN ArrivalDx LIKE '%ND2%' THEN 'ND1/2'
            WHEN ArrivalDx LIKE '%REL%' THEN 'REL'
            WHEN ArrivalDx LIKE '%REF%' THEN 'REF'
            ELSE 'Unknown'
        END ;

UPDATE caisis.playground a, caisis.vdatasetpatients b
    SET a.PtDeathDate = b.PtDeathDate
    , a.PtDeathType = b.PtDeathType
    , a.PtBirthDate = b.PtBirthDate
    , a.PtLastName = b.PtLastName
    WHERE a.PatientId = b.PatientId ;


/*************************************************************************************
   Indexing
*/

ALTER TABLE `caisis`.`playground` 
    ADD INDEX `PKEY`               (`PatientId` ASC, `ArrivalYear` ASC),
    ADD INDEX `PatientId`          (`PatientId` ASC),
    ADD INDEX `ArrivalDate`        (`ArrivalDate` ASC),
    ADD INDEX `MedTxAgent`         (`MedTxAgent`(45) ASC), 
    ADD INDEX `OriginalMedTxAgent` (`OriginalMedTxAgent`(45) ASC),
    ADD INDEX `MedTxAgentNoParen`  (`MedTxAgentNoParen`(45) ASC);


/*************************************************************************************
    Arrivals with Treatment found
*/
UPDATE caisis.`Playground` a, temp.Treatment b
    SET a.TreatmentStartDate = b.MedTxDate
        , a.Treatment = b.MedTxAgent
        , a.MedTxAgent = b.MedTxAgent
        , a.TreatmentIntent = b.MedTxIntent
        , a.TreatmentCount = a.TreatmentCount + 1
        , a.OtherTreatment = ''
    WHERE a.PatientId = b.PatientId 
        AND ((b.MedTxDate BETWEEN a.ArrivalDate AND a.NextArrivalDate 
              AND  b.MedTxDate BETWEEN a.ArrivalDate and DATE_ADD(a.ArrivalDate, INTERVAL 100 DAY))
        OR  b.MedTxDate BETWEEN a.ArrivalDate and DATE_ADD(a.ArrivalDate, INTERVAL 100 DAY)) ;


UPDATE caisis.`Playground` a, temp.Treatment b
    SET a.TreatmentStartDate = b.MedTxDate 
        , a.Treatment = b.MedTxAgent
        , a.MedTxAgent = b.MedTxAgent
        , a.TreatmentIntent = b.MedTxIntent
        , a.TreatmentCount = a.TreatmentCount + 1
        , a.OtherTreatment = CONCAT(a.OtherTreatment, a.Treatment, ' on ', a.TreatmentStartDate, CHAR(13))
    WHERE a.PatientId = b.PatientId 
        AND b.MedTxDate < a.TreatmentStartDate
        AND ((b.MedTxDate BETWEEN a.ArrivalDate AND a.NextArrivalDate 
              AND  b.MedTxDate BETWEEN a.ArrivalDate and DATE_ADD(a.ArrivalDate, INTERVAL 100 DAY))
        OR  b.MedTxDate BETWEEN a.ArrivalDate and DATE_ADD(a.ArrivalDate, INTERVAL 100 DAY)) ;


UPDATE caisis.`Playground` a, temp.Treatment b
    SET a.TreatmentStartDate = b.MedTxDate 
        , a.Treatment = b.MedTxAgent
        , a.MedTxAgent = b.MedTxAgent
        , a.TreatmentIntent = b.MedTxIntent
        , a.TreatmentCount = a.TreatmentCount + 1
        , a.OtherTreatment = CONCAT(a.OtherTreatment, a.Treatment, ' on ', a.TreatmentStartDate, CHAR(13))
    WHERE a.PatientId = b.PatientId 
        AND b.MedTxDate < a.TreatmentStartDate
        AND ((b.MedTxDate BETWEEN a.ArrivalDate AND a.NextArrivalDate 
              AND  b.MedTxDate BETWEEN a.ArrivalDate and DATE_ADD(a.ArrivalDate, INTERVAL 100 DAY))
        OR  b.MedTxDate BETWEEN a.ArrivalDate and DATE_ADD(a.ArrivalDate, INTERVAL 100 DAY)) ;



/*************************************************************************************
    Arrivals where no Treatment found
*/
UPDATE caisis.`Playground` a, temp.NonTreatment b
    SET a.TreatmentStartDate = b.MedTxDate
        , a.Treatment = b.MedTxAgent
        , a.MedTxAgent = b.MedTxAgent
        , a.TreatmentIntent = concat('Non-Treatment: ',b.MedTxIntent)
        , a.TreatmentCount = a.TreatmentCount + 1
        , a.OtherTreatment = ''
    WHERE a.PatientId = b.PatientId
        AND a.TreatmentStartDate IS NULL
        AND a.TreatmentIntent IS NULL 
        AND a.MedTxAgent IS NULL
        AND ((b.MedTxDate BETWEEN a.ArrivalDate AND a.NextArrivalDate 
              AND  b.MedTxDate BETWEEN a.ArrivalDate and DATE_ADD(a.ArrivalDate, INTERVAL 100 DAY))
        OR  b.MedTxDate BETWEEN a.ArrivalDate and DATE_ADD(a.ArrivalDate, INTERVAL 100 DAY)) ;

UPDATE caisis.`Playground` a, temp.NonTreatment b
    SET a.TreatmentStartDate = b.MedTxDate 
        , a.Treatment = b.MedTxAgent
        , a.MedTxAgent = b.MedTxAgent
        , a.TreatmentIntent = concat('Non-Treatment: ',b.MedTxIntent)
        , a.TreatmentCount = a.TreatmentCount + 1
        , a.OtherTreatment = CONCAT(a.OtherTreatment, a.Treatment, ' on ', a.TreatmentStartDate, CHAR(13))
    WHERE a.PatientId = b.PatientId
        AND a.TreatmentStartDate IS NULL
        AND a.TreatmentIntent IS NULL 
        AND a.MedTxAgent IS NULL
        AND b.MedTxDate < a.TreatmentStartDate
        AND ((b.MedTxDate BETWEEN a.ArrivalDate AND a.NextArrivalDate 
              AND  b.MedTxDate BETWEEN a.ArrivalDate and DATE_ADD(a.ArrivalDate, INTERVAL 100 DAY))
        OR  b.MedTxDate BETWEEN a.ArrivalDate and DATE_ADD(a.ArrivalDate, INTERVAL 100 DAY)) ;


UPDATE caisis.`Playground` a, temp.NonTreatment b
    SET a.TreatmentStartDate = b.MedTxDate 
        , a.Treatment = b.MedTxAgent
        , a.MedTxAgent = b.MedTxAgent
        , a.TreatmentIntent = concat('Non-Treatment: ',b.MedTxIntent)
        , a.TreatmentCount = a.TreatmentCount + 1
        , a.OtherTreatment = CONCAT(a.OtherTreatment, a.Treatment, ' on ', a.TreatmentStartDate, CHAR(13))
    WHERE a.PatientId = b.PatientId
        AND a.TreatmentStartDate IS NULL
        AND a.TreatmentIntent IS NULL 
        AND a.MedTxAgent IS NULL
        AND b.MedTxDate < a.TreatmentStartDate
        AND ((b.MedTxDate BETWEEN a.ArrivalDate AND a.NextArrivalDate 
              AND  b.MedTxDate BETWEEN a.ArrivalDate and DATE_ADD(a.ArrivalDate, INTERVAL 100 DAY))
        OR  b.MedTxDate BETWEEN a.ArrivalDate and DATE_ADD(a.ArrivalDate, INTERVAL 100 DAY)) ;


/************************************************************************
    Making case variations and versions without the extra parens
*/
UPDATE caisis.`Playground`
    SET MedTxAgent = LTRIM(RTRIM(UPPER(MedTxAgent)))
        , OriginalMedTxAgent = MedTxAgent ;
UPDATE caisis.`Playground`
    SET MedTxAgentNoParen  = RTRIM(SUBSTRING(MedTxAgent,1,LOCATE('(',MedTxAgent)-1)) ;

/************************************************************************
    In this section mapping MedTxAgent to the backbone, or common name
*/

/************************************************************************
    Join to OriginalProtocol in backbonemapping
*/
UPDATE caisis.`Playground` a, caisis.backbonemapping b
    SET   a.BackBoneType       = b.BackBoneType
        , a.BackBoneName       = b.BackBoneName
        , a.BackBoneAddOn      = b.BackBoneAddOn
    where a.MedTxAgent         = b.OriginalProtocol
        AND a.BackBoneName IS NULL 
        AND TreatmentIntent NOT LIKE '%Non-Treatment:%';

UPDATE caisis.`Playground` a, caisis.backbonemapping b
    SET   a.BackBoneType       = b.BackBoneType
        , a.BackBoneName       = b.BackBoneName
        , a.BackBoneAddOn      = b.BackBoneAddOn
    where a.OriginalMedTxAgent = b.OriginalProtocol
        AND a.BackBoneName IS NULL 
        AND TreatmentIntent NOT LIKE '%Non-Treatment:%';

UPDATE caisis.`Playground` a, caisis.backbonemapping b
    SET   a.BackBoneType       = b.BackBoneType
        , a.BackBoneName       = b.BackBoneName
        , a.BackBoneAddOn      = b.BackBoneAddOn
    where a.MedTxAgentNoParen  = b.OriginalProtocol
        AND a.BackBoneName IS NULL 
        AND TreatmentIntent NOT LIKE '%Non-Treatment:%';

/************************************************************************
    Join to upper case version OriginalMedTxAgent in backbonemapping
*/
UPDATE caisis.`Playground` a, caisis.backbonemapping b
    SET   a.BackBoneType       = b.BackBoneType
        , a.BackBoneName       = b.BackBoneName
        , a.BackBoneAddOn      = b.BackBoneAddOn
    where a.MedTxAgent         = b.OriginalMedTxAgent
        AND a.BackBoneName IS NULL 
        AND TreatmentIntent NOT LIKE '%Non-Treatment:%';

UPDATE caisis.`Playground` a, caisis.backbonemapping b
    SET   a.BackBoneType       = b.BackBoneType
        , a.BackBoneName       = b.BackBoneName
        , a.BackBoneAddOn      = b.BackBoneAddOn
    where a.OriginalMedTxAgent = b.OriginalMedTxAgent
        AND a.BackBoneName IS NULL 
        AND TreatmentIntent NOT LIKE '%Non-Treatment:%';

UPDATE caisis.`Playground` a, caisis.backbonemapping b
    SET   a.BackBoneType       = b.BackBoneType
        , a.BackBoneName       = b.BackBoneName
        , a.BackBoneAddOn      = b.BackBoneAddOn
    where a.MedTxAgentNoParen  = b.OriginalMedTxAgent
        AND a.BackBoneName IS NULL 
        AND TreatmentIntent NOT LIKE '%Non-Treatment:%';

/************************************************************************
    Join to upper case MedTxAgent
*/UPDATE caisis.`Playground` a, caisis.backbonemapping b
    SET   a.BackBoneType       = b.BackBoneType
        , a.BackBoneName       = b.BackBoneName
        , a.BackBoneAddOn      = b.BackBoneAddOn
    where a.OriginalMedTxAgent = b.MedTxAgent
        AND a.BackBoneName IS NULL 
        AND TreatmentIntent NOT LIKE '%Non-Treatment:%';

UPDATE caisis.`Playground` a, caisis.backbonemapping b
    SET   a.BackBoneType       = b.BackBoneType
        , a.BackBoneName       = b.BackBoneName
        , a.BackBoneAddOn      = b.BackBoneAddOn
    where a.MedTxAgent         = b.MedTxAgent
        AND a.BackBoneName IS NULL 
        AND TreatmentIntent NOT LIKE '%Non-Treatment:%';

UPDATE caisis.`Playground` a, caisis.backbonemapping b
    SET   a.BackBoneType       = b.BackBoneType
        , a.BackBoneName       = b.BackBoneName
        , a.BackBoneAddOn      = b.BackBoneAddOn
    where a.MedTxAgentNoParen  = b.MedTxAgent
        AND a.BackBoneName IS NULL 
        AND TreatmentIntent NOT LIKE '%Non-Treatment:%';

/************************************************************************
    Join to upper case MedTxAgentNoParen
*/UPDATE caisis.`Playground` a, caisis.backbonemapping b
    SET   a.BackBoneType       = b.BackBoneType
        , a.BackBoneName       = b.BackBoneName
        , a.BackBoneAddOn      = b.BackBoneAddOn
    where a.OriginalMedTxAgent = b.MedTxAgentNoParen
        AND a.BackBoneName IS NULL 
        AND TreatmentIntent NOT LIKE '%Non-Treatment:%';

UPDATE caisis.`Playground` a, caisis.backbonemapping b
    SET   a.BackBoneType       = b.BackBoneType
        , a.BackBoneName       = b.BackBoneName
        , a.BackBoneAddOn      = b.BackBoneAddOn
    where a.MedTxAgent         = b.MedTxAgentNoParen
        AND a.BackBoneName IS NULL 
        AND TreatmentIntent NOT LIKE '%Non-Treatment:%';

UPDATE caisis.`Playground` a, caisis.backbonemapping b
    SET   a.BackBoneType       = b.BackBoneType
        , a.BackBoneName       = b.BackBoneName
        , a.BackBoneAddOn      = b.BackBoneAddOn
    where a.MedTxAgentNoParen  = b.MedTxAgentNoParen
        AND a.BackBoneName IS NULL 
        AND TreatmentIntent NOT LIKE '%Non-Treatment:%';

/*
SELECT * FROM caisis.Playground;
SELECT * FROM caisis.backbonemapping;
SELECT * FROM temp.Treatment;
SELECT * FROM temp.NonTreatment;
Arrivals with no treatment date was recorded
SELECT * FROM caisis.Playground where TreatmentStartDate IS NULL;
Arrivals with multiple treatment dates recorded
SELECT * FROM caisis.Playground where TreatmentCount > 1;
*/



/**********************************************************************************************************
    Find all responses entered that occur after a patient treatment
*/
UPDATE caisis.Playground
    SET Response = ''
        , ResponseDate = NULL
        , DaysToResponse = NULL;

DROP TABLE IF EXISTS temp.Response ;
CREATE TABLE Temp.Response
    SELECT a.`PtMRN`
        , a.`PatientId`
        , b.ArrivalDate 
        , b.ArrivalDx
        , b.TreatmentStartDate
        , b.Treatment
        , b.BackBoneName
        , b.BackBoneAddOn
        , a.StatusDisease AS ResponseDisease
        , a.Status AS Response
        , a.StatusDate AS ResponseDate
        , b.NextArrivalDate
        , TIMESTAMPDIFF(DAY,b.TreatmentStartDate,a.StatusDate) AS DaysToResponse
        , a.StatusDate BETWEEN b.TreatmentStartDate and DATE_ADD(b.TreatmentStartDate, INTERVAL 100 DAY) AS ResponseWithin100Days
        , a.StatusDate < b.NextArrivalDate OR b.NextArrivalDate IS NULL AS ResponseBeforeNextArrival
        , 0 AS Used
        FROM caisis.vdatasetstatus a
            LEFT JOIN Caisis.Playground b
                ON a.PatientId = b.PatientId
        WHERE (a.StatusDisease LIKE '%aml%')
            AND (a.Status like '%unk response%'
            OR a.Status In (
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
        )) 
        AND b.TreatmentStartDate <= a.StatusDate;

/*
SELECT * FROM temp.Response ;
SELECT * FROM temp.Response WHERE DaysToResponse > 100;
SELECT Response, LENGTH(Response), Count(*) FROM temp.Response GROUP BY Response;
SELECT ArrivalDx,  Count(*) FROM temp.Response GROUP BY Response;
*/
   

UPDATE caisis.Playground a, temp.response b
    SET a.Response = b.Response
        , a.ResponseDate = b.ResponseDate
        , a.DaysToResponse = b.DaysToResponse
        , b.Used = 1
        WHERE a.PtMRN       = b.PtMRN
        AND   a.ArrivalDate = b.ArrivalDate
        AND   a.ArrivalDx   = b.ArrivalDx 
        AND   a.BackBoneName IS NOT NULL
        AND   b.ResponseWithin100Days ;

/*
Do this 2 more times
*/
UPDATE caisis.Playground a, temp.response b
    SET a.Response = b.Response
        , a.ResponseDate = b.ResponseDate
        , a.DaysToResponse = b.DaysToResponse
        , b.Used = 1
        WHERE a.PtMRN       = b.PtMRN
        AND   a.ArrivalDate = b.ArrivalDate
        AND   a.ArrivalDx   = b.ArrivalDx 
        AND   a.BackBoneName IS NOT NULL
        AND   b.DaysToResponse < a.DaysToResponse ;        

UPDATE caisis.Playground a, temp.response b
    SET a.Response = b.Response
        , a.ResponseDate = b.ResponseDate
        , a.DaysToResponse = b.DaysToResponse
        , b.Used = 1
        WHERE a.PtMRN       = b.PtMRN
        AND   a.ArrivalDate = b.ArrivalDate
        AND   a.ArrivalDx   = b.ArrivalDx 
        AND   a.BackBoneName IS NOT NULL
        AND   b.DaysToResponse < a.DaysToResponse ;        
            

/*
    Now look to see if there is a response for all the orphaned AZA or DECICTABINE Treatments
*/
UPDATE caisis.Playground a, temp.response b
    SET a.Response = b.Response
        , a.ResponseDate = b.ResponseDate
        , a.DaysToResponse = b.DaysToResponse
        , b.Used = 1
        WHERE a.PtMRN       = b.PtMRN
        AND   a.ArrivalDate = b.ArrivalDate
        AND   a.ArrivalDx   = b.ArrivalDx 
        AND   b.BackBoneName IN ('AZA','DECITABINE') 
        AND   a.ResponseDate IS NULL 
        AND   a.TreatmentStartDate < b.ResponseDate 
        AND   (b.ResponseDate < a.NextArrivalDate OR a.NextArrivalDate IS NULL);


UPDATE caisis.Playground a, temp.response b
    SET a.Response = b.Response
        , a.ResponseDate = b.ResponseDate
        , a.DaysToResponse = b.DaysToResponse
        , b.Used = 1
        WHERE a.PtMRN       = b.PtMRN
        AND   a.ArrivalDate = b.ArrivalDate
        AND   a.ArrivalDx   = b.ArrivalDx 
        AND   b.BackBoneName IN ('AZA','DECITABINE') 
        AND   a.ResponseDate IS NULL 
        AND   a.TreatmentStartDate < b.ResponseDate 
        AND   (b.ResponseDate < a.NextArrivalDate OR a.NextArrivalDate IS NULL)
        AND   b.ResponseDate < a.ResponseDate 
        ;


UPDATE caisis.Playground
    SET CRNumber = CASE
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
     END ;



/*
SELECT * FROM caisis.Playground ORDER BY PtMRN, TreatmentStartDate ;
SELECT * FROM temp.Response ORDER BY PtMRN, ResponseDate ;
SELECT a.* FROM temp.Response a 
    LEFT JOIN (SELECT PtMRN, ArrivalDx, ArrivalDate FROM temp.Response WHERE USED = 1) b
        ON    a.PtMRN       = b.PtMRN
        AND   a.ArrivalDate = b.ArrivalDate
        AND   a.ArrivalDx   = b.ArrivalDx 
        WHERE b.PtMRN IS NULL
        ORDER BY PtMRN, ResponseDate 
        ;
SELECT Treatment, count(*) FROM caisis.Playground WHERE BackBoneName IS NULL GROUP BY Treatment;
SELECT PtMRN, ArrivalDx, ArrivalDate, TreatmentStartDate, Treatment, BackBoneName, Response, ResponseDate, DaysToResponse, nextarrivaldate
    FROM caisis.playground ;
*/

/***********************************************************************************
    Get blasts by FLOW at response
*/
DROP TABLE IF EXISTS temp.ResponseFlowblasts ;
CREATE TABLE temp.ResponseFlowblasts
    SELECT b.*
        FROM caisis.vdatasetpathtest b join caisis.playground a
        on a.PtMRN = b.PtMRN
        AND timestampdiff(day,a.ResponseDate,b.DateObtained) between -7 and 7
        WHERE b.PathResult IS NOT NULL
        AND b.PathResult NOT IN ('ND', 'N/A')
        AND PathTest IN ( 'Blasts (FLOW POS/NEG)'
            , 'Blasts (FLOW)'
            , 'Blasts (PB FLOW)'
            , 'Blasts (PBFLOW POS/NEG)'
            , 'Blasts (PBFLOW)'
            , 'Evidence of Disease (FLOW)'
            , 'Evidence of Disease (PB FLOW)'
            , 'PB Blast (FLOW)'
        );

/*
SELECT * FROM temp.ResponseFlowblasts ;
*/



/***********************************************************************************
Update the result table to contain the Flow blasts at response, when the flow date
exactly matches response date


Looking at flow not entered as a numeric value
SELECT PathResult, count(*) FROM temp.ResponseFlowblasts where PathResult RLIKE "~|-|[?]|<|>"  GROUP BY 1;

SELECT * FROM temp.ResponseFlowblasts where          
        CASE
             WHEN PathResult IS NULL             THEN False  # Nothing to map
             WHEN PathResult = ''                THEN False  # Nothing to map
             WHEN PathResult RLIKE "~|-|[?]|<|>" THEN True  # a dash or approximate symbol
             WHEN PathResult RLIKE '[a-zA-Z]'    THEN False  # contains letters
             WHEN PathResult RLIKE '\d*[\.\d]?'  THEN False
             ELSE False
         END ;

SELECT PathResult FROM temp.ResponseFlowblasts where PathResult RLIKE "~|-|[?]|<" ;

*/


/***********************************************************************************
Update the result table to contain the Flow blasts at response, when the flow date
matches response date within 7 days
AND timestampdiff(day,a.ResponseDate,b.DateObtained) between -7 and 7

Note that blasts by flow can be recorded several ways in the database, as such 
several updates are below in order to handle the different ways the data
are entered.

*/

UPDATE caisis.Playground a, temp.ResponseFlowblasts b
    SET a.ResponseFlowDate     = b.DateObtained
    , a.ResponseFlowSource     = b.PathTestCategory
    , a.ResponseFlowBlasts =
         CASE
             WHEN b.PathResult IS NULL             THEN NULL  # Nothing to map
             WHEN b.PathResult = ''                THEN NULL  # Nothing to map
             WHEN b.PathResult RLIKE "~|-|[?]|<|>" THEN NULL  # a dash or approximate symbol
             WHEN b.PathResult RLIKE '[a-zA-Z]'    THEN NULL  # contains letters
             WHEN b.PathResult RLIKE '\d*[\.\d]?'  THEN CAST(b.PathResult AS DECIMAL(7,3))  
             ELSE CAST(b.PathResult AS DECIMAL(7,3))
         END
    , a.ResponseFlowBlastsText =
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
        AND a.ResponseFlowBlastsText IS NULL
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

UPDATE caisis.Playground a, temp.ResponseFlowblasts b
    SET a.ResponseFlowDate     = b.DateObtained
    , a.ResponseFlowSource     = b.PathTestCategory
    , a.ResponseFlowBlasts =
         CASE
             WHEN b.PathResult IS NULL             THEN NULL  # Nothing to map
             WHEN b.PathResult = ''                THEN NULL  # Nothing to map
             WHEN b.PathResult RLIKE "~|-|[?]|<|>" THEN NULL  # a dash or approximate symbol
             WHEN b.PathResult RLIKE '[a-zA-Z]'    THEN NULL  # contains letters
             WHEN b.PathResult RLIKE '\d*[\.\d]?'  THEN CAST(b.PathResult AS DECIMAL(7,3))  
             ELSE CAST(b.PathResult AS DECIMAL(7,3))
         END
    , a.ResponseFlowBlastsText =
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
        AND a.ResponseFlowBlastsText IS NULL
        AND a.PtMRN = b.PtMRN
        AND timestampdiff(day,a.ResponseDate,b.DateObtained) between -7 and 7
        AND b.PathTest IN ('Blasts (FLOW)') ;


/*
Error Code: 1054. Unknown column 'a.FlowBlastsText' in 'field list'


*/



/***********************************************************************************
Update the result table to contain the Flow blasts at response, when the flow date
matches response date exactly
***********************************************************************************/

UPDATE caisis.Playground a, temp.ResponseFlowblasts b
    SET a.ResponseFlowDate     = b.DateObtained
    , a.ResponseFlowSource     = b.PathTestCategory
    , a.ResponseFlowBlasts =
         CASE
             WHEN b.PathResult IS NULL             THEN NULL  # Nothing to map
             WHEN b.PathResult = ''                THEN NULL  # Nothing to map
             WHEN b.PathResult RLIKE "~|-|[?]|<|>" THEN NULL  # a dash or approximate symbol
             WHEN b.PathResult RLIKE '[a-zA-Z]'    THEN NULL  # contains letters
             WHEN b.PathResult RLIKE '\d*[\.\d]?'  THEN CAST(b.PathResult AS DECIMAL(7,3))  
             ELSE CAST(b.PathResult AS DECIMAL(7,3))
         END
    , a.ResponseFlowBlastsText =
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
        AND a.ResponseFlowBlastsText IS NULL
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

UPDATE caisis.Playground a, temp.ResponseFlowblasts b
    SET a.ResponseFlowDate     = b.DateObtained
    , a.ResponseFlowSource     = b.PathTestCategory
    , a.ResponseFlowBlasts =
         CASE
             WHEN b.PathResult IS NULL             THEN NULL  # Nothing to map
             WHEN b.PathResult = ''                THEN NULL  # Nothing to map
             WHEN b.PathResult RLIKE "~|-|[?]|<|>" THEN NULL  # a dash or approximate symbol
             WHEN b.PathResult RLIKE '[a-zA-Z]'    THEN NULL  # contains letters
             WHEN b.PathResult RLIKE '\d*[\.\d]?'  THEN CAST(b.PathResult AS DECIMAL(7,3))  
             ELSE CAST(b.PathResult AS DECIMAL(7,3))
         END
    , a.ResponseFlowBlastsText =
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
        AND a.ResponseFlowBlastsText IS NULL
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

UPDATE caisis.Playground a, temp.ResponseFlowblasts b
    SET a.ResponseFlowDate     = b.DateObtained
    , a.ResponseFlowSource     = b.PathTestCategory
    , a.ResponseFlowBlasts =
         CASE
             WHEN b.PathResult IS NULL             THEN NULL  # Nothing to map
             WHEN b.PathResult = ''                THEN NULL  # Nothing to map
             WHEN b.PathResult RLIKE "~|-|[?]|<|>" THEN NULL  # a dash or approximate symbol
             WHEN b.PathResult RLIKE '[a-zA-Z]'    THEN NULL  # contains letters
             WHEN b.PathResult RLIKE '\d*[\.\d]?'  THEN CAST(b.PathResult AS DECIMAL(7,3))  
             ELSE CAST(b.PathResult AS DECIMAL(7,3))
         END
    , a.ResponseFlowBlastsText =
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
        AND a.ResponseFlowBlastsText IS NULL
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


UPDATE caisis.Playground a, temp.ResponseFlowblasts b
    SET a.ResponseFlowDate     = b.DateObtained
    , a.ResponseFlowSource     = b.PathTestCategory
    , a.ResponseFlowBlasts =
         CASE
             WHEN b.PathResult IS NULL             THEN NULL  # Nothing to map
             WHEN b.PathResult = ''                THEN NULL  # Nothing to map
             WHEN b.PathResult RLIKE "~|-|[?]|<|>" THEN NULL  # a dash or approximate symbol
             WHEN b.PathResult RLIKE '[a-zA-Z]'    THEN NULL  # contains letters
             WHEN b.PathResult RLIKE '\d*[\.\d]?'  THEN CAST(b.PathResult AS DECIMAL(7,3))  
             ELSE CAST(b.PathResult AS DECIMAL(7,3))
         END
    , a.ResponseFlowBlastsText =
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
        AND a.ResponseFlowBlastsText IS NULL
        AND a.PtMRN = b.PtMRN
        AND timestampdiff(day,a.ResponseDate,b.DateObtained) between -7 and 7
        AND b.PathTest IN ( 'Blasts (FLOW POS/NEG)'
            , 'Blasts (PBFLOW POS/NEG)'
            , 'Evidence of Disease (FLOW)'
            , 'Evidence of Disease (PB FLOW)'
        );
        
/*
SELECT count(*) FROM caisis.Playground WHERE NOT ResponseFlowBlasts IS NULL;
SELECT * FROM caisis.Playground  WHERE ResponseFlowDate IS NOT NULL;
*/



/***********************************************************************************
    Figure out if patients have relapsed since treatment or between arrivals
*/
SELECT * FROM caisis.Playground  ;
SELECT Status, count(*) 
    FROM caisis.vdatasetstatus 
    WHERE status like '%relapse%' 
    GROUP BY status ;

DROP TABLE IF EXISTS temp.RelapseStatus ;
CREATE TABLE temp.RelapseStatus
    SELECT PatientId
        , Status AS RelapseType
        , StatusDate AS RelapseDate
        , StatusDisease AS RelapseDisease
        , StatusNotes AS RelapseNotes
        FROM caisis.vdatasetstatus a
        WHERE status like '%relapse%';


DROP TABLE IF EXISTS temp.EarliestRelapse ;
CREATE TABLE temp.EarliestRelapse
    SELECT PatientId, MIN(RelapseDate) AS EarliestRelapseDate
        FROM temp.RelapseStatus
        WHERE RelapseDate IS NOT NULL 
        GROUP BY PatientId ;

/*
SELECT * FROM temp.EarliestRelapse ;
SELECT PtMRN FROM temp.RelapseStatus GROUP BY 1;

UPDATE caisis.Playground a
    SET a.RelapseType  = NULL
    , a.RelapseDate    = NULL
    , a.RelapseDisease = NULL
    , a.RelapseNotes   = '' ;
*/

UPDATE caisis.Playground  a, temp.RelapseStatus b
    SET a.RelapseType  = b.RelapseType
    , a.RelapseDate    = b.RelapseDate
    , a.RelapseDisease = b.RelapseDisease
    , a.RelapseNotes   = b.RelapseNotes
    WHERE a.PatientId = b.PatientId
        AND b.RelapseDate BETWEEN a.ResponseDate and a.NextArrivalDate ;

UPDATE caisis.Playground  a, temp.RelapseStatus b
    SET a.RelapseType  = b.RelapseType
    , a.RelapseDate    = b.RelapseDate
    , a.RelapseDisease = b.RelapseDisease
    , a.RelapseNotes   = b.RelapseNotes
    WHERE a.PatientId = b.PatientId
        AND a.RelapseDate IS NULL
        AND a.NextArrivalDate IS NULL
        AND b.RelapseDate > a.ResponseDate ;

# Is there an earlier relapse ?
UPDATE caisis.Playground  a, temp.RelapseStatus b
    SET a.RelapseType  = b.RelapseType
    , a.RelapseDate    = b.RelapseDate
    , a.RelapseDisease = b.RelapseDisease
    , a.RelapseNotes   = b.RelapseNotes
    WHERE a.PatientId = b.PatientId
        AND b.RelapseDate < a.RelapseDate
        AND a.NextArrivalDate IS NULL
        AND b.RelapseDate > a.ResponseDate ;

# Is there a relapse between when treatmentstarted and the next arrival (response may be missing)
UPDATE caisis.Playground  a, temp.RelapseStatus b
    SET a.RelapseType  = b.RelapseType
    , a.RelapseDate    = b.RelapseDate
    , a.RelapseDisease = b.RelapseDisease
    , a.RelapseNotes   = b.RelapseNotes
    WHERE a.PatientId = b.PatientId
        AND a.RelapseType IS NULL
        AND b.RelapseDate BETWEEN a.TreatmentStartDate and a.NextArrivalDate ;

# Is there a relapse after treatment start when there are not future arrivals (response may be missing)?
UPDATE caisis.Playground  a, temp.RelapseStatus b
    SET a.RelapseType  = b.RelapseType
    , a.RelapseDate    = b.RelapseDate
    , a.RelapseDisease = b.RelapseDisease
    , a.RelapseNotes   = b.RelapseNotes
    WHERE a.PatientId = b.PatientId
        AND a.RelapseDate IS NULL
        AND a.NextArrivalDate IS NULL
        AND b.RelapseDate > a.TreatmentStartDate ;


UPDATE caisis.Playground a, temp.EarliestRelapse b
    SET a.EarliestRelapseDate = b.EarliestRelapseDate 
    , a.Relapse = CASE 
            WHEN RelapseDate IS NOT NULL THEN 'Y'
            ELSE ''
        END 
    WHERE a.PatientId = b.PatientId ;


/*
SELECT * FROM caisis.Playground  ;
SELECT ArrivalDx, RelapseDisease FROM temp.Result2 ;
*/


UPDATE caisis.Playground a, caisis.vdatasetlastvisit b
    SET a.LastInformationDate =
        CASE
            WHEN a.LastInformationDate IS NULL THEN b.LastLabDate
            ELSE a.LastInformationDate
        END
    WHERE a.PtMRN = b.PtMRN
        AND a.PtDeathDate IS NULL ;
        

DROP TABLE IF EXISTS temp.LastStatus ;
CREATE TABLE temp.LastStatus
    SELECT a.PtMRN
        , a.PatientId
        , a.Status
        , b.LastStatus
        FROM vdatasetstatus a
            JOIN (SELECT PatientId, max(statusdate) AS LastStatus FROM vdatasetstatus GROUP BY PatientId) b
        ON a.PatientId = b.PatientId
        AND a.StatusDate = b.LastStatus ;
        
select * from temp.LastStatus ;

UPDATE Caisis.Playground
    SET LastInformationDate = NULL
    , LastStatusDate = NULL
    , LastStatusType = '' ;

    
UPDATE Caisis.Playground a, temp.LastStatus b
    SET a.LastStatusDate = b.LastStatus
    , a.LastStatusType = CASE 
        WHEN b.Status IN ( 'CR'
            , 'CR CYTO MRD'
            , 'CR MRD'
            , 'CR-MRD'
            , 'CRi'
            , 'CRi MRD'
            , 'CRp'
            , 'CRp MRD') THEN 'Remission'
        WHEN b.Status LIKE '%recover%' 
            OR b.Status IN ( 'Recovery of ANC 1000'
            , 'Recovery of ANC 500'
            , 'Recovery of Counts'
            , 'Recovery of Plts 100k'
            , 'Recovery of Plts 50k') THEN 'Count Recovery'
        WHEN b.Status LIKE '%relapse%' OR b.Status IN ( 'CYTO Relapse'
            , 'FLOW Relapse'
            , 'PB Relapse') THEN 'Relapse'

        WHEN b.Status IN ( 'Persistent Disease'
            , 'PR'
            , 'Refractory'
            , 'Resistant'
            , 'Not Categorized') THEN 'No Remission'

        WHEN b.Status IN ( 'Diagnosis Date'
            , 'Newly Diagnosed') THEN 'Diagnosis'

        WHEN b.Status LIKE '%unk%' THEN 'Unknown'
        
        WHEN b.Status IN ( 'Alive'
            , 'Initial AHD Date' 
            , 'Non-Heme Cancer Diagnosis'
            , 'Arrival Work-up' ) THEN b.Status

        ELSE ''
    END
    WHERE a.PatientId = b.PatientId  ;

  
UPDATE caisis.playground 
    SET lastinformationdate = CASE
            WHEN LastInformationDate IS NULL AND LastStatusDate IS NOT NULL THEN LastStatusDate
            WHEN LastStatusDate > LastInformationDate THEN LastStatusDate
            ELSE lastinformationdate
        END ;


UPDATE caisis.playground 
    SET LastInformationDate = CASE
            WHEN PtDeathDate IS NOT NULL THEN PtDeathDate
            ELSE LastInformationDate
        END
        , LastStatusDate = CASE
            WHEN PtDeathDate IS NOT NULL THEN PtDeathDate
            ELSE LastStatusDate
        END
        , LastStatusType = CASE
            WHEN PtDeathDate IS NOT NULL THEN 'Death'
            ELSE LastStatusType
        END;

UPDATE caisis.playground
    SET FollowUpDays = timestampdiff(day,ArrivalDate,LastInformationDate)
    , FollowUpMonths = timestampdiff(day,ArrivalDate,LastInformationDate)/30.44  #  Days per month 365.25/12
    , FollowUpYears  = timestampdiff(month,ArrivalDate,LastInformationDate)/12 ;

/*
SELECT * FROM caisis.playground;
*/


/*
    Identify return patient arrival at UW/SCCA


CREATE TABLE temp.ReturnPatient
    SELECT PtMRN
            , PatientId
            , MIN(ArrivalDate) AS FirstArrivalDate
            FROM caisis.playground
            GROUP BY PtMRN, PatientId  ;

*/


/************************************************************************************************
    Update to show the first time the patient arrived, and what type of arrival the
    patient had then.  This is helpful when looking at stay vs go.
*/
DROP TABLE IF EXISTS temp.FirstArrival ;
CREATE TABLE temp.FirstArrival
    SELECT PtMRN
                , PatientId
                , MIN(ArrivalDate) AS FirstArrivalDate
                FROM caisis.playground
                GROUP BY PtMRN, PatientId ;

UPDATE caisis.playground a
        , temp.firstarrival b
    SET a.ReturnPatient = CASE
            WHEN a.ArrivalDate = b.FirstArrivalDate THEN 'No'
            ELSE 'Yes'
        END
    WHERE a.PatientId = b.PatientId
    AND a.ArrivalDate = b.FirstArrivalDate ;

UPDATE caisis.playground a
        , (SELECT b2.*
            , a2.ArrivalDx AS FirstArrivalDx 
            , a2.ArrivalType AS FirstArrivalType
            FROM Caisis.Playground a2 
            JOIN temp.FirstArrival b2
            ON a2.PatientId = b2.PatientId AND a2.ArrivalDate = b2.FirstArrivalDate) b
    SET a.FirstArrivalDx = b.FirstArrivalDx
        , a.FirstArrivalType = b.FirstArrivalType
        , a.FirstArrivalDate = b.FirstArrivalDate
    WHERE a.PatientId = b.PatientId ;



# START 1

/************************************************************************************************
    Update to find the karyotype closest to arrival
    UPDATE caisis.playground SET ArrivalKaryotype = NULL, ArrivalKaryotypeDate = NULL ;
*/

DROP TABLE IF EXISTS temp.relevantkaryo ;
CREATE TABLE temp.relevantkaryo
    SELECT a.ArrivalDate
            , a.ArrivalDX
            , a.TreatmentStartDate
            , CASE
                WHEN a.TreatmentStartDate IS NULL 
                    THEN timestampdiff(day,b.DateObtained,a.ArrivalDate)
                ELSE timestampdiff(day,b.DateObtained,a.TreatmentStartDate)
            END AS KaryotypeDaysBeforeTreatmentorArrival
            , b.* FROM caisis.playground a
        JOIN caisis.allkaryo b
        ON a.PatientId = b.PatientId 
        WHERE b.PathResult <> ''
            AND b.PathTest IN ('UW Cyto','SCCA Karyotype','Cyto Karyotype')
            AND b.PathResult NOT RLIKE '^[a-zA-Z]+'
            AND b.DateObtained BETWEEN date_add(a.ArrivalDate, INTERVAL -90 DAY) 
            AND CASE 
                WHEN a.TreatmentStartDate IS NULL THEN ArrivalDate
                ELSE a.TreatmentStartDate
            END
    ;
    

UPDATE caisis.playground a, 
    (SELECT PatientId, ArrivalDate, PathResult, DateObtained, KaryotypeDaysBeforeTreatmentorArrival
        FROM temp.relevantkaryo 
        WHERE PathResult <> ''
        GROUP BY PatientId, ArrivalDate
        HAVING COUNT(*)=1 )  b
    SET a.ArrivalKaryotype = b.PathResult
        , a.ArrivalKaryotypeDate = b.DateObtained
    WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
    ;

# Repeat until no earlier found
UPDATE caisis.playground a, temp.relevantkaryo b
    SET a.ArrivalKaryotype = b.PathResult
        , a.ArrivalKaryotypeDate = b.DateObtained
    WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
        AND CASE 
            WHEN a.ArrivalKaryotype IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
            WHEN a.ArrivalKaryotypeDate < b.DateObtained THEN True
            ELSE False
        END
    ;


UPDATE caisis.playground a, temp.relevantkaryo b
    SET a.ArrivalKaryotype = b.PathResult
        , a.ArrivalKaryotypeDate = b.DateObtained
    WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
        AND CASE 
            WHEN a.ArrivalKaryotype IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
            WHEN a.ArrivalKaryotypeDate < b.DateObtained THEN True
            ELSE False
        END
    ;

UPDATE caisis.playground a, temp.relevantkaryo b
    SET a.ArrivalKaryotype = b.PathResult
        , a.ArrivalKaryotypeDate = b.DateObtained
    WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
        AND CASE 
            WHEN a.ArrivalKaryotype IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
            WHEN a.ArrivalKaryotypeDate < b.DateObtained THEN True
            ELSE False
        END
    ;

UPDATE caisis.playground a, temp.relevantkaryo b
    SET a.ArrivalKaryotype = b.PathResult
        , a.ArrivalKaryotypeDate = b.DateObtained
    WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
        AND CASE 
            WHEN a.ArrivalKaryotype IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
            WHEN a.ArrivalKaryotypeDate < b.DateObtained THEN True
            ELSE False
        END
    ;

UPDATE caisis.playground a, temp.relevantkaryo b
    SET a.ArrivalKaryotype = b.PathResult
        , a.ArrivalKaryotypeDate = b.DateObtained
    WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
        AND CASE 
            WHEN a.ArrivalKaryotype IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
            WHEN a.ArrivalKaryotypeDate < b.DateObtained THEN True
            ELSE False
        END
    ;


UPDATE caisis.playground a, temp.relevantkaryo b
    SET a.ArrivalKaryotype = b.PathResult
        , a.ArrivalKaryotypeDate = b.DateObtained
    WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
        AND CASE 
            WHEN a.ArrivalKaryotype IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
            WHEN a.ArrivalKaryotypeDate < b.DateObtained THEN True
            ELSE False
        END
    ;


UPDATE caisis.playground a, temp.relevantkaryo b
    SET a.ArrivalKaryotype = b.PathResult
        , a.ArrivalKaryotypeDate = b.DateObtained
    WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
        AND CASE 
            WHEN a.ArrivalKaryotype IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
            WHEN a.ArrivalKaryotypeDate < b.DateObtained THEN True
            ELSE False
        END
    ;

# END 1

# START 2

/************************************************************************************************
    Update to find the FISH closest to arrival
    UPDATE caisis.playground SET FISHKaryotype = NULL, ArrivalFISHDate = NULL ;
*/

DROP TABLE IF EXISTS temp.relevantfish ;
CREATE TABLE temp.relevantfish
    SELECT a.ArrivalDate
            , a.ArrivalDX
            , a.TreatmentStartDate
            , CASE
                WHEN a.TreatmentStartDate IS NULL 
                    THEN timestampdiff(day,b.DateObtained,a.ArrivalDate)
                ELSE timestampdiff(day,b.DateObtained,a.TreatmentStartDate)
            END AS FISHDaysBeforeTreatmentorArrival
            , b.* FROM caisis.playground a
        JOIN caisis.allkaryo b
        ON a.PatientId = b.PatientId 
        WHERE b.PathResult <> ''
            AND b.PathTest IN ('UW FISH','SCCA FISH','FISH Karyotype')
            AND b.PathResult NOT IN (''
                ,'Cancelled','No growth'
                ,'FAILED TO IDENTIFY ICSN DIAGNOSIS'
                ,'?'
                ,'No growth or insufficient growth'
                ,'Insufficient cells for analysis'
                ,'VVVV'
                ,'No IFISH','FAILED TO FIND END OF ICSN DIAGNOSIS'
            )
            AND LOCATE('ish',b.PathResult) <> 0
            AND b.DateObtained BETWEEN date_add(a.ArrivalDate, INTERVAL -90 DAY) 
                AND CASE 
                    WHEN a.TreatmentStartDate IS NULL THEN ArrivalDate
                    ELSE a.TreatmentStartDate
                END
    ;


UPDATE caisis.playground a, 
    (SELECT PatientId, ArrivalDate, PathResult, DateObtained, FISHDaysBeforeTreatmentorArrival
        FROM temp.relevantfish 
        WHERE PathResult <> ''
        GROUP BY PatientId, ArrivalDate
        HAVING COUNT(*)=1 )  b
    SET a.ArrivalFISH = b.PathResult
        , a.ArrivalFISHDate = b.DateObtained
    WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
    ;

# Repeat until no earlier found
UPDATE caisis.playground a, temp.relevantfish b
    SET a.ArrivalFISH = b.PathResult
        , a.ArrivalFISHDate = b.DateObtained
    WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
        AND CASE 
            WHEN a.ArrivalFISH IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
            WHEN a.ArrivalFISHDate < b.DateObtained THEN True
            ELSE False
        END
    ;

# Repeat until no earlier found
UPDATE caisis.playground a, temp.relevantfish b
    SET a.ArrivalFISH = b.PathResult
        , a.ArrivalFISHDate = b.DateObtained
    WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
        AND CASE 
            WHEN a.ArrivalFISH IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
            WHEN a.ArrivalFISHDate < b.DateObtained THEN True
            ELSE False
        END
    ;
    
# Repeat until no earlier found
UPDATE caisis.playground a, temp.relevantfish b
    SET a.ArrivalFISH = b.PathResult
        , a.ArrivalFISHDate = b.DateObtained
    WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
        AND CASE 
            WHEN a.ArrivalFISH IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
            WHEN a.ArrivalFISHDate < b.DateObtained THEN True
            ELSE False
        END
    ;
    
# Repeat until no earlier found
UPDATE caisis.playground a, temp.relevantfish b
    SET a.ArrivalFISH = b.PathResult
        , a.ArrivalFISHDate = b.DateObtained
    WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
        AND CASE 
            WHEN a.ArrivalFISH IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
            WHEN a.ArrivalFISHDate < b.DateObtained THEN True
            ELSE False
        END
    ;
    
# Repeat until no earlier found
UPDATE caisis.playground a, temp.relevantfish b
    SET a.ArrivalFISH = b.PathResult
        , a.ArrivalFISHDate = b.DateObtained
    WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
        AND CASE 
            WHEN a.ArrivalFISH IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
            WHEN a.ArrivalFISHDate < b.DateObtained THEN True
            ELSE False
        END
    ;
    
# Repeat until no earlier found
UPDATE caisis.playground a, temp.relevantfish b
    SET a.ArrivalFISH = b.PathResult
        , a.ArrivalFISHDate = b.DateObtained
    WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
        AND CASE 
            WHEN a.ArrivalFISH IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
            WHEN a.ArrivalFISHDate < b.DateObtained THEN True
            ELSE False
        END
    ;
    
# Repeat until no earlier found
UPDATE caisis.playground a, temp.relevantfish b
    SET a.ArrivalFISH = b.PathResult
        , a.ArrivalFISHDate = b.DateObtained
    WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
        AND CASE 
            WHEN a.ArrivalFISH IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
            WHEN a.ArrivalFISHDate < b.DateObtained THEN True
            ELSE False
        END
    ;
# END 2

# START 3

/************************************************************************************************
    Update to find the CGAT closest to arrival
    UPDATE caisis.playground SET CGATKaryotype = NULL, ArrivalFISHDate = NULL ;
*/

DROP TABLE IF EXISTS temp.relevantCGAT ;
CREATE TABLE temp.relevantCGAT
    SELECT a.ArrivalDate
            , a.ArrivalDX
            , a.TreatmentStartDate
            , CASE
                WHEN a.TreatmentStartDate IS NULL 
                    THEN timestampdiff(day,b.DateObtained,a.ArrivalDate)
                ELSE timestampdiff(day,b.DateObtained,a.TreatmentStartDate)
            END AS CGATDaysBeforeTreatmentorArrival
            , b.* FROM caisis.playground a
        JOIN caisis.allkaryo b
        ON a.PatientId = b.PatientId 
        WHERE b.PathResult <> ''
            AND b.PathTest IN ('UW CGAT','SCCA CGAT')
            AND b.PathResult NOT IN (''
                ,'FAILED TO IDENTIFY ICSN DIAGNOSIS'
                ,'See Table'
                ,'Inadequate for Chromosome Genomic Array Testing (CGAT)'
            )
            AND b.DateObtained BETWEEN date_add(a.ArrivalDate, INTERVAL -90 DAY) 
                AND CASE 
                    WHEN a.TreatmentStartDate IS NULL THEN ArrivalDate
                    ELSE a.TreatmentStartDate
                END
    ;

UPDATE caisis.playground a, 
    (SELECT PatientId, ArrivalDate, PathResult, DateObtained, CGATDaysBeforeTreatmentorArrival
        FROM temp.relevantCGAT
        WHERE PathResult <> ''
        GROUP BY PatientId, ArrivalDate
        HAVING COUNT(*)=1 )  b
    SET a.ArrivalCGAT = b.PathResult
        , a.ArrivalCGATDate = b.DateObtained
    WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
    ;

# Repeat until no earlier found
UPDATE caisis.playground a, temp.relevantCGAT b
    SET a.ArrivalCGAT = b.PathResult
        , a.ArrivalCGATDate = b.DateObtained
    WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
        AND CASE 
            WHEN a.ArrivalCGAT IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
            WHEN a.ArrivalCGATDate < b.DateObtained THEN True
            ELSE False
        END
    ;

# Repeat until no earlier found
UPDATE caisis.playground a, temp.relevantCGAT b
    SET a.ArrivalCGAT = b.PathResult
        , a.ArrivalCGATDate = b.DateObtained
    WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
        AND CASE 
            WHEN a.ArrivalCGAT IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
            WHEN a.ArrivalCGATDate < b.DateObtained THEN True
            ELSE False
        END
    ;

# Repeat until no earlier found
UPDATE caisis.playground a, temp.relevantCGAT b
    SET a.ArrivalCGAT = b.PathResult
        , a.ArrivalCGATDate = b.DateObtained
    WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
        AND CASE 
            WHEN a.ArrivalCGAT IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
            WHEN a.ArrivalCGATDate < b.DateObtained THEN True
            ELSE False
        END
    ;

# Repeat until no earlier found
UPDATE caisis.playground a, temp.relevantCGAT b
    SET a.ArrivalCGAT = b.PathResult
        , a.ArrivalCGATDate = b.DateObtained
    WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
        AND CASE 
            WHEN a.ArrivalCGAT IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
            WHEN a.ArrivalCGATDate < b.DateObtained THEN True
            ELSE False
        END
    ;

# Repeat until no earlier found
UPDATE caisis.playground a, temp.relevantCGAT b
    SET a.ArrivalCGAT = b.PathResult
        , a.ArrivalCGATDate = b.DateObtained
    WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
        AND CASE 
            WHEN a.ArrivalCGAT IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
            WHEN a.ArrivalCGATDate < b.DateObtained THEN True
            ELSE False
        END
    ;

# Repeat until no earlier found
UPDATE caisis.playground a, temp.relevantCGAT b
    SET a.ArrivalCGAT = b.PathResult
        , a.ArrivalCGATDate = b.DateObtained
    WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
        AND CASE 
            WHEN a.ArrivalCGAT IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
            WHEN a.ArrivalCGATDate < b.DateObtained THEN True
            ELSE False
        END
    ;

# Repeat until no earlier found
UPDATE caisis.playground a, temp.relevantCGAT b
    SET a.ArrivalCGAT = b.PathResult
        , a.ArrivalCGATDate = b.DateObtained
    WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
        AND CASE 
            WHEN a.ArrivalCGAT IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
            WHEN a.ArrivalCGATDate < b.DateObtained THEN True
            ELSE False
        END
    ;

# END 3


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

