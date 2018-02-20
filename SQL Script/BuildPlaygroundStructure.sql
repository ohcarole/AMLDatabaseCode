/*************************************************************************************
Recreates the table vdatasetarrivalwithprevnext
*/
call caisis.`Create Arrival with Prev and Next 20180125`();

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
       
       # response for this arrival treatment
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

DROP TABLE IF EXISTS `Temp`.`PlaygroundTemp`;
DROP TABLE IF EXISTS `Temp`.`Treatment`;
DROP TABLE IF EXISTS `Temp`.`NonTreatment`;