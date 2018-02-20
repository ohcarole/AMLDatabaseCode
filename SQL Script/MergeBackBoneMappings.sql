SELECT * FROM `caisis`.`backbonemapping2`;

SELECT `OriginalProtocol`
    , `BackboneType`
    , `BackboneName`
    , `Anthracyclin`
    , `AnthracyclinDose`
    , `BackboneAddOn`
    , `Intensity`
FROM `caisis`.`backbonemapping`;

DROP TABLE TEMP.BackBoneMapping2 ;
CREATE TABLE TEMP.BackBoneMapping2
SELECT `FailureId`
    , `OriginalMedTxAgent`
    , `MedTxAgent`
    , `MedTxAgentNoParen`
    , `MedTxIntent`
    , `BackboneName`
    , `BackboneAddOn`
    , `Anthracyclin`
    , `Dose`
    , `Intensity` AS `Intenstity_`
    , GROUP_CONCAT(CASE
        WHEN `Intensity` IS NULL THEN `Intensity_`
        WHEN `Intensity` = '' THEN `Intensity_`
        ELSE `Intensity`
    END) AS `Intensity`
    , `Pull Out Consolidation/Conditioning/Maintenance`
    , `Total`
FROM `caisis`.`backbonemapping2` a
    LEFT JOIN (
        SELECT `BackBoneName` as `BackBoneName_`
                , `Intensity` AS `Intensity_`
            FROM `caisis`.`backbonemapping` Group By `BackBoneName_`, `Intensity_`) b 
    ON a.`BackBoneName` = b.`BackBoneName_` 
GROUP BY FailureId ;

SELECT * FROM  caisis.BackBoneMapping2 WHERE BackBoneName = 'Skip';


DROP TABLE IF EXISTS `caisis`.`BackboneMapping3`;
CREATE TABLE `caisis`.`BackboneMapping3`
    SELECT 'BackBone1' AS `Mapping`
        , -9 AS `FailureId`
        , `OriginalProtocol`
        , '' AS `OriginalMedTxAgent`
        , '' AS `MedTxAgent`
        , '' AS `MedTxAgentNoParen`
        , `BackboneType`
        , `BackboneName`
        , `Anthracyclin`
        , `AnthracyclinDose`
        , `BackboneAddOn`
        , `Intensity`
    FROM `caisis`.`backbonemapping`
    UNION
    SELECT 'BackBone2a' AS `Mapping`
        , `FailureId`
        , `OriginalMedTxAgent` AS `OriginalProtocol`
        , `OriginalMedTxAgent`
        , `MedTxAgent`
        , `MedTxAgentNoParen`
        , '' AS `BackboneType`
        , `BackboneName`
        , `Anthracyclin`
        , CASE
            WHEN `Dose` = 0 THEN NULL
            ELSE `Dose`
        END AS `AnthracyclinDose`
        , `BackboneAddOn`
        , `Intensity`
    FROM `temp`.`backbonemapping2`
    WHERE `OriginalMedTxAgent` IS NOT NULL 
        AND `OriginalMedTxAgent` > '' 
    UNION 
    SELECT 'BackBone2b' AS `Mapping`
        , `FailureId`
        , `MedTxAgent` AS `OriginalProtocol`
        , `OriginalMedTxAgent`
        , `MedTxAgent`
        , `MedTxAgentNoParen`
        , '' AS `BackboneType`
        , `BackboneName`
        , `Anthracyclin`
        , CASE
            WHEN `Dose` = 0 THEN NULL
            ELSE `Dose`
        END AS `AnthracyclinDose`
        , `BackboneAddOn`
        , `Intensity`
    FROM `temp`.`backbonemapping2`
    WHERE `MedTxAgent` IS NOT NULL 
        AND `MedTxAgent` > '' 
    UNION 
    SELECT 'BackBone2c' AS `Mapping`
        , `FailureId`
        , `MedTxAgentNoParen` AS `OriginalProtocol`
        , `OriginalMedTxAgent`
        , `MedTxAgent`
        , `MedTxAgentNoParen`
        , '' AS `BackboneType`
        , `BackboneName`
        , `Anthracyclin`
        , CASE
            WHEN `Dose` = 0 THEN NULL
            ELSE `Dose`
        END AS `AnthracyclinDose`
        , `BackboneAddOn`
        , `Intensity`
    FROM `temp`.`backbonemapping2`
    WHERE `MedTxAgentNoParen` IS NOT NULL 
        AND `MedTxAgentNoParen` > '' 
    ;

SELECT * FROM `caisis`.`backbonemapping2` WHERE FailureID BETWEEN 215 and 220;
SELECT * FROM `caisis`.`backbonemapping3` WHERE FailureID BETWEEN 215 and 220;

SELECT * FROM `caisis`.`BackboneMapping3` ;    

SELECT * FROM `caisis`.`BackboneMapping3` 
        WHERE `Intensity` = 'IT,Low,Unknown,High,Intermediate';    

UPDATE `caisis`.`BackboneName` 
        SET   `BackboneName` = 'Not Mapped'
              , `Intensity`    = ''
        WHERE `BackboneName` = '???' ;

UPDATE `caisis`.`BackboneMapping3` 
        SET   `Intensity` = 'Low'
        WHERE `Intensity` = 'Low,Unknown - Low' ;


UPDATE `caisis`.`BackboneMapping3` 
        SET   `Intensity` = 'Low'
        WHERE `Intensity` = 'Unknown - Low' ;

UPDATE `caisis`.`BackboneMapping3` 
        SET   `Intensity` = 'Low or Intermediate'
        WHERE `Intensity` = 'Intermediate,Low' ;

UPDATE `caisis`.`BackboneMapping3` 
        SET `Intensity` = 'Multiple Intensities Exist'
        WHERE LOCATE(',',`Intensity`) > 0;
        

UPDATE `caisis`.`BackboneMapping3` 
    SET `BackBoneName` = 'No BackBone'
    WHERE `BackboneName` = '' AND `BackboneAddOn` > '';
        
SELECT * FROM `caisis`.`BackboneMapping3` ;    
        