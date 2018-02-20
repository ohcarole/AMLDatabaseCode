SELECT * FROM caisis.vdatasetmedicaltherapy ;
SELECT * FROM caisis.backbonemapping ;
SELECT BackboneName, count(*) AS BackboneCount FROM caisis.backbonemapping GROUP BY BackboneName ;

DROP TABLE IF EXISTS TEMP.CaisisMedTherapy ;
CREATE TABLE TEMP.CaisisMedTherapy
        SELECT 
            a.PtMRN
            , a.PatientId
            , a.MedicalTherapyId
            , a.MedTxDate
            , a.MedTxDateText
            , a.MedTxDate AS CycleDate
            , a.MedTxDateText AS CycleDateText
            , a.MedTxType
            , a.MedTxDisease
            , concat(a.MedTxIntent,' Regimen') AS MedTxIntent
            , a.MedTxAgent AS OriginalMedTxAgent
            , LTRIM(RTRIM(UPPER(a.MedTxAgent))) AS MedTxAgent
            , LTRIM(RTRIM(UPPER(a.MedTxAgent))) AS MedTxAgentNoParen
            , LOCATE('(',LTRIM(RTRIM(UPPER(a.MedTxAgent)))) AS FirstParen
            # , LOCATE(')',LTRIM(RTRIM(UPPER(a.MedTxAgent)))) AS LastParen
            , LENGTH(LTRIM(RTRIM(UPPER(a.MedTxAgent)))) - LOCATE(')',LTRIM(RTRIM(REVERSE(UPPER(a.MedTxAgent)))))  AS LastParen
            , a.MedTxCycle
            , a.MedTxNotes
            , 'Regimen' AS TreatmentType
            , ' 1' AS DisplayOrder
        FROM
            caisis.vdatasetmedicaltherapy a
        WHERE YEAR(MedTxDate) > 2007
    UNION
        SELECT 
            a.PtMRN
            , a.PatientId
            , a.MedicalTherapyId
            , a.MedTxDate
            , a.MedTxDateText
            , b.MedTxAdminStartDate AS CycleDate
            , b.MedTxAdminStartDateText AS CycleDateText
            , a.MedTxType
            , a.MedTxDisease
            , CASE
                WHEN a.MedTxDate = b.MedTxAdminStartDate THEN concat('First ',a.MedTxIntent,' Cycle')
                ELSE 'Cycle'
            END AS MedTxIntent
            , b.MedTxAdminAgent AS OriginalMedTxAgent
            , LTRIM(RTRIM(UPPER(b.MedTxAdminAgent))) AS MedTxAgent
            , LTRIM(RTRIM(UPPER(b.MedTxAdminAgent))) AS MedTxAgentNoParen
            , LOCATE('(',LTRIM(RTRIM(UPPER(b.MedTxAdminAgent)))) AS FirstParen
            # , LOCATE(')',LTRIM(RTRIM(UPPER(b.MedTxAdminAgent)))) AS LastParen
            , LENGTH(LTRIM(RTRIM(UPPER(b.MedTxAdminAgent)))) - LOCATE(')',LTRIM(RTRIM(REVERSE(UPPER(b.MedTxAdminAgent)))))  AS LastParen
            , a.MedTxCycle
            , b.MedTxAdminNotes AS MedTxNotes
            , 'Cycle' AS TreatmentType
            , ' 2' AS DisplayOrder
        FROM
            caisis.vdatasetmedicaltherapy a
            LEFT JOIN caisis.vdatasetmedtxadministration b ON a.MedicalTherapyId = b.MedicalTherapyId and a.MedTxDate <> b.MedTxAdminStartDate
        WHERE b.MedicalTherapyId IS NOT NULL
        AND YEAR(MedTxDate) > 2007
    ORDER BY PtMRN, MedTxDate;




UPDATE TEMP.CaisisMedTherapy 
    SET MedTxAgentNoParen = LTRIM(RTRIM(REPLACE(MedTxAgent, SUBSTRING(MedTxAgent,FirstParen,LastParen-FirstParen+2),''))),
    CycleDate = IF(DisplayOrder = ' 1',NULL,CycleDate),
    CycleDateText = IF(DisplayOrder = ' 1',NULL,CycleDateText);

SELECT * FROM TEMP.CaisisMedTherapy WHERE FirstParen > 0 ;  

ALTER TABLE `temp`.`caisismedtherapy` DROP COLUMN `LastParen`, DROP COLUMN `FirstParen`;

SELECT * FROM TEMP.CaisisMedTherapy ;  

SELECT PtMRN,
    PatientId,
    MedicalTherapyId,
    MedTxDate,
    MedTxDateText,
    MedTxType,
    MedTxDisease,
    MedTxIntent,
    OriginalMedTxAgent,
    MedTxAgent,
    MedTxAgentNoParen,
    MedTxCycle,
    TreatmentType
FROM
    TEMP.CaisisMedTherapy;

/*
Looking at frequency of disease and treatments entered
*/
SELECT MedTxDisease, COUNT(*) FROM TEMP.CaisisMedTherapy GROUP BY MedTxDisease ;
SELECT MedTxIntent, COUNT(*) FROM TEMP.CaisisMedTherapy GROUP BY MedTxIntent ;

/*
This query looks at all the various other types of disease with treatment information
*/
SELECT MedTxDisease, COUNT(*) FROM TEMP.CaisisMedTherapy 
    WHERE MedTxDisease NOT RLIKE 'A.L' 
        AND MedTxDisease NOT RLIKE 'MDS'
        AND MedTxDisease NOT RLIKE 'RAEB'
        AND MedTxDisease NOT RLIKE 'RCMD'
        AND MedTxDisease NOT RLIKE 'BPDCN'
        AND UPPER(MedTxDisease) NOT RLIKE 'REL'
    GROUP BY MedTxDisease ;


/*
Remove treatment records other diseases
since we don't need to map them.
When exporting these treatment we will map to "other"
*/
DELETE FROM TEMP.CaisisMedTherapy 
    WHERE MedTxDisease NOT RLIKE 'A.L' 
        AND MedTxDisease NOT RLIKE 'MDS'
        AND MedTxDisease NOT RLIKE 'RAEB'
        AND MedTxDisease NOT RLIKE 'RCMD'
        AND MedTxDisease NOT RLIKE 'BPDCN'
        AND UPPER(MedTxDisease) NOT RLIKE 'REL';

/*
Looking at frequency of disease and treatments entered
*/
SELECT MedTxDisease, COUNT(*) FROM TEMP.CaisisMedTherapy GROUP BY MedTxDisease ;
SELECT MedTxIntent, COUNT(*) FROM TEMP.CaisisMedTherapy GROUP BY MedTxIntent ;


DROP TABLE IF EXISTS TEMP.CaisisJoinedToBackBone ;
CREATE TABLE TEMP.CaisisJoinedToBackBone    
    SELECT DISTINCT a.*, 
        b.* FROM TEMP.CaisisMedTherapy a 
        LEFT JOIN (
        SELECT LTRIM(RTRIM(UPPER(OriginalProtocol))) AS Protocol
            , OriginalProtocol
            , BackboneType
            , BackboneName
            , Anthracyclin
            , AnthracyclinDose
            , BackboneAddOn
            , Intensity
        FROM caisis.backbonemapping) b on a.MedTxAgent = b.OriginalProtocol OR a.MedTxAgentNoParen = b.OriginalProtocol
        WHERE MedTxIntent IN ( 'Induction Regimen'
            , 'Re-induction Regimen'
            , 'Regimen'
            , 'Cycle'
            , 'Reinduction Regimen'
            , 'Salvage 1 Regimen'
            , 'Salvage 2 Regimen'
            , 'Salvage 3 Regimen'
            , 'Salvage 4 Regimen'
            , 'Salvage 5 Regimen'
            , 'Salvage >5 Regimen'
            , 'Salvage Regimen'
            );    

SELECT * FROM TEMP.CaisisJoinedToBackBone ;
SELECT * FROM TEMP.CaisisJoinedToBackBone WHERE BackBoneName IS NULL;
SELECT PtMRN
        , MedTxDate
        , OriginalMedTxAgent
        , MedTxAgent
        , MedTxAgentNoParen
        , MedTxIntent
        , OriginalProtocol
        , BackboneType
        , BackboneName
        , Anthracyclin
        , AnthracyclinDose
        , BackboneAddOn
        , Intensity
        , COUNT(*) AS Total
    FROM TEMP.CaisisJoinedToBackBone 
    WHERE BackBoneName IS NOT NULL 
    GROUP BY PtMRN, MedTxAgent, MedTxDate, MedTxAgentNoParen, MedTxIntent ;
    
SELECT OriginalMedTxAgent
        , MedTxAgent
        , MedTxAgentNoParen
        , MedTxIntent
        , OriginalProtocol
        , BackboneType
        , BackboneName
        , Anthracyclin
        , AnthracyclinDose
        , BackboneAddOn
        , Intensity
        , COUNT(*) AS Total
    FROM TEMP.CaisisJoinedToBackBone 
    WHERE BackBoneName IS NULL 
    GROUP BY MedTxAgent, MedTxAgentNoParen, MedTxIntent ;

    
# Run this query to ascertain if the numbers of patients identified as GCLAM makes sense.
SELECT YEAR(MedTxDate) AS Year, 
    SUM(IF(LOCATE('CLAM',BackBoneName)>0,1,0)) AS GCLAM, 
    SUM(IF(LOCATE('FLAG',BackBoneName)>0,1,0)) AS `FLAG or FLAG/IDA`, 
    COUNT(*) AS `All Induction/Salvage`
    FROM TEMP.CaisisJoinedToBackBone 
    WHERE MedTxIntent <> 'Cycle' AND DisplayOrder = ' 1' AND YEAR(MedTxDate) = 2008 
UNION
SELECT YEAR(MedTxDate) AS Year, 
    SUM(IF(LOCATE('CLAM',BackBoneName)>0,1,0)) AS GCLAM, 
    SUM(IF(LOCATE('FLAG',BackBoneName)>0,1,0)) AS `FLAG or FLAG/IDA`, 
    COUNT(*) AS `All Induction/Salvage`
    FROM TEMP.CaisisJoinedToBackBone 
    WHERE MedTxIntent <> 'Cycle' AND DisplayOrder = ' 1' AND YEAR(MedTxDate) = 2009
UNION
SELECT YEAR(MedTxDate) AS Year, 
    SUM(IF(LOCATE('CLAM',BackBoneName)>0,1,0)) AS GCLAM, 
    SUM(IF(LOCATE('FLAG',BackBoneName)>0,1,0)) AS `FLAG or FLAG/IDA`, 
    COUNT(*) AS `All Induction/Salvage`
    FROM TEMP.CaisisJoinedToBackBone 
    WHERE MedTxIntent <> 'Cycle' AND DisplayOrder = ' 1' AND YEAR(MedTxDate) = 2010
UNION
SELECT YEAR(MedTxDate) AS Year, 
    SUM(IF(LOCATE('CLAM',BackBoneName)>0,1,0)) AS GCLAM, 
    SUM(IF(LOCATE('FLAG',BackBoneName)>0,1,0)) AS `FLAG or FLAG/IDA`, 
    COUNT(*) AS `All Induction/Salvage`
    FROM TEMP.CaisisJoinedToBackBone 
    WHERE MedTxIntent <> 'Cycle' AND DisplayOrder = ' 1' AND YEAR(MedTxDate) = 2011
UNION
SELECT YEAR(MedTxDate) AS Year, 
    SUM(IF(LOCATE('CLAM',BackBoneName)>0,1,0)) AS GCLAM, 
    SUM(IF(LOCATE('FLAG',BackBoneName)>0,1,0)) AS `FLAG or FLAG/IDA`, 
    COUNT(*) AS `All Induction/Salvage`
    FROM TEMP.CaisisJoinedToBackBone 
    WHERE MedTxIntent <> 'Cycle' AND DisplayOrder = ' 1' AND YEAR(MedTxDate) = 2012
UNION
SELECT YEAR(MedTxDate) AS Year, 
    SUM(IF(LOCATE('CLAM',BackBoneName)>0,1,0)) AS GCLAM, 
    SUM(IF(LOCATE('FLAG',BackBoneName)>0,1,0)) AS `FLAG or FLAG/IDA`, 
    COUNT(*) AS `All Induction/Salvage`
    FROM TEMP.CaisisJoinedToBackBone 
    WHERE MedTxIntent <> 'Cycle' AND DisplayOrder = ' 1' AND YEAR(MedTxDate) = 2013
UNION
SELECT YEAR(MedTxDate) AS Year, 
    SUM(IF(LOCATE('CLAM',BackBoneName)>0,1,0)) AS GCLAM, 
    SUM(IF(LOCATE('FLAG',BackBoneName)>0,1,0)) AS `FLAG or FLAG/IDA`, 
    COUNT(*) AS `All Induction/Salvage`
    FROM TEMP.CaisisJoinedToBackBone 
    WHERE MedTxIntent <> 'Cycle' AND DisplayOrder = ' 1' AND YEAR(MedTxDate) = 2014
UNION
SELECT YEAR(MedTxDate) AS Year, 
    SUM(IF(LOCATE('CLAM',BackBoneName)>0,1,0)) AS GCLAM, 
    SUM(IF(LOCATE('FLAG',BackBoneName)>0,1,0)) AS `FLAG or FLAG/IDA`, 
    COUNT(*) AS `All Induction/Salvage`
    FROM TEMP.CaisisJoinedToBackBone 
    WHERE MedTxIntent <> 'Cycle' AND DisplayOrder = ' 1' AND YEAR(MedTxDate) = 2015
UNION
SELECT YEAR(MedTxDate) AS Year, 
    SUM(IF(LOCATE('CLAM',BackBoneName)>0,1,0)) AS GCLAM, 
    SUM(IF(LOCATE('FLAG',BackBoneName)>0,1,0)) AS `FLAG or FLAG/IDA`, 
    COUNT(*) AS `All Induction/Salvage`
    FROM TEMP.CaisisJoinedToBackBone 
    WHERE MedTxIntent <> 'Cycle' AND DisplayOrder = ' 1' AND YEAR(MedTxDate) = 2016
UNION
SELECT YEAR(MedTxDate) AS Year, 
    SUM(IF(LOCATE('CLAM',BackBoneName)>0,1,0)) AS GCLAM, 
    SUM(IF(LOCATE('FLAG',BackBoneName)>0,1,0)) AS `FLAG or FLAG/IDA`, 
    COUNT(*) AS `All Induction/Salvage`
    FROM TEMP.CaisisJoinedToBackBone 
    WHERE MedTxIntent <> 'Cycle' AND DisplayOrder = ' 1' AND YEAR(MedTxDate) = 2017
UNION
SELECT YEAR(MedTxDate) AS Year, 
    SUM(IF(LOCATE('CLAM',BackBoneName)>0,1,0)) AS GCLAM, 
    SUM(IF(LOCATE('FLAG',BackBoneName)>0,1,0)) AS `FLAG or FLAG/IDA`, 
    COUNT(*) AS `All Induction/Salvage`
    FROM TEMP.CaisisJoinedToBackBone 
    WHERE MedTxIntent <> 'Cycle' AND DisplayOrder = ' 1' AND YEAR(MedTxDate) > 2007 ;

SELECT COUNT(*) AS GCLAM
FROM (SELECT COUNT(*) 
        FROM TEMP.CaisisJoinedToBackBone
        WHERE MedTxIntent <> 'Cycle' AND LOCATE('CLAM', BackBoneName) > 0 AND YEAR(MedTxDate) > 2007
        GROUP BY PtMRN) a ;

SELECT COUNT(*) AS `FLAG or FLAG/IDA`
FROM (SELECT COUNT(*) 
        FROM TEMP.CaisisJoinedToBackBone
        WHERE MedTxIntent <> 'Cycle' AND LOCATE('FLAG', BackBoneName) > 0 AND YEAR(MedTxDate) > 2007
        GROUP BY PtMRN) b ;

SELECT COUNT(*) AS `All Induction/Salvage`
FROM (SELECT COUNT(*) 
        FROM TEMP.CaisisJoinedToBackBone
        WHERE MedTxIntent <> 'Cycle' AND YEAR(MedTxDate) > 2007  
        GROUP BY PtMRN) c ;
        
        
/*
At this point the data are as clean as I can make it with respect to the mapping from Kelda, 
however there remains mapping issues since additional data have been entered or migrated with differing
codes.  Below code attempts to map these where an existing mapping has not been found.
*/        




DROP TABLE IF EXISTS Temp.BackBoneMappingComplete ;
CREATE TABLE Temp.BackBoneMappingComplete
    SELECT OriginalMedTxAgent
            , MedTxAgent
            , MedTxAgentNoParen
            , MedTxIntent
            , OriginalProtocol
            , BackboneType
            , BackboneName
            , Anthracyclin
            , AnthracyclinDose
            , LTRIM(RTRIM(SUBSTRING(BackboneAddOn,2,99))) AS BackboneAddOn
            , Intensity
            , COUNT(*) AS Total
        FROM TEMP.CaisisJoinedToBackBone 
        WHERE BackBoneName IS NOT NULL
        GROUP BY MedTxAgent, MedTxAgentNoParen, MedTxIntent ;

ALTER TABLE `temp`.`BackBoneMappingComplete` 
    ADD COLUMN `BackBoneMappingCompleteId` INT(4) NOT NULL AUTO_INCREMENT FIRST,
    ADD PRIMARY KEY (`BackBoneMappingCompleteId`);

UPDATE `temp`.`BackBoneMappingComplete` 
    SET BackBoneAddOn = REPLACE(BackBoneAddOn,',','+') ;

SELECT * FROM Temp.BackBoneMappingComplete ;

DROP TABLE IF EXISTS Temp.BackBoneList ;
CREATE TABLE Temp.BackBoneList
    SELECT BackBoneName
        , group_concat(CASE
                WHEN LTRIM(RTRIM(BackBoneAddOn)) = '' THEN NULL
                ELSE BackBoneAddOn 
        END) AS BackBoneAddOnList
    FROM (SELECT DISTINCT BackBoneName, BackBoneAddOn FROM Temp.BackBoneMappingComplete) a GROUP BY 1 ;

UPDATE Temp.BackBoneList SET BackBoneAddOnList = REPLACE(BackBoneAddOnList,',','|CTRL J|');
SELECT * FROM Temp.BackBoneList ;


DROP TABLE IF EXISTS Temp.BackBoneMapping ;
CREATE TABLE Temp.BackBoneMapping
    SELECT Intensity
            , BackboneType
            , BackboneName
            , group_concat(Anthracyclin,
                CASE
                    WHEN AnthracyclinDose > '' THEN CONCAT(" (",AnthracyclinDose,")")
                    ELSE NULL
                END )
                AS `Accociated Anthracyclins`
            , group_concat(CASE
                    WHEN LTRIM(RTRIM(Anthracyclin)) = '' THEN NULL
                    ELSE Anthracyclin 
            END) AS `Various Add On's Seen`
            , Sum(Total) AS Total
        FROM TEMP.BackBoneMappingComplete  
        WHERE BackBoneName IS NOT NULL
        GROUP BY BackBoneName 
        ORDER BY Intensity, BackBoneType, BackBoneName;

ALTER TABLE `temp`.`BackBoneMapping` 
    ADD COLUMN `BackBoneMappingId` INT(4) NOT NULL AUTO_INCREMENT FIRST,
    ADD PRIMARY KEY (`BackBoneMappingId`);

SELECT * FROM Temp.BackBoneMapping ;


DROP TABLE IF EXISTS Temp.BackBoneMappingFailures ;
CREATE TABLE Temp.BackBoneMappingFailures
    SELECT OriginalMedTxAgent
            , MedTxAgent
            , MedTxAgentNoParen
            , MedTxIntent
            , OriginalProtocol
            , BackboneType
            , BackboneName
            , Anthracyclin
            , AnthracyclinDose
            , BackboneAddOn
            , Intensity
            , COUNT(*) AS Total
        FROM TEMP.CaisisJoinedToBackBone 
        WHERE BackBoneName IS NULL AND OriginalMedTxAgent IS NOT NULL
        GROUP BY MedTxAgent, MedTxAgentNoParen, MedTxIntent ;
        
ALTER TABLE `temp`.`backbonemappingfailures` 
    ADD COLUMN `backbonemappingfailuresid` INT(4) NOT NULL AUTO_INCREMENT FIRST,
    ADD PRIMARY KEY (`backbonemappingfailuresid`);

SELECT * FROM Temp.BackBoneMappingFailures ;

SELECT CONCAT("     , '",MedTxAgent,"'") AS MedTxAgent
    , CONCAT("     , '",MedTxAgentNoParen,"'") AS MedTxAgentNoParen
    FROM Temp.BackBoneMappingFailures ;

-- GCLAM FIXES
UPDATE temp.BackBoneMappingFailures 
    SET BackboneName = 'G-CLAM'
        , Anthracyclin = ',mitoxantrone'
    WHERE BackboneName IS NULL
        AND MedTxAgent IN ('2734 G-CLAM'
            , 'DECITABINE+G-CLAM+SORAFENIB'
            , 'G CLAM'
            , 'G-CLAM + SORAFENIB'
            , 'G-CLAM + VINCRISTINE'
            , 'G-CLAM + VINCRISTINE + DEX'
            , 'G-CLAM +TKI'
            , 'G-CLAM+GO'
            , 'G-CLAM+SORAFENIB'
            , 'G-CLAM+SORAFENIB'
            , 'G-CLAM+VP'
            , 'GCLAM + SORA'
            , 'S3-GCLAM');

UPDATE temp.BackBoneMappingFailures 
    SET BackboneAddOn = CONCAT(BackboneAddOn,',decitabine')
    WHERE BackboneName IS NULL
        AND MedTxAgent IN ('DECITABINE+G-CLAM+SORAFENIB');

UPDATE temp.BackBoneMappingFailures 
    SET BackboneAddOn = CONCAT(BackboneAddOn,',sorafenib')
    WHERE BackboneName IS NULL
        AND MedTxAgent IN ('DECITABINE+G-CLAM+SORAFENIB'
            , 'G-CLAM + SORAFENIB'
            , 'G-CLAM+SORAFENIB'
            , 'G-CLAM+SORAFENIB'
            , 'GCLAM + SORA');

UPDATE temp.BackBoneMappingFailures 
    SET BackboneAddOn = CONCAT(BackboneAddOn,',dexamethesone')
    WHERE BackboneName IS NULL
        AND MedTxAgent IN ('G-CLAM + VINCRISTINE + DEX');

UPDATE temp.BackBoneMappingFailures 
    SET BackboneAddOn = CONCAT(BackboneAddOn,',vincristine')
    WHERE BackboneName IS NULL
        AND MedTxAgent IN ('G-CLAM + VINCRISTINE'
            , 'G-CLAM + VINCRISTINE + DEX'
            , 'G-CLAM+VP');

UPDATE temp.BackBoneMappingFailures 
    SET BackboneAddOn = CONCAT(BackboneAddOn,',tyrosine kinase inhibitor')
    WHERE BackboneName IS NULL
        AND MedTxAgent IN ('G-CLAM +TKI');

UPDATE temp.BackBoneMappingFailures 
    SET BackboneAddOn = CONCAT(BackboneAddOn,',gemtuzumab ozogamicin')
    WHERE BackboneName IS NULL
        AND MedTxAgent IN ('G-CLAM+GO');
        
