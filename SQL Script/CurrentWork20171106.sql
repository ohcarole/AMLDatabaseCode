SELECT * FROM caisis.vdatasetmedicaltherapy;
SELECT * FROM temp.BackBoneMappingFailures;
SELECT * FROM caisis.BackBoneMapping;



DROP TABLE IF EXISTS temp.temp1;
CREATE TABLE temp.temp1
    SELECT DISTINCT
        PtMRN, 
        PatientId,
        MedicalTherapyId,
        MedTxDate,
        MedTxType,
        MedTxIntent,
        MedTxAgent,
        LEFT(a.MedTxAgent,IF(LOCATE('(',a.MedTxAgent)>0,LOCATE('(',a.MedTxAgent)-1,100)) AS LeftMedTxAgent,
        OriginalProtocol,
        BackboneType,
        BackboneName,
        Anthracyclin,
        AnthracyclinDose,
        BackboneAddOn,
        Intensity
    FROM caisis.vdatasetmedicaltherapy a
        LEFT JOIN caisis.BackBoneMapping b 
            ON LEFT(a.MedTxAgent,IF(LOCATE('(',a.MedTxAgent)>0,LOCATE('(',a.MedTxAgent)-1,100)) = b.OriginalProtocol
    WHERE BackboneName IS NOT NULL
        AND MedTxDisease LIKE '%aml%'
        AND (MedTxIntent LIKE '%induct%' OR MedTxIntent LIKE '%salvage%');
SELECT * FROM temp.temp1 ;


DROP TABLE IF EXISTS temp.temp2;
CREATE TABLE temp.temp2
    SELECT DISTINCT
        PtMRN, 
        PatientId,
        a.MedicalTherapyId,
        MedTxDate,
        MedTxType,
        MedTxIntent,
        a.MedTxAgent,
        LEFT(a.MedTxAgent,IF(LOCATE('(',a.MedTxAgent)>0,LOCATE('(',a.MedTxAgent)-1,100)) AS LeftMedTxAgent,
        OriginalProtocol,
        SPACE(20) AS BackBoneType,
        T_BackBone AS BackboneName,
        T_Anthracyclin AS Anthracyclin,
        T_AnthracyclinDose AS AnthracyclinDose,
        T_AddOn AS BackboneAddOn,
        SPACE(45) AS Intensity
    FROM caisis.vdatasetmedicaltherapy a
    LEFT JOIN temp.BackBoneMappingFailures b ON LEFT(a.MedTxAgent,IF(LOCATE('(',a.MedTxAgent)>0,LOCATE('(',a.MedTxAgent)-1,100))  = LEFT(b.MedTxAgent,IF(LOCATE('(',b.MedTxAgent)>0,LOCATE('(',b.MedTxAgent)-1,100)) 
    LEFT JOIN (SELECT MedicalTherapyId FROM temp.temp1) c ON a.MedicalTherapyId = c.MedicalTherapyId
    WHERE   c.MedicalTherapyId IS NULL
        AND a.MedTxDisease LIKE '%aml%'
        # AND T_BackBone IS NOT NULL
        AND (a.MedTxIntent LIKE '%induct%' OR a.MedTxIntent LIKE '%salvage%');   
SELECT * FROM temp.temp2 ;



DROP TABLE IF EXISTS temp.temp3;
CREATE TABLE temp.temp3
    SELECT * FROM temp.temp1 where year(medtxdate) > 2007
    UNION    
    SELECT * FROM temp.temp2 where year(medtxdate) > 2007;    

SELECT  MedTxAgent,
        LeftMedTxAgent,
        OriginalProtocol,
        BackboneType,
        BackboneName,
        Anthracyclin,
        AnthracyclinDose,
        BackboneAddOn,
        Intensity 
FROM Temp.temp3 
WHERE BackboneName IS NULL OR BackBoneName = '' 
GROUP BY MedTxAgent ;