DROP TABLE IF EXISTS Temp.GCLAM ;
CREATE TABLE Temp.GCLAM 
    SELECT PtMRN, ArrivalDate AS AnnaTreatmentStartDate, space(100) as AnnaResponseOriginal
    FROM TEMP.temp1
    WHERE FALSE;
    
INSERT INTO Temp.GCLAM (PtMRN, AnnaTreatmentStartDate, AnnaResponseOriginal) 
VALUES ('U3629271 ', '2014-6-20', 'CR (MRDpos)')
        , ('U3631012 ', '2014-6-20', 'CR (MRDneg)')
        , ('U3637894 ', '2014-7-9', 'CRi (MRDneg)')
        , ('U3327893 ', '2014-7-25', 'CRp (MRDneg)')
        , ('U2020516 ', '2014-7-29', 'MLFS (MRDpos)')
        , ('U3653691 ', '2014-8-2', 'CR (MRDneg)')
        , ('U9423118 ', '2014-9-1', 'DEATH IN APLASIA')
        , ('U2248896', '2014-9-9', 'CR (MRDneg)')
        , ('U3669819', '2014-9-9', 'CR (MRDneg)')
        , ('U3655622 ', '2014-9-10', 'CR (MRDneg)')
        , ('U3679468', '2014-9-15', 'CR (MRDneg)')
        , ('U3680899', '2014-9-22', 'CR (MRDneg)')
        , ('U3492325 ', '2014-9-25', 'CRi (MRDneg)')
        , ('U3678724 ', '2014-10-1', 'CR (MRDneg)')
        , ('U3688691 ', '2014-10-3', 'CR (MRDneg)')
        , ('U3690206 ', '2014-10-15', 'CRp (MRDneg)')
        , ('U3127546 ', '2014-10-17', 'CR (MRDneg)')
        , ('U3685147', '2014-10-24', 'CR (MRDneg)')
        , ('U3705779', '2014-11-5', 'CR (MRDneg)')
        , ('U3404088', '2014-11-13', 'CR (MRDpos)')
        , ('U3717477', '2014-11-24', 'CR (MRDneg)')
        , ('U2713926', '2014-11-25', 'CR (MRDneg)')
        , ('U3716863 ', '2014-12-3', 'CR (MRDneg)')
        , ('U3491268 ', '2014-12-12', 'CR (MRDneg)')
        , ('U3719266 ', '2014-12-17', 'CR (MRDneg)')
        , ('U3731950', '2014-12-29', 'CR (MRDneg)')
        , ('U3736024', '2015-1-2', 'CR (MRDneg)')
        , ('U3576160', '2015-1-5', 'CRi (MRDpos)')
        , ('U3205208', '2015-1-5', 'CR (MRDneg)')
        , ('U2516875', '2015-1-9', 'CR (MRDneg)')
        , ('U3746722', '2015-1-22', 'CR (MRDpos)')
        , ('U3743228', '2015-1-22', 'CR (MRDneg)')
        , ('U8253885', '2015-1-30', 'UNABLE TO ASSESS')
        , ('U3657867', '2015-2-10', 'RESISTANT DISEASE')
        , ('U3757805', '2015-2-12', 'RESISTANT DISEASE')
        , ('U3728905', '2015-2-17', 'CR (MRDneg)')
        , ('U3604701', '2015-2-23', 'RESISTANT DISEASE')
        , ('U3763039', '2015-2-21', 'CR (MRDneg)')
        , ('U3754152', '2015-2-24', 'CR (MRDneg)')
        , ('U2735757', '2015-3-2', 'MLFS (MRDneg)')
        , ('U3765238', '2015-3-5', 'CR (MRDneg)')
        , ('U3772155', '2015-3-16', 'CR (MRDneg)')
        , ('U4921157', '2015-4-1', 'CR (MRDneg)')
        , ('U3779331', '2015-4-10', 'CR (MRDneg)')
        , ('U3788365', '2015-4-13', 'CR (MRDneg)')
        , ('U3792776', '2015-4-18', 'CR (MRDneg)')
        , ('U3794902', '2015-4-24', 'CR (MRDneg)')
        , ('U3803455', '2015-5-9', 'CR (MRDneg)')
        , ('U3813906', '2015-6-8', 'CR (MRDpos)')
        , ('U3823991', '2015-6-17', 'CR (MRDneg)')
        , ('U3818367', '2015-6-17', 'PR (MRDnegmarow)')
        , ('U3354538', '2015-7-6', 'CR (MRDneg)')
        , ('U3835085', '2015-7-6', 'CR (MRDneg)')
        , ('U3825416', '2015-7-11', 'CR (MRDneg)')
        , ('U3816912', '2015-7-15', 'CR (MRDneg)')
        , ('U3853629', '2015-8-7', 'CRp (MRDneg)')
        , ('U3857715', '2015-8-24', 'CR (MRDneg)')
        , ('U3861645', '2015-8-25', 'CR (MRDneg)')
        , ('U3863894', '2015-8-31', 'CR (MRDneg)')
        , ('U2310437', '2015-9-9', 'CR (MRDneg)')
        , ('U3949968', '2015-9-25', 'CR (MRDpos)')
        , ('U3969388', '2015-10-13', 'CR (MRDpos)')
        , ('U2480552', '2015-10-7', 'CR (MRDpos)')
        , ('U3862245', '2015-10-20', 'CR (MRDneg)')
        , ('U3950532', '2015-11-6', 'CR (MRDneg)')
        , ('U3983335', '2015-11-6', 'CR (MRDneg)')
        , ('U3991792', '2015-11-18', 'CR (MRDneg)')
        , ('U3943257', '2015-11-19', 'CRp (MRDpos)')
        , ('U3994880', '2015-11-23', 'CR (MRDneg)')
        , ('U3113443', '2015-12-3', 'MLFS (MRDneg)')
        , ('U2357911', '2015-12-4', 'CR (MRDneg)')
        , ('U4044443', '2015-12-15', 'CR (MRDneg)')
        , ('U3887326', '2015-12-16', 'CR (MRD neg)')
        , ('U3855956', '2015-12-17', 'MLFS (MRDpos)')
        , ('U4050617', '2015-12-28', 'CR (MRDneg)')
        , ('U3971477', '2016-1-4', 'CR (MRDneg)')
        , ('U4045788', '2015-12-17', 'CR (MRDneg)')
        , ('U4058030', '2016-1-10', 'CR (MRDneg)')
        , ('U4065913', '2015-1-20', 'CR (MRDneg)')
        , ('U4066909', '2016-1-25', 'CRp (MRDneg)')
        , ('U0931224', '2016-1-19', 'CR (MRDneg)')
        , ('U2574508', '2016-1-27', 'DEATH FROM INDETERMINATE CAUSE')
        , ('U4052691', '2016-1-1', 'CR (MRDneg)')
        , ('U4068337', '2016-1-30', 'CR (MRDneg)')
        , ('U3997811', '2016-1-28', 'MLFS (MRDneg)')
        , ('U6163541', '2016-2-1', 'RESISTANT DISEASE')
        , ('U4766533', '2016-2-9', 'CR (MRDpos)')
        , ('U3088796', '2016-2-11', 'CR (MRDneg)')
        , ('U4061608', '2016-2-4', 'CR (MRDneg)');

DROP TABLE IF EXISTS temp.agreement;
CREATE TABLE temp.agreement
SELECT a.PtMRN
    , b.ArrivalDate
    , StatusDisease AS ArrivalDiagnosis
    , Disease AS DiseaseCategory
    , AnnaTreatmentStartDate
    , b.MedTxDate AS DatabaseTreatmentStartDate
    , a.AnnaResponseOriginal
    , CONCAT(CASE
        WHEN AnnaResponseOriginal LIKE 'CRi%' THEN 'CRi'
        WHEN AnnaResponseOriginal LIKE 'CRp%' THEN 'CRi'
        WHEN AnnaResponseOriginal LIKE 'CR %' THEN 'CR'
        WHEN AnnaResponseOriginal LIKE 'MLFS%' THEN 'MLFS'
        WHEN AnnaResponseOriginal LIKE '%resistant%' THEN 'Resistant/Persistant'
        WHEN AnnaResponseOriginal LIKE '%death%' THEN 'Death'
        ELSE AnnaResponseOriginal
    END,' ',CASE
        WHEN AnnaResponseOriginal LIKE '%neg%' THEN 'no MRD'
        WHEN AnnaResponseOriginal LIKE '%pos%' THEN 'with MRD'
        WHEN AnnaResponseOriginal LIKE '%resistant%' THEN ''
        WHEN AnnaResponseOriginal LIKE '%death%' THEN ''
        ELSE AnnaResponseOriginal
    END) AS AnnaResponse
    , Status AS DatabaseResponseOriginal
    , CASE
        WHEN Response IS NULL THEN 'Not Recorded'
        WHEN Response = 'No CR' 
        THEN CASE
                WHEN Status LIKE '%resist%'  THEN 'Resistant/Persistant'
                WHEN Status LIKE '%persist%' THEN 'Resistant/Persistant'
                WHEN Status LIKE '%death%'   THEN 'Death'
                ELSE Status 
            END
        ELSE Response
    END AS DatabaseResponse
    , CONCAT(CASE
        WHEN AnnaResponseOriginal LIKE 'CRi%' THEN 'CRi'
        WHEN AnnaResponseOriginal LIKE 'CRp%' THEN 'CRi'
        WHEN AnnaResponseOriginal LIKE 'CR %' THEN 'CR'
        WHEN AnnaResponseOriginal LIKE '%resistant%' THEN 'Resistant/Persistant'
        WHEN AnnaResponseOriginal LIKE '%death%' THEN 'Death'
        ELSE AnnaResponseOriginal
    END,' ',CASE
        WHEN AnnaResponseOriginal LIKE '%neg%' THEN 'no MRD'
        WHEN AnnaResponseOriginal LIKE '%pos%' THEN 'with MRD'
        WHEN AnnaResponseOriginal LIKE '%resistant%' THEN ''
        WHEN AnnaResponseOriginal LIKE '%death%' THEN ''
        ELSE AnnaResponseOriginal
    END) = CASE
        WHEN Response IS NULL THEN 'Not Recorded'
        WHEN Response = 'No CR' 
        THEN CASE
                WHEN Status LIKE '%resist%'  THEN 'Resistant/Persistant'
                WHEN Status LIKE '%persist%' THEN 'Resistant/Persistant'
                WHEN Status LIKE '%death%'   THEN 'Death'
                ELSE Status 
            END
        ELSE Response
    END AS Agreement
    , CASE
        WHEN CASE
            WHEN AnnaResponseOriginal LIKE '%neg%' THEN 0
            WHEN AnnaResponseOriginal LIKE '%pos%' THEN 1
            ELSE -9
        END =
        CASE
            WHEN Response IS NULL THEN -9
            WHEN Status LIKE '%resist%'  THEN -9
            WHEN Status LIKE '%persist%' THEN -9
            WHEN Status LIKE '%death%'   THEN -9
            WHEN Status LIKE '%mrd%' THEN 1
            ELSE 0
        END THEN 'Yes'
        ELSE 'No'
    END AS `MRD Agrees`
    , CASE
        WHEN 
            CASE
                WHEN AnnaResponseOriginal LIKE 'CRi%' THEN 'CRi'
                WHEN AnnaResponseOriginal LIKE 'CRp%' THEN 'CRi'
                WHEN AnnaResponseOriginal LIKE 'CR %' THEN 'CR'
                WHEN AnnaResponseOriginal LIKE '%resistant%' THEN 'Resistant/Persistant'
                WHEN AnnaResponseOriginal LIKE '%death%' THEN 'Death'
                ELSE AnnaResponseOriginal
            END = CASE
                WHEN Status LIKE 'CRi%' THEN 'CRi'
                WHEN Status LIKE 'CRp%' THEN 'CRi'
                WHEN Status LIKE 'CR %' THEN 'CR'
                WHEN Status LIKE '%resistant%' THEN 'Resistant/Persistant'
                WHEN Status LIKE '%death%' THEN 'Death'
                ELSE Status
            END THEN 'Yes'
        ELSE 'No'
    END AS `CR Agrees`
    , ResponseDate AS DatabaseResponseDate
    FROM Temp.GCLAM a
    LEFT JOIN temp.temp1 b ON a.PtMRN = b.PtMRN AND timestampdiff(day,a.AnnaTreatmentStartDate,b.MedTxDate) between -7 and 7
    ORDER BY a.PtMRN, b.ArrivalDate ;
    
SELECT PtMRN
    , ArrivalDate
    , ArrivalDiagnosis
    , DiseaseCategory
    , AnnaTreatmentStartDate
    , DatabaseTreatmentStartDate
    , CAST(NULL as datetime) AS AnnaResponseDate
    , DatabaseResponseDate
    , AnnaResponseOriginal
    , DatabaseResponseOriginal
    , AnnaResponse
    , DatabaseResponse
    , CASE
        WHEN DatabaseResponseOriginal IS NULL THEN 'UNK:  Not Recorded in DB'
        WHEN `MRD Agrees` = 'No'  AND `CR Agrees` = 'No'  THEN 'No:  Both CR and MRD disagree'
        WHEN `MRD Agrees` = 'No'  AND `CR Agrees` = 'Yes' THEN 'No:  CR but not MRD'
        WHEN `MRD Agrees` = 'Yes' AND `CR Agrees` = 'No'  THEN 'No:  MRD but not CR'
        WHEN `MRD Agrees` = 'Yes' AND `CR Agrees` = 'Yes' THEN 'Yes'
        ELSE 'Unknown'
    END AS `CR/MRD Agreement?`
    , CASE
        WHEN DatabaseResponseOriginal IS NULL THEN 5
        WHEN `MRD Agrees` = 'No'  AND `CR Agrees` = 'No'  THEN 4
        WHEN `MRD Agrees` = 'No'  AND `CR Agrees` = 'Yes' THEN 2
        WHEN `MRD Agrees` = 'Yes' AND `CR Agrees` = 'No'  THEN 3
        WHEN `MRD Agrees` = 'Yes' AND `CR Agrees` = 'Yes' THEN 1
        ELSE 9
    END AS DisplayOrder    
    FROM temp.Agreement 
    ORDER BY DisplayOrder;

DROP TABLE IF EXISTS TEMP.temp1 ;
CREATE TABLE TEMP.temp1
SELECT 
    a.PtMRN
    ,  b.StatusDisease
    ,  a.ArrivalDate
    ,  a.MedicalTherapyId
    ,  a.MedTxDate
    ,  a.MedTxAgent
    ,  a.BackBoneName
    ,  CASE
            WHEN b.StatusDisease LIKE '%aml%' AND b.StatusDisease LIKE '%nd%' THEN 1
            WHEN b.StatusDisease LIKE '%aml%' AND b.StatusDisease LIKE '%rel%' THEN 3
            WHEN b.StatusDisease LIKE '%aml%' AND b.StatusDisease LIKE '%ref%' THEN 3
            ELSE 5
    END AS DisplayOrder
    ,  CASE
            WHEN b.StatusDisease LIKE '%aml%' AND b.StatusDisease LIKE '%nd%' THEN 'ND1/2'
            WHEN b.StatusDisease LIKE '%aml%' AND b.StatusDisease LIKE '%rel%' THEN 'R/R'
            WHEN b.StatusDisease LIKE '%aml%' AND b.StatusDisease LIKE '%ref%' THEN 'R/R'
            ELSE b.StatusDisease
    END AS Disease
    , CASE
            WHEN UPPER(b.Status) RLIKE 'CR.?(I|P)' AND b.Status LIKE '%mrd%' THEN 'CRi with MRD'
            WHEN UPPER(b.Status) RLIKE 'CR.?(I|P)' THEN 'CRi no MRD'
            WHEN b.Status RLIKE '%crp%' AND b.Status LIKE '%mrd%' THEN 'CRi with MRD'
            WHEN b.Status LIKE '%cr%' AND b.Status LIKE '%mrd%' THEN 'CR with MRD'
            WHEN b.Status LIKE '%cr%' THEN 'CR no MRD'
            ELSE 'No CR'
    END AS Response
    , b.Status
    , MIN(b.StatusDate) AS ResponseDate
    ,  a.NextArrivalDate
FROM
    temp.ArrivalTreatments_ a
        LEFT JOIN
    caisis.vdatasetstatus b ON a.PtMRN = b.PtMRN
WHERE
    BackBoneName LIKE '%clam%'
        AND CASE
            WHEN a.MedTxDate >= b.StatusDate THEN FALSE
            WHEN b.StatusDate IS NULL THEN FALSE
            WHEN b.StatusDate BETWEEN a.ArrivalDate AND a.NextArrivalDate THEN TRUE
            WHEN b.StatusDate BETWEEN a.ArrivalDate AND a.NextArrivalDate IS NULL THEN TRUE
            ELSE FALSE
        END
        AND NOT b.Status IN ('Alive'
         , 'Antecedent Hematologic Disorder'
         , 'Arrival Work-up'
         , 'CNS Relapse'
         , 'CYTO Relapse'
         , 'Diagnosis Date'
         , 'FISH Relapse'
         , 'FLOW Relapse'
         , 'Initial AHD Date'
         , 'Newly Diagnosed'
         , 'Non-Heme Cancer Diagnosis'
         , 'Non-Heme Cancer Diagnosis/Diagnosis Date'
         , 'Not Categorized'
         , 'PB Relapse'
         , 'Recovery of ANC 1000'
         , 'Recovery of ANC 500'
         , 'Recovery of Counts'
         , 'Recovery of Plts 100k'
         , 'Recovery of Plts 50k'
         , 'Relapse'
         , 'Unknown'        )
         AND NOT b.StatusDisease IN ('Breast Ca'
         , 'CLL/SLL'
         , 'CML'
         , 'NHL'
         , 'NHL (B Cell)'
         , 'NHL (DLBCL)'
         , 'NHL (follicular)'
         , 'Rectal/Colon CA'
         , 'AML MRD' ) 
GROUP BY a.PtMRN, a.ArrivalDate, a.MedicalTherapyId
ORDER BY a.PtMRN ;

SELECT * FROM Temp.temp1;

SELECT 
    CASE
        WHEN StatusDisease LIKE '%aml%' AND StatusDisease LIKE '%nd%' THEN 1
        WHEN StatusDisease LIKE '%aml%' AND StatusDisease LIKE '%rel%' THEN 6
        WHEN StatusDisease LIKE '%aml%' AND StatusDisease LIKE '%ref%' THEN 6
        ELSE 5
    END AS DisplayOrder
    , Disease, Response, COUNT(*) AS Result
FROM TEMP.temp1
GROUP BY Disease, Response
UNION SELECT 
    CASE
        WHEN StatusDisease LIKE '%aml%' AND StatusDisease LIKE '%nd%' THEN 2
        WHEN StatusDisease LIKE '%aml%' AND StatusDisease LIKE '%rel%' THEN 7
        WHEN StatusDisease LIKE '%aml%' AND StatusDisease LIKE '%ref%' THEN 7
        ELSE 5
    END AS DisplayOrder
    , Disease, CONCAT('% CR no MRD of ALL ',Disease," CR's")
    , CAST(SUM(CASE 
        WHEN Response = 'CR no MRD' THEN 1
        ELSE 0
    END * 100)/SUM(CASE
        WHEN Response LIKE 'CR %' THEN 1
        ELSE 0
    END) AS SIGNED) AS Result
FROM TEMP.temp1
GROUP BY Disease
UNION SELECT 
    CASE
        WHEN StatusDisease LIKE '%aml%' AND StatusDisease LIKE '%nd%' THEN 3
        WHEN StatusDisease LIKE '%aml%' AND StatusDisease LIKE '%rel%' THEN 8
        WHEN StatusDisease LIKE '%aml%' AND StatusDisease LIKE '%ref%' THEN 8
        ELSE 5
    END AS DisplayOrder
    , Disease, CONCAT('% CRi no MRD of ALL ',Disease," CRi's")
    , CAST(SUM(CASE 
        WHEN Response = 'CRi no MRD' THEN 1
        ELSE 0
    END * 100)/SUM(CASE
        WHEN Response LIKE 'CRi %' THEN 1
        ELSE 0
    END) AS SIGNED) AS Result
FROM TEMP.temp1
GROUP BY Disease
UNION SELECT 
    CASE
        WHEN StatusDisease LIKE '%aml%' AND StatusDisease LIKE '%nd%' THEN 4
        WHEN StatusDisease LIKE '%aml%' AND StatusDisease LIKE '%rel%' THEN 9                                                     
        WHEN StatusDisease LIKE '%aml%' AND StatusDisease LIKE '%ref%' THEN 9
        ELSE 6
    END AS DisplayOrder
    , CONCAT('Total ',Disease), '', COUNT(*)
FROM TEMP.temp1
GROUP BY Disease
UNION SELECT 
    0 AS DisplayOrder
    , 'GCLAM from the AML Database', '', COUNT(*)
FROM temp.temp1

ORDER BY DisplayOrder ;

        