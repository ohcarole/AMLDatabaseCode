SELECT * FROM caisis.vdatasetmedicaltherapy where medtxagent like '%CONS%';
SELECT MedTxIntent, count(*) FROM caisis.vdatasetmedicaltherapy group by 1;

-- DROP TABLE temp.BackboneMappingCodeTemp;
-- CREATE TABLE temp.BackboneMappingCodeTemp
--     SELECT a.PtMRN
--             , RTRIM(LTRIM(UPPER(a.MedTxAgent))) AS MedTxAgent
--             , MedTxDate
--             , c.TreatmentStartDate
--             , timestampdiff(day,a.MedTxDate,c.ArrivalDate) AS DaysFromDBArrival
--             , timestampdiff(day,a.MedTxDate,d.field4) AS DaysFromEliArrival
--             , c.Protocol
--             , d.field4 as EliArrivalDate
--             , d.treatment as EliTreatment
--             , b.* 
--         FROM temp.vdatasetmedicaltherapy a
--         LEFT JOIN amldatabase2.`pattreatment with prev and next arrival` c
--         ON a.PtMRN = c.UWID and timestampdiff(DAY, a.MedTxDate, c.TreatmentStartDate) BETWEEN -3 and 3
--         LEFT JOIN caisis.backbonemapping b
--         ON c.Protocol = b.OriginalProtocol 
--             OR  LEFT(MedTxAgent,IF(LOCATE('(',MedTxAgent)>0,LOCATE('(',MedTxAgent)-1,100)) = b.OriginalProtocol
--         LEFT JOIN caisis.elipatientlist d on a.PtMRN = d.UWID AND timestampdiff(day,a.MedTxDate,d.field4) BETWEEN -10 and 35
--         WHERE a.MedTxDisease LIKE '%AML%'
--         AND a.MedTxIntent = 'induction';
-- 
DROP TABLE temp.BackboneMappingCodeTemp;
CREATE TABLE temp.BackboneMappingCodeTemp
    SELECT a.PtMRN
            , RTRIM(LTRIM(UPPER(a.MedTxAgent))) AS MedTxAgent
            , MedTxDate
            , MedTxIntent
            , c.TreatmentStartDate
            , timestampdiff(day,a.MedTxDate,c.ArrivalDate) AS DaysFromDBArrival
            , c.Protocol
            , b.* 
        FROM temp.vdatasetmedicaltherapy a
        LEFT JOIN amldatabase2.`pattreatment with prev and next arrival` c
        ON a.PtMRN = c.UWID and timestampdiff(DAY, a.MedTxDate, c.TreatmentStartDate) BETWEEN -3 and 3
        LEFT JOIN caisis.backbonemapping b
        ON c.Protocol = b.OriginalProtocol 
            OR  LEFT(MedTxAgent,IF(LOCATE('(',MedTxAgent)>0,LOCATE('(',MedTxAgent)-1,100)) = b.OriginalProtocol
        WHERE a.MedTxDisease LIKE '%AML%'
        AND a.MedTxIntent LIKE '%induction%' or a.MedTxIntent LIKE '%salvage%';

SELECT * FROM temp.BackboneMappingCodeTemp ;
SELECT * FROM temp.BackboneMappingCodeTemp ;
SELECT year(medtxdate) as year, COUNT(*) FROM temp.BackboneMappingCodeTemp WHERE BackboneName IS NULL GROUP BY 1;
SELECT * FROM temp.BackboneMappingCodeTemp WHERE BackboneName IS NULL ;
SELECT * FROM temp.BackboneMappingCodeTemp WHERE BackboneName IS NULL AND YEAR(medtxdate) < 2008;
SELECT * FROM temp.BackboneMappingCodeTemp WHERE BackboneName IS NULL AND YEAR(medtxdate) = 2008;
SELECT * FROM temp.BackboneMappingCodeTemp WHERE BackboneName IS NULL AND YEAR(medtxdate) = 2009;
SELECT * FROM temp.BackboneMappingCodeTemp WHERE BackboneName IS NULL AND YEAR(medtxdate) = 2010;
SELECT * FROM temp.BackboneMappingCodeTemp WHERE BackboneName IS NULL AND YEAR(medtxdate) = 2011;
SELECT * FROM temp.BackboneMappingCodeTemp WHERE BackboneName IS NULL AND YEAR(medtxdate) = 2012;
SELECT * FROM temp.BackboneMappingCodeTemp WHERE BackboneName IS NULL AND YEAR(medtxdate) = 2013;
SELECT * FROM temp.BackboneMappingCodeTemp WHERE BackboneName IS NULL AND YEAR(medtxdate) = 2014;
SELECT * FROM temp.BackboneMappingCodeTemp WHERE BackboneName IS NULL AND YEAR(medtxdate) = 2015;
SELECT * FROM temp.BackboneMappingCodeTemp WHERE BackboneName IS NULL AND YEAR(medtxdate) = 2016;
SELECT * FROM temp.BackboneMappingCodeTemp WHERE BackboneName IS NULL AND YEAR(medtxdate) = 2017;


DROP TABLE IF EXISTS temp.UniqueMedTxAgents ;
CREATE TABLE temp.UniqueMedTxAgents 
    SELECT 
        LEFT(MedTxAgent,IF(LOCATE('(',MedTxAgent)>0,LOCATE('(',MedTxAgent)-1,100)) AS MedTxAgent
        , COUNT(*) MedTxAgentUses 
    FROM temp.BackboneMappingCodeTemp 
    GROUP BY 1; 

DROP TABLE IF EXISTS temp.BackboneMappingCodes;
CREATE TABLE temp.BackboneMappingCodes
SELECT DISTINCT
    b.MedTxAgentUses,
    LEFT(a.MedTxAgent,IF(LOCATE('(',a.MedTxAgent)>0,LOCATE('(',a.MedTxAgent)-1,100)) AS MedTxAgent,
    Protocol,
    OriginalProtocol,
    BackboneType,
    BackboneName,
    Anthracyclin,
    AnthracyclinDose,
    BackboneAddOn,
    Intensity
FROM
    temp.BackboneMappingCodeTemp a
    LEFT JOIN temp.UniqueMedTxAgents b
        ON LEFT(a.MedTxAgent,IF(LOCATE('(',a.MedTxAgent)>0,LOCATE('(',a.MedTxAgent)-1,100)) = b.MedTxAgent;


SELECT * FROM temp.BackboneMappingCodes;
    
DROP TABLE IF EXISTS temp.BackBoneMappingFailures;
CREATE TABLE temp.BackBoneMappingFailures
    SELECT  MedTxAgentUses
            , SPACE(100) AS T_BackBone
            , SPACE(100) AS T_Anthracyclin
            , SPACE(100) AS T_AnthracyclinDose
            , SPACE(100) AS T_AddOn
            , MedTxAgent
            , Protocol
            , OriginalProtocol
            , BackboneType
            , BackboneName
            , Anthracyclin
            , AnthracyclinDose
            , BackboneAddOn
            , Intensity
    FROM temp.BackboneMappingCodes a 
    WHERE OriginalProtocol IS NULL 
    GROUP BY MedTxAgent,     
        Protocol,
        OriginalProtocol,
        BackboneType,
        BackboneName,
        Anthracyclin,
        AnthracyclinDose,
        BackboneAddOn,
        Intensity;

UPDATE temp.BackBoneMappingFailures
    SET T_BackBone = ''
        , T_AddOn = ''
        , T_Anthracyclin = ''
        , T_AnthracyclinDose = '';

SELECT * FROM temp.backbonemappingfailures;


-- GCLAM FIXES
UPDATE temp.BackBoneMappingFailures 
    SET T_BackBone = 'G-CLAM'
        , T_Anthracyclin = ',mitoxantrone'
    WHERE OriginalProtocol IS NULL AND T_BackBone = ''
        AND MedTxAgent IN (
        'S3-GCLAM'
        , 'G CLAM'
        );

UPDATE temp.BackBoneMappingFailures 
    SET T_BackBone = 'G-CLAM mini'
        , T_Anthracyclin = ',mitoxantrone'
    WHERE OriginalProtocol IS NULL AND T_BackBone = ''
        AND MedTxAgent IN ('MINI G-CLAM');

UPDATE temp.BackBoneMappingFailures 
    SET T_BackBone = 'G-CLAM'
        , T_Anthracyclin = ',mitoxantrone'
        , T_AddOn = ',gemtuzumab ozogamicin'
    WHERE OriginalProtocol IS NULL AND T_BackBone = ''
        AND MedTxAgent IN ('G-CLAM+GO');

-- ARA-C
UPDATE temp.BackBoneMappingFailures 
SET T_BackBone = 'ARA-C'
    , T_Anthracyclin = CASE
            WHEN MedTxAgent IN ('MITOXANTRONE AND CYTARABINE',
                'S1-MITOX.+CYTARABINE ') THEN ',mitoxantrone'
            ELSE ''
    END
WHERE  T_BackBone = '' AND MedTxAgent IN ('CYTARABINE', 'CYTARABINE LIPOSOMAL'
            'MITOXANTRONE AND CYTARABINE',
            'S1-MITOX.+CYTARABINE');

-- ABT
UPDATE temp.BackBoneMappingFailures
    SET T_BackBone = 'ABT-199|ABT199'
        , T_AddOn = ',azacitidine'
    WHERE T_BackBone = '' AND MedTxAgent = 'ABT + AZACITIDINE';


-- 7+3
UPDATE temp.BackBoneMappingFailures
    SET     T_BackBone = '7+3'
            , T_Anthracyclin = CASE 
                WHEN MedTxAgent     RLIKE 'IDA' THEN ',idarubicin'
                WHEN MedTxAgent     RLIKE 'DAU' THEN ',daunorubicin'
                ELSE ''
            END 
            , T_AnthracyclinDose = CASE 
                WHEN MedTxAgent RLIKE '45' AND MedTxAgent RLIKE '(IDA|DAU)' THEN 45
                WHEN MedTxAgent RLIKE '60' AND MedTxAgent RLIKE '(IDA|DAU)' THEN 60
                WHEN MedTxAgent RLIKE '90' AND MedTxAgent RLIKE '(IDA|DAU)' THEN 90
                ELSE ''
            END 
            , T_AddOn = CASE 
                WHEN MedTxAgent RLIKE 'DECI'    THEN ',decitabine'
                WHEN MedTxAgent RLIKE 'DACO'    THEN ',decitabine'
                WHEN MedTxAgent RLIKE 'DECAGEN' THEN ',decitabine'
                WHEN MedTxAgent RLIKE '5?.?AZA' THEN ',azacitidine'
                ELSE ''
            END     
    WHERE T_BackBone = ''
    AND (MedTxAgent RLIKE '7[ ]*[+][ ]*3'
    OR   MedTxAgent RLIKE '3[ ]*[+][ ]*7') ;
       
-- 7+3 agents spelled out
UPDATE temp.BackBoneMappingFailures
    SET     T_BackBone = '7+3'
            , T_Anthracyclin = ',daunorubicin'
            , T_AnthracyclinDose = CASE 
                WHEN MedTxAgent RLIKE '45' THEN 45
                WHEN MedTxAgent RLIKE '60' THEN 60
                WHEN MedTxAgent RLIKE '90' THEN 90
                ELSE ''
            END 
            , T_AddOn = CASE 
                WHEN MedTxAgent RLIKE 'DECI'    THEN ',decitabine'
                WHEN MedTxAgent RLIKE 'DACO'    THEN ',decitabine'
                WHEN MedTxAgent RLIKE '5?.?AZA' THEN ',azacitidine'
                ELSE ''
            END     
    WHERE T_BackBone = ''
    AND MedTxAgent IN ('ARA-C + DAUNORUBICIN'
        , 'DAUNORUBICIN AND CYTARABINE'
        , 'ARA-C + DAUNO'
        , 'HIDAC + DAUNO'
        , 'HIDAC + GEMTUZUMAB OZOGAMICIN'
        , 'HIDAC AND DAUNORUBICIN') ;

-- IA (Consider for 7+3)
UPDATE temp.BackBoneMappingFailures
    SET     T_BackBone = 'IA'
            , T_Anthracyclin = ',idarubicin'
            , T_AnthracyclinDose = CASE 
                WHEN MedTxAgent RLIKE '45' THEN 45
                WHEN MedTxAgent RLIKE '60' THEN 60
                WHEN MedTxAgent RLIKE '90' THEN 90
                ELSE ''
            END 
            , T_AddOn = CASE 
                WHEN MedTxAgent RLIKE 'DECI'    THEN ',decitabine'
                WHEN MedTxAgent RLIKE 'DACO'    THEN ',decitabine'
                WHEN MedTxAgent RLIKE '5?.?AZA' THEN ',azacitidine'
                ELSE ''
            END     
    WHERE T_BackBone = ''
    AND MedTxAgent IN ('IDARUBICIN 12MG/M + CYTARABINE'
        , 'IDA+ARA-C'
        , 'CYTARABINE AND IDARUBICINE'
        , 'CYTARABINE AND IDARUBICIN '
        , 'IDARUBICINE AND ARA-C'
        , 'ARA-C+IDARUBICIN'
        , 'IDARUBICIN AND ARA-C FOR MDS');

-- IAP
UPDATE temp.BackBoneMappingFailures
    SET   T_BackBone = 'IAP'
        , T_AddOn = ',gemtuzumab ozogamicin'
    WHERE T_BackBone = ''
    AND MedTxAgent = 'IAP +GO';

-- VORINO +GO
UPDATE temp.BackBoneMappingFailures
    SET T_BackBone = 'VORINO+GO'
    WHERE T_BackBone = '' AND MedTxAgent = 'VORIN + GO' ;

-- CLOFARABINE+HiDAC
UPDATE temp.BackBoneMappingFailures
    SET T_BackBone = 'CLOFARABINE+HiDAC'
    WHERE T_BackBone = '' AND (MedTxAgent RLIKE 'CLO.*H.?DAC' 
        OR MedTxAgent = 'CLOFARABINE, HIGH-DOSE ARAC AND G-CSF ');

-- CLOFARABINE+LDAC
UPDATE temp.BackBoneMappingFailures
    SET T_BackBone = 'CLOFARABINE+LDAC'
    WHERE T_BackBone = '' 
        AND (MedTxAgent RLIKE 'CLO.*LDAC'
            OR MedTxAgent = 'LOW-DOSE CLOFARABINE AND ARA-C');

-- CLOFARABINE+ARA-C (Dose Undetermined)
UPDATE temp.BackBoneMappingFailures
    SET T_BackBone = 'CLOFARABINE+ARA-C'
        , T_AddOn = CASE 
            WHEN MedTxAgent = 'RADIATION  FOR EXTRAMED. AML&ALSO CLOF.+ARAC' THEN ',XRT'
            WHEN MedTxAgent = 'CLOF.+CYTARABINE+VELCADE'                     THEN ',velcade'
            WHEN MedTxAgent = 'CLOFARABINE, HIGH-DOSE ARAC AND G-CSF '       THEN ',G-CSF'
            WHEN MedTxAgent = 'IDA+CYTARABINE+CLOFARB.'                      THEN ',idarubicin'
            WHEN MedTxAgent = 'ORAL SORAFENIB, CLOFARABINE AND CYTARABINE'   THEN ',sorafenib'
            ELSE ''
        END   
        WHERE T_BackBone = '' AND MedTxAgent IN ('ARA-C/CLOFARABINE',
            'CLOF. + ARA-C',
            'CLOF.+ARA-C ',
            'CLOFARABINE + CYTARABINE',
            'CLOFARABINE AND ARA-C OVER 6 DAYS',
            'CLOFARABINE PLUS ARA-C',
            'CLOFARABINE+ ARA-C',
            'S1-CLOFARABINE/ARA-C ',

            'RADIATION  FOR EXTRAMED. AML&ALSO CLOF.+ARAC',
            'CLOF.+CYTARABINE+VELCADE',
            'CLOFARABINE, HIGH-DOSE ARAC AND G-CSF ',
            'IDA+CYTARABINE+CLOFARB.',
            'ORAL SORAFENIB, CLOFARABINE AND CYTARABINE');
            
-- CLOFARABINE
UPDATE temp.BackBoneMappingFailures
    SET T_BackBone = 'CLOFARABINE'
        , T_AddOn = CASE 
            WHEN MedTxAgent = 'CLOFARABINE, DONOR LYMPHOCYTES' THEN ',DLI'
            ELSE ''
        END
        WHERE T_BackBone = '' AND MedTxAgent IN ('CLOFARABINE  FOR CYTO REF',
            'CLOFARABINE 40MG/SQ M WITHOUT G-CSF PRIMING',
            'CLOFARABINE ONLY',
            'CLOFARABINE, DONOR LYMPHOCYTES');

SELECT * FROM temp.BackBoneMappingFailures
        WHERE MedTxAgent IN ('CLOFARABINE  FOR CYTO REF',
            'CLOFARABINE 40MG/SQ M WITHOUT G-CSF PRIMING',
            'CLOFARABINE ONLY',
            'CLOFARABINE, DONOR LYMPHOCYTES');

-- HiDAC
UPDATE temp.BackBoneMappingFailures
    SET T_BackBone = 'HiDAC'
    WHERE T_BackBone = '' 
        AND MedTxAgent IN ('HDAC'
        , 'HIDAC -1 DOSE ONLY'
        , 'HIDAC-CONSOLIDATION'
        , 'S1- HIDAC'
        , 'CONSOLIDATION - HIGH-DOSE ARA-C'
        , 'HIGH-DOSE ARA-C');


-- DECITABINE
UPDATE temp.BackBoneMappingFailures
    SET     T_BackBone = 'DECITABINE'
            , T_AddOn = CASE 
                WHEN MedTxAgent = 'LENALIDOMIDE + DECITABIN'    THEN ',lenalidomide'
                WHEN MedTxAgent = 'RADIATION ANDDECITABINE'     THEN ',XRT'
                WHEN MedTxAgent = 'DECITABINE+HU'               THEN ',hydroxyurea'
                WHEN MedTxAgent = 'HU +DACOGEN'                 THEN ',hydroxyurea'
                WHEN MedTxAgent = 'DECITABINE+SORAFENIB'        THEN ',sorafenib'
                WHEN MedTxAgent = 'DECITABIN +VALPROIC ACID'    THEN ',valproic acid'
                WHEN MedTxAgent = 'DECITABINE +VALPROIC ACID'   THEN ',valproic acid'
                WHEN MedTxAgent = 'DECITABINE + GEMTUZUMAB'     THEN ',gemtuzumab ozogamicin'
                WHEN MedTxAgent = 'DECITABINE+DLI '             THEN ',DLI'
                ELSE ''
            END     
    WHERE T_BackBone = ''
    AND MedTxAgent IN ('DECITABIN +VALPROIC ACID',
        'DECITABINE + GEMTUZUMAB',
        'DECITABINE +VALPROIC ACID',
        'DECITABINE 15 MG/M2',
        'DECITABINE 20 MG/M2/DAY IV FOR 5 DAYS',
        'DECITABINE 20MG/M',
        'DECITABINE+DLI',
        'DECITABINE+HU',
        'HU +DACOGEN',
        'DECITABINE+SORAFENIB',
        'DECITABINE-20 MG/M2 INITIATED',
        'DECITABINE-20MG/M^2 FOR FIVE DAYS',
        'LENALIDOMIDE + DECITABIN',
        'RADIATION ANDDECITABINE',
        'DACOGEN X 5 CYCLES-CONSOLIDATION');

-- AZA
UPDATE temp.BackBoneMappingFailures
    SET     T_BackBone = 'AZA'
            , T_AddOn = CASE 
                WHEN MedTxAgent IN ('VIDAZA + SORAFENIB',
                    'AZA, SORAFINIB'
                    ) THEN ',sorafenib'
                WHEN MedTxAgent IN (
                    'AZA + GO',
                    'VIDAZA+GO',
                    'VIDAZA +GO',
                    'VIDAZA +MYLOTARG',
                    'AZA+GENTUZUMAB',
                    'AZACITIDINE + GEMTUZUMAB'
                    ) THEN ',gemtuzumab ozogamicin'
                WHEN MedTxAgent = 'DLI & VIDAZA'               THEN ',DLI'
                WHEN MedTxAgent = 'TIPIFARNIB + 5-AZA'         THEN ',tipifarnib'
                WHEN MedTxAgent = '5-AZA+ DASATINIB'           THEN ',dasatinib'
                WHEN MedTxAgent = 'AZACITIDINE + ATEZOLIZUMAB'  THEN ',atezolizumab'
                WHEN MedTxAgent = 'VIDAZA, VORINOSTAT 200MG'    THEN ',vorinostat'
                WHEN MedTxAgent = 'VIDAZA+RADIATION'            THEN ',XRT'
                ELSE ''
            END     
    WHERE T_BackBone = ''
    AND MedTxAgent IN ('DLI & VIDAZA',
        'TIPIFARNIB + 5-AZA',
        'VIDAZA + SORAFENIB',
        'AZA, SORAFINIB',
        'AZA + GO',
        'VIDAZA+GO',
        'VIDAZA +GO',
        'VIDAZA +MYLOTARG',
        'AZA+GENTUZUMAB',
        'AZACITIDINE + GEMTUZUMAB',
        '5-AZA+ DASATINIB ',
        'AZACITIDINE + ATEZOLIZUMAB',
        'VIDAZA+RADIATION',
        'VIDAZA, VORINOSTAT 200MG'
        '5-AZA',
        'AZA 1WK ON & 3 WK OFF',
        'AZA FOR AML',
        'AZA X 10',
        '5-AZA',
        '5-AZACYTIDINE',
        'VIDAZA 75 MG/M',
        'AZACITIDINE FOR 3 WEEKS PRIOR TO SCCA',
        '5-AZACYTIDINE',
        'ATRA +  PIOGLITAZONE + AZACYTIDINE',
        'AZAACITIDINE',
        'AZACYTADINE',
        'AZACYTIDINE',
        'S2-AZACITIDINE');

-- FLAG-IDA
UPDATE temp.BackBoneMappingFailures
    SET     T_BackBone = 'FLAG-IDA'
            , T_AddOn = CASE 
                WHEN MedTxAgent IN ('VIDAZA + SORAFENIB',
                    'AZA, SORAFINIB'
                    ) THEN ',sorafenib'
                WHEN MedTxAgent IN ('FLAG,IDARUBICIN+GO',
                    'FLAG +IDA+GO'
                    ) THEN ',gemtuzumab ozogamicin'
                ELSE ''
            END     
    WHERE T_BackBone = ''
    AND MedTxAgent IN ('FLAG IDARUBCIN',
        'FLAG/IDA',
        'S1-FLAG-IDA',
        'FLAG,IDARUBICIN+GO',
        'FLAG +IDA+GO');

-- FLAG
UPDATE temp.BackBoneMappingFailures
    SET     T_BackBone = 'FLAG'
            , T_AddOn = CASE 
                WHEN MedTxAgent IN ('MYLOTARG + FLAG') THEN ',gemtuzumab ozogamicin'
                ELSE 'variety'
            END     
    WHERE T_BackBone = '' AND MedTxAgent IN ('FLAG  & HIDAC &TBI &IT MTX',
        'FLAG + IT-MTX,ARA-C & HYDROCORTISONE',
        'FLAG-AMSA',
        'MYLOTARG + FLAG');
              
-- HYDROXYUREA
UPDATE temp.BackBoneMappingFailures
    SET     T_BackBone = 'HYDROXYUREA'
    WHERE T_BackBone = ''
    AND MedTxAgent IN ('HU','HYDREA','HYDROXYUREA');

-- G-CLAC
UPDATE temp.BackBoneMappingFailures
    SET     T_BackBone = 'G-CLAC'
    WHERE T_BackBone = ''
    AND MedTxAgent = '6562';


-- LENALIDOMIDE
UPDATE temp.BackBoneMappingFailures
    SET     T_BackBone = 'LENALIDOMIDE'
    WHERE T_BackBone = ''
    AND MedTxAgent = 'LENALID';


-- METHOTREXATE
UPDATE temp.BackBoneMappingFailures
    SET     T_BackBone = 'IT'
    WHERE T_BackBone = ''
    AND MedTxAgent IN ('METHOTREXATE','IT METHOTREXATE');

-- MITOXANTRONE + ETOPOSIDE (a.k.a. ETOPOSIDE) 
UPDATE temp.BackBoneMappingFailures
    SET     T_BackBone = 'MITOXANTRONE + ETOPOSIDE'
    WHERE T_BackBone = ''
    AND MedTxAgent IN ('ETOPOSIDE + MITOXANTRONE',
        'MITOX. + ETOPOSIDE',
        'MITOX.+VP16 ',
        'MITOXANT.+ ETOPOSIDE',
        'MITOXANTRONE + ETOPOSIDE',
        'MITOXANTRONE +VP16 ',
        'MITOXANTRONE AND VP-16',
        'MITOXANTRONE+ETOPOSIDE',
        'MITOXANTRONE+ETOPOSODE',
        'MITOXANTRONE+VP-16',
        'MITOXANTRONE+VP16');

-- ARA-C (with MITOXANTRONE)
UPDATE temp.BackBoneMappingFailures
    SET     T_BackBone = 'MITOXANTRONE + ETOPOSIDE'
            , T_AddOn = CASE 
                WHEN MedTxAgent IN ('MITOX. +ETOPOSIDE +ARA-C',
                    'MITOXANTRONE + ETOPOSIDE & LDAC',
                    'MITOXANTRONE, ETOPOSIDE, AND ARA-C',
                    'MITOXANTRONE, ETOPOSIDE, CYTARABINE') 
                    THEN ',cytarabine'
                WHEN MedTxAgent IN ('MITOX. +ARA-C+GO') THEN ',gemtuzumab ozogamicin'
                ELSE ''
            END     
WHERE T_BackBone = '' AND MedTxAgent IN ('HIDA+MITO.',
        'HIDAC+MITOXANTRONE',
        'MITOXANTRONE AND ETOPOSIDE',
        'MITOX.+ARAC',
        'MITOX. +ARA-C+GO',
        'MITOX. +ETOPOSIDE +ARA-C',
        'MITOXANTRONE + ETOPOSIDE & LDAC',
        'MITOXANTRONE, ETOPOSIDE, AND ARA-C',
        'MITOXANTRONE, ETOPOSIDE, CYTARABINE');

-- FLX
UPDATE temp.BackBoneMappingFailures
    SET     T_BackBone = 'FLX925'
    WHERE T_BackBone = '' AND MedTxAgent = 'FLX925';


-- PAD
UPDATE temp.BackBoneMappingFailures
    SET     T_BackBone = 'PICKaDRUG'
    WHERE T_BackBone = '' 
    AND (MedTxAgent LIKE 'PAD%' OR MedTxAgent IN ('PICK-A-DRUG'));

-- CLADRABINE
UPDATE temp.BackBoneMappingFailures
    SET     T_BackBone = 'CLADRABINE'
    WHERE T_BackBone = '' AND MedTxAgent IN ('CLADRABINE','CLADRIBINE');

-- BEND-IDA
UPDATE temp.BackBoneMappingFailures
    SET   T_BackBone = 'BEND-IDA'
    WHERE T_BackBone = '' AND MedTxAgent IN ('BENDAMUSTINE AND IDARUBICIN');

-- Consult only, no treatment
UPDATE temp.BackBoneMappingFailures
    SET   T_BackBone = 'Consult only, no treatment'
    WHERE T_BackBone = '' AND MedTxAgent IN ('NON-TREATEMENT','NON-TREATMENT');

-- FLU
UPDATE temp.BackBoneMappingFailures
    SET   T_BackBone = 'DLI'
    WHERE T_BackBone = '' AND MedTxAgent = 'DLI';


-- DLI
UPDATE temp.BackBoneMappingFailures
    SET   T_BackBone = 'DLI'
    WHERE T_BackBone = '' AND MedTxAgent = 'DLI';


SELECT * FROM temp.BackBoneMappingFailures ;
SELECT * FROM temp.BackBoneMappingFailures WHERE T_Backbone = '';
SELECT CONCAT("'",MedTxAgent,"',") FROM temp.BackBoneMappingFailures WHERE T_Backbone = '' GROUP BY 1;

CREATE TABLE temp.ProtocolsNowMapped
    SELECT 
        b.PtMRN,
        T_BackBone,
        T_Anthracyclin,
        T_AnthracyclinDose,
        T_AddOn,
        b.MedTxAgent,
        b.MedTxDate,
        b.TreatmentStartDate,
        b.DaysFromDBArrival,
        b.Protocol,
        b.OriginalProtocol,
        b.BackboneType,
        b.BackboneName,
        b.Anthracyclin,
        b.AnthracyclinDose,
        b.BackboneAddOn,
        b.Intensity
    FROM temp.BackBoneMappingFailures a
        LEFT JOIN temp.backbonemappingcodetemp b ON LTRIM(RTRIM(UPPER(a.MedTxAgent))) = LTRIM(RTRIM(UPPER(b.MedTxAgent)))
    WHERE b.PtMRN IS NOT NULL
    GROUP BY b.PtMRN , b.MedTxDate ;

SELECT * FROM temp.ProtocolsNowMapped WHERE YEAR(MedTxDate) > 2008 AND T_BackBone = '';


SELECT DISTINCT UWId
    , `Last Name`
    , Field4 as ArrivalDate
    , `Treated With <5% blasts`
    , Age
    , ECOG
    , Field8
    , Treatment
    , `Other Notes from Dr Estey`
FROM caisis.elipatientlist a 
LEFT JOIN (
    SELECT b.PtMRN 
        FROM temp.BackBoneMappingFailures a
        LEFT JOIN temp.backbonemappingcodetemp b
        on a.MedTxAgent = ltrim(rtrim(upper(b.MedTxAgent)))
        WHERE b.PtMRN IS NOT NULL
        GROUP BY b.PtMRN, b.MedTxDate ) b ON a.UWID = b.PtMRN
    WHERE a.UWID IS NOT NULL AND a.UWID <> ''
    ORDER BY UWID, ArrivalDate;

/*-----------------------------------------------------------------------------------------------------------/*

UPDATE temp.BackBoneMappingFailures
    SET T_BackBone = 'MITOXANTRONE + ETOPOSIDE'
    WHERE OriginalProtocol IS NULL  AND T_BackBone = ''
    AND MedTxAgent RLIKE 'MITO.*VP' ;

# ARA-C	,mitoxantrone		,mitoxantrone
UPDATE temp.BackBoneMappingFailures
    SET T_BackBone = 'ARA-C'
        , T_Anthracyclin = ',mitoxantrone'
    WHERE OriginalProtocol IS NULL  AND T_BackBone = ''
    AND MedTxAgent RLIKE 'ARA.*MITO' ;

UPDATE temp.BackBoneMappingFailures
    SET T_BackBone = 'ABT-199|ABT199'
        , T_AddOn = ',azacitadine'
    WHERE OriginalProtocol IS NULL  AND T_BackBone = ''
    AND MedTxAgent RLIKE 'ABT'
    AND MedTxAgent RLIKE 'AZA';

UPDATE temp.BackBoneMappingFailures
    SET T_BackBone = 'CLAG'
    WHERE OriginalProtocol IS NULL  AND T_BackBone = ''
    AND MedTxAgent RLIKE 'CLAG';
    
UPDATE temp.BackBoneMappingFailures 
SET 
    T_BackBone = 'AZA+GO+VORINOSTAT'
    WHERE OriginalProtocol IS NULL  AND T_BackBone = ''
        AND (MedTxAgent RLIKE '5?.?AZA'
        AND  MedTxAgent RLIKE '(GO|TARG|GEMTU)'
        AND  MedTxAgent RLIKE 'VORINO');

UPDATE temp.BackBoneMappingFailures 
SET 
    T_BackBone = 'AZA',
    T_AddOn = ',gemtuzumab ozogamicin'
    WHERE OriginalProtocol IS NULL  AND T_BackBone = ''
        AND (MedTxAgent RLIKE '5?.?AZA'
        AND  MedTxAgent RLIKE '(GO|TARG|GEMTU)');

UPDATE temp.BackBoneMappingFailures 
SET 
    T_BackBone = 'AZA',
    T_AddOn = ',vorinostat'
    WHERE OriginalProtocol IS NULL  AND T_BackBone = ''
        AND (MedTxAgent RLIKE '5?.?AZA'
        AND MedTxAgent RLIKE 'VORINO');

UPDATE temp.BackBoneMappingFailures 
SET T_BackBone = 'AZA'
    WHERE OriginalProtocol IS NULL  AND T_BackBone = ''
        AND MedTxAgent RLIKE '5?.?AZA';

UPDATE temp.BackBoneMappingFailures 
    SET   T_BackBone = 'HU'
    WHERE OriginalProtocol IS NULL AND T_BackBone = ''
        AND MedTxAgent RLIKE 'HYDROX';

UPDATE temp.BackBoneMappingFailures 
    SET T_BackBone = 'AZA'
    WHERE OriginalProtocol IS NULL  AND T_BackBone = ''
        AND MedTxAgent RLIKE '5?.?AZA';

UPDATE temp.BackBoneMappingFailures
    SET T_BackBone = 'HiDAC'
    WHERE OriginalProtocol IS NULL  AND T_BackBone = ''
    AND MedTxAgent RLIKE 'HI?DAC';
    
UPDATE temp.BackBoneMappingFailures 
    SET T_BackBone = 'MDX-1338'
    WHERE OriginalProtocol IS NULL  AND T_BackBone = ''
        AND MedTxAgent RLIKE '9036';

UPDATE temp.BackBoneMappingFailures
    SET T_BackBone = 'HCT'
    WHERE OriginalProtocol IS NULL  AND T_BackBone = ''
    AND (MedTxAgent RLIKE 'Y.*DOTA'
    OR   MedTxAgent RLIKE 'BU.?CY') ;

UPDATE temp.backbonemappingfailures
SET T_Backbone = 'DECITABINE'
WHERE OriginalProtocol IS NULL  AND T_BackBone = ''
    AND MedTxAgent IN ( 
        'DACOGEN X 5 CYCLES-CONSOLIDATION'
        , 'DECITABINE 20 MG/M2/DAY IV FOR 5 DAYS'
        , 'DECITABIN (AT UCLA)'
        , 'HU +DACOGEN(ON 9/1/2007) GOT 12 CYCLES OF DECIT.'
        , 'DECITABINE (FOR AML ND2)'
        , 'DECITABINE 20MG/M'
        , 'DECITABINE (INDUCTION FOR RAEB-2)'
        , 'DECITABIN +VALPROIC ACID(X10)'
        , 'DECITABIN(1 CYCLE)'
        , 'DECITABINE-20MG/M^2 FOR FIVE DAYS'
        , 'DECITABINE 20MG/M(2) (X7)'
        , 'DECITABINE 15 MG/M2 (9DOSES)'
        , 'DECITABINE+HU'
        , 'DECITABINE(1 CYCLE)'
        , 'DECITABINE-20 MG/M2 INITIATED'
        , 'DECITABINE (FOR AML ND1)'
    );


UPDATE temp.backbonemappingfailures
SET T_Backbone = 'MITOXANTRONE'
WHERE OriginalProtocol IS NULL  AND T_BackBone = ''
    AND MedTxAgent IN ('MITOXANTRONE');


UPDATE temp.backbonemappingfailures
SET T_Backbone = 'MITOXANTRONE + ETOPOSIDE '
WHERE OriginalProtocol IS NULL  AND T_BackBone = ''
    AND MedTxAgent IN (
    'MITOXANT.+ ETOPOSIDE'
    , 'MITOX. + ETOPOSIDE '
    , 'MITOXANTRONE+ETOPOSIDE'
    , 'MITOXANTRONE AND ETOPOSIDE'
    , 'MITOXANTRONE AND ETOPOSIDE (5 + 2).'
    , 'MITOXANTRONE+ETOPOSODE'
    , 'MITOXANTRONE+ETOPOSIDE'
    , 'ETOPOSIDE + MITOXANTRONE'
    , 'MITOXANTRONE + ETOPOSIDE');

    , 'CYTARABINE+ETOPOSIDE+MTX'
    , 'MITOXANTRONE + ETOPOSIDE & LDAC'
    , 'PETI 10-30 (PLERIXIFOR,CYTAR./ETOP.)'
    , 'MEC (MITOX.+ETOPOS.+ARA-C)'
    , 'MITOX. +ETOPOSIDE +ARA-C'
    , 'ETOPOSIDE +CYCLOPHOSPHAMIDE'
    , 'ADE (ADRIAMYCIN, CYTARABINE, ETOPOSIDE) X1,'
    , 'MITOXANTRONE, ETOPOSIDE, AND ARA-C'
    , 'ETOPOSIDE+CYCLOPHOSPHAMIDE'
    , 'TOPOTECAN+CYTARABINE+ETOPOSIDE '
    , 'MITOXANTRONE, ETOPOSIDE, CYTARABINE'
    , 'S2-CEC (CLOF.+ETOPOSIDE+CYCLOPHOSPH.)'
    , 'ARA-C + ETOPOSIDE  S1(FOR AML REF)'
    , 'ETOPOSIDE/CYCLOPHOSPHAMIDE' );



SELECT DISTINCT * FROM temp.BackBoneMappingFailures ;
SELECT DISTINCT * FROM temp.BackBoneMappingFailures WHERE T_BackBone <> '';
SELECT DISTINCT * FROM temp.BackBoneMappingFailures WHERE T_BackBone = '' AND BackboneName IS NULL;
SELECT MedTxAgent FROM temp.BackBoneMappingFailures WHERE T_BackBone = '' AND BackboneName IS NULL ;



