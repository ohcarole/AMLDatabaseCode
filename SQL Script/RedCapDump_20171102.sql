DROP TABLE IF EXISTS temp.df_aml;
CREATE TABLE temp.df_aml
    SELECT a.PtMRN AS df_mrn    # Medical Record Number
        , 'disease_features' AS redcap_repeat_instrument
        , -9999 AS redcap_repeat_instance
        , CASE 
            WHEN b.StatusDisease LIKE '%nd%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN b.statusdate # 'Diagnosis' really new diagnosis arrival date
            WHEN b.StatusDisease LIKE '%nd%' AND b.Status LIKE '%newly%' AND b.Status LIKE '%diagnosed%' THEN b.statusdate # 'Diagnosis' really new diagnosis arrival date
            WHEN b.StatusDisease LIKE '%rel%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN b.statusdate # 'Diagnosis' really relapsed arrival date
            WHEN b.StatusDisease LIKE '%ref%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN b.statusdate# 'Diagnosis' really refractory arrival date
            ELSE NULL
        END AS df_date          # Date of Disease Assessment
        , CASE 
            WHEN b.StatusDisease LIKE '%nd%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN 1 # 'Diagnosis' really new diagnosis arrival date
            WHEN b.StatusDisease LIKE '%nd%' AND b.Status LIKE '%newly%' AND b.Status LIKE '%diagnosed%' THEN 1 # 'Diagnosis' really new diagnosis arrival date
            WHEN b.StatusDisease LIKE '%rel%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN 3 # 'Diagnosis' really relapsed arrival date
            WHEN b.StatusDisease LIKE '%ref%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN 5 # 'Diagnosis' really refractory arrival date
            ELSE NULL
        END AS df_phase         # Disease Phase
        , 100 AS df_dx          # Diagnosis
        , '' AS df_dx_ot        # Other diagnosis, please specify
        , CASE 
            WHEN b.StatusDisease LIKE '%apl%' THEN 105
            WHEN c.SecondaryType LIKE '%de novo aml%' 
                AND NOT (c.SecondaryType LIKE '%AHD (type%'
                OR       c.SecondaryType LIKE '%Chemotherapy History%'
                OR       c.SecondaryType LIKE '%Secondary AML%' )
                THEN 101 # De Novo AML
            WHEN b.StatusDisease LIKE '%nd1%' AND c.SecondaryType NOT LIKE '%hx of ahd/chemo not recorded%' AND c.SecondaryType <> ''  THEN 198
            WHEN b.StatusDisease LIKE '%nd1%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN 101
            WHEN b.StatusDisease LIKE '%nd1%' AND b.Status LIKE '%newly%' AND b.Status LIKE '%diagnosed%' THEN 101
            WHEN b.StatusDisease LIKE '%nd1%' AND b.Status LIKE '%newly%' AND b.Status LIKE '%diagnosed%' THEN 101
            WHEN b.StatusDisease LIKE '%nd2%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN 102
            WHEN b.StatusDisease LIKE '%nd2%' AND b.Status LIKE '%newly%' AND b.Status LIKE '%diagnosed%' THEN 102
            WHEN b.StatusDisease LIKE '%nd2%' AND b.Status LIKE '%newly%' AND b.Status LIKE '%diagnosed%' THEN 102
            WHEN c.SecondaryType LIKE '%hx of ahd/chemo not recorded%' THEN 198
            WHEN c.SecondaryType <> '' THEN 102
            WHEN b.StatusDisease LIKE '%rel%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN -9 
            WHEN b.StatusDisease LIKE '%ref%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN -9
            ELSE NULL
        END AS df_dx_aml1       # Initial AML Subtype
        , CASE 
            
            WHEN (b.StatusDisease LIKE '%nd1%' or c.SecondaryType LIKE '%De Novo AML%' )
                AND NOT (c.SecondaryType LIKE '%AHD (type%'
                OR       c.SecondaryType LIKE '%Chemotherapy History%'
                OR       c.SecondaryType LIKE '%Secondary AML%' )
                THEN NULL # De Novo AML
            
            WHEN b.StatusDisease LIKE '%apl%' 
                THEN NULL # APL
            
            WHEN (b.StatusDisease LIKE '%nd1%' or c.SecondaryType LIKE '%De Novo AML%' )
                AND (c.SecondaryType LIKE '%AHD (type%'
                OR   c.SecondaryType LIKE '%Chemotherapy History%'
                OR   c.SecondaryType LIKE '%Secondary AML%'
                ) THEN 998 # Inconsistent    
            
            WHEN b.StatusDisease NOT RLIKE '(ND|REL|REF)+' THEN 998 # Missing type of arrival
            
            WHEN b.StatusDisease NOT LIKE '%nd1%' 
                AND  c.SecondaryType NOT LIKE '%Chemotherapy History%'
                AND (c.SecondaryType LIKE '%AHD (type MDS%'
                OR   c.SecondaryType LIKE '%AHD (type RC%'
                OR   c.SecondaryType LIKE '%AHD (type RA%') # ALL types of MDS
                THEN 105 # MDS (Documented prior MDS diagnosis)
            
            WHEN b.StatusDisease NOT LIKE '%nd1%' 
                AND  c.SecondaryType LIKE '%Chemotherapy History%'
                AND NOT (c.SecondaryType LIKE '%AHD (type MDS%'
                OR       c.SecondaryType LIKE '%AHD (type RC%'
                OR       c.SecondaryType LIKE '%AHD (type RA%'
                OR       c.SecondaryType LIKE '%AHD (type%')   # ALL types of AHD
                THEN 107 # Both AHD and t-AML
            

            WHEN b.StatusDisease NOT LIKE '%nd1%' 
                AND  c.SecondaryType NOT LIKE '%Chemotherapy History%'
                AND (c.SecondaryType LIKE '%AHD (type MF%'
                OR   c.SecondaryType LIKE '%AHD (type MP%'
                OR   c.SecondaryType LIKE '%AHD (type Thrombo%'
                OR   c.SecondaryType LIKE '%AHD (type Pancy%'
                OR   c.SecondaryType LIKE '%AHD (type AHD/ET)%'
                OR   c.SecondaryType LIKE '%AHD (type Anemia)%'
                OR   c.SecondaryType LIKE '%AHD (type AHD/MF%'
                OR   c.SecondaryType LIKE '%AHD (type AHD/PV%'
                OR   c.SecondaryType LIKE '%AHD (type AHD/ITP)%'
                OR   c.SecondaryType LIKE '%AHD (type AHD/Pancy%'
                OR   c.SecondaryType LIKE '%AHD (type AHD/Thrombo%') # ALL Types of MPN
                THEN 108 # Undiagnosed / Unknown / AHD (Prior cytopeinia of unclear etiology > 1 month)

            WHEN b.StatusDisease NOT LIKE '%nd1%' 
                AND  c.SecondaryType NOT LIKE '%Chemotherapy History%'
                AND (c.SecondaryType LIKE '%type unknown%'
                OR   c.SecondaryType LIKE '%type Not Applicable%'
                OR   c.SecondaryType LIKE '%Documented AHD (type CM%'
                OR   c.SecondaryType LIKE '%AHD (type%')
                THEN 109 # Undiagnosed / Unknown / AHD (Prior cytopeinia of unclear etiology > 1 month)

            WHEN b.StatusDisease NOT LIKE '%nd1%' 
                AND  c.SecondaryType LIKE '%Chemotherapy History%'
                AND (c.SecondaryType LIKE '%AHD (type MDS%'
                OR   c.SecondaryType LIKE '%AHD (type RC%'
                OR   c.SecondaryType LIKE '%AHD (type RA%'
                OR   c.SecondaryType LIKE '%AHD (type%')   # ALL types of AHD
                THEN 117 # Both AHD and t-AML

            ELSE 999
        END AS df_dx_aml2       # Secondary AML details
        , c.SecondaryType
        , CASE 
            WHEN b.StatusDisease LIKE '%nd%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN ' 1 -- Newly Diagnosed Arrival' # 'Diagnosis' really new diagnosis arrival date
            WHEN b.StatusDisease LIKE '%nd%' AND b.Status LIKE '%newly%' AND b.Status LIKE '%diagnosed%' THEN ' 1 -- Newly Diagnosed Arrival' # 'Diagnosis' really new diagnosis arrival date
            WHEN b.StatusDisease LIKE '%nd%' AND b.Status LIKE '%newly%' AND b.Status LIKE '%diagnosed%' THEN ' 1 -- Newly Diagnosed Arrival' # 'Diagnosis' really new diagnosis arrival date
            WHEN b.StatusDisease LIKE '%rel%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN ' 3 -- Relapsed Arrival' # 'Diagnosis' really relapsed arrival date
            WHEN b.StatusDisease LIKE '%ref%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN ' 5 -- Refractory Arrival' # 'Diagnosis' really refractory arrival date
            ELSE NULL
        END AS desc_df_phase
        , b.status AS conv_df_phase
        , b.*
        FROM caisis.vdatasetpatients a
        JOIN caisis.vdatasetstatus b
            ON a.PtMRN = b.PtMRN
        LEFT JOIN caisis.secondarystatus c
            ON a.PtMRN = c.PtMRN
        WHERE (b.StatusDisease LIKE '%aml%' OR b.StatusDisease LIKE '%apl%')
        AND b.Status NOT LIKE '%diagnosis date%' 
        AND b.status NOT LIKE '%alive%'
        AND b.status NOT LIKE '%dead%'
        AND b.status NOT LIKE '%recovery%'
        AND b.status NOT LIKE '%response%'
        AND b.status NOT LIKE '%persist%'
        AND b.status NOT LIKE '%resistant%'
        AND b.status NOT LIKE '%refractory%'
        AND b.status NOT LIKE '%relapse%'
        AND b.status NOT RLIKE '(CR|PR).*(MRD)?'
        ORDER BY a.PtMRN, b.statusdate
    ;
SET @instanceid = 0;
UPDATE temp.df_aml
    SET redcap_repeat_instance=@instanceid:=@instanceid+1;
SELECT * FROM temp.df_aml;

SELECT ptmrn, df_dx_aml1, SecondaryType, status, statusdisease FROM temp.df_aml WHERE StatusDisease NOT RLIKE '(ND|REL|REF)+' ;
SELECT ptmrn, df_dx_aml1, df_dx_aml2, SecondaryType, status, statusdisease from temp.df_aml;
SELECT IF(statusdisease LIKE '%nd1%','De Novo','Secondary'), SecondaryType, count(*) 
    from temp.df_aml group by 1,2 ORDER BY statusdisease, 1;

SELECT IF(statusdisease LIKE '%nd1%','De Novo','Secondary'), SecondaryType, count(*) 
    from temp.df_aml where df_dx_aml2 = 999 group by 1,2 ORDER BY statusdisease, 1;

# Records where comorbidities show ahd/chemo but ND1
DROP TABLE IF EXISTS temp.temp;
CREATE TABLE temp.temp 
    SELECT PtMRN FROM temp.df_aml
        WHERE (statusdisease like '%nd1%' and df_dx_aml1 = 102)
            OR (statusdisease like '%nd2%' and df_dx_aml1 = 101)
            OR (secondarytype like '%hx of ahd/chemo not recorded%' and df_dx_aml1 = 102)
            OR (secondarytype not like '%hx of ahd/chemo not recorded%' and secondarytype not like '%novo%' and df_dx_aml1 = 101)
            OR df_dx_aml1 = 198;

# Patient Arrivals where ArrivalDx does not always agree with SecondaryType Algorithm
SELECT a.ptmrn, a.df_dx_aml1, a.df_dx_aml2, a.SecondaryType, a.status, a.statusdisease
    FROM temp.df_aml a
    LEFT JOIN temp.temp b ON a.PtMRN = b.PtMRN
    WHERE b.PtMRN IS NOT NULL;

SELECT `df_aml`.`df_mrn`,
    `df_aml`.`redcap_repeat_instrument`,
    `df_aml`.`redcap_repeat_instance`,
    date_format(`df_aml`.`df_date`,'%Y-%m-%d') AS `df_date`,
    `df_aml`.`df_phase`,
    `df_aml`.`df_dx`,
    `df_aml`.`df_dx_ot`,
    `df_aml`.`df_dx_aml1`,
    `df_aml`.`df_dx_aml2`
FROM `temp`.`df_aml`;


