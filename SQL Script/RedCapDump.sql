

SET @instanceid = 0;

DROP TABLE IF EXISTS temp.df_aml;
CREATE TABLE temp.df_aml
    SELECT a.PtMRN AS df_mrn    # Medical Record Number
        , 'Disease Features' AS redcap_repeat_instrument
        , @instanceid:=@instanceid+1 AS redcap_repeat_instance
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
            WHEN c.SecondaryType LIKE '%de novo aml%' THEN 101
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
SELECT * FROM temp.df_aml;
SELECT ptmrn, df_dx_aml1, SecondaryType, status, statusdisease from temp.df_aml;
SELECT ptmrn, df_dx_aml1, SecondaryType, status, statusdisease from temp.df_aml;

# Records where comorbidities show ahd/chemo but ND1
SELECT ptmrn, df_dx_aml1, SecondaryType, status, statusdisease
FROM temp.df_aml
WHERE (statusdisease like '%nd1%' and df_dx_aml1 = 102)
OR (statusdisease like '%nd2%' and df_dx_aml1 = 101)
OR (secondarytype like '%hx of ahd/chemo not recorded%' and df_dx_aml1 = 102)
OR (secondarytype not like '%hx of ahd/chemo not recorded%' and secondarytype not like '%novo%' and df_dx_aml1 = 101)
OR df_dx_aml1 = 198;

SET @instanceid = 0;

DROP TABLE IF EXISTS temp.df_nonaml;
CREATE TABLE temp.df_nonaml
    SELECT a.PtMRN AS df_mrn
        , 'Disease Features' AS redcap_repeat_instrument
        , @instanceid:=@instanceid+1 AS redcap_repeat_instance
        , CASE 
            WHEN b.StatusDisease LIKE '%nd%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN b.statusdate # 'Diagnosis' really new diagnosis arrival date
            WHEN b.StatusDisease LIKE '%nd%' AND b.Status LIKE '%newly%' AND b.Status LIKE '%diagnosed%' THEN b.statusdate # 'Diagnosis' really new diagnosis arrival date
            WHEN b.StatusDisease LIKE '%rel%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN b.statusdate # 'Diagnosis' really relapsed arrival date
            WHEN b.StatusDisease LIKE '%ref%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN b.statusdate# 'Diagnosis' really refractory arrival date
            ELSE NULL
        END AS df_date
        , CASE 
            WHEN b.StatusDisease LIKE '%nd%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN 1 # 'Diagnosis' really new diagnosis arrival date
            WHEN b.StatusDisease LIKE '%nd%' AND b.Status LIKE '%newly%' AND b.Status LIKE '%diagnosed%' THEN 1 # 'Diagnosis' really new diagnosis arrival date
            WHEN b.StatusDisease LIKE '%rel%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN 3 # 'Diagnosis' really relapsed arrival date
            WHEN b.StatusDisease LIKE '%ref%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN 5 # 'Diagnosis' really refractory arrival date
            ELSE NULL
        END AS df_phase
        , 'AML' AS df_dx
        , CASE 
            WHEN b.StatusDisease LIKE '%nd%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN ' 1 -- Newly Diagnosed Arrival' # 'Diagnosis' really new diagnosis arrival date
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
        WHERE NOT (b.StatusDisease LIKE '%aml%' OR b.StatusDisease LIKE '%apl%')
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


SELECT * FROM temp.df_aml;
SELECT * FROM temp.df_nonaml;
# All status variations
SELECT status, count(*) FROM caisis.vdatasetstatus GROUP BY status ;
# All status variations containing 'diagnosis'
SELECT status, count(*) FROM caisis.vdatasetstatus WHERE status LIKE '%diagnosis%' GROUP BY status ;
# All status variations containing 'work'
SELECT status, count(*) FROM caisis.vdatasetstatus WHERE status LIKE '%work%' GROUP BY status ;
# All status variations containing 'arrival'
SELECT status, count(*) FROM caisis.vdatasetstatus WHERE status LIKE '%arrival%' GROUP BY status ;
# All status variations containing 'new'
SELECT status, count(*) FROM caisis.vdatasetstatus WHERE status LIKE '%new%' GROUP BY status ;
# All status variations containing 'rel'
SELECT statusdisease, status, count(*) FROM caisis.vdatasetstatus WHERE status LIKE '%rel%' GROUP BY statusdisease, status ;