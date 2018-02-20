    DROP TABLE IF EXISTS Temp.ArrivalTRM_GCLAM;
    CREATE TABLE Temp.ArrivalTRM_GCLAM
        SELECT a.`PtMRN`
            , a.`Status`
            , a.`StatusDisease`
            , a.`StatusDate`
            , a.`MedTxDate`
            , a.`Protocol`
            , a.`WildCard`
            , a.`Categorized`
            , a.`Intensity`
            , a.`PtBirthDate`
            , a.`PtGender`
            , a.`ResponseDescription`
            , a.`ArrivalDate`
            , a.`TreatmentStartDate`
            , a.`ResponseDate`

            , CASE
                WHEN `Categorized` = 'NOT TREATED' THEN 'Cannot Calculate'
                WHEN `Protocol` IS NULL THEN 'Cannot Calculate'
                WHEN `ECOG` IS NULL THEN 'Cannot Calculate'
                WHEN `AgeAtTreatmentStart` IS NULL THEN 'Cannot Calculate'
                WHEN `calcuated_platelet` IS NULL THEN 'Cannot Calculate'
                WHEN `calcuated_albumin` IS NULL THEN 'Cannot Calculate'
                WHEN `secondary` IS NULL THEN 'Cannot Calculate'
                WHEN `calcuated_wbc` IS NULL THEN 'Cannot Calculate'
                WHEN `calculated blasts and unclassified` IS NULL THEN 'Cannot Calculate'
                WHEN `calcuated_creatinine` IS NULL THEN 'Cannot Calculate'
                ELSE ROUND(-4.08
                    + (0.031 * `AgeAtTreatmentStart`
                    + 0.890 * `ECOG`
                    + 0.473 * `secondary`
                    - 0.008 * `calcuated_platelet`
                    - 0.483 * `calcuated_albumin`
                    + 0.341 * `calcuated_creatinine`
                    + 0.008 * `calcuated_wbc`
                    - 0.007 * `calculated blasts and unclassified`),4)
            END AS X

            , CASE
                WHEN `Categorized` = 'NOT TREATED' THEN 'Cannot Calculate'
                WHEN `Protocol` IS NULL THEN 'Cannot Calculate'
                WHEN `ECOG` IS NULL THEN 'Cannot Calculate'
                WHEN `AgeAtTreatmentStart` IS NULL THEN 'Cannot Calculate'
                WHEN `calcuated_platelet` IS NULL THEN 'Cannot Calculate'
                WHEN `calcuated_albumin` IS NULL THEN 'Cannot Calculate'
                WHEN `secondary` IS NULL THEN 'Cannot Calculate'
                WHEN `calcuated_wbc` IS NULL THEN 'Cannot Calculate'
                WHEN `calculated blasts and unclassified` IS NULL THEN 'Cannot Calculate'
                WHEN `calcuated_creatinine` IS NULL THEN 'Cannot Calculate'
                ELSE ROUND(100/(1+EXP(-1*(-4.08
                    + (0.031 * `AgeAtTreatmentStart`
                    + 0.890 * `ECOG`
                    + 0.473 * `secondary`
                    - 0.008 * `calcuated_platelet`
                    - 0.483 * `calcuated_albumin`
                    + 0.341 * `calcuated_creatinine`
                    + 0.008 * `calcuated_wbc`
                    - 0.007 * `calculated blasts and unclassified`)))),2)
            END AS `TRM_Version1 (Paper)`
            , CASE
                WHEN `Categorized` = 'NOT TREATED' THEN 'Cannot Calculate'
                WHEN `Protocol` IS NULL THEN 'Cannot Calculate'
                WHEN `ECOG` IS NULL THEN 'Cannot Calculate'
                WHEN `AgeAtTreatmentStart` IS NULL THEN 'Cannot Calculate'
                WHEN `calcuated_platelet` IS NULL THEN 'Cannot Calculate'
                WHEN `calcuated_albumin` IS NULL THEN 'Cannot Calculate'
                WHEN `secondary` IS NULL THEN 'Cannot Calculate'
                WHEN `calcuated_wbc` IS NULL THEN 'Cannot Calculate'
                WHEN `calculated blasts and unclassified` IS NULL THEN 'Cannot Calculate'
                WHEN `calcuated_creatinine` IS NULL THEN 'Cannot Calculate'
                ELSE ROUND(100/(1+EXP(-1*(-4.08
                    + (0.031 * `AgeAtTreatmentStart`
                    + 0.890 * `ECOG`
                    + 0.47 * `secondary`
                    - 0.008 * `calcuated_platelet`
                    - 0.48 * `calcuated_albumin`
                    + 0.34 * `calcuated_creatinine`
                    + 0.007 * `calcuated_wbc`
                    - 0.007 * `calculated blasts and unclassified`)))),2)
            END AS `TRM_Version2 (Online)`
            , CAST(NULL AS SIGNED INTEGER) as TRMRangeOrder
            , space(40) as TRMRange
            , a.`ECOG`
            , a.`AgeAtTreatmentStart`
            , a.`calcuated_platelet`
            , a.`calcuated_albumin`
            , a.`secondary`
            , a.`calcuated_wbc`
            , a.`calculated blasts and unclassified`
            , a.`calcuated_creatinine`
            , a.`calculated blast`
            , a.`calculated unclassified`

            # reasons TRM not calculated
            , CASE WHEN `Categorized` = 'NOT TREATED' THEN 1 ELSE NULL END as `Not Treated`
            , CASE WHEN `Protocol` IS NULL  THEN 1 ELSE NULL END as `Protocol Unknown`
            , CASE WHEN a.`ECOG` IS NULL THEN 1 ELSE NULL END as `ECOG Missing`
            , CASE WHEN a.`AgeAtTreatmentStart` IS NULL THEN 1 ELSE NULL END as `Age Missing`
            , CASE WHEN a.`calcuated_platelet` IS NULL THEN 1 ELSE NULL END as `Platelet Missing`
            , CASE WHEN a.`secondary` IS NULL THEN 1 ELSE NULL END as `Secondary Missing`
            , CASE WHEN a.`calcuated_wbc` IS NULL THEN 1 ELSE NULL END as `WBC Missing`
            , CASE WHEN a.`calculated blasts and unclassified` IS NULL THEN 1 ELSE NULL END as `Circulating Blasts Missing`
            , CASE WHEN a.`calcuated_creatinine` IS NULL THEN 1 ELSE NULL END as `Creatinine Missing`

            , a.`arrival_albumin`
            , a.`arrival_albumin_units`
            , a.`arrival_creatinine`
            , a.`arrival_creatinine_units`
            , a.`arrival_wbc`
            , a.`arrival_wbc_units`
            , a.`arrival_platelet`
            , a.`arrival_platelet_units`
            , a.`arrival circulating blast`
            , a.`arrival circulating blast units`
            , a.`arrival circulating blast date`
            , a.`arrival circulating unclassified`
            , a.`arrival circulating unclassified units`
            , a.`arrival circulating unclassified date`
            , a.`treatment_albumin`
            , a.`treatment_albumin_units`
            , a.`treatment_creatinine`
            , a.`treatment_creatinine_units`
            , a.`treatment_wbc`
            , a.`treatment_wbc_units`
            , a.`treatment_platelet`
            , a.`treatment_platelet_units`
            , a.`treatment circulating blast`
            , a.`treatment circulating blast units`
            , a.`treatment circulating blast date`
            , a.`treatment circulating unclassified`
            , a.`treatment circulating unclassified units`
            , a.`treatment circulating unclassified date`
        FROM Temp.Temp2_GCLAM a
        WHERE StatusDisease LIKE '%AML%'
        AND (StatusDisease LIKE '%ND%'
        OR StatusDisease LIKE '%REL%'
        OR StatusDisease LIKE '%REF%') ;

    UPDATE Temp.ArrivalTRM_GCLAM SET TRMRange =
        CASE
            WHEN `TRM_Version1 (Paper)` = 'Cannot Calculate' THEN 'Not Calculated'
            WHEN `TRM_Version1 (Paper)` BETWEEN  0    AND  7    THEN '0-7'
            WHEN `TRM_Version1 (Paper)` BETWEEN  7.01 AND  9.2  THEN '7.01 - 9.20'
            WHEN `TRM_Version1 (Paper)` BETWEEN  9.21 AND 13.09 THEN '9.21 - 13.09'
            WHEN `TRM_Version1 (Paper)` BETWEEN 13.10 AND 20.00 THEN '13.10 - 20.00'
            WHEN `TRM_Version1 (Paper)` BETWEEN 20.01 AND 39.99 THEN '20.01 - 39.99'
            WHEN `TRM_Version1 (Paper)` BETWEEN 40.00 AND 59.99 THEN '40.00 - 59.99'
            WHEN `TRM_Version1 (Paper)`      >= 60              THEN '60+'
            ELSE ''
        END
        , TRMRangeOrder = CASE
            WHEN `TRM_Version1 (Paper)` = 'Cannot Calculate' THEN NULL
            WHEN `TRM_Version1 (Paper)` BETWEEN  0     AND  7    THEN 1
            WHEN `TRM_Version1 (Paper)` BETWEEN  7.01 AND  9.2  THEN 2
            WHEN `TRM_Version1 (Paper)` BETWEEN  9.21 AND 13.09 THEN 3
            WHEN `TRM_Version1 (Paper)` BETWEEN 13.10 AND 20.00 THEN 4
            WHEN `TRM_Version1 (Paper)` BETWEEN 20.01 AND 39.99 THEN 5
            WHEN `TRM_Version1 (Paper)` BETWEEN 40.00 AND 59.99 THEN 6
            WHEN `TRM_Version1 (Paper)`      >= 60              THEN 7
            ELSE ''
        END;

select * from temp.arrivaltrm_gclam;