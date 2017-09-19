from MySQLdbUtils import *


reload(sys)

sys.setdefaultencoding('utf8')

cnxdict = connect_to_mysql_db_prod('relevanttrm')
cnxdict['out_filepath'] = buildfilepath(cnxdict, filename='relevanttrm')
writer = pd.ExcelWriter(cnxdict['out_filepath'], datetime_format='mm/dd/yyyy')  # datetime_format='mmm d yyyy hh:mm:ss'


def CreateFirstMedicalTherapyAfterArrival(cnxdict):
    cnxdict['sql'] = """
    # Find the first treatment for each arrival in two steps, the table temp.temp helps build temp.firsttxdate
    DROP TABLE IF EXISTS Temp.Temp ;
    CREATE TABLE Temp.Temp
        SELECT Status.PtMRN, Status.StatusDate, MIN(Protocol.MedTxDate) AS MedTxDate
            FROM (
                SELECT DISTINCT
                    PtMRN
                    , StatusDate
                    , statusDisease
                    , Status
                    FROM Caisis.vdatasetstatus
                        WHERE status like '%arrival%' ) as Status
            LEFT JOIN (
                SELECT DISTINCT
                    PtMRN
                    , MedTxDate
                    , MedTxAgent
                    , Categorized
                    , Intensity
                    , Wildcard
                FROM ProtocolCategory.PatientProtocol ) as Protocol
            ON Protocol.ptmrn = status.PtMRN AND  Status.StatusDate < Protocol.MedTxDate
            GROUP BY Status.PtMRN, Status.StatusDate ;

    DROP TABLE IF EXISTS Temp.FirstTxDate ;
    CREATE TABLE Temp.FirstTxDate
        SELECT PtMRN, MIN(StatusDate) as StatusDate, MedTxDate
            FROM Temp.Temp
            GROUP BY PtMRN, MedTxDate
            ORDER BY PtMRN, StatusDate, MedTxDate;

    """
    dosqlexecute(cnxdict)


def ArrivalTRM_GCLAM():
    cnxdict['sql'] = """
    DROP TABLE IF EXISTS Temp.Temp2_GCLAM ;
    CREATE TABLE Temp.Temp2_GCLAM
        SELECT
            Status.PtMRN
            , Status.Status
            , Status.StatusDisease
            , Status.StatusDate
            , FirstTxDate.MedTxDate
            , `labsummary`.`Protocol`
            , `labsummary`.`WildCard`
            , Protocol.Categorized
            , Protocol.Intensity
            , vdatasetpatients.PtBirthDate
            , vdatasetpatients.PtGender
            , timestampdiff(year,vdatasetpatients.PtBirthDate,Protocol.MedTxDate) as AgeAtTreatmentStart
            , `labsummary`.`ResponseDescription`
            , `labsummary`.`ArrivalDate`
            , `labsummary`.`TreatmentStartDate`
            , `labsummary`.`ResponseDate`
            , v_secondary.secondary
            , EncECOG_Score AS ECOG

            , CASE
                WHEN arrival_wbc IS NOT NULL
                    THEN CASE
                        WHEN arrival_circulating_blast IS NOT NULL
                            THEN CASE
                                WHEN arrival_circulating_blast_units LIKE '%thou/ul%'
                                    THEN arrival_wbc/arrival_circulating_blast
                                WHEN arrival_circulating_blast_units = '%'
                                    THEN arrival_circulating_blast
                                ELSE NULL
                            END
                        WHEN treatment_circulating_blast IS NOT NULL
                            THEN CASE
                                WHEN treatment_circulating_blast_units LIKE '%thou/ul%'
                                    THEN arrival_wbc/treatment_circulating_blast
                                WHEN treatment_circulating_blast_units = '%'
                                    THEN arrival_circulating_blast
                                ELSE NULL
                            END
                        ELSE NULL
                    END
                WHEN arrival_wbc IS NULL AND treatment_wbc IS NOT NULL
                    THEN CASE
                        WHEN arrival_circulating_blast IS NOT NULL
                            THEN CASE
                                WHEN arrival_circulating_blast_units LIKE '%thou/ul%'
                                    THEN treatment_wbc/arrival_circulating_blast
                                WHEN arrival_circulating_blast_units = '%'
                                    THEN arrival_circulating_blast
                                ELSE NULL
                            END
                        WHEN treatment_circulating_blast IS NOT NULL
                            THEN CASE
                                WHEN treatment_circulating_blast_units LIKE '%thou/ul%'
                                    THEN treatment_wbc/treatment_circulating_blast
                                WHEN treatment_circulating_blast_units = '%'
                                    THEN arrival_circulating_blast
                                ELSE NULL
                            END
                        ELSE NULL
                    END
                ELSE NULL
            END AS `calculated blast`

            , CASE
                WHEN arrival_wbc IS NOT NULL
                    THEN CASE
                        WHEN arrival_circulating_unclassified IS NOT NULL
                            THEN CASE
                                WHEN arrival_circulating_unclassified_units LIKE '%thou/ul%'
                                    THEN arrival_wbc/arrival_circulating_unclassified
                                WHEN arrival_circulating_unclassified_units = '%'
                                    THEN arrival_circulating_unclassified
                                ELSE NULL
                            END
                        WHEN treatment_circulating_unclassified IS NOT NULL
                            THEN CASE
                                WHEN treatment_circulating_unclassified_units LIKE '%thou/ul%'
                                    THEN arrival_wbc/treatment_circulating_unclassified
                                WHEN treatment_circulating_unclassified_units = '%'
                                    THEN arrival_circulating_unclassified
                                ELSE NULL
                            END
                        ELSE NULL
                    END
                WHEN arrival_wbc IS NULL AND treatment_wbc IS NOT NULL
                    THEN CASE
                        WHEN arrival_circulating_unclassified IS NOT NULL
                            THEN CASE
                                WHEN arrival_circulating_unclassified_units LIKE '%thou/ul%'
                                    THEN treatment_wbc/arrival_circulating_unclassified
                                WHEN arrival_circulating_unclassified_units = '%'
                                    THEN arrival_circulating_unclassified
                                ELSE -1
                            END
                        WHEN treatment_circulating_unclassified IS NOT NULL
                            THEN CASE
                                WHEN treatment_circulating_unclassified_units LIKE '%thou/ul%'
                                    THEN treatment_wbc/treatment_circulating_unclassified
                                WHEN treatment_circulating_unclassified_units = '%'
                                    THEN arrival_circulating_unclassified
                                ELSE NULL
                            END
                        ELSE NULL
                    END
                ELSE NULL
            END AS `calculated unclassified`
            , -999 as `calculated blasts and unclassified`
            , CASE
                WHEN arrival_wbc IS NULL THEN treatment_wbc
                ELSE arrival_wbc
            END AS calcuated_wbc
            , CASE
                WHEN arrival_albumin IS NULL THEN treatment_albumin
                ELSE arrival_albumin
            END AS calcuated_albumin
            , CASE
                WHEN arrival_creatinine IS NULL THEN treatment_creatinine
                ELSE arrival_creatinine
            END AS calcuated_creatinine
            , CASE
                WHEN arrival_platelet IS NULL THEN treatment_platelet
                ELSE arrival_platelet
            END AS calcuated_platelet

            , `labsummary`.`arrival_albumin`
            , `labsummary`.`arrival_albumin_units`
            , `labsummary`.`arrival_creatinine`
            , `labsummary`.`arrival_creatinine_units`
            , `labsummary`.`arrival_wbc`
            , `labsummary`.`arrival_wbc_units`
            , `labsummary`.`arrival_platelet`
            , `labsummary`.`arrival_platelet_units`
            , `labsummary`.`arrival_circulating_blast` AS `arrival circulating blast`
            , `labsummary`.`arrival_circulating_blast_units` AS `arrival circulating blast units`
            , `labsummary`.`arrival_circulating_blast_date` AS `arrival circulating blast date`
            , `labsummary`.`arrival_circulating_unclassified` AS `arrival circulating unclassified`
            , `labsummary`.`arrival_circulating_unclassified_units` AS `arrival circulating unclassified units`
            , `labsummary`.`arrival_circulating_unclassified_date` AS `arrival circulating unclassified date`

            , `labsummary`.`treatment_albumin`
            , `labsummary`.`treatment_albumin_units`
            , `labsummary`.`treatment_creatinine`
            , `labsummary`.`treatment_creatinine_units`
            , `labsummary`.`treatment_wbc`
            , `labsummary`.`treatment_wbc_units`
            , `labsummary`.`treatment_platelet`
            , `labsummary`.`treatment_platelet_units`
            , `labsummary`.`treatment_circulating_blast` AS `treatment circulating blast`
            , `labsummary`.`treatment_circulating_blast_units` AS `treatment circulating blast units`
            , `labsummary`.`treatment_circulating_blast_date` AS `treatment circulating blast date`
            , `labsummary`.`treatment_circulating_unclassified` AS `treatment circulating unclassified`
            , `labsummary`.`treatment_circulating_unclassified_units` AS `treatment circulating unclassified units`
            , `labsummary`.`treatment_circulating_unclassified_date` AS `treatment circulating unclassified date`
            FROM (
                SELECT DISTINCT
                    PtMRN
                    , StatusDate
                    , statusDisease
                    , Status
                    FROM Caisis.vdatasetstatus
                        WHERE status like '%arrival%' ) as Status
            LEFT JOIN Temp.FirstTxDate
                ON FirstTxDate.PtMRN = Status.PtMRN AND FirstTxDate.StatusDate = Status.StatusDate
            LEFT JOIN Caisis.vdatasetpatients
                ON Status.PtMRN = vdatasetpatients.PtMRN
            LEFT JOIN (
                SELECT DISTINCT
                    PtMRN
                    , MedTxDate
                    , Categorized
                    , Intensity
                FROM ProtocolCategory.PatientProtocol ) as Protocol
                ON  FirstTxDate.PtMRN = Protocol.PtMRN AND FirstTxDate.MedTxDate = Protocol.MedTxDate
            LEFT JOIN  Caisis.v_secondary # v_secondary is a view in the caisis database
                ON v_secondary.PtMRN = FirstTxDate.PtMRN
            LEFT JOIN  Caisis.encounter
                ON encounter.PtMRN = Status.PtMRN and encounter.encdate = status.statusdate
            LEFT JOIN `relevantlab`.`labsummary`
                ON `labsummary`.`PtMRN` = FirstTxDate.PtMRN
                    AND `labsummary`.`ArrivalDate` = FirstTxDate.StatusDate
                    and `labsummary`.`TreatmentStartDate` = FirstTxDate.MedTxDate
            GROUP BY
                Status.PtMRN
                , Status.Status
                , Status.StatusDisease
                , Status.StatusDate
                , Protocol.MedTxDate
                , Protocol.Categorized
                , Protocol.Intensity
                , vdatasetpatients.PtBirthDate
            ORDER BY Status.PtMRN, Status.StatusDate, Protocol.MedTxDate;

    DELETE FROM Temp.Temp2_GCLAM WHERE Categorized <> 'G-CLAM' OR Categorized IS NULL OR Protocol IS NULL;

    ALTER TABLE Temp.Temp2_GCLAM
        CHANGE COLUMN `calculated blasts and unclassified` `calculated blasts and unclassified` INT(4) NULL ;

    UPDATE Temp.Temp2_GCLAM
        SET `calculated blast` = round(`calculated blast`,2)
        ,   `calculated unclassified` = round(`calculated unclassified`,2)
        ,   `calculated blasts and unclassified` = NULL;

    UPDATE Temp.Temp2_GCLAM
        SET `calculated blasts and unclassified` = CASE
            WHEN `calculated blast` IS NULL
                    AND `calculated unclassified` IS NOT NULL
                THEN `calculated unclassified`
            WHEN `calculated blast` IS NOT NULL
                    AND `calculated unclassified` IS NULL
                THEN `calculated blast`
            WHEN `calculated blast` IS NOT NULL AND `calculated unclassified` IS NOT NULL
                    AND `arrival circulating blast date` = `arrival circulating unclassified date`
                    AND `calculated blast` + `calculated unclassified` > 100
                THEN `calculated blast`
            WHEN `calculated blast` IS NOT NULL AND `calculated unclassified` IS NOT NULL
                    AND `arrival circulating blast date` = `arrival circulating unclassified date`
                    AND `calculated blast` + `calculated unclassified` < 100
                THEN `calculated blast` + `calculated unclassified`
            ELSE `calculated blast`
        END ;

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

    DROP TABLE IF EXISTS Temp.TRMRangeOrder;
    CREATE TABLE Temp.TRMRangeOrder
        SELECT TRMRangeOrder
            , TRMRange
            FROM Temp.ArrivalTRM
            WHERE Categorized <> 'NOT TREATED'
            GROUP BY TRMRange
            ORDER BY TRMRangeOrder ;

    """
    dosqlexecute(cnxdict)


def ArrivalTRM():
    cnxdict['sql'] = """
    DROP TABLE IF EXISTS Temp.Temp2 ;
    CREATE TABLE Temp.Temp2
    SELECT
            Status.PtMRN
            , Status.Status
            , Status.StatusDisease
            , Status.StatusDate
            , FirstTxDate.MedTxDate
            , `labsummary`.`Protocol`
            , `labsummary`.`WildCard`
            , Protocol.Categorized
            , CASE
                WHEN Protocol.Intensity <> 'Intermediate' THEN Protocol.Intensity
                WHEN Protocol.Intensity = 'Intermediate'
                    AND `labsummary`.`Protocol` IN (
                        '3+7'
                        , '3 + 7'
                        , '3+7 (ida)'
                        , '3+7(ida)'
                        , '3+7 (dauno)'
                        , '3+7(dauno)'
                        , '3+7 (dauno) for AML ND2'
                        , '3+7 (for AML ND)'
                        , '3+7 (for AML ND1)'
                        , '3+7 (for AML ND2)'
                        , '3+7 (for AML REL-S1)'
                        , '3+7(2 courses)'
                        , '3+7(dauno) (2 courses)'
                        , '3+7(dauno) 2 courses'
                        , '3+7(ida) 2 courses'
                        , '3+7(ida)(s1) got 2 courses'
                        , '3+7(ida)-2 courses'
                        , '3+7(induction for AML)'
                        , '3+7(SWOG)'
                        , '7+3'
                        , '7 + 3'
                        , '7+3 (dauno.)'
                        , '7+3(dauno)'
                        , '7+3(ida)'
                        , '7+3 standard dose'
                        , '7+3 (Cytarabine + Idarubicin)'
                        , 'cytarabine and idarubicin (7 + 3).'
                        , 'Induct. For AML 3+7'
                        , 'Induction -3+7'
                        , 'SO106'
                        )
                    THEN 'Intermediate'
                WHEN Protocol.Intensity = 'Intermediate'
                    AND `labsummary`.`Protocol` in ('HIDAC', 'HDAC')
                    THEN 'High'
                WHEN Protocol.Intensity = 'Intermediate' THEN 'Not Categorized'
                ELSE Protocol.Intensity
            END AS Intensity
            , vdatasetpatients.PtBirthDate
            , vdatasetpatients.PtGender
            , timestampdiff(year,vdatasetpatients.PtBirthDate,Protocol.MedTxDate) as AgeAtTreatmentStart
            , `labsummary`.`ResponseDescription`
            , `labsummary`.`ArrivalDate`
            , `labsummary`.`TreatmentStartDate`
            , `labsummary`.`ResponseDate`
            , v_secondary.secondary
            , EncECOG_Score AS ECOG

            , CASE
                WHEN arrival_wbc IS NOT NULL
                    THEN CASE
                        WHEN arrival_circulating_blast IS NOT NULL
                            THEN CASE
                                WHEN arrival_circulating_blast_units LIKE '%thou/ul%'
                                    THEN arrival_wbc/arrival_circulating_blast
                                WHEN arrival_circulating_blast_units = '%'
                                    THEN arrival_circulating_blast
                                ELSE NULL
                            END
                        WHEN treatment_circulating_blast IS NOT NULL
                            THEN CASE
                                WHEN treatment_circulating_blast_units LIKE '%thou/ul%'
                                    THEN arrival_wbc/treatment_circulating_blast
                                WHEN treatment_circulating_blast_units = '%'
                                    THEN arrival_circulating_blast
                                ELSE NULL
                            END
                        ELSE NULL
                    END
                WHEN arrival_wbc IS NULL AND treatment_wbc IS NOT NULL
                    THEN CASE
                        WHEN arrival_circulating_blast IS NOT NULL
                            THEN CASE
                                WHEN arrival_circulating_blast_units LIKE '%thou/ul%'
                                    THEN treatment_wbc/arrival_circulating_blast
                                WHEN arrival_circulating_blast_units = '%'
                                    THEN arrival_circulating_blast
                                ELSE NULL
                            END
                        WHEN treatment_circulating_blast IS NOT NULL
                            THEN CASE
                                WHEN treatment_circulating_blast_units LIKE '%thou/ul%'
                                    THEN treatment_wbc/treatment_circulating_blast
                                WHEN treatment_circulating_blast_units = '%'
                                    THEN arrival_circulating_blast
                                ELSE NULL
                            END
                        ELSE NULL
                    END
                ELSE NULL
            END AS `calculated blast`

            , CASE
                WHEN arrival_wbc IS NOT NULL
                    THEN CASE
                        WHEN arrival_circulating_unclassified IS NOT NULL
                            THEN CASE
                                WHEN arrival_circulating_unclassified_units LIKE '%thou/ul%'
                                    THEN arrival_wbc/arrival_circulating_unclassified
                                WHEN arrival_circulating_unclassified_units = '%'
                                    THEN arrival_circulating_unclassified
                                ELSE NULL
                            END
                        WHEN treatment_circulating_unclassified IS NOT NULL
                            THEN CASE
                                WHEN treatment_circulating_unclassified_units LIKE '%thou/ul%'
                                    THEN arrival_wbc/treatment_circulating_unclassified
                                WHEN treatment_circulating_unclassified_units = '%'
                                    THEN arrival_circulating_unclassified
                                ELSE NULL
                            END
                        ELSE NULL
                    END
                WHEN arrival_wbc IS NULL AND treatment_wbc IS NOT NULL
                    THEN CASE
                        WHEN arrival_circulating_unclassified IS NOT NULL
                            THEN CASE
                                WHEN arrival_circulating_unclassified_units LIKE '%thou/ul%'
                                    THEN treatment_wbc/arrival_circulating_unclassified
                                WHEN arrival_circulating_unclassified_units = '%'
                                    THEN arrival_circulating_unclassified
                                ELSE -1
                            END
                        WHEN treatment_circulating_unclassified IS NOT NULL
                            THEN CASE
                                WHEN treatment_circulating_unclassified_units LIKE '%thou/ul%'
                                    THEN treatment_wbc/treatment_circulating_unclassified
                                WHEN treatment_circulating_unclassified_units = '%'
                                    THEN arrival_circulating_unclassified
                                ELSE NULL
                            END
                        ELSE NULL
                    END
                ELSE NULL
            END AS `calculated unclassified`
            , -999 as `calculated blasts and unclassified`
            , CASE
                WHEN arrival_wbc IS NULL THEN treatment_wbc
                ELSE arrival_wbc
            END AS calcuated_wbc
            , CASE
                WHEN arrival_albumin IS NULL THEN treatment_albumin
                ELSE arrival_albumin
            END AS calcuated_albumin
            , CASE
                WHEN arrival_creatinine IS NULL THEN treatment_creatinine
                ELSE arrival_creatinine
            END AS calcuated_creatinine
            , CASE
                WHEN arrival_platelet IS NULL THEN treatment_platelet
                ELSE arrival_platelet
            END AS calcuated_platelet

            , `labsummary`.`arrival_albumin`
            , `labsummary`.`arrival_albumin_units`
            , `labsummary`.`arrival_creatinine`
            , `labsummary`.`arrival_creatinine_units`
            , `labsummary`.`arrival_wbc`
            , `labsummary`.`arrival_wbc_units`
            , `labsummary`.`arrival_platelet`
            , `labsummary`.`arrival_platelet_units`
            , `labsummary`.`arrival_circulating_blast` AS `arrival circulating blast`
            , `labsummary`.`arrival_circulating_blast_units` AS `arrival circulating blast units`
            , `labsummary`.`arrival_circulating_blast_date` AS `arrival circulating blast date`
            , `labsummary`.`arrival_circulating_unclassified` AS `arrival circulating unclassified`
            , `labsummary`.`arrival_circulating_unclassified_units` AS `arrival circulating unclassified units`
            , `labsummary`.`arrival_circulating_unclassified_date` AS `arrival circulating unclassified date`

            , `labsummary`.`treatment_albumin`
            , `labsummary`.`treatment_albumin_units`
            , `labsummary`.`treatment_creatinine`
            , `labsummary`.`treatment_creatinine_units`
            , `labsummary`.`treatment_wbc`
            , `labsummary`.`treatment_wbc_units`
            , `labsummary`.`treatment_platelet`
            , `labsummary`.`treatment_platelet_units`
            , `labsummary`.`treatment_circulating_blast` AS `treatment circulating blast`
            , `labsummary`.`treatment_circulating_blast_units` AS `treatment circulating blast units`
            , `labsummary`.`treatment_circulating_blast_date` AS `treatment circulating blast date`
            , `labsummary`.`treatment_circulating_unclassified` AS `treatment circulating unclassified`
            , `labsummary`.`treatment_circulating_unclassified_units` AS `treatment circulating unclassified units`
            , `labsummary`.`treatment_circulating_unclassified_date` AS `treatment circulating unclassified date`
            FROM (
                SELECT DISTINCT
                    PtMRN
                    , StatusDate
                    , statusDisease
                    , Status
                    FROM Caisis.vdatasetstatus
                        WHERE status like '%arrival%' ) as Status
            LEFT JOIN Temp.FirstTxDate
                ON FirstTxDate.PtMRN = Status.PtMRN AND FirstTxDate.StatusDate = Status.StatusDate
            LEFT JOIN Caisis.vdatasetpatients
                ON Status.PtMRN = vdatasetpatients.PtMRN
            LEFT JOIN (
                SELECT DISTINCT
                    PtMRN
                    , MedTxDate
                    , Categorized
                    , Intensity
                FROM ProtocolCategory.PatientProtocol ) as Protocol
                ON  FirstTxDate.PtMRN = Protocol.PtMRN AND FirstTxDate.MedTxDate = Protocol.MedTxDate
            LEFT JOIN  Caisis.v_secondary # v_secondary is a view in the caisis database
                ON v_secondary.PtMRN = FirstTxDate.PtMRN
            LEFT JOIN  Caisis.encounter
                ON encounter.PtMRN = Status.PtMRN and encounter.encdate = status.statusdate
            LEFT JOIN `relevantlab`.`labsummary`
                ON `labsummary`.`PtMRN` = FirstTxDate.PtMRN
                    AND `labsummary`.`ArrivalDate` = FirstTxDate.StatusDate
                    and `labsummary`.`TreatmentStartDate` = FirstTxDate.MedTxDate
            GROUP BY
                Status.PtMRN
                , Status.Status
                , Status.StatusDisease
                , Status.StatusDate
                , Protocol.MedTxDate
                , Protocol.Categorized
                , Protocol.Intensity
                , vdatasetpatients.PtBirthDate
            ORDER BY Status.PtMRN, Status.StatusDate, Protocol.MedTxDate;


    ALTER TABLE Temp.Temp2
        CHANGE COLUMN `calculated blasts and unclassified` `calculated blasts and unclassified` INT(4) NULL ;

    UPDATE Temp.Temp2
        SET `calculated blast` = round(`calculated blast`,2)
        ,   `calculated unclassified` = round(`calculated unclassified`,2)
        ,   `calculated blasts and unclassified` = NULL;

    UPDATE Temp.Temp2
        SET `calculated blasts and unclassified` = CASE
            WHEN `calculated blast` IS NULL
                    AND `calculated unclassified` IS NOT NULL
                THEN `calculated unclassified`
            WHEN `calculated blast` IS NOT NULL
                    AND `calculated unclassified` IS NULL
                THEN `calculated blast`
            WHEN `calculated blast` IS NOT NULL AND `calculated unclassified` IS NOT NULL
                    AND `arrival circulating blast date` = `arrival circulating unclassified date`
                    AND `calculated blast` + `calculated unclassified` > 100
                THEN `calculated blast`
            WHEN `calculated blast` IS NOT NULL AND `calculated unclassified` IS NOT NULL
                    AND `arrival circulating blast date` = `arrival circulating unclassified date`
                    AND `calculated blast` + `calculated unclassified` < 100
                THEN `calculated blast` + `calculated unclassified`
            ELSE `calculated blast`
        END ;

    DROP TABLE IF EXISTS Temp.ArrivalTRM;
    CREATE TABLE Temp.ArrivalTRM
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
        FROM Temp.Temp2 a
        WHERE StatusDisease LIKE '%AML%'
        AND (StatusDisease LIKE '%ND%'
        OR StatusDisease LIKE '%REL%'
        OR StatusDisease LIKE '%REF%') ;

    UPDATE Temp.ArrivalTRM SET TRMRange =
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
    """
    dosqlexecute(cnxdict)


def ArrivalTRM_():
    cnxdict['sql'] = """
    DROP TABLE IF EXISTS Temp.Temp2 ;
    CREATE TABLE Temp.Temp2
        SELECT
            Status.PtMRN
            , Status.Status
            , Status.StatusDisease
            , Status.StatusDate
            , FirstTxDate.MedTxDate
            , `labsummary`.`Protocol`
            , `labsummary`.`WildCard`
            , Protocol.Categorized
            , Protocol.Intensity
            , vdatasetpatients.PtBirthDate
            , vdatasetpatients.PtGender
            , timestampdiff(year,vdatasetpatients.PtBirthDate,Protocol.MedTxDate) as AgeAtTreatmentStart
            , `labsummary`.`ResponseDescription`
            , `labsummary`.`ArrivalDate`
            , `labsummary`.`TreatmentStartDate`
            , `labsummary`.`ResponseDate`
            , v_secondary.secondary
            , EncECOG_Score AS ECOG

            , CASE
                WHEN arrival_wbc IS NOT NULL
                    THEN CASE
                        WHEN arrival_circulating_blast IS NOT NULL
                            THEN CASE
                                WHEN arrival_circulating_blast_units LIKE '%thou/ul%'
                                    THEN arrival_wbc/arrival_circulating_blast
                                WHEN arrival_circulating_blast_units = '%'
                                    THEN arrival_circulating_blast
                                ELSE NULL
                            END
                        WHEN treatment_circulating_blast IS NOT NULL
                            THEN CASE
                                WHEN treatment_circulating_blast_units LIKE '%thou/ul%'
                                    THEN arrival_wbc/treatment_circulating_blast
                                WHEN treatment_circulating_blast_units = '%'
                                    THEN arrival_circulating_blast
                                ELSE NULL
                            END
                        ELSE NULL
                    END
                WHEN arrival_wbc IS NULL AND treatment_wbc IS NOT NULL
                    THEN CASE
                        WHEN arrival_circulating_blast IS NOT NULL
                            THEN CASE
                                WHEN arrival_circulating_blast_units LIKE '%thou/ul%'
                                    THEN treatment_wbc/arrival_circulating_blast
                                WHEN arrival_circulating_blast_units = '%'
                                    THEN arrival_circulating_blast
                                ELSE NULL
                            END
                        WHEN treatment_circulating_blast IS NOT NULL
                            THEN CASE
                                WHEN treatment_circulating_blast_units LIKE '%thou/ul%'
                                    THEN treatment_wbc/treatment_circulating_blast
                                WHEN treatment_circulating_blast_units = '%'
                                    THEN arrival_circulating_blast
                                ELSE NULL
                            END
                        ELSE NULL
                    END
                ELSE NULL
            END AS `calculated blast`

            , CASE
                WHEN arrival_wbc IS NOT NULL
                    THEN CASE
                        WHEN arrival_circulating_unclassified IS NOT NULL
                            THEN CASE
                                WHEN arrival_circulating_unclassified_units LIKE '%thou/ul%'
                                    THEN arrival_wbc/arrival_circulating_unclassified
                                WHEN arrival_circulating_unclassified_units = '%'
                                    THEN arrival_circulating_unclassified
                                ELSE NULL
                            END
                        WHEN treatment_circulating_unclassified IS NOT NULL
                            THEN CASE
                                WHEN treatment_circulating_unclassified_units LIKE '%thou/ul%'
                                    THEN arrival_wbc/treatment_circulating_unclassified
                                WHEN treatment_circulating_unclassified_units = '%'
                                    THEN arrival_circulating_unclassified
                                ELSE NULL
                            END
                        ELSE NULL
                    END
                WHEN arrival_wbc IS NULL AND treatment_wbc IS NOT NULL
                    THEN CASE
                        WHEN arrival_circulating_unclassified IS NOT NULL
                            THEN CASE
                                WHEN arrival_circulating_unclassified_units LIKE '%thou/ul%'
                                    THEN treatment_wbc/arrival_circulating_unclassified
                                WHEN arrival_circulating_unclassified_units = '%'
                                    THEN arrival_circulating_unclassified
                                ELSE -1
                            END
                        WHEN treatment_circulating_unclassified IS NOT NULL
                            THEN CASE
                                WHEN treatment_circulating_unclassified_units LIKE '%thou/ul%'
                                    THEN treatment_wbc/treatment_circulating_unclassified
                                WHEN treatment_circulating_unclassified_units = '%'
                                    THEN arrival_circulating_unclassified
                                ELSE NULL
                            END
                        ELSE NULL
                    END
                ELSE NULL
            END AS `calculated unclassified`
            , -999 as `calculated blasts and unclassified`
            , CASE
                WHEN arrival_wbc IS NULL THEN treatment_wbc
                ELSE arrival_wbc
            END AS calcuated_wbc
            , CASE
                WHEN arrival_albumin IS NULL THEN treatment_albumin
                ELSE arrival_albumin
            END AS calcuated_albumin
            , CASE
                WHEN arrival_creatinine IS NULL THEN treatment_creatinine
                ELSE arrival_creatinine
            END AS calcuated_creatinine
            , CASE
                WHEN arrival_platelet IS NULL THEN treatment_platelet
                ELSE arrival_platelet
            END AS calcuated_platelet

            , `labsummary`.`arrival_albumin`
            , `labsummary`.`arrival_albumin_units`
            , `labsummary`.`arrival_creatinine`
            , `labsummary`.`arrival_creatinine_units`
            , `labsummary`.`arrival_wbc`
            , `labsummary`.`arrival_wbc_units`
            , `labsummary`.`arrival_platelet`
            , `labsummary`.`arrival_platelet_units`
            , `labsummary`.`arrival_circulating_blast` AS `arrival circulating blast`
            , `labsummary`.`arrival_circulating_blast_units` AS `arrival circulating blast units`
            , `labsummary`.`arrival_circulating_blast_date` AS `arrival circulating blast date`
            , `labsummary`.`arrival_circulating_unclassified` AS `arrival circulating unclassified`
            , `labsummary`.`arrival_circulating_unclassified_units` AS `arrival circulating unclassified units`
            , `labsummary`.`arrival_circulating_unclassified_date` AS `arrival circulating unclassified date`

            , `labsummary`.`treatment_albumin`
            , `labsummary`.`treatment_albumin_units`
            , `labsummary`.`treatment_creatinine`
            , `labsummary`.`treatment_creatinine_units`
            , `labsummary`.`treatment_wbc`
            , `labsummary`.`treatment_wbc_units`
            , `labsummary`.`treatment_platelet`
            , `labsummary`.`treatment_platelet_units`
            , `labsummary`.`treatment_circulating_blast` AS `treatment circulating blast`
            , `labsummary`.`treatment_circulating_blast_units` AS `treatment circulating blast units`
            , `labsummary`.`treatment_circulating_blast_date` AS `treatment circulating blast date`
            , `labsummary`.`treatment_circulating_unclassified` AS `treatment circulating unclassified`
            , `labsummary`.`treatment_circulating_unclassified_units` AS `treatment circulating unclassified units`
            , `labsummary`.`treatment_circulating_unclassified_date` AS `treatment circulating unclassified date`
            FROM (
                SELECT DISTINCT
                    PtMRN
                    , StatusDate
                    , statusDisease
                    , Status
                    FROM Caisis.vdatasetstatus
                        WHERE status like '%arrival%' ) as Status
            LEFT JOIN Temp.FirstTxDate
                ON FirstTxDate.PtMRN = Status.PtMRN AND FirstTxDate.StatusDate = Status.StatusDate
            LEFT JOIN Caisis.vdatasetpatients
                ON Status.PtMRN = vdatasetpatients.PtMRN
            LEFT JOIN (
                SELECT DISTINCT
                    PtMRN
                    , MedTxDate
                    , Categorized
                    , Intensity
                FROM ProtocolCategory.PatientProtocol ) as Protocol
                ON  FirstTxDate.PtMRN = Protocol.PtMRN AND FirstTxDate.MedTxDate = Protocol.MedTxDate
            LEFT JOIN  Caisis.v_secondary # v_secondary is a view in the caisis database
                ON v_secondary.PtMRN = FirstTxDate.PtMRN
            LEFT JOIN  Caisis.encounter
                ON encounter.PtMRN = Status.PtMRN and encounter.encdate = status.statusdate
            LEFT JOIN `relevantlab`.`labsummary`
                ON `labsummary`.`PtMRN` = FirstTxDate.PtMRN
                    AND `labsummary`.`ArrivalDate` = FirstTxDate.StatusDate
                    and `labsummary`.`TreatmentStartDate` = FirstTxDate.MedTxDate
            GROUP BY
                Status.PtMRN
                , Status.Status
                , Status.StatusDisease
                , Status.StatusDate
                , Protocol.MedTxDate
                , Protocol.Categorized
                , Protocol.Intensity
                , vdatasetpatients.PtBirthDate
            ORDER BY Status.PtMRN, Status.StatusDate, Protocol.MedTxDate;

    ALTER TABLE Temp.Temp2
        CHANGE COLUMN `calculated blasts and unclassified` `calculated blasts and unclassified` INT(4) NULL ;

    UPDATE Temp.Temp2
        SET `calculated blast` = round(`calculated blast`,2)
        ,   `calculated unclassified` = round(`calculated unclassified`,2)
        ,   `calculated blasts and unclassified` = NULL;

    UPDATE Temp.Temp2
        SET `calculated blasts and unclassified` = CASE
            WHEN `calculated blast` IS NULL
                    AND `calculated unclassified` IS NOT NULL
                THEN `calculated unclassified`
            WHEN `calculated blast` IS NOT NULL
                    AND `calculated unclassified` IS NULL
                THEN `calculated blast`
            WHEN `calculated blast` IS NOT NULL AND `calculated unclassified` IS NOT NULL
                    AND `arrival circulating blast date` = `arrival circulating unclassified date`
                    AND `calculated blast` + `calculated unclassified` > 100
                THEN `calculated blast`
            WHEN `calculated blast` IS NOT NULL AND `calculated unclassified` IS NOT NULL
                    AND `arrival circulating blast date` = `arrival circulating unclassified date`
                    AND `calculated blast` + `calculated unclassified` < 100
                THEN `calculated blast` + `calculated unclassified`
            ELSE `calculated blast`
        END ;

    DROP TABLE IF EXISTS Temp.ArrivalTRM;
    CREATE TABLE Temp.ArrivalTRM
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
        FROM Temp.Temp2 a
        WHERE StatusDisease LIKE '%AML%'
        AND (StatusDisease LIKE '%ND%'
        OR StatusDisease LIKE '%REL%'
        OR StatusDisease LIKE '%REF%') ;

    UPDATE Temp.ArrivalTRM SET TRMRange =
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

    DROP TABLE IF EXISTS Temp.TRMRangeOrder;
    CREATE TABLE Temp.TRMRangeOrder
        SELECT TRMRangeOrder
            , TRMRange
            FROM Temp.ArrivalTRM
            WHERE Categorized <> 'NOT TREATED'
            GROUP BY TRMRange
            ORDER BY TRMRangeOrder ;

    """
    dosqlexecute(cnxdict)


CreateFirstMedicalTherapyAfterArrival(cnxdict)
ArrivalTRM()
ArrivalTRM_GCLAM()

df = pd.read_sql("""
    SELECT CASE
                WHEN StatusDisease LIKE '%ND%' THEN 1
                WHEN StatusDisease LIKE '%REL%' THEN 3
                WHEN StatusDisease LIKE '%REF%' THEN 3
            END AS Display
           ,CASE
                WHEN StatusDisease LIKE '%ND%' THEN 'ND'
                WHEN StatusDisease LIKE '%REL%' THEN 'R/R'
                WHEN StatusDisease LIKE '%REF%' THEN 'R/R'
            END AS Type
            , StatusDisease
            , count(*) AS Total
            from temp.arrivaltrm
            WHERE StatusDisease LIKE '%AML%'
            AND (StatusDisease LIKE '%ND%'
            OR StatusDisease LIKE '%REL%'
            OR StatusDisease LIKE '%REF%')
            GROUP BY StatusDisease
    UNION SELECT  4 AS Display
            , 'R/R Total' AS  Type
            , 'All R/R'
            , count(*) from temp.arrivaltrm
            WHERE StatusDisease LIKE '%AML%'
            AND (StatusDisease LIKE '%REL%'
            OR StatusDisease LIKE '%REF%')
    UNION SELECT  2 AS Display
            , 'ND Total' AS Type
            , 'All ND'
            , count(*) from temp.arrivaltrm
            WHERE StatusDisease LIKE '%AML%'
            AND StatusDisease LIKE '%ND%'
    UNION SELECT 9 AS Display
            , 'Overall Total' AS  Type
            , 'All'
            , count(*) from temp.arrivaltrm
            WHERE StatusDisease LIKE '%AML%'
            AND (StatusDisease LIKE '%REL%'
            OR StatusDisease LIKE '%ND%'
            OR StatusDisease LIKE '%REF%')
    ORDER BY Display, Total DESC;
""", cnxdict['cnx'])
df.to_excel(writer, sheet_name='Arrivals Used', index=False)

df = pd.read_sql("""
        SELECT
            MedTxAgent, Categorized, Intensity, count(*)
        FROM
            ProtocolCategory.PatientProtocol
            WHERE Intensity like '%intermediate%'
            GROUP BY MedTxAgent, Categorized, Intensity
        UNION
        SELECT
            MedTxAgent, Categorized, Intensity, count(*)
        FROM
            ProtocolCategory.PatientProtocol
            WHERE Intensity like '%high%'
            GROUP BY MedTxAgent, Categorized, Intensity;
    """, cnxdict['cnx'])
df.to_excel(writer, sheet_name='Protocol Categories', index=False)

df = pd.read_sql("""
        SELECT
            Categorized, Intensity, count(*)
        FROM
            ProtocolCategory.PatientProtocol
            WHERE Intensity like '%intermediate%'
            GROUP BY Categorized, Intensity
        UNION
        SELECT
            Categorized, Intensity, count(*)
        FROM
            ProtocolCategory.PatientProtocol
            WHERE Intensity like '%high%'
            GROUP BY Categorized, Intensity;
    """, cnxdict['cnx'])
df.to_excel(writer, sheet_name='Category Intensity', index=False)

df = pd.read_sql("""
    SELECT a.TRMRange
        , 'All Patients Treated' AS Population
        , 'All Intensities'      AS `Treatment Intensity`
        , COUNT(*)               AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED') b
        ON a.TRMRange = b.TRMRange
        GROUP by a.TRMRangeOrder
    UNION
    SELECT a.TRMRange
        , 'ND Patients' AS Population
        , 'High'        AS `Treatment Intensity`
        , COUNT(*)      AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   StatusDisease LIKE '%ND%'
                    AND   Intensity IN ('High')) b
        ON a.TRMRange = b.TRMRange
        GROUP by a.TRMRangeOrder
    UNION
    SELECT a.TRMRange
        , 'ND Patients'  AS Population
        , 'Intermediate' AS `Treatment Intensity`
        , COUNT(*)       AS `Patient Arrivals`
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   StatusDisease LIKE '%ND%'
                    AND   Intensity IN ('Intermediate')) b
        ON a.TRMRange = b.TRMRange
        GROUP by a.TRMRangeOrder
    UNION
    SELECT a.TRMRange
        , 'ND Patients'          AS Population
        , 'High or Intermediate' AS `Treatment Intensity`
        , COUNT(*) AS Patients
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   StatusDisease LIKE '%ND%'
                    AND   Intensity IN ('High','Intermediate')) b
        ON a.TRMRange = b.TRMRange
        GROUP by a.TRMRangeOrder
    UNION
    SELECT a.TRMRange
        , 'ND Patients'  AS Population
        , 'Low'          AS `Treatment Intensity`
        , COUNT(*) AS Patients
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   StatusDisease LIKE '%ND%'
                    AND   Intensity IN ('Low')) b
        ON a.TRMRange = b.TRMRange
        GROUP by a.TRMRangeOrder
    UNION
    SELECT a.TRMRange
        , 'ND Patients'  AS Population
        , 'Other'        AS `Treatment Intensity`
        , COUNT(*) AS Patients
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   StatusDisease LIKE '%ND%'
                    AND   Intensity NOT IN ('Low','High','Intermediate')) b
        ON a.TRMRange = b.TRMRange
        GROUP by a.TRMRangeOrder
    UNION
    SELECT a.TRMRange
        , 'ND Patients' AS Population
        , 'All Intensities' AS `Treatment Intensity`
        , COUNT(*) AS Patients
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                    AND   StatusDisease LIKE '%ND%') b
        ON a.TRMRange = b.TRMRange
        GROUP by a.TRMRangeOrder
    UNION
    SELECT a.TRMRange
        , 'R/R Patients' AS Population
        , 'High' AS `Treatment Intensity`
        , COUNT(*) AS Patients
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                AND   (StatusDisease LIKE '%REL%' OR StatusDisease LIKE '%REF%')
                AND   Intensity IN ('High'))b
        ON a.TRMRange = b.TRMRange
        GROUP by a.TRMRangeOrder
    UNION
    SELECT a.TRMRange
        , 'R/R Patients' AS Population
        , ' Intermediate' AS `Treatment Intensity`
        , COUNT(*) AS Patients
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                AND   (StatusDisease LIKE '%REL%' OR StatusDisease LIKE '%REF%')
                AND   Intensity IN ('Intermediate'))b
        ON a.TRMRange = b.TRMRange
        GROUP by a.TRMRangeOrder
    UNION
    SELECT a.TRMRange
        , 'R/R Patients' AS Population
        , 'High or Intermediate' AS `Treatment Intensity`
        , COUNT(*) AS Patients
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                AND   (StatusDisease LIKE '%REL%' OR StatusDisease LIKE '%REF%')
                AND   Intensity IN ('High','Intermediate'))b
        ON a.TRMRange = b.TRMRange
        GROUP by a.TRMRangeOrder
    UNION
    SELECT a.TRMRange
        , 'R/R Patients' AS Population
        , 'Low' AS `Treatment Intensity`
        , COUNT(*) AS Patients
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                AND   (StatusDisease LIKE '%REL%' OR StatusDisease LIKE '%REF%')
                AND   Intensity IN ('Low'))b
        ON a.TRMRange = b.TRMRange
        GROUP by a.TRMRangeOrder
    UNION
    SELECT a.TRMRange
        , 'R/R Patients' AS Population
        , 'Other' AS `Treatment Intensity`
        , COUNT(*) AS Patients
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                AND   (StatusDisease LIKE '%REL%' OR StatusDisease LIKE '%REF%')
                AND   Intensity NOT IN ('Low','High','Intermediate'))b
        ON a.TRMRange = b.TRMRange
        GROUP by a.TRMRangeOrder
    UNION
    SELECT a.TRMRange
        , 'R/R Patients' AS Population
        , 'All Intensities' AS `Treatment Intensity`
        , COUNT(*) AS Patients
        , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
        , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
        , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
        FROM Temp.TRMRangeOrder a
        LEFT JOIN (
            SELECT * FROM Temp.ArrivalTRM
                WHERE Categorized <> 'NOT TREATED'
                AND   (StatusDisease LIKE '%REL%' OR StatusDisease LIKE '%REF%'))b
        ON a.TRMRange = b.TRMRange
        GROUP by a.TRMRangeOrder;
""", cnxdict['cnx'])
df.to_excel(writer, sheet_name='TRM for All Arrivals', index=False)

df = pd.read_sql("""
    # GCLAM ND
        SELECT
        a.TRMRange,
        'GCLAM ND Patients' AS Population,
        'High' AS `Treatment Intensity`,
        COUNT(*) AS `Patient Arrivals`,
        SUM(IF(ResponseDescription = 'Death', 1, 0)) AS `Deaths Observed`,
        CONCAT(CAST(SUM(IF(ResponseDescription = 'Death', 1, 0))
                    AS CHARACTER (5)),
                '/',
                COUNT(*),
                ' (',
                ROUND(100 * SUM(IF(ResponseDescription = 'Death', 1, 0)) / COUNT(*),
                        1),
                '%)') AS `Death/Arrivals`,
        ROUND(SUM(`TRM_Version1 (Paper)`) / 100, 2) AS `Deaths Expected (Paper Algorithm)`,
        ROUND(SUM(`TRM_Version2 (Online)`) / 100, 2) AS `Deaths Expected (Online Calculator)`
    FROM
        Temp.TRMRangeOrder a
            LEFT JOIN
        (SELECT
            *
        FROM
            Temp.ArrivalTRM_GCLAM
        WHERE
            Categorized <> 'NOT TREATED'
                AND StatusDisease LIKE '%ND%'
                AND Intensity IN ('High')) b ON a.TRMRange = b.TRMRange
    WHERE
        a.TRMRangeOrder IN (1 , 2, 3)
    GROUP BY a.TRMRangeOrder
    UNION SELECT
        '<13.1' AS TRMRange,
        'GCLAM ND Patients' AS Population,
        'High' AS `Treatment Intensity`,
        COUNT(*) AS `Patient Arrivals`,
        SUM(IF(ResponseDescription = 'Death', 1, 0)) AS `Deaths Observed`,
        CONCAT(CAST(SUM(IF(ResponseDescription = 'Death', 1, 0))
                    AS CHARACTER (5)),
                '/',
                COUNT(*),
                ' (',
                ROUND(100 * SUM(IF(ResponseDescription = 'Death', 1, 0)) / COUNT(*),
                        1),
                '%)') AS `Death/Arrivals`,
        ROUND(SUM(`TRM_Version1 (Paper)`) / 100, 2) AS `Deaths Expected (Paper Algorithm)`,
        ROUND(SUM(`TRM_Version2 (Online)`) / 100, 2) AS `Deaths Expected (Online Calculator)`
    FROM
        Temp.TRMRangeOrder a
            LEFT JOIN
        (SELECT
            *
        FROM
            Temp.ArrivalTRM_GCLAM
        WHERE
            Categorized <> 'NOT TREATED'
                AND StatusDisease LIKE '%ND%'
                AND Intensity IN ('High')) b ON a.TRMRange = b.TRMRange
    WHERE
        a.TRMRangeOrder IN (1 , 2, 3)
    UNION SELECT
        a.TRMRange,
        'GCLAM ND Patients' AS Population,
        'High' AS `Treatment Intensity`,
        COUNT(*) AS `Patient Arrivals`,
        SUM(IF(ResponseDescription = 'Death', 1, 0)) AS `Deaths Observed`,
        CONCAT(CAST(SUM(IF(ResponseDescription = 'Death', 1, 0))
                    AS CHARACTER (5)),
                '/',
                COUNT(*),
                ' (',
                ROUND(100 * SUM(IF(ResponseDescription = 'Death', 1, 0)) / COUNT(*),
                        1),
                '%)') AS `Death/Arrivals`,
        ROUND(SUM(`TRM_Version1 (Paper)`) / 100, 2) AS `Deaths Expected (Paper Algorithm)`,
        ROUND(SUM(`TRM_Version2 (Online)`) / 100, 2) AS `Deaths Expected (Online Calculator)`
    FROM
        Temp.TRMRangeOrder a
            LEFT JOIN
        (SELECT
            *
        FROM
            Temp.ArrivalTRM_GCLAM
        WHERE
            Categorized <> 'NOT TREATED'
                AND StatusDisease LIKE '%ND%'
                AND Intensity IN ('High')) b ON a.TRMRange = b.TRMRange
    WHERE
        a.TRMRangeOrder IN (4 , 5, 6, 7)
    GROUP BY a.TRMRangeOrder
    UNION SELECT
        '>=13.1' AS TRMRange,
        'GCLAM ND Patients' AS Population,
        'High' AS `Treatment Intensity`,
        COUNT(*) AS `Patient Arrivals`,
        SUM(IF(ResponseDescription = 'Death', 1, 0)) AS `Deaths Observed`,
        CONCAT(CAST(SUM(IF(ResponseDescription = 'Death', 1, 0))
                    AS CHARACTER (5)),
                '/',
                COUNT(*),
                ' (',
                ROUND(100 * SUM(IF(ResponseDescription = 'Death', 1, 0)) / COUNT(*),
                        1),
                '%)') AS `Death/Arrivals`,
        ROUND(SUM(`TRM_Version1 (Paper)`) / 100, 2) AS `Deaths Expected (Paper Algorithm)`,
        ROUND(SUM(`TRM_Version2 (Online)`) / 100, 2) AS `Deaths Expected (Online Calculator)`
    FROM
        Temp.TRMRangeOrder a
            LEFT JOIN
        (SELECT
            *
        FROM
            Temp.ArrivalTRM_GCLAM
        WHERE
            Categorized <> 'NOT TREATED'
                AND StatusDisease LIKE '%ND%'
                AND Intensity IN ('High')) b ON a.TRMRange = b.TRMRange
    WHERE
        a.TRMRangeOrder IN (4 , 5, 6, 7)
    UNION

    # R/R GCLAM
    SELECT
        a.TRMRange,
        'GCLAM R/R Patients' AS Population,
        'High' AS `Treatment Intensity`,
        COUNT(*) AS `Patient Arrivals`,
        SUM(IF(ResponseDescription = 'Death', 1, 0)) AS `Deaths Observed`,
        CONCAT(CAST(SUM(IF(ResponseDescription = 'Death', 1, 0))
                    AS CHARACTER (5)),
                '/',
                COUNT(*),
                ' (',
                ROUND(100 * SUM(IF(ResponseDescription = 'Death', 1, 0)) / COUNT(*),
                        1),
                '%)') AS `Death/Arrivals`,
        ROUND(SUM(`TRM_Version1 (Paper)`) / 100, 2) AS `Deaths Expected (Paper Algorithm)`,
        ROUND(SUM(`TRM_Version2 (Online)`) / 100, 2) AS `Deaths Expected (Online Calculator)`
    FROM
        Temp.TRMRangeOrder a
            LEFT JOIN
        (SELECT
            *
        FROM
            Temp.ArrivalTRM_GCLAM
        WHERE
            Categorized <> 'NOT TREATED'
                AND (StatusDisease LIKE '%REL%'
                OR StatusDisease LIKE '%REF%')
                AND Intensity IN ('High')) b ON a.TRMRange = b.TRMRange
    WHERE
        a.TRMRangeOrder IN (1 , 2, 3)
    GROUP BY a.TRMRangeOrder
    UNION SELECT
        '<13.1' AS TRMRange,
        'GCLAM R/R Patients' AS Population,
        'High' AS `Treatment Intensity`,
        COUNT(*) AS `Patient Arrivals`,
        SUM(IF(ResponseDescription = 'Death', 1, 0)) AS `Deaths Observed`,
        CONCAT(CAST(SUM(IF(ResponseDescription = 'Death', 1, 0))
                    AS CHARACTER (5)),
                '/',
                COUNT(*),
                ' (',
                ROUND(100 * SUM(IF(ResponseDescription = 'Death', 1, 0)) / COUNT(*),
                        1),
                '%)') AS `Death/Arrivals`,
        ROUND(SUM(`TRM_Version1 (Paper)`) / 100, 2) AS `Deaths Expected (Paper Algorithm)`,
        ROUND(SUM(`TRM_Version2 (Online)`) / 100, 2) AS `Deaths Expected (Online Calculator)`
    FROM
        Temp.TRMRangeOrder a
            LEFT JOIN
        (SELECT
            *
        FROM
            Temp.ArrivalTRM_GCLAM
        WHERE
            Categorized <> 'NOT TREATED'
                AND (StatusDisease LIKE '%REL%'
                OR StatusDisease LIKE '%REF%')
                AND Intensity IN ('High')) b ON a.TRMRange = b.TRMRange
    WHERE
        a.TRMRangeOrder IN (1 , 2, 3)
    UNION SELECT
        a.TRMRange,
        'GCLAM R/R Patients' AS Population,
        'High' AS `Treatment Intensity`,
        COUNT(*) AS `Patient Arrivals`,
        SUM(IF(ResponseDescription = 'Death', 1, 0)) AS `Deaths Observed`,
        CONCAT(CAST(SUM(IF(ResponseDescription = 'Death', 1, 0))
                    AS CHARACTER (5)),
                '/',
                COUNT(*),
                ' (',
                ROUND(100 * SUM(IF(ResponseDescription = 'Death', 1, 0)) / COUNT(*),
                        1),
                '%)') AS `Death/Arrivals`,
        ROUND(SUM(`TRM_Version1 (Paper)`) / 100, 2) AS `Deaths Expected (Paper Algorithm)`,
        ROUND(SUM(`TRM_Version2 (Online)`) / 100, 2) AS `Deaths Expected (Online Calculator)`
    FROM
        Temp.TRMRangeOrder a
            LEFT JOIN
        (SELECT
            *
        FROM
            Temp.ArrivalTRM_GCLAM
        WHERE
            Categorized <> 'NOT TREATED'
                AND (StatusDisease LIKE '%REL%'
                OR StatusDisease LIKE '%REF%')
                AND Intensity IN ('High')) b ON a.TRMRange = b.TRMRange
    WHERE
        a.TRMRangeOrder IN (4 , 5, 6, 7)
    GROUP BY a.TRMRangeOrder
    UNION SELECT
        '>=13.1' AS TRMRange,
        'GCLAM R/R Patients' AS Population,
        'High' AS `Treatment Intensity`,
        COUNT(*) AS `Patient Arrivals`,
        SUM(IF(ResponseDescription = 'Death', 1, 0)) AS `Deaths Observed`,
        CONCAT(CAST(SUM(IF(ResponseDescription = 'Death', 1, 0))
                    AS CHARACTER (5)),
                '/',
                COUNT(*),
                ' (',
                ROUND(100 * SUM(IF(ResponseDescription = 'Death', 1, 0)) / COUNT(*),
                        1),
                '%)') AS `Death/Arrivals`,
        ROUND(SUM(`TRM_Version1 (Paper)`) / 100, 2) AS `Deaths Expected (Paper Algorithm)`,
        ROUND(SUM(`TRM_Version2 (Online)`) / 100, 2) AS `Deaths Expected (Online Calculator)`
    FROM
        Temp.TRMRangeOrder a
            LEFT JOIN
        (SELECT
            *
        FROM
            Temp.ArrivalTRM_GCLAM
        WHERE
            Categorized <> 'NOT TREATED'
                AND (StatusDisease LIKE '%REL%'
                OR StatusDisease LIKE '%REF%')
                AND Intensity IN ('High')) b ON a.TRMRange = b.TRMRange
    WHERE
        a.TRMRangeOrder IN (4 , 5, 6, 7)

    UNION

    # ND High
        SELECT a.TRMRange
            , 'ND Patients' AS Population
            , 'High'        AS `Treatment Intensity`
            , COUNT(*)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , concat(
                cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
                ,'/'
                ,count(*)
                , ' ('
                , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
                , '%)'
                ) AS `Death/Arrivals`
            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   StatusDisease LIKE '%ND%'
                        AND   Intensity IN ('High')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder in (1,2,3)
            GROUP by a.TRMRangeOrder
        UNION
        SELECT '<13.1' AS TRMRange
            , 'ND Patients' AS Population
            , 'High'        AS `Treatment Intensity`
            , COUNT(*)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , concat(
                cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
                ,'/'
                ,count(*)
                , ' ('
                , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
                , '%)'
                ) AS `Death/Arrivals`
            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   StatusDisease LIKE '%ND%'
                        AND   Intensity IN ('High')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder in (1,2,3)
        UNION
        SELECT a.TRMRange
            , 'ND Patients' AS Population
            , 'High'        AS `Treatment Intensity`
            , COUNT(*)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , concat(
                cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
                ,'/'
                ,count(*)
                , ' ('
                , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
                , '%)'
                ) AS `Death/Arrivals`
            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   StatusDisease LIKE '%ND%'
                        AND   Intensity IN ('High')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder in (4,5,6,7)
            GROUP by a.TRMRangeOrder
        UNION
        SELECT '>=13.1' AS TRMRange
            , 'ND Patients' AS Population
            , 'High'        AS `Treatment Intensity`
            , COUNT(*)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , concat(
                cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
                ,'/'
                ,count(*)
                , ' ('
                , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
                , '%)'
                ) AS `Death/Arrivals`

            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   StatusDisease LIKE '%ND%'
                        AND   Intensity IN ('High')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder in (4,5,6,7)
        UNION

    # ND intermediate
        SELECT a.TRMRange
            , 'ND Patients' AS Population
            , 'Intermediate'        AS `Treatment Intensity`
            , COUNT(*)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , concat(
                cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
                ,'/'
                ,count(*)
                , ' ('
                , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
                , '%)'
                ) AS `Death/Arrivals`

            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   StatusDisease LIKE '%ND%'
                        AND   Intensity IN ('Intermediate')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder in (1,2,3)
            GROUP by a.TRMRangeOrder
        UNION
        SELECT '<13.1' AS TRMRange
            , 'ND Patients' AS Population
            , 'Intermediate'        AS `Treatment Intensity`
            , COUNT(*)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , concat(
                cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
                ,'/'
                ,count(*)
                , ' ('
                , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
                , '%)'
                ) AS `Death/Arrivals`

            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   StatusDisease LIKE '%ND%'
                        AND   Intensity IN ('Intermediate')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder in (1,2,3)
        UNION
        SELECT a.TRMRange
            , 'ND Patients' AS Population
            , 'Intermediate'        AS `Treatment Intensity`
            , COUNT(*)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , concat(
                cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
                ,'/'
                ,count(*)
                , ' ('
                , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
                , '%)'
                ) AS `Death/Arrivals`

            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   StatusDisease LIKE '%ND%'
                        AND   Intensity IN ('Intermediate')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder in (4,5,6,7)
            GROUP by a.TRMRangeOrder
        UNION
        SELECT '>=13.1' AS TRMRange
            , 'ND Patients' AS Population
            , 'Intermediate'        AS `Treatment Intensity`
            , COUNT(*)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , concat(
                cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
                ,'/'
                ,count(*)
                , ' ('
                , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
                , '%)'
                ) AS `Death/Arrivals`

            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   StatusDisease LIKE '%ND%'
                        AND   Intensity IN ('Intermediate')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder in (4,5,6,7)
        UNION
    # ND low
        SELECT a.TRMRange
            , 'ND Patients' AS Population
            , 'Low'        AS `Treatment Intensity`
            , COUNT(*)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , concat(
                cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
                ,'/'
                ,count(*)
                , ' ('
                , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
                , '%)'
                ) AS `Death/Arrivals`

            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   StatusDisease LIKE '%ND%'
                        AND   Intensity IN ('Low')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder in (1,2,3)
            GROUP by a.TRMRangeOrder
        UNION
        SELECT '<13.1' AS TRMRange
            , 'ND Patients' AS Population
            , 'Low'        AS `Treatment Intensity`
            , COUNT(*)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , concat(
                cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
                ,'/'
                ,count(*)
                , ' ('
                , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
                , '%)'
                ) AS `Death/Arrivals`

            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   StatusDisease LIKE '%ND%'
                        AND   Intensity IN ('Low')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder in (1,2,3)
        UNION
        SELECT a.TRMRange
            , 'ND Patients' AS Population
            , 'Low'        AS `Treatment Intensity`
            , COUNT(*)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , concat(
                cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
                ,'/'
                ,count(*)
                , ' ('
                , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
                , '%)'
                ) AS `Death/Arrivals`

            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   StatusDisease LIKE '%ND%'
                        AND   Intensity IN ('Low')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder in (4,5,6,7)
            GROUP by a.TRMRangeOrder
        UNION
        SELECT '>=13.1' AS TRMRange
            , 'ND Patients' AS Population
            , 'Low'        AS `Treatment Intensity`
            , COUNT(*)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , concat(
                cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
                ,'/'
                ,count(*)
                , ' ('
                , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
                , '%)'
                ) AS `Death/Arrivals`

            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   StatusDisease LIKE '%ND%'
                        AND   Intensity IN ('Low')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder in (4,5,6,7)
    union

    # R/R High
        SELECT a.TRMRange
            , 'R/R Patients' AS Population
            , 'High'        AS `Treatment Intensity`
            , COUNT(*)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , concat(
                cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
                ,'/'
                ,count(*)
                , ' ('
                , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
                , '%)'
                ) AS `Death/Arrivals`

            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   (StatusDisease LIKE '%REL%' or StatusDisease LIKE '%REF%')
                        AND   Intensity IN ('High')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder in (1,2,3)
            GROUP by a.TRMRangeOrder
        UNION
        SELECT '<13.1' AS TRMRange
            , 'R/R Patients' AS Population
            , 'High'        AS `Treatment Intensity`
            , COUNT(*)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , concat(
                cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
                ,'/'
                ,count(*)
                , ' ('
                , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
                , '%)'
                ) AS `Death/Arrivals`

            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   (StatusDisease LIKE '%REL%' or StatusDisease LIKE '%REF%')
                        AND   Intensity IN ('High')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder in (1,2,3)
        UNION
        SELECT a.TRMRange
            , 'R/R Patients' AS Population
            , 'High'        AS `Treatment Intensity`
            , COUNT(*)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , concat(
                cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
                ,'/'
                ,count(*)
                , ' ('
                , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
                , '%)'
                ) AS `Death/Arrivals`

            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   (StatusDisease LIKE '%REL%' or StatusDisease LIKE '%REF%')
                        AND   Intensity IN ('High')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder in (4,5,6,7)
            GROUP by a.TRMRangeOrder
        UNION
        SELECT '>=13.1' AS TRMRange
            , 'R/R Patients' AS Population
            , 'High'        AS `Treatment Intensity`
            , COUNT(*)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , concat(
                cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
                ,'/'
                ,count(*)
                , ' ('
                , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
                , '%)'
                ) AS `Death/Arrivals`

            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   (StatusDisease LIKE '%REL%' or StatusDisease LIKE '%REF%')
                        AND   Intensity IN ('High')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder in (4,5,6,7)

    union

    # R/R Intermediate
        SELECT a.TRMRange
            , 'R/R Patients' AS Population
            , 'Intermediate'        AS `Treatment Intensity`
            , COUNT(*)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , concat(
                cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
                ,'/'
                ,count(*)
                , ' ('
                , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
                , '%)'
                ) AS `Death/Arrivals`

            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   (StatusDisease LIKE '%REL%' or StatusDisease LIKE '%REF%')
                        AND   Intensity IN ('Intermediate')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder in (1,2,3)
            GROUP by a.TRMRangeOrder
        UNION
        SELECT '<13.1' AS TRMRange
            , 'R/R Patients' AS Population
            , 'Intermediate'        AS `Treatment Intensity`
            , COUNT(*)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , concat(
                cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
                ,'/'
                ,count(*)
                , ' ('
                , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
                , '%)'
                ) AS `Death/Arrivals`

            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   (StatusDisease LIKE '%REL%' or StatusDisease LIKE '%REF%')
                        AND   Intensity IN ('Intermediate')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder in (1,2,3)
        UNION
        SELECT a.TRMRange
            , 'R/R Patients' AS Population
            , 'Intermediate'        AS `Treatment Intensity`
            , COUNT(*)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , concat(
                cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
                ,'/'
                ,count(*)
                , ' ('
                , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
                , '%)'
                ) AS `Death/Arrivals`

            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   (StatusDisease LIKE '%REL%' or StatusDisease LIKE '%REF%')
                        AND   Intensity IN ('Intermediate')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder in (4,5,6,7)
            GROUP by a.TRMRangeOrder
        UNION
        SELECT '>=13.1' AS TRMRange
            , 'R/R Patients' AS Population
            , 'Intermediate'        AS `Treatment Intensity`
            , COUNT(*)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , concat(
                cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
                ,'/'
                ,count(*)
                , ' ('
                , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
                , '%)'
                ) AS `Death/Arrivals`

            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   (StatusDisease LIKE '%REL%' or StatusDisease LIKE '%REF%')
                        AND   Intensity IN ('Intermediate')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder in (4,5,6,7)

    union

    # R/R Low
        SELECT a.TRMRange
            , 'R/R Patients' AS Population
            , 'Low'        AS `Treatment Intensity`
            , COUNT(*)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , concat(
                cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
                ,'/'
                ,count(*)
                , ' ('
                , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
                , '%)'
                ) AS `Death/Arrivals`

            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   (StatusDisease LIKE '%REL%' or StatusDisease LIKE '%REF%')
                        AND   Intensity IN ('Low')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder in (1,2,3)
            GROUP by a.TRMRangeOrder
        UNION
        SELECT '<13.1' AS TRMRange
            , 'R/R Patients' AS Population
            , 'Low'        AS `Treatment Intensity`
            , COUNT(*)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , concat(
                cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
                ,'/'
                ,count(*)
                , ' ('
                , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
                , '%)'
                ) AS `Death/Arrivals`

            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   (StatusDisease LIKE '%REL%' or StatusDisease LIKE '%REF%')
                        AND   Intensity IN ('Low')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder in (1,2,3)
        UNION
        SELECT a.TRMRange
            , 'R/R Patients' AS Population
            , 'Low'        AS `Treatment Intensity`
            , COUNT(*)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , concat(
                cast(SUM(IF(ResponseDescription='Death',1,0)) as character(5))
                ,'/'
                ,count(*)
                , ' ('
                , round(100*SUM(IF(ResponseDescription='Death',1,0))/count(*),1)
                , '%)'
                ) AS `Death/Arrivals`

            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   (StatusDisease LIKE '%REL%' or StatusDisease LIKE '%REF%')
                        AND   Intensity IN ('Low')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder in (4,5,6,7)
            GROUP by a.TRMRangeOrder
        UNION
        SELECT '>=13.1' AS TRMRange
            , 'R/R Patients' AS Population
            , 'Low'        AS `Treatment Intensity`
            , COUNT(*)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , concat(SUM(IF(ResponseDescription='Death',1,0)),'/',count(*)) AS `Death/Arrivals`
            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   (StatusDisease LIKE '%REL%' or StatusDisease LIKE '%REF%')
                        AND   Intensity IN ('Low')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder in (4,5,6,7)
    ;
""", cnxdict['cnx'])
df.to_excel(writer, sheet_name='HIL TRM for All Arrivals', index=False)

df = pd.read_sql("""
    SELECT a.TRMRange
            , 'ND Patients' AS Population
            , 'High'        AS `Treatment Intensity`
            , SUM(CASE
                WHEN b.TRMRange IS NOT NULL THEN 1
                ELSE 0
            END)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM_GCLAM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   StatusDisease LIKE '%ND%'
                        AND   Intensity IN ('High')) b
            ON a.TRMRange = b.TRMRange
            GROUP by a.TRMRangeOrder
        UNION
        SELECT CASE
                WHEN a.TRMRangeOrder in (1,2,3) THEN '<13.1'
                WHEN a.TRMRangeOrder in (4,5,6,7) THEN '>=13.1'
                ELSE 'Unknown'
            END AS TRMRange
            , 'ND Patients' AS Population
            , 'High'        AS `Treatment Intensity`
            , SUM(CASE
                WHEN b.TRMRange IS NOT NULL THEN 1
                ELSE 0
            END)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM_GCLAM
                    WHERE Categorized <> 'NOT TREATED'
                        AND   StatusDisease LIKE '%ND%'
                        AND   Intensity IN ('High')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder <> 0
            GROUP by CASE
                WHEN a.TRMRangeOrder in (1,2,3) THEN '<13.1'
                WHEN a.TRMRangeOrder in (4,5,6,7) THEN '>=13.1'
            END
        UNION
        SELECT a.TRMRange
            , 'R/R Patients' AS Population
            , 'High' AS `Treatment Intensity`
            , SUM(CASE
                WHEN b.TRMRange IS NOT NULL THEN 1
                ELSE 0
            END)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM_GCLAM
                    WHERE Categorized <> 'NOT TREATED'
                    AND   (StatusDisease LIKE '%REL%' OR StatusDisease LIKE '%REF%')
                    AND   Intensity IN ('High'))b
            ON a.TRMRange = b.TRMRange
            GROUP by a.TRMRangeOrder
        UNION
        SELECT CASE
                WHEN a.TRMRangeOrder in (1,2,3) THEN '<13.1'
                WHEN a.TRMRangeOrder in (4,5,6,7) THEN '>=13.1'
                ELSE 'Unknown'
            END AS TRMRange
            , 'R/R Patients' AS Population
            , 'High'        AS `Treatment Intensity`
            , SUM(CASE
                WHEN b.TRMRange IS NOT NULL THEN 1
                ELSE 0
            END)      AS `Patient Arrivals`
            , SUM(IF(ResponseDescription='Death',1,0)) AS `Deaths Observed`
            , ROUND(SUM(`TRM_Version1 (Paper)`)/100,2) AS `Deaths Expected (Paper Algorithm)`
            , ROUND(SUM(`TRM_Version2 (Online)`)/100,2) AS `Deaths Expected (Online Calculator)`
            FROM Temp.TRMRangeOrder a
            LEFT JOIN (
                SELECT * FROM Temp.ArrivalTRM_GCLAM
                    WHERE Categorized <> 'NOT TREATED'
                    AND   (StatusDisease LIKE '%REL%' OR StatusDisease LIKE '%REF%')
                        AND   Intensity IN ('High')) b
            ON a.TRMRange = b.TRMRange
            WHERE a.TRMRangeOrder <> 0
            GROUP by CASE
                WHEN a.TRMRangeOrder in (1,2,3) THEN '<13.1'
                WHEN a.TRMRangeOrder in (4,5,6,7) THEN '>=13.1'
            END ;
""", cnxdict['cnx'])
df.to_excel(writer, sheet_name='TRM for GCLAM Arrivals', index=False)

df = pd.read_sql("""
    SELECT `arrivaltrm`.`PtMRN`,
        `arrivaltrm`.`StatusDate`,
        `arrivaltrm`.`StatusDisease`,
        `arrivaltrm`.`MedTxDate`,
        `arrivaltrm`.`Protocol`,
        `arrivaltrm`.`WildCard`,
        `arrivaltrm`.`Categorized` AS `Protocol Category`,
        `arrivaltrm`.`Intensity`,
        `arrivaltrm`.`ResponseDescription`,
        `arrivaltrm`.`ResponseDate`,
        `arrivaltrm`.`TRM_Version1 (Paper)`,
        `arrivaltrm`.`TRM_Version2 (Online)`,
        `arrivaltrm`.`TRMRange`,
        `arrivaltrm`.`ECOG`,
        `arrivaltrm`.`AgeAtTreatmentStart`,
        `arrivaltrm`.`calcuated_platelet`,
        `arrivaltrm`.`calcuated_albumin`,
        `arrivaltrm`.`secondary`,
        `arrivaltrm`.`calcuated_wbc`,
        `arrivaltrm`.`calculated blasts and unclassified`,
        `arrivaltrm`.`calcuated_creatinine`,
        `arrivaltrm`.`calculated blast`,
        `arrivaltrm`.`calculated unclassified`,
        `arrivaltrm`.`Not Treated`,
        `arrivaltrm`.`Protocol Unknown`,
        `arrivaltrm`.`ECOG Missing`,
        `arrivaltrm`.`Age Missing`,
        `arrivaltrm`.`Platelet Missing`,
        `arrivaltrm`.`Secondary Missing`,
        `arrivaltrm`.`WBC Missing`,
        `arrivaltrm`.`Circulating Blasts Missing`,
        `arrivaltrm`.`Creatinine Missing`
    FROM `temp`.`arrivaltrm`;
""", cnxdict['cnx'])
df.to_excel(writer, sheet_name='Detail for all arrivals', index=False)
dowritersave(writer,cnxdict)

sheet_name='Detail for all arrivals'
print(len(sheet_name))

