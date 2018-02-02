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
        SELECT Status.PtMRN, status.PatientId, Status.StatusDate, MIN(Protocol.MedTxDate) AS MedTxDate
            FROM (
                SELECT DISTINCT
                    PtMRN
                    , PatientId
                    , StatusDate
                    , statusDisease
                    , Status
                    FROM Caisis.vdatasetstatus
                        WHERE status like '%arrival%' ) as Status
            LEFT JOIN (
                SELECT DISTINCT
                    PtMRN
                    , PatientId
                    , MedTxDate
                    , MedTxAgent
                    , Categorized
                    , Intensity
                    , Wildcard
                FROM ProtocolCategory.PatientProtocol ) as Protocol
            ON Protocol.ptmrn = status.PtMRN AND date_add(Status.StatusDate, INTERVAl -3 DAY) and Protocol.MedTxDate
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
            , Status.PatientId
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
                    , PatientId
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
                    , PatientId
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
                , Status.PatientId
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
            , a.`PatientId`
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


def ArrivalTRM_ForYlinne(cnxdict):

    cnxdict['sql'] = """
    DROP TABLE IF EXISTS Temp.Temp2 ;
    CREATE TABLE Temp.Temp2
    SELECT
            Status.PtMRN
            , Status.PatientId
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
            , Status.StatusDate AS `ArrivalDate`
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
                    , PatientId
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
                    , PatientId
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
            , a.`PatientId`
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
            , a.`StatusDate` as `ArrivalDate`
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

    DROP TABLE IF EXISTS caisis.ArrivalTRM;
    CREATE TABLE caisis.ArrivalTRM SELECT * FROM temp.ArrivalTRM;

    DELETE FROM temp.ArrivalTRM
        WHERE PtMRN NOT IN  ('U0458051'
          , 'U0698876'
          , 'U0800727'
          , 'U0817252'
          , 'U0868519'
          , 'U0974020'
          , 'U1173533'
          , 'U1178074'
          , 'U1188252'
          , 'U2028987'
          , 'U2052524'
          , 'U2060155'
          , 'U2115037'
          , 'U2156325'
          , 'U2179018'
          , 'U2189274'
          , 'U2218915'
          , 'U2227603'
          , 'U2246550'
          , 'U2255610'
          , 'U2271406'
          , 'U2272138'
          , 'U2274859'
          , 'U2281530'
          , 'U2297253'
          , 'U2322661'
          , 'U2333780'
          , 'U2351000'
          , 'U2368130'
          , 'U2368733'
          , 'U2375147'
          , 'U2378203'
          , 'U2409251'
          , 'U2416366'
          , 'U2423797'
          , 'U2429269'
          , 'U2437442'
          , 'U2460956'
          , 'U2477732'
          , 'U2501601'
          , 'U2509306'
          , 'U2515202'
          , 'U2521929'
          , 'U2524799'
          , 'U2537293'
          , 'U2541303'
          , 'U2542168'
          , 'U2542179'
          , 'U2554796'
          , 'U2560984'
          , 'U2562783'
          , 'U2563991'
          , 'U2564956'
          , 'U2573622'
          , 'U2585877'
          , 'U2585885'
          , 'U2592438'
          , 'U2596143'
          , 'U2605660'
          , 'U2605671'
          , 'U2605676'
          , 'U2616497'
          , 'U2625293'
          , 'U2626037'
          , 'U2628588'
          , 'U2629586'
          , 'U2631335'
          , 'U2631363'
          , 'U2631376'
          , 'U2631381'
          , 'U2632910'
          , 'U2634120'
          , 'U2635020'
          , 'U2635052'
          , 'U2635054'
          , 'U2637232'
          , 'U2639545'
          , 'U2640007'
          , 'U2640592'
          , 'U2645024'
          , 'U2650398'
          , 'U2650592'
          , 'U2651732'
          , 'U2651748'
          , 'U2651753'
          , 'U2651754'
          , 'U2651761'
          , 'U2655501'
          , 'U2658001'
          , 'U2658020'
          , 'U2658411'
          , 'U2664548'
          , 'U2664582'
          , 'U2664623'
          , 'U2664688'
          , 'U2668544'
          , 'U2668577'
          , 'U2670311'
          , 'U2670336'
          , 'U2671604'
          , 'U2671607'
          , 'U2671610'
          , 'U2671623'
          , 'U2671627'
          , 'U2671632'
          , 'U2671636'
          , 'U2671655'
          , 'U2671682'
          , 'U2671685'
          , 'U2671694'
          , 'U2675206'
          , 'U2675211'
          , 'U2675257'
          , 'U2675278'
          , 'U2678483'
          , 'U2679581'
          , 'U2684923'
          , 'U2684945'
          , 'U2698159'
          , 'U2699179'
          , 'U2699558'
          , 'U2699581'
          , 'U2711839'
          , 'U2711882'
          , 'U2711899'
          , 'U2714047'
          , 'U2715489'
          , 'U2717406'
          , 'U2717414'
          , 'U2717448'
          , 'U2717457'
          , 'U2717476'
          , 'U2717486'
          , 'U2717488'
          , 'U2721001'
          , 'U2721038'
          , 'U2721051'
          , 'U2721064'
          , 'U2725042'
          , 'U2728171'
          , 'U2733006'
          , 'U2735631'
          , 'U2741321'
          , 'U2746826'
          , 'U2746848'
          , 'U2746917'
          , 'U2747223'
          , 'U2750269'
          , 'U2750705'
          , 'U2753312'
          , 'U2753646'
          , 'U2753658'
          , 'U2753693'
          , 'U2754446'
          , 'U3004473'
          , 'U3007126'
          , 'U3013166'
          , 'U3014311'
          , 'U3016792'
          , 'U3024128'
          , 'U3025641'
          , 'U3026144'
          , 'U3027206'
          , 'U3030021'
          , 'U3031791'
          , 'U3032654'
          , 'U3033236'
          , 'U3033958'
          , 'U3037958'
          , 'U3039637'
          , 'U3040125'
          , 'U3043009'
          , 'U3044373'
          , 'U3045328'
          , 'U3045919'
          , 'U3048199'
          , 'U3048812'
          , 'U3051421'
          , 'U3051884'
          , 'U3052216'
          , 'U3059704'
          , 'U3061274'
          , 'U3063068'
          , 'U3068173'
          , 'U3072894'
          , 'U3072946'
          , 'U3075896'
          , 'U3076632'
          , 'U3085351'
          , 'U3089384'
          , 'U3089639'
          , 'U3090110'
          , 'U3091473'
          , 'U3091817'
          , 'U3091834'
          , 'U3092541'
          , 'U3092776'
          , 'U3096314'
          , 'U3098492'
          , 'U3101501'
          , 'U3106238'
          , 'U3107391'
          , 'U3111618'
          , 'U3113556'
          , 'U3116655'
          , 'U3117040'
          , 'U3118241'
          , 'U3119824'
          , 'U3121512'
          , 'U3121979'
          , 'U3122274'
          , 'U3123942'
          , 'U3125229'
          , 'U3125902'
          , 'U3127372'
          , 'U3127798'
          , 'U3129795'
          , 'U3132054'
          , 'U3132081'
          , 'U3134414'
          , 'U3135461'
          , 'U3136036'
          , 'U3137973'
          , 'U3141183'
          , 'U3144840'
          , 'U3147629'
          , 'U3149742'
          , 'U3152537'
          , 'U3153806'
          , 'U3154122'
          , 'U3155719'
          , 'U3156816'
          , 'U3158505'
          , 'U3161339'
          , 'U3162875'
          , 'U3167385'
          , 'U3168948'
          , 'U3169948'
          , 'U3172758'
          , 'U3173405'
          , 'U3174421'
          , 'U3177007'
          , 'U3177572'
          , 'U3177630'
          , 'U3177774'
          , 'U3178079'
          , 'U3179142'
          , 'U3180181'
          , 'U3181897'
          , 'U3182898'
          , 'U3183636'
          , 'U3184744'
          , 'U3184789'
          , 'U3184884'
          , 'U3186519'
          , 'U3189365'
          , 'U3191827'
          , 'U3200761'
          , 'U3201388'
          , 'U3201789'
          , 'U3202324'
          , 'U3205384'
          , 'U3206393'
          , 'U3208343'
          , 'U3208941'
          , 'U3209685'
          , 'U3210227'
          , 'U3211055'
          , 'U3211981'
          , 'U3212548'
          , 'U3212757'
          , 'U3214954'
          , 'U3215290'
          , 'U3215852'
          , 'U3216088'
          , 'U3216312'
          , 'U3219312'
          , 'U3220141'
          , 'U3220568'
          , 'U3221205'
          , 'U3221801'
          , 'U3222062'
          , 'U3222202'
          , 'U3223433'
          , 'U3223542'
          , 'U3227108'
          , 'U3227468'
          , 'U3227927'
          , 'U3229818'
          , 'U3230519'
          , 'U3230951'
          , 'U3231620'
          , 'U3231648'
          , 'U3232497'
          , 'U3233940'
          , 'U3234388'
          , 'U3234514'
          , 'U3235908'
          , 'U3236263'
          , 'U3236497'
          , 'U3238477'
          , 'U3241565'
          , 'U3241652'
          , 'U3242660'
          , 'U3244182'
          , 'U3247195'
          , 'U3250192'
          , 'U3259688'
          , 'U3262563'
          , 'U3267829'
          , 'U3271798'
          , 'U3272178'
          , 'U3272231'
          , 'U3273155'
          , 'U3275249'
          , 'U3276741'
          , 'U3282058'
          , 'U3285262'
          , 'U3296126'
          , 'U3296981'
          , 'U3306790'
          , 'U3307738'
          , 'U3307850'
          , 'U3309697'
          , 'U3310124'
          , 'U3317449'
          , 'U3319201'
          , 'U3322089'
          , 'U3322698'
          , 'U3326366'
          , 'U3331618'
          , 'U3334405'
          , 'U3337455'
          , 'U3350132'
          , 'U3350655'
          , 'U3353606'
          , 'U3354899'
          , 'U3369083'
          , 'U3371126'
          , 'U3371216'
          , 'U3371751'
          , 'U3372665'
          , 'U3871596'
          , 'U3958434'
          , 'U4012049'
          , 'U4250105'
          , 'U4294473'
          , 'U4404773'
          , 'U4708703'
          , 'U4900937'
          , 'U4976381'
          , 'U5243652'
          , 'U5572817'
          , 'U5769852'
          , 'U5934852'
          , 'U5950149'
          , 'U6039306'
          , 'U6107588'
          , 'U6164866'
          , 'U6801309'
          , 'U6934663'
          , 'U7190940'
          , 'U7672222'
          , 'U8226161'
          , 'U8964183'
          , 'U9641951'
          , 'U3465865'
          , 'U4785876'
          , 'U5713233'
          , 'U3479697'
          , 'U3494196'
          , 'U2081300'
          , 'U3512068'
          , 'U3517538'
          , 'U3246273'
          , 'U2396031'
          , 'U2160542'
          , 'U4155250'
          , 'U3521839'
          , 'U3437736'
          , 'U3533265'
          , 'U2665781'
          , 'U3535220'
          , 'U3534641'
          , 'U9352960'
          , 'U3546124'
          , 'U3543182'
          , 'U3543033'
          , 'U3254872'
          , 'U3367934'
          , 'U3557963'
          , 'U3551770'
          , 'U3444286'
          , 'U2499953'
          , 'U3577124'
          , 'U2246608'
          , 'U3580399'
          , 'U3576319'
          , 'U3081660'
          , 'U3589874'
          , 'U6376892'
          , 'U0329243'
          , 'U3592669'
          , 'U3590734'
          , 'U3603696'
          , 'U3610860'
          , 'U3610857'
          , 'U2299723'
          , 'U3626220'
          , 'U3627493'
          , 'U0742011'
          , 'U3629271'
          , 'U3634599'
          , 'U3630853'
          , 'U3610415'
          , 'U2448260'
          , 'U3639068'
          , 'U4027317'
          , 'U2020516'
          , 'U3384260'
          , 'U3653691'
          , 'U3657867'
          , 'U3665207'
          , 'U3072894'
          , 'U2739034'
          , 'U3593893'
          , 'U9423118'
          , 'U2248896'
          , 'U3669819'
          , 'U3679468'
          , 'U3655622'
          , 'U3107391'
          , 'U3680899'
          , 'U3492325'
          , 'U2150242'
          , 'U3688691'
          , 'U3482133'
          , 'U3127546'
          , 'U3690206'
          , 'U3679183'
          , 'U7703780'
          , 'U3685147'
          , 'U3674835'
          , 'U3705779'
          , 'U3660564'
          , 'U3404088'
          , 'U3708458'
          , 'U3712144'
          , 'U3721900'
          , 'U3716863'
          , 'U3491268'
          , 'U3722256'
          , 'U3731707'
          , 'U3208343'
          , 'U3731772'
          , 'U3576160'
          , 'U3205208'
          , 'U3674045'
          , 'U2516875'
          , 'U3565400'
          , 'U3743228'
          , 'U4009117'
          , 'U2863709'
          , 'U3757805'
          , 'U3233864'
          , 'U3757885'
          , 'U3728905'
          , 'U3587846'
          , 'U3764151'
          , 'U3604701'
          , 'U3211305'
          , 'U2713926'
          , 'U2735757'
          , 'U3765238'
          , 'U2422200'
          , 'U3754152'
          , 'U3765928'
          , 'U3375050'
          , 'U3772155'
          , 'U3774366'
          , 'U3767084'
          , 'U4921157'
          , 'U3358208'
          , 'U2605673'
          , 'U3779999'
          , 'U3781281'
          , 'U3761559'
          , 'U3792776'
          , 'U3794902'
          , 'U3795964'
          , 'U5702055'
          , 'U3783633'
          , 'U2723678'
          , 'U3777506'
          , 'U3673111'
          , 'U3803939'
          , 'U3803455'
          , 'U3800888'
          , 'U3807395'
          , 'U2030085'
          , 'U2417042'
          , 'U3777760'
          , 'U3813906'
          , 'U3823991'
          , 'U3827748'
          , 'U6119534'
          , 'U3354538'
          , 'U3712752'
          , 'U3835085'
          , 'U2568534'
          , 'U3832616'
          , 'U3825416'
          , 'U3838874'
          , 'U3848160'
          , 'U3854197'
          , 'U3852808'
          , 'U2262916'
          , 'U3858144'
          , 'U3809658'
          , 'U3857715'
          , 'U3861645'
          , 'U3863894'
          , 'U2310437'
          , 'U3242989'
          , 'U3794752'
          , 'U3956813'
          , 'U3859681'
          , 'U3957045'
          , 'U3949968'
          , 'U3937785'
          , 'U3962398'
          , 'U3778680'
          , 'U3969388'
          , 'U3746581'
          , 'U7066262'
          , 'U3862245'
          , 'U3969221'
          , 'U3962079'
          , 'U3466221'
          , 'U3859948'
          , 'U2347495'
          , 'U9089719'
          , 'U3950532'
          , 'U3964768'
          , 'U0291848'
          , 'U3967391'
          , 'U3943257'
          , 'U3992527'
          , 'U3994880'
          , 'U2480552'
          , 'U3113443'
          , 'U3991792'
          , 'U2357911'
          , 'U4044443'
          , 'U3887326'
          , 'U3796551'
          , 'U4050617'
          , 'U3971477'
          , 'U3235560'
          , 'U4052691'
          , 'U3855956'
          , 'U4058030'
          , 'U4056987'
          , 'U2179907'
          , 'U4066909'
          , 'U0931224'
          , 'U2574508'
          , 'U4067561'
          , 'U7442679'
          , 'U4061608'
          , 'U6163541'
          , 'U4766533'
          , 'U4083061'
          , 'U3088796'
          , 'U3907909'
          , 'U3747477'
          , 'U3979552'
          , 'U3995837'
          , 'U4101516'
          , 'U4098912'
          , 'U4099955'
          , 'U3593023'
          , 'U3510535'
          , 'U9970310'
          , 'U4118375'
          , 'U3998175'
          , 'U2272358'
          , 'U4118605'
          , 'U3976152'
          , 'U4098832'
          , 'U3938156'
          , 'U3148638'
          , 'U3998051'
          , 'U3559699'
          , 'U3216312'
          , 'U4130885'
          , 'U4128764'
          , 'U4130289'
          , 'U3118542'
          , 'U4067240'
          , 'U6982220'
          , 'U6299455'
          , 'U4136512'
          , 'U4135562'
          , 'U4139332'
          , 'U3479699'
          , 'U4097301'
          , 'U3970792'
          , 'U3710917'
          , 'U4152042'
          , 'U4156863'
          , 'U4148445'
          , 'U4119032'
          , 'U3197787'
          , 'U4150330'
          , 'U4041566'
          , 'U2566039'
          , 'U4076450'
          , 'U4173801'
          , 'U3138654'
          , 'U6657302'
          , 'U6422752'
          , 'U4178128'
          , 'U3170859'
          , 'U4172675'
          , 'U4183117'
          , 'U8058695'
          , 'U4188185'
          , 'U4143849'
          , 'U2679988'
          , 'U4098105'
          , 'U4187231'
          , 'U4143939'
          , 'U3648137'
          , 'U2568400'
          , 'U4181936'
          , 'U4218909'
          , 'U4212185'
          , 'U2259595'
          , 'U3795203'
          , 'U4248428'
          , 'U4158320'
          , 'U4189707'
          , 'U4240954'
          , 'U4255208'
          , 'U4146382'
          , 'U4263653'
          , 'U4258568'
          , 'U4265371'
          , 'U4269414'
          , 'U4126429'
          , 'U4281991'
          , 'U4278563'
          , 'U3818543'
          , 'U4290745'
          , 'U3259474'
          , 'U4300612'
          , 'U4195711'
          , 'U4221426'
          , 'U4301272'
          , 'U4242138'
          , 'U4183542'
          , 'U4088170'
          , 'U4103782'
          , 'U4266929'
          , 'U2486471'
          , 'U5177223'
          , 'U0322656'
          , 'U4223928'
          , 'U0765187'
          , 'U4309700'
          , 'U4323732'
          , 'U4277864'
          , 'U3965025'
          , 'U2242389'
          , 'U2739023'
          , 'U4328009'
          , 'U4318043'
          , 'U4335728'
          , 'U4269054'
          , 'U9546181'
          , 'U4339888'
          , 'U4336230'
          , 'U4334552'
          , 'U3181351'
          , 'U4327276'
          , 'U3577135'
          , 'U4338982'
          , 'U3353606'
          , 'U4286354'
          , 'U4330759'
          , 'U3068298'
          , 'U0867591'
          , 'U4365008'
          , 'U4365879'
          , 'U4227512'
          , 'U4367758'
        );

    DROP TABLE IF EXISTS temp.ArrivalTRM_Ylinne;
    CREATE TABLE temp.ArrivalTRM_Ylinne SELECT * FROM temp.ArrivalTRM;

    """
    dosqlexecute(cnxdict)


def ArrivalTRM(cnxdict):
    cnxdict['sql'] = """
    DROP TABLE IF EXISTS Temp.Temp2 ;
    CREATE TABLE Temp.Temp2
    SELECT
            Status.PtMRN
            , Status.PatientId
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
            , Status.StatusDate AS `ArrivalDate`
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
                    , PatientId
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
                    , PatientId
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
            , a.`PatientId`
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
            , a.`StatusDate` as `ArrivalDate`
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

    DROP TABLE IF EXISTS caisis.ArrivalTRM;
    CREATE TABLE caisis.ArrivalTRM SELECT * FROM temp.ArrivalTRM;
    """
    dosqlexecute(cnxdict)


CreateFirstMedicalTherapyAfterArrival(cnxdict)
ArrivalTRM(cnxdict)
# ArrivalTRM_GCLAM()
ArrivalTRM_ForYlinne(cnxdict)


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

"""
TRM for Ylinne Arrivals
"""
# df = pd.read_sql("""
#     SELECT `PtMRN`,
#         `StatusDate` AS ArrivalDate,
#         `StatusDisease`,
#         `MedTxDate`,
#         `TRM_Version1 (Paper)`,
#         `TRM_Version2 (Online)`,
#         `TRMRange`,
#         `ECOG`,
#         `AgeAtTreatmentStart`,
#         `calcuated_platelet`,
#         `calcuated_albumin`,
#         `secondary`,
#         `calcuated_wbc`,
#         `calculated blasts and unclassified`,
#         `calcuated_creatinine`,
#         `calculated blast`,
#         `calculated unclassified`,
#         `Not Treated`,
#         `Protocol Unknown`,
#         `ECOG Missing`,
#         `Age Missing`,
#         `Platelet Missing`,
#         `Secondary Missing`,
#         `WBC Missing`,
#         `Circulating Blasts Missing`,
#         `Creatinine Missing`
#     FROM `temp`.`ArrivalTRM_Ylinne`
#     ORDER BY PtMRN, ArrivalDate;
# """, cnxdict['cnx'])
# df.to_excel(writer, sheet_name='TRM for Ylinne Arrivals', index=False)

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
    FROM `caisis`.`arrivaltrm`;
""", cnxdict['cnx'])
df.to_excel(writer, sheet_name='Detail for all arrivals', index=False)
dowritersave(writer,cnxdict)

sheet_name='Detail for all arrivals'
print(len(sheet_name))

