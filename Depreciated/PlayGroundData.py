from SecondaryDiseaseIdentification import *

reload(sys)

from DefineCaisisViews import create_all_views

"""
SQL FOR PLAYGROUND
"""



def ShowMessage(title='',program=None):
    MsgResp = tkMessageBox.showinfo(title=title
                                    , message='Delete old {} and recreate?'.format(title)
                                    , type="yesno")
    window.wm_withdraw()

    if MsgResp == 'yes' and program is not None:
        print('Running the program: {}'.format(program))
        try:
            eval(program)
            print('Completed program run:  {}'.format(program))
        except:
            print('Failed to run program:  {}'.format(program))

    return MsgResp


def makeplayground():
    cnxdict = connect_to_mysql_db_prod('caisismysql')
    cnxdict['sql'] = """
        /*
        Building Playground Table
        Up to this point we could build the playground information for diagnosis, arrival, AND encounter through joins
        but from here on out because of the nature of parent-child relationships the following must be updates
        to place holders:
        Induction
        Response
        Cycle

        */
        DROP TABLE IF EXISTS caisis.playground ;
        CREATE TABLE caisis.playground
            SELECT a.PtMRN
                , a.PatientId
                , a.PtName
                , a.PtBirthdate
                , a.PtGender
                , a.PtRace
                , a.PtEthnicity
                , a.PtDeathDate
                , a.PtDeathType
                , a.PtDeathCause
                , b.AMLDxDate
                , a.ArrivalAge
                , a.ArrivalDate
                , a.ArrivalDx
                , d.SecondaryType
                , c.EncounterType
                , c.ArrivalDate AS EncounterDate
                , CASE
                    WHEN c.EncounterECOG IS NULL THEN -9
                    ELSE c.EncounterECOG
                END AS ArrivalECOG
                , STR_TO_DATE(NULL,'%m%h') AS InductionStartDate
                , cast(' ' AS CHAR) AS TreatmentProtocol
                , STR_TO_DATE(NULL,'%m%h') AS ResponseDate
                , cast(' ' AS CHAR) AS ResponseDescription
                , STR_TO_DATE(NULL,'%m%h') AS HCTDate
                , cast(' ' AS CHAR) AS HCTDescription
                , STR_TO_DATE(NULL,'%m%h') AS RelapseDate
                , cast(' ' AS CHAR) AS RelapseDescription
                # Cycles before response
                , STR_TO_DATE(NULL,'%m%h') AS Cycle1Date
                , cast(' ' AS CHAR)        AS Cycle1Treatment
                , STR_TO_DATE(NULL,'%m%h') AS Cycle2Date
                , cast(' ' AS CHAR)        AS Cycle2Treatment
                , STR_TO_DATE(NULL,'%m%h') AS Cycle3Date
                , cast(' ' AS CHAR)        AS Cycle3Treatment
                , STR_TO_DATE(NULL,'%m%h') AS Cycle4Date
                , cast(' ' AS CHAR)        AS Cycle4Treatment
                # Cycles after response
                , STR_TO_DATE(NULL,'%m%h') AS PRT1Date
                , cast(' ' AS CHAR)        AS PRT1Treatment
                , STR_TO_DATE(NULL,'%m%h') AS PRT2Date
                , cast(' ' AS CHAR)        AS PRT2Treatment
                , STR_TO_DATE(NULL,'%m%h') AS PRT3Date
                , cast(' ' AS CHAR)        AS PRT3Treatment
                , STR_TO_DATE(NULL,'%m%h') AS PRT4Date
                , cast(' ' AS CHAR)        AS PRT4Treatment
                , -9 as CyclesToResponse
                , space(100) as CyclesCalculationMethod
                , -9 AS DaysToResponse
                , space(800) as FirstCyto
                , STR_TO_DATE(NULL,'%m%h') AS FirstCytoDate
                , space(800) as DiagnosisCyto
                , STR_TO_DATE(NULL,'%m%h') AS DiagnosisCytoDate
                , space(800) as ArrivalCyto
                , STR_TO_DATE(NULL,'%m%h') AS ArrivalCytoDate
                , e.`X`
                , e.`TRM_Version1 (Paper)`
                , e.`TRM_Version2 (Online)`
                , e.`TRMRangeOrder`
                , e.`TRMRange`
                , e.`ECOG`
                , e.`AgeAtTreatmentStart`
                , e.`calcuated_platelet`
                , e.`calcuated_albumin`
                , e.`secondary`
                , e.`calcuated_wbc`
                , e.`calculated blasts and unclassified`
                , e.`calcuated_creatinine`
                , e.`calculated blast`
                , e.`calculated unclassified`
                , e.`Not Treated`
                , e.`Protocol Unknown`
                , e.`ECOG Missing`
                , e.`Age Missing`
                , e.`Platelet Missing`
                , e.`Secondary Missing`
                , e.`WBC Missing`
                , e.`Circulating Blasts Missing`
                , e.`Creatinine Missing`
                , e.`arrival_albumin`
                , e.`arrival_albumin_units`
                , e.`arrival_creatinine`
                , e.`arrival_creatinine_units`
                , e.`arrival_wbc`
                , e.`arrival_wbc_units`
                , e.`arrival_platelet`
                , e.`arrival_platelet_units`
                , e.`arrival circulating blast`
                , e.`arrival circulating blast units`
                , e.`arrival circulating blast date`
                , e.`arrival circulating unclassified`
                , e.`arrival circulating unclassified units`
                , e.`arrival circulating unclassified date`
                , e.`treatment_albumin`
                , e.`treatment_albumin_units`
                , e.`treatment_creatinine`
                , e.`treatment_creatinine_units`
                , e.`treatment_wbc`
                , e.`treatment_wbc_units`
                , e.`treatment_platelet`
                , e.`treatment_platelet_units`
                , e.`treatment circulating blast`
                , e.`treatment circulating blast units`
                , e.`treatment circulating blast date`
                , e.`treatment circulating unclassified`
                , e.`treatment circulating unclassified units`
                , e.`treatment circulating unclassified date`
                , a.NextArrivalDate, a.NextArrivalDx, a.PrevArrivalDate, a.PrevArrivalDx
                FROM temp.v_arrival_with_prev_next AS a
                LEFT JOIN temp.v_diagnosis b ON a.ptmrn = b.ptmrn
                LEFT JOIN temp.v_encounter c ON a.ptmrn = c.ptmrn AND a.ArrivalDate = c.ArrivalDate
                LEFT JOIN caisis.secondarystatus d ON a.ptmrn = d.ptmrn
                LEFT JOIN caisis.arrivaltrm e ON a.ptmrn = e.ptmrn AND a.ArrivalDate = e.ArrivalDate
                ORDER BY a.ptmrn, a.eventdate ;

        ALTER TABLE `caisis`.`playground`
            CHANGE COLUMN `TreatmentProtocol`     `TreatmentProtocol`   MEDIUMTEXT
            , CHANGE COLUMN `ResponseDescription` `ResponseDescription` MEDIUMTEXT
            , CHANGE COLUMN `HCTDescription`      `HCTDescription`      MEDIUMTEXT
            , CHANGE COLUMN `RelapseDescription`  `RelapseDescription`  MEDIUMTEXT
            , CHANGE COLUMN `Cycle1Treatment`     `Cycle1Treatment`     MEDIUMTEXT
            , CHANGE COLUMN `Cycle2Treatment`     `Cycle2Treatment`     MEDIUMTEXT
            , CHANGE COLUMN `Cycle3Treatment`     `Cycle3Treatment`     MEDIUMTEXT
            , CHANGE COLUMN `Cycle4Treatment`     `Cycle4Treatment`     MEDIUMTEXT
            , CHANGE COLUMN `PRT1Treatment`       `PRT1Treatment`       MEDIUMTEXT
            , CHANGE COLUMN `PRT2Treatment`       `PRT2Treatment`       MEDIUMTEXT
            , CHANGE COLUMN `PRT3Treatment`       `PRT3Treatment`       MEDIUMTEXT
            , CHANGE COLUMN `PRT4Treatment`       `PRT4Treatment`       MEDIUMTEXT
            , CHANGE COLUMN `PrevArrivalDx`       `PrevArrivalDx`       MEDIUMTEXT ;

        ALTER TABLE `caisis`.`playground`
            ADD INDEX `PtMRN` (`PtMRN`(10) ASC),
            ADD INDEX `ArrivalDate` (`ArrivalDate` ASC),
            ADD INDEX `ArrivalDx` (`ArrivalDx` (30) ASC);

        ALTER TABLE `caisis`.`playground`
            ADD COLUMN `DaysRxToResponse` INT(3) NULL AFTER `PrevArrivalDx`,
            ADD COLUMN `DaysArrivalToResponse` INT(3) NULL AFTER `PrevArrivalDx`;

        /*

        Induction cycles
        */
        UPDATE caisis.playground a, caisis.induction b
            SET a.InductionStartDate =
            CASE
                WHEN b.InductionStartDate BETWEEN a.ArrivalDate AND a.NextArrivalDate THEN b.InductionStartDate
                WHEN b.InductionStartDate > a.ArrivalDate AND a.NextArrivalDate IS NULL THEN b.InductionStartDate
                WHEN b.InductionStartDate = a.ArrivalDate THEN b.InductionStartDate
                ELSE NULL
            END
            , a.TreatmentProtocol    = b.TreatmentProtocol
            , a.Cycle1Date           = b.InductionStartDate
            , a.Cycle1Treatment      = b.TreatmentProtocol
            WHERE a.PtMRN = b.PtMRN AND a.ArrivalDate = b.ArrivalDate
            AND   (b.InductionStartDate BETWEEN a.ArrivalDate AND a.NextArrivalDate
                   OR
                   (b.InductionStartDate > a.ArrivalDate AND a.NextArrivalDate IS NULL));
        
        # UPDATE caisis.playground a, caisis.induction b
        #     SET a.InductionStartDate = b.InductionStartDate
        #     , a.TreatmentProtocol    = b.TreatmentProtocol
        #     , a.Cycle1Date           = b.InductionStartDate
        #     , a.Cycle1Treatment      = b.TreatmentProtocol
        #     WHERE a.PtMRN = b.PtMRN AND a.ArrivalDate = b.ArrivalDate
        #     AND   b.InductionStartDate <  a.InductionStartDate
        #     AND   b.InductionStartDate >= a.ArrivalDate ;
        #
        # UPDATE caisis.playground a, caisis.induction b
        #     SET a.InductionStartDate = b.InductionStartDate
        #     , a.TreatmentProtocol    = b.TreatmentProtocol
        #     , a.Cycle1Date           = b.InductionStartDate
        #     , a.Cycle1Treatment      = b.TreatmentProtocol
        #     WHERE a.PtMRN = b.PtMRN AND a.InductionStartDate IS NULL
        #     AND   b.InductionStartDate BETWEEN a.ArrivalDate AND a.NextArrivalDate ;

        #
        # Response
        #
        # In the following queries, the thing that changes is the WHERE clause to account for the variety
        # of pieces of missing information such as missing date of next arrival, or date of induction start
        #
        #
        # CASE WHEN RESPONSE BETWEEN INDUCTION START AND NEXT ARRIVAL
        #
        DROP TEMPORARY TABLE IF EXISTS t_FirstResp ;
        CREATE TEMPORARY TABLE t_FirstResp
            AS SELECT a.PtMRN, a.ArrivalDate, min(b.responsedate) AS FirstResponseDate
                , b.ResponseDescription as FirstResponseDescription
                FROM caisis.v_response b
                LEFT JOIN caisis.playground a
                    ON a.PtMRN = b.PtMRN
                WHERE b.ResponseDate BETWEEN a.InductionStartDate AND a.NextArrivalDate
                GROUP BY a.PtMRN, a.ArrivalDate;


        UPDATE caisis.playground a, t_FirstResp b
            SET a.ResponseDescription = b.FirstResponseDescription
                , a.ResponseDate = b.FirstResponseDate
        WHERE a.PtMRN = b.PtMRN and a.ArrivalDate = b.ArrivalDate
            AND b.FirstResponseDate BETWEEN a.InductionStartDate AND a.NextArrivalDate;

        #
        # CASE WHERE NO NEXT ARRIVAL, TAKE EARLIEST RESPONSE THAT IS AFTER TREATMENT BEGAN
        #
        DROP TEMPORARY TABLE IF EXISTS t_FirstResp ;
        CREATE TEMPORARY TABLE t_FirstResp
            AS SELECT a.PtMRN, a.ArrivalDate, min(b.responsedate) AS FirstResponseDate
                , b.ResponseDescription as FirstResponseDescription
                FROM caisis.v_response b
                LEFT JOIN caisis.playground a
                    ON a.PtMRN = b.PtMRN
                WHERE b.ResponseDate > a.InductionStartDate
                    AND a.ResponseDate IS NULL
                    AND a.NextArrivalDate IS NULL
                GROUP BY a.PtMRN, a.ArrivalDate;

        UPDATE caisis.playground a, t_FirstResp b
            SET a.ResponseDescription = b.FirstResponseDescription
                , a.ResponseDate = b.FirstResponseDate
        WHERE a.PtMRN = b.PtMRN and a.ArrivalDate = b.ArrivalDate
            AND b.FirstResponseDate > a.InductionStartDate
            AND a.ResponseDate IS NULL
            AND a.NextArrivalDate IS NULL;

        #
        # CASE WHERE NO NEXT ARRIVAL AND NO INDUCTION DATE, TAKE EARLIEST RESPONSE THAT IS AFTER ARRIVAL
        #
        DROP TEMPORARY TABLE IF EXISTS t_FirstResp ;
        CREATE TEMPORARY TABLE t_FirstResp
            AS SELECT a.PtMRN, a.ArrivalDate, min(b.responsedate) AS FirstResponseDate
                , b.ResponseDescription as FirstResponseDescription
                FROM caisis.v_response b
                LEFT JOIN caisis.playground a
                    ON a.PtMRN = b.PtMRN
                WHERE b.ResponseDate > a.ArrivalDate
                    AND a.ResponseDate IS NULL
                    AND a.InductionStartDate IS NULL
                    AND a.NextArrivalDate IS NULL
                GROUP BY a.PtMRN, a.ArrivalDate;

        UPDATE caisis.playground a, t_FirstResp b
            SET a.ResponseDescription = b.FirstResponseDescription
                , a.ResponseDate = b.FirstResponseDate
        WHERE a.PtMRN = b.PtMRN AND a.ArrivalDate = b.ArrivalDate
            AND b.FirstResponseDate > a.ArrivalDate
            AND a.ResponseDate IS NULL
            AND a.InductionStartDate IS NULL
            AND a.NextArrivalDate IS NULL;


        UPDATE caisis.playground SET DaysArrivalToResponse = timestampdiff(day,arrivaldate,responsedate),
                    DaysRxToResponse = timestampdiff(day,inductionstartdate,responsedate);

        #
        # THIS NEXT SECTION CREATES A TABLE WHERE AN EXPECTED RESPONSE IS NOT
        # FOUND BETWEEN INDUCTION START AND NEXT ARRIVAL
        #
        DROP TABLE IF EXISTS temp.error;
        CREATE TABLE temp.error
        SELECT
            a.ptmrn,
            'Responses between arrival and treatment' as OddResponseTimingDescription,
            YEAR(a.arrivaldate),
            a.arrivaldate,
            a.TreatmentProtocol,
            a.inductionstartdate,
            a.responsedate,
            a.responsedescription,
            a.nextarrivaldate
        FROM
            caisis.playground a
            LEFT JOIN caisis.v_response b
        ON a.PtMRN = b.PtMRN
        WHERE a.nextarrivaldate is not null
            and a.inductionstartdate is not null
            and a.ResponseDate is null
            and a.treatmentprotocol not like '%palli%'
            and a.treatmentprotocol not like '%no treat%'
            and a.treatmentprotocol not like '%not treat%'
            and a.treatmentprotocol not like '%unk%'
            and b.responsedate between a.arrivaldate and a.inductionstartdate
        UNION
        SELECT
            a.ptmrn,
            'Responses between inducition and next arrival' as OddResponseTimingDescription,
            YEAR(a.arrivaldate),
            a.arrivaldate,
            a.TreatmentProtocol,
            a.inductionstartdate,
            a.responsedate,
            a.responsedescription,
            a.nextarrivaldate
        FROM
            caisis.playground a
            LEFT JOIN caisis.v_response b
        ON a.PtMRN = b.PtMRN
        WHERE a.nextarrivaldate is not null
            and a.inductionstartdate is not null
            and a.ResponseDate is null
            and a.treatmentprotocol not like '%palli%'
            and a.treatmentprotocol not like '%no treat%'
            and a.treatmentprotocol not like '%not treat%'
            and a.treatmentprotocol not like '%unk%'
            and b.responsedate between a.inductionstartdate and a.nextarrivaldate
        UNION
        SELECT
            a.ptmrn,
            'Response not found between arrival and next arrival' as OddResponseTimingDescription,
            YEAR(a.arrivaldate),
            a.arrivaldate,
            a.TreatmentProtocol,
            a.inductionstartdate,
            a.responsedate,
            a.responsedescription,
            a.nextarrivaldate
        FROM
            caisis.playground a
            LEFT JOIN caisis.v_response b
        ON a.PtMRN = b.PtMRN
        WHERE a.nextarrivaldate is not null
            and a.inductionstartdate is not null
            and a.ResponseDate is null
            and a.treatmentprotocol not like '%palli%'
            and a.treatmentprotocol not like '%no treat%'
            and a.treatmentprotocol not like '%not treat%'
            and a.treatmentprotocol not like '%unk%'
            and not (b.responsedate between a.inductionstartdate and a.nextarrivaldate)
        ORDER BY YEAR(ArrivalDate), OddResponseTimingDescription, TreatmentProtocol ;
        #
        # Cycles after induction but before response
        #
        # second cycle
        UPDATE caisis.playground a, temp.v_treatment b
            SET a.Cycle2Date    = b.TreatmentStartDate
            , a.Cycle2Treatment = b.TreatmentProtocol
            WHERE a.PtMRN = b.PtMRN
                AND b.TreatmentStartDate BETWEEN date_add(a.Cycle1Date, INTERVAL 7 DAY) and date_add(a.ResponseDate, INTERVAL -1 DAY);
        # third cycle
        UPDATE caisis.playground a, temp.v_treatment b
            SET a.Cycle3Date    = b.TreatmentStartDate
            , a.Cycle3Treatment = b.TreatmentProtocol
            WHERE a.PtMRN = b.PtMRN
                AND b.TreatmentStartDate BETWEEN date_add(a.Cycle2Date, INTERVAL 7 DAY) and date_add(a.ResponseDate, INTERVAL -1 DAY);
        # fourth cycle
        UPDATE caisis.playground a, temp.v_treatment b
            SET a.Cycle4Date    = b.TreatmentStartDate
            , a.Cycle4Treatment = b.TreatmentProtocol
            WHERE a.PtMRN = b.PtMRN
                AND b.TreatmentStartDate BETWEEN date_add(a.Cycle3Date, INTERVAL 7 DAY) and date_add(a.ResponseDate, INTERVAL -1 DAY);
        # total cycles found
        UPDATE caisis.playground a, caisis.v_response b
            SET a.CyclesToResponse =
                CASE
                    WHEN a.Cycle4Date IS NOT NULL AND a.Cycle4Date <= a.ResponseDate   THEN 4
                    WHEN a.Cycle3Date IS NOT NULL AND a.Cycle3Date <= a.ResponseDate   THEN 3
                    WHEN a.Cycle2Date IS NOT NULL AND a.Cycle2Date <= a.ResponseDate   THEN 2
                    WHEN timestampdiff(day,  a.InductionStartDate,a.ResponseDate) < 35 THEN 1
                    WHEN timestampdiff(month,a.InductionStartDate,a.ResponseDate) <= 2 THEN 2
                    ELSE -9
                END
                , a.CyclesCalculationMethod =
                CASE
                    WHEN a.Cycle4Date IS NOT NULL AND a.Cycle4Date <= a.ResponseDate   THEN '4 Cycles Recorded'
                    WHEN a.Cycle3Date IS NOT NULL AND a.Cycle3Date <= a.ResponseDate   THEN '3 Cycles Recorded'
                    WHEN a.Cycle2Date IS NOT NULL AND a.Cycle2Date <= a.ResponseDate   THEN '2 Cycles Recorded'
                    WHEN timestampdiff(day,  a.InductionStartDate,a.ResponseDate) < 35 THEN 'Less than 35 days to Response'
                    WHEN timestampdiff(month,a.InductionStartDate,a.ResponseDate) <= 2 THEN 'Response two months after Induction Start'
                    ELSE ''
                END
            WHERE a.PtMRN = b.PtMRN
            and b.ResponseDescription IS NOT NULL;

        #
        # Days from induction to response
        #
        UPDATE caisis.playground SET DaysToResponse =
            CASE
                WHEN InductionStartDate IS NULL THEN -9
                WHEN ResponseDate IS NULL       THEN -9
                ELSE timestampdiff(day,InductionStartDate,ResponseDate)
            END;

        #
        # Relapse
        #
        UPDATE playground a, temp.v_relapse b
            SET a.RelapseDate = b.RelapseDate,
            a.RelapseDescription = b.RelapseDescription
            WHERE a.PtMRN = b.PtMRN
            AND b.RelapseDate BETWEEN date_add(a.ResponseDate, INTERVAL 1 DAY) AND date_add(a.NextArrivalDate, INTERVAL 1 DAY);

        #
        # Post Remission Therapy (PRT)
        #
        # first PRT
        UPDATE caisis.playground a, temp.v_treatment b
            SET a.PRT1Date    = b.TreatmentStartDate
            , a.PRT1Treatment = b.TreatmentProtocol
            WHERE a.PtMRN = b.PtMRN
                AND b.TreatmentStartDate BETWEEN date_add(a.ResponseDate, INTERVAL 7 DAY) and date_add(a.NextArrivalDate, INTERVAL -1 DAY);

        # second PRT
        UPDATE caisis.playground a, temp.v_treatment b
            SET a.PRT2Date    = b.TreatmentStartDate
            , a.PRT2Treatment = b.TreatmentProtocol
            WHERE a.PtMRN = b.PtMRN
                AND b.TreatmentStartDate BETWEEN date_add(a.PRT1Date, INTERVAL 7 DAY) and date_add(a.NextArrivalDate, INTERVAL -1 DAY);

        # third PRT
        UPDATE caisis.playground a, temp.v_treatment b
            SET a.PRT3Date    = b.TreatmentStartDate
            , a.PRT3Treatment = b.TreatmentProtocol
            WHERE a.PtMRN = b.PtMRN
                AND b.TreatmentStartDate BETWEEN date_add(a.PRT2Date, INTERVAL 7 DAY) and date_add(a.NextArrivalDate, INTERVAL -1 DAY);

        # fourth PRT
        UPDATE caisis.playground a, temp.v_treatment b
            SET a.PRT4Date    = b.TreatmentStartDate
            , a.PRT4Treatment = b.TreatmentProtocol
            WHERE a.PtMRN = b.PtMRN
                AND b.TreatmentStartDate BETWEEN date_add(a.PRT3Date, INTERVAL 7 DAY) and date_add(a.NextArrivalDate, INTERVAL -1 DAY);

        /*
        HCT information
        */
        UPDATE Caisis.playground a, temp.v_hct b
            SET a.HCTDate = b.HCTDate
            , a.HCTDescription = concat(b.HCTDescription,' (HCT occurred before next arrival)')
            WHERE b.HCTDate BETWEEN a.ArrivalDate and a.NextArrivalDate;

        UPDATE Caisis.playground a, temp.v_hct b
                    SET a.HCTDate = b.HCTDate
                    , a.HCTDescription = concat(b.HCTDescription,' (HCT occurred after this arrival)')
                    WHERE b.HCTDate IS NULL AND b.HCTDate >= a.ArrivalDate;

        UPDATE Caisis.playground a, temp.v_hct b
            SET a.HCTDate = b.HCTDate
            , a.HCTDescription = concat(b.HCTDescription,' (HCT occurred prior to this arrival)')
            WHERE b.HCTDate IS NULL AND b.HCTDate < a.ArrivalDate;

        /*
        FirstCyto
        */
        Update Caisis.playground a,
            (SELECT PtMRN, min(DateObtained) AS FirstCytoDate, PathResult AS FirstCyto FROM temp.v_cyto
                where lower(pathresult) NOT IN ( 'not done'
                    , 'no result'
                    , 'no growth'
                    , 'unk'
                    , 'not reported'
                    , 'n/a'
                    , 'insufficient'
                    , 'insufficient sample'
                    , 'insufficient growth'
                    , 'cancelled'
                    , 'inadequate for diagnosis'
                    , 'no growth or insufficient growth' )
                GROUP BY PtMRN ) b
            SET a.FirstCyto = b.FirstCyto
            , a.FirstCytoDate = b.FirstCytoDate
            WHERE a.PtMRN = b.PtMRN;

        /*
        ArrivalCyto -- NEEDS WORK!!!!!!!!!!!!!!!!
        */
        Update Caisis.playground a,
            (SELECT PtMRN, DateObtained, PathResult FROM temp.v_cyto
                where lower(pathresult) NOT IN ( 'not done'
                    , 'no result'
                    , 'no growth'
                    , 'unk'
                    , 'not reported'
                    , 'n/a'
                    , 'insufficient'
                    , 'insufficient sample'
                    , 'insufficient growth'
                    , 'cancelled'
                    , 'inadequate for diagnosis'
                    , 'no growth or insufficient growth' )
                GROUP BY PtMRN ) b
            SET a.ArrivalCyto = b.PathResult
            , a.ArrivalCytoDate = b.DateObtained
            WHERE a.PtMRN = b.PtMRN
            AND b.DateObtained BETWEEN date_add(a.ArrivalDate, INTERVAL -30 day) AND a.InductionStartDate;

        /*
        DiagnosisCyto -- NEEDS WORK!!!!!!!!!!!!!!!!
        */
        Update Caisis.playground a,
            (SELECT PtMRN, DateObtained, PathResult FROM temp.v_cyto
                where lower(pathresult) NOT IN ( 'not done'
                    , 'no result'
                    , 'no growth'
                    , 'unk'
                    , 'not reported'
                    , 'n/a'
                    , 'insufficient'
                    , 'insufficient sample'
                    , 'insufficient growth'
                    , 'cancelled'
                    , 'inadequate for diagnosis'
                    , 'no growth or insufficient growth' )
                GROUP BY PtMRN ) b
            SET a.DiagnosisCyto = b.PathResult
            , a.DiagnosisCytoDate = b.DateObtained
            WHERE a.PtMRN = b.PtMRN
            AND b.DateObtained
                BETWEEN date_add(a.AMLDxDate, INTERVAL -30 day) AND a.AMLDxDate;

        /*
        ArrivalCyto -- NEEDS WORK!!!!!!!!!!!!!!!!
        */
        Update Caisis.playground a,
            (SELECT PtMRN, DateObtained, PathResult FROM temp.v_cyto
                where lower(pathresult) NOT IN ( 'not done'
                    , 'no result'
                    , 'no growth'
                    , 'unk'
                    , 'not reported'
                    , 'n/a'
                    , 'insufficient'
                    , 'insufficient sample'
                    , 'insufficient growth'
                    , 'cancelled'
                    , 'inadequate for diagnosis'
                    , 'no growth or insufficient growth' )
                GROUP BY PtMRN ) b
            SET a.ArrivalCyto = b.PathResult
            , a.ArrivalCytoDate = b.DateObtained
            WHERE a.PtMRN = b.PtMRN
            AND b.DateObtained
                BETWEEN date_add(a.ArrivalDate, INTERVAL -30 day) AND a.InductionStartDate;

        DROP VIEW IF EXISTS caisis.v_treatment;
        CREATE VIEW caisis.v_treatment AS
            SELECT a.*
                , b.ResponseDescription AS EventResult
                FROM caisis.induction a
                LEFT JOIN caisis.playground b
                ON a.PtMRN = b.PtMRN and a.ArrivalDate = b.ArrivalDate;



        """
    dosqlexecute(cnxdict) # normally do not need to recreate views
    return


def create_playground(cnxdict=None):
    print('creating playground')

    """
    In this section call any programs that need to be run to update data needed for the playground below
    the first.

    Item "0" in the dictionary is the playground table itself.  Users will be asked if they want to recreate the
    playground table.  If they do then they will be asked one-by-one whether or not the supporting tables need updated.
    """
    CreationList = ({'name': 'Playground Data:  "Caisis.Playground"',
                     'message': """
                        If you wish to recreate the playground table you will be asked one-by-one
                        whether or not the supporting tables need updated.  Once all supporting
                        tables have been updated, then the playground will be recreated.""",
                     'program': 'makeplayground()'},

                    {'name': 'Secondary Disease Table:  "Caisis.SecondaryStatus"',
                     'message': """
                        The SecondaryStatus table calculates from various supporting pieces
                        of information in caisis whether or not a patient has secondary disease
                        either via an AHD, previous chemo treatment or chemical exposure.
                        Would you like to recreate it?""",
                     'program': 'Create_Secondary()'},

                    {'name': 'Arrival TRM Table:  "Caisis.ArrivalTRM"',
                     'message': """
                        The ArrivalTRM table calculates from various supporting pieces
                        of information in caisis what a patient's risk of mortality is from
                        induction chemotherapy.
                        Would you like to recreate it?""",
                    'program': 'ArrivalTRM()'},

                    {'name': 'Various Caisis Views',
                     'message': """
                        Our working database in MySQL contains many views that are based on the data
                        imported directly from Caisis.  Views need to be recreated only when fressh
                        data have been imported OR a change has been made to the view definition code.
                        Would you like to recreate it?""",
                     'program': 'create_all_views'}
                    )
    if ShowMessage(CreationList[0]['name'],None)=='yes':
        for creationitem in CreationList[1:]:
            currentname = creationitem['name']
            currentmsg  = creationitem['message']
            currentprg  = creationitem['program']
            ShowMessage(title=currentname, program=currentprg)

        currentname = CreationList[0]['name']
        currentprg  = CreationList[0]['program']
        print('Creating {}'.format(currentname))
        eval(currentprg)
        print('Completed program {}'.format(currentname))

    return

# cnxdict = connect_to_mysql_db_prod('caisismysql')
# cnxdict['EchoSQL']=True
create_all_views(cnxdict)
# cnxdict['EchoSQL']=True
# create_playground(cnxdict)