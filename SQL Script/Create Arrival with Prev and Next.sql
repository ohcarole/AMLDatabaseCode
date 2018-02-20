USE caisis ;

DROP TABLE IF EXISTS v_arrival ;
DROP TABLE IF EXISTS vdatasetarrivalwithprevnext ;
SET @rownum:=0;
CREATE TABLE v_arrival 
    SELECT @rownum:=@rownum+1 AS rownum, newview_arrival.* FROM
        ( SELECT 
            `a`.`PtMRN` AS `PtMRN`,
            `a`.`PatientId` AS `PatientId`,
            `c`.`PtName` AS `PtName`,
            `c`.`PtBirthdate` AS `PtBirthDate`,
            `c`.`PtGender` AS `PtGender`,
            `c`.`PtRace` AS `PtRace`,
            `c`.`PtEthnicity` AS `PtEthnicity`,
            `c`.`PtDeathDate` AS `PtDeathDate`,
            `c`.`PtDeathType` AS `PtDeathType`,
            `c`.`PtDeathCause` AS `PtDeathCause`,
            `a`.`Status` AS `ArrivalStatus`,
            `a`.`StatusDate` AS `ArrivalDate`,
            `a`.`StatusDisease` AS `ArrivalDx`,
            TIMESTAMPDIFF(YEAR,
                `c`.`PtBirthdate`,
                `a`.`StatusDate`) AS `ArrivalAge`,
            '2 ARRIVAL' AS `Event`,
            `a`.`StatusDisease` AS `EventResult`,
            `a`.`StatusDate` AS `EventDate`,
            CONCAT(`a`.`StatusDisease`, ': ', `a`.`Status`) AS `EventDescription`
        FROM
            ((`vdatasetstatus` `a`
            LEFT JOIN (SELECT 
                `vdatasetstatus`.`PatientId` AS `JoinedPatientId`,
                    MAX(YEAR(`vdatasetstatus`.`StatusDate`)) AS `MostRecentStatus`
            FROM
                `vdatasetstatus`
            -- WHERE
            --     (YEAR(`vdatasetstatus`.`StatusDate`) >= 2008)
            GROUP BY `vdatasetstatus`.`PatientId`) `b` ON ((`a`.`PatientId` = `b`.`JoinedPatientId`)))
            LEFT JOIN (SELECT 
                `vdatasetpatients`.`PatientId` AS `PatientId_`,
                    CONCAT((CASE
                        WHEN ISNULL(`vdatasetpatients`.`PtLastName`) THEN ''
                        ELSE CONCAT(UPPER(`vdatasetpatients`.`PtLastName`), ', ')
                    END), (CASE
                        WHEN ISNULL(`vdatasetpatients`.`PtFirstName`) THEN ''
                        ELSE CONCAT(UPPER(`vdatasetpatients`.`PtFirstName`), ' ')
                    END), (CASE
                        WHEN ISNULL(`vdatasetpatients`.`PtMiddleName`) THEN ''
                        ELSE CONCAT(LEFT(UPPER(`vdatasetpatients`.`PtMiddleName`), 1), '.')
                    END)) AS `PtName`,
                    `vdatasetpatients`.`PtBirthDate`  AS `PtBirthdate`,
                    `vdatasetpatients`.`PtGender`     AS `PtGender`,
                    `vdatasetpatients`.`PtRace`       AS `PtRace`,
                    `vdatasetpatients`.`PtEthnicity`  AS `PtEthnicity`,
                    `vdatasetpatients`.`PtDeathDate`  AS `PtDeathDate`,
                    `vdatasetpatients`.`PtDeathType`  AS `PtDeathType`,
                    `vdatasetpatients`.`PtDeathCause` AS `PtDeathCause`
            FROM
                `vdatasetpatients`) `c` ON ((`a`.`PatientId` = `c`.`PatientId_`)))
        WHERE
            ((`b`.`JoinedPatientId` IS NOT NULL)
                AND (`a`.`Status` LIKE '%work%'))
        GROUP BY `a`.`PtMRN` , `a`.`StatusDate`) AS newview_arrival;

CREATE TABLE vdatasetarrivalwithprevnext
SELECT CAST(NULL AS unsigned) AS arrival_id,
        a.*,
        `b`.`ArrivalDate` AS `PrevArrivalDate`,
        `b`.`ArrivalDx` AS `PrevArrivalDx`
    FROM
        (SELECT `a`.*,
                `b`.`ArrivalDate` AS `NextArrivalDate`,
                `b`.`ArrivalDx` AS `NextArrivalDx`
        FROM
            `v_arrival` `a`
        LEFT JOIN `v_arrival` `b` ON `a`.`PtMRN` = `b`.`PtMRN`
            AND `a`.`RowNum` = `b`.`RowNum` - 1
    UNION 
        SELECT `a`.*,
                    `b`.`ArrivalDate` AS `NextArrivalDate`,
                    `b`.`ArrivalDx` AS `NextArrivalDx`
        FROM `v_arrival` `a`
                LEFT JOIN `v_arrival` `b` ON `a`.`PtMRN` = `b`.`PtMRN`
                    AND `a`.`RowNum` = `b`.`RowNum` - 1
            WHERE
                `b`.`RowNum` IS NULL) `a`
            LEFT JOIN `v_arrival` `b` ON `a`.`PtMRN` = `b`.`PtMRN`
                AND `a`.`RowNum` = `b`.`RowNum` + 1

ORDER BY `a`.`PtMRN` , `a`.`ArrivalDate`;

UPDATE vdatasetarrivalwithprevnext a, arrivalidmapping b
    SET a.arrival_id = b.arrival_id
    WHERE a.PatientId = b.PatientId and a.ArrivalDate = b.ArrivalDate ;

DROP TABLE IF EXISTS v_arrival;