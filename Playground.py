from Utilities.MySQLdbUtils import *

showstackinfo = True
debugmode = True

def CreatePrevNextArrivalTable(cnxdict):
    printtext('Program: ')
    cnxdict['sql'] = """
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
                        `b`.`ArrivalDx`   AS `NextArrivalDx`
                FROM
                    `v_arrival` `a`
                LEFT JOIN `v_arrival` `b` ON `a`.`PtMRN` = `b`.`PtMRN`
                    AND `a`.`RowNum` = `b`.`RowNum` - 1
            UNION 
                SELECT `a`.*,
                            `b`.`ArrivalDate` AS `NextArrivalDate`,
                            `b`.`ArrivalDx`   AS `NextArrivalDx`
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

        ALTER TABLE `caisis`.`vdatasetarrivalwithprevnext` 
            ADD INDEX `PatientId`          (`PatientId` ASC),
            ADD INDEX `ArrivalDate`        (`ArrivalDate` ASC);


    """
    dosqlexecute(cnxdict)
    return


def CreatePlaygroundTemplate(cnxdict):
    """
    This script builds the playground template table structure with minimal data in it.
    Using this as a base I avoid the multiplying that can happen with joins to related tables
    :param cnxdict:
    :return:
    """
    printtext('Program: ')
    cnxdict['sql']="""
        /*************************************************************************************
        Patients with an arrival date joined to view arrival with next in
        order to find a range between which all relevant dates must fall
        */
        
        DROP TABLE IF EXISTS temp.PlaygroundTemp ;
        CREATE TABLE temp.PlaygroundTemp
            SELECT a.PtMRN
                , a.PatientId
                , StatusDisease AS ArrivalDx # test comment
                , StatusDate AS ArrivalDate
                , YEAR(StatusDate) AS ArrivalYear
                , b.NextArrivalDate
            FROM caisis.vdatasetstatus a
            JOIN caisis.vdatasetarrivalwithprevnext b ON a.PatientId = b.PatientId and a.StatusDate = b.ArrivalDate
            WHERE a.status like '%work%' 
            ;
        
         alter table `temp`.`playgroundtemp`
              add column `relapsenotes`             text
            , add column `arrivalkaryotype`         text
            , add column `arrivalfish`              text
            , add column `arrivalcgat`              text
           
            , add column `backboneaddon`            tinytext
            , add column `responseflowblaststext`   tinytext
            , add column `othertreatment`           tinytext
        
            , add column `treatmentcount`           integer  default 0
            , add column `anthracyclindose`         integer
            , add column `daystoresponse`           integer
            , add column `followupdays`             integer
        
            , add column `responseflowblasts`       decimal(5,2)
            , add column `followupmonths`           decimal(5,2) 
            , add column `followupyears`            decimal(5,2) 
        
            , add column `intensity`                varchar(50)
            , add column `response`                 varchar(50)
            , add column `responseflowsource`       varchar(50)
            , add column `relapsetype`              varchar(20)
            , add column `relapsedisease`           varchar(20)
            , add column `treatment`                varchar(50)
            , add column `treatmentintent`          varchar(30)
            , add column `medtxintent`              varchar(50)
            , add column `originalmedtxagent`       varchar(50)
            , add column `medtxagent`               varchar(50)
            , add column `medtxagentnoparen`        varchar(50)
        
            , add column `firstarrivaldx`           varchar(30)
            , add column `diagnosis`                varchar(20)
        
            , add column `anthracyclin`             varchar(50)
            , add column `backbonename`             varchar(50)
            , add column `backbonetype`             varchar(10)
            , add column `arrivaltype`              varchar(20)
            , add column `firstarrivaltype`         varchar(20)
            , add column `crnumber`                 varchar(10)
            , add column `relapse`                  varchar(1)
            , add column `laststatustype`           varchar(50)
            , add column `ptdeathtype`              varchar(50)
            , add column `ptlastname`               varchar(50)
            , add column `returnpatient`            varchar(3)
        
            , add column `diagnosisdate`            datetime
            , add column `treatmentstartdate`       datetime
            , add column `responsedate`             datetime
            , add column `responseflowdate`         datetime
            , add column `relapsedate`              datetime
            , add column `earliestrelapsedate`      datetime
            , add column `lastinformationdate`      datetime
            , add column `laststatusdate`           datetime
            , add column `ptdeathdate`              datetime
            , add column `ptbirthdate`              datetime
            , add column `firstarrivaldate`         datetime
            , add column `arrivalkaryotypedate`     datetime
            , add column `arrivalfishdate`          datetime
            , add column `arrivalcgatdate`          datetime
        ;


        /*
        Records where the patient was not arriving for AML
        ALTER TABLE `caisis`.`Playground`
           ADD COLUMN `FirstArrivalType`              VARCHAR(45)  NULL FIRST
        ;
        
        SELECT * FROM temp.PlaygroundTemp 
            WHERE ArrivalDx NOT LIKE '%aml%' 
            AND   ArrivalDx NOT LIKE '%mds%'
            AND   ArrivalDx NOT LIKE '%raeb%'
            ;
        */
        
        /*
        Remove APL
        */
        
        DELETE FROM temp.PlaygroundTemp 
        WHERE
            (ArrivalDx NOT LIKE '%aml%' AND ArrivalDx NOT LIKE '%mds%' AND ArrivalDx NOT LIKE '%raeb%')
        ;
        
        
        /*
        The main purpose of this requerying is to re-order the fields
        excluding APL
        */
        DROP TABLE IF EXISTS caisis.`Playground` ;
        CREATE TABLE caisis.`Playground`
            SELECT b.`arrival_id`
                   , a.`ptmrn`
                   , a.`patientid`
                   , a.`ptbirthdate`
                   , a.`ptlastname`
                   
                   # information about this arrival at uw/scca
                   , a.`returnpatient`
                   , a.`diagnosis`
                   , a.`diagnosisdate`
                   , a.`arrivaldx`
                   , a.`arrivaltype`
                   , a.`arrivaldate`
                   , a.`arrivalyear`
                   , a.`arrivalkaryotype`
                   , a.`arrivalkaryotypedate`
                   , a.`arrivalfish`
                   , a.`arrivalfishdate`
                   , a.`arrivalcgat`
                   , a.`arrivalcgatdate`
                   
                   # treatment at this arrival
                   , a.`treatmentstartdate`
                   , a.`treatment`
                   , a.`treatmentintent`
                   , a.`treatmentcount`
                   , a.`othertreatment`
                
                   , a.`backbonetype`
                   , a.`backbonename`
                   , a.`backboneaddon`
                   , a.`anthracyclin`
                   , a.`anthracyclindose`
                   , a.`intensity`
                   
                   # response for this arrival's treatment
                   , a.`daystoresponse`
                   , a.`response`
                   , a.`responsedate`
                   , a.`crnumber`
                
                   , a.`responseflowdate`
                   , a.`responseflowsource`
                   , a.`responseflowblasts`
                   , a.`responseflowblaststext`
                
                
                   # relapse from remission at this arrival
                   , a.`relapse`
                   , a.`relapsedate`
                   , a.`relapsedisease`
                   , a.`relapsetype`
                   , a.`relapsenotes`
                
                
                   # first date patient ever relapsed
                   , a.`earliestrelapsedate`
                
                   # information about the very first arrival at uw/scca
                   , a.`firstarrivaldx`
                   , a.`firstarrivaltype`
                   , a.`firstarrivaldate`
                
                   , a.`followupdays`
                   , a.`followupmonths`
                   , a.`followupyears`
                   , a.`lastinformationdate`
                   , a.`laststatusdate`
                   , a.`laststatustype`
                   , a.`ptdeathdate`
                   , a.`ptdeathtype`
                
                   , a.`nextarrivaldate`
                   , a.`medtxintent`
                   , a.`originalmedtxagent`
                   , a.`medtxagent`
                   , a.`medtxagentnoparen`
                

            FROM `temp`.`PlaygroundTemp` a 
            JOIN arrivalidmapping b
            ON a.patientid = b.patientid and a.arrivaldate = b.arrivaldate
            WHERE
            a.`ArrivalDx` LIKE '%aml%'
            OR a.`ArrivalDx` LIKE '%mds%';
        
        /*
        QUICK ALTER not part of script
        Alter table caisis.playground ADD COLUMN `FirstArrivalDx` TEXT NULL FIRST
            , ADD COLUMN `arrivalkaryotype` TEXT NULL FIRST
            , Alter table caisis.playground ADD COLUMN `ArrivalKaryotypeDate`         DATETIME NULL FIRST
        
        */
        
        
        -- AML Categorization into Arrival Type
        UPDATE caisis.Playground
            SET ArrivalType = CASE
                    WHEN ArrivalDx NOT LIKE '%AML%' THEN 'Other'
                    WHEN ArrivalDx LIKE '%ND1%' THEN 'ND1/2'
                    WHEN ArrivalDx LIKE '%ND2%' THEN 'ND1/2'
                    WHEN ArrivalDx LIKE '%REL%' THEN 'REL'
                    WHEN ArrivalDx LIKE '%REF%' THEN 'REF'
                    ELSE 'Unknown'
                END ;
        
        -- Get patient Death, Name and Birthdate
        -- Refactor add in ethnic/race
        UPDATE caisis.playground a, caisis.vdatasetpatients b
            SET a.PtDeathDate = b.PtDeathDate
            , a.PtDeathType = b.PtDeathType
            , a.PtBirthDate = b.PtBirthDate
            , a.PtLastName = b.PtLastName
            WHERE a.PatientId = b.PatientId ;
        
        
        /*************************************************************************************
           Indexing
        */
        
        ALTER TABLE `caisis`.`playground` 
            ADD INDEX `PKEY`               (`PatientId` ASC, `ArrivalYear` ASC),
            ADD INDEX `PatientId`          (`PatientId` ASC),
            ADD INDEX `ArrivalDate`        (`ArrivalDate` ASC),
            ADD INDEX `MedTxAgent`         (`MedTxAgent`(45) ASC), 
            ADD INDEX `OriginalMedTxAgent` (`OriginalMedTxAgent`(45) ASC),
            ADD INDEX `MedTxAgentNoParen`  (`MedTxAgentNoParen`(45) ASC);
    """
    dosqlexecute(cnxdict)


def CreatePlaygroundMolecularTemplate(cnxdict):
    printtext('stack')
    cnxdict['sql'] = """
        DROP TABLE IF EXISTS caisis.`PlaygroundMolecularStru` ;
        CREATE TABLE caisis.`PlaygroundMolecularStru`
            SELECT b.`arrival_id`
            FROM `temp`.`PlaygroundTemp` a 
            JOIN arrivalidmapping b
            ON a.patientid = b.patientid and a.arrivaldate = b.arrivaldate
            WHERE
            a.`ArrivalDx` LIKE '%aml%'
            OR a.`ArrivalDx` LIKE '%mds%'
            OR a.`ArrivalDx` LIKE '%raeb%';
    """
    dosqlexecute(cnxdict)

    # add molecular test fields
    timepointlist   = ['Diagnosis','Arrival','Treatment','Response','Relapse']

    molecularfieldlist = ['LabGroupId`       INTEGER',
                          'LabTest`          TINYTEXT',
                          'SpecimenType`     TINYTEXT',
                          'LabDate`          DATETIME',
                          'LabResult`        VARCHAR(50)',
                          'LabResultText`    TINYTEXT',
                          'LabUnits`         TINYTEXT',
                          'Interpretation`   TINYTEXT',
                          'Summary`          TINYTEXT', ]

    testlist = copy.deepcopy(cnxdict['molelist'])
    testlist[testlist.index('FLT3')] = ['FLT3', ['BasesTest`       TINYTEXT',
                                  'Bases`           TINYTEXT',
                                  'RatioTest`       TINYTEXT',
                                  'Ratio`           TINYTEXT']]

    for timepoint in timepointlist:
        for test in testlist:
            molecularinsertlist = ''
            if test[0] == 'FLT3':  # add-on special fields if FLT3
                testname = test[0]
                currfieldlist = molecularfieldlist + test[1]
            else:
                testname = test
                currfieldlist = molecularfieldlist
            for currfield in currfieldlist:  #fields to insert in playground molecular table
                insertfield     = "{0}, ADD COLUMN `{1}{2}{3}".format(' '*12,testname,timepoint,currfield.lower())
                molecularinsertlist   = molecularinsertlist + insertfield + '\n'

            cnxdict['sql'] = """
                ALTER TABLE `caisis`.`PlaygroundMolecularStru` {0} ;
            """.format(molecularinsertlist.strip(', ').lower())
            dosqlexecute(cnxdict)

            cnxdict['sql'] = """
                DROP TABLE IF EXISTS caisis.`PlaygroundMolecular` ;
                CREATE TABLE caisis.`PlaygroundMolecular`
                    SELECT * FROM `caisis`.`PlaygroundMolecularStru`
            """
            dosqlexecute(cnxdict)


def CreatePlaygroundLabsTemplate(cnxdict):
    printtext('Program: ')
    cnxdict['sql'] = """
        DROP TABLE IF EXISTS caisis.`PlaygroundLabs` ;
        CREATE TABLE caisis.`PlaygroundLabs`
            SELECT b.`arrival_id`
            FROM `temp`.`PlaygroundTemp` a 
            JOIN arrivalidmapping b
            ON a.patientid = b.patientid and a.arrivaldate = b.arrivaldate
            WHERE
            a.`ArrivalDx` LIKE '%aml%'
            OR a.`ArrivalDx` LIKE '%mds%';
    """
    dosqlexecute(cnxdict)

    # add lab test fields
    timepointlist   = ['Diagnosis','Arrival','Treatment','Response','Relapse']
    labtestfieldlist =   ['LabGroupId`       INTEGER',
                          'LabTest`          TINYTEXT',
                          'SpecimenType`     TINYTEXT',
                          'LabDate`          DATETIME',
                          'LabResult`        VARCHAR(50)',
                          'LabResultText`    TINYTEXT',
                          'LabUnits`         TINYTEXT',
                          'Interpretation`   TINYTEXT',
                          'Summary`          TINYTEXT', ]
    testlist = cnxdict['lablist']

    for timepoint in timepointlist:
        currenttable = 'Playground{0}Labs'.format(timepoint)
        cnxdict['sql'] = """
            DROP TABLE IF EXISTS caisis.`{0}` ;
            CREATE TABLE caisis.`{0}`
                SELECT b.`arrival_id`
                FROM `temp`.`PlaygroundTemp` a 
                JOIN arrivalidmapping b
                ON a.patientid = b.patientid and a.arrivaldate = b.arrivaldate
                WHERE
                a.`ArrivalDx` LIKE '%aml%'
                OR a.`ArrivalDx` LIKE '%mds%';
        """.format(currenttable)
        dosqlexecute(cnxdict)

        for testname in testlist:
            labtestinsertlist = ''
            for currfield in labtestfieldlist:  #fields to insert in playground labs table
                insertfield     = "{0}, ADD COLUMN `{1}{2}{3}".format(' '*12,testname,timepoint,currfield)
                labtestinsertlist   = labtestinsertlist + insertfield + '\n'

            cnxdict['sql'] = """
                ALTER TABLE `caisis`.`{0}` {1} ;
            """.format(currenttable, labtestinsertlist.strip(', ').lower())
            dosqlexecute(cnxdict)


def AssociateTreatment(cnxdict):
    printtext('Program: ')
    cnxdict['sql'] = """
    /*************************************************************************************
    Find all treatments where a treatment was actually given
    */
    
    DROP TABLE IF EXISTS Temp.Treatment ;
    CREATE TABLE Temp.Treatment
        SELECT * FROM caisis.vdatasetmedicaltherapy
            where (medtxdisease like '%aml%' or medtxdisease like '%mds%')
            and upper(medtxagent) not in ('HU'
                , 'HU/PALLIATIVE'
                , 'PALLIATIVE CARE'
                , 'NO TREATMENT'
                , 'HOSPICE'
                , 'UNKNOWN'
            )
            AND MedTxIntent NOT IN ( 'Consolidation Regimen'
                , 'Immunosuppressive Regimen'
                , 'Maintenance Regimen'
                , 'Other Regimen'
                , 'Palliative Regimen'
                , 'Consolidation'
                , 'Immunosuppressive'
                );
    
    /*************************************************************************************
        Arrivals with Treatment found
    */
    UPDATE caisis.`Playground` a, temp.Treatment b
        SET a.TreatmentStartDate = b.MedTxDate
            , a.Treatment = b.MedTxAgent
            , a.MedTxAgent = b.MedTxAgent
            , a.TreatmentIntent = b.MedTxIntent
            , a.TreatmentCount = a.TreatmentCount + 1
            , a.OtherTreatment = ''
        WHERE a.PatientId = b.PatientId 
            AND ((b.MedTxDate BETWEEN a.ArrivalDate AND a.NextArrivalDate 
                  AND  b.MedTxDate BETWEEN a.ArrivalDate and DATE_ADD(a.ArrivalDate, INTERVAL 100 DAY))
            OR  b.MedTxDate BETWEEN a.ArrivalDate and DATE_ADD(a.ArrivalDate, INTERVAL 100 DAY)) ;
    
    
    UPDATE caisis.`Playground` a, temp.Treatment b
        SET a.TreatmentStartDate = b.MedTxDate 
            , a.Treatment = b.MedTxAgent
            , a.MedTxAgent = b.MedTxAgent
            , a.TreatmentIntent = b.MedTxIntent
            , a.TreatmentCount = a.TreatmentCount + 1
            , a.OtherTreatment = CONCAT(a.OtherTreatment, a.Treatment, ' on ', a.TreatmentStartDate, CHAR(13))
        WHERE a.PatientId = b.PatientId 
            AND b.MedTxDate < a.TreatmentStartDate
            AND ((b.MedTxDate BETWEEN a.ArrivalDate AND a.NextArrivalDate 
                  AND  b.MedTxDate BETWEEN a.ArrivalDate and DATE_ADD(a.ArrivalDate, INTERVAL 100 DAY))
            OR  b.MedTxDate BETWEEN a.ArrivalDate and DATE_ADD(a.ArrivalDate, INTERVAL 100 DAY)) ;
    
    
    UPDATE caisis.`Playground` a, temp.Treatment b
        SET a.TreatmentStartDate = b.MedTxDate 
            , a.Treatment = b.MedTxAgent
            , a.MedTxAgent = b.MedTxAgent
            , a.TreatmentIntent = b.MedTxIntent
            , a.TreatmentCount = a.TreatmentCount + 1
            , a.OtherTreatment = CONCAT(a.OtherTreatment, a.Treatment, ' on ', a.TreatmentStartDate, CHAR(13))
        WHERE a.PatientId = b.PatientId 
            AND b.MedTxDate < a.TreatmentStartDate
            AND ((b.MedTxDate BETWEEN a.ArrivalDate AND a.NextArrivalDate 
                  AND  b.MedTxDate BETWEEN a.ArrivalDate and DATE_ADD(a.ArrivalDate, INTERVAL 100 DAY))
            OR  b.MedTxDate BETWEEN a.ArrivalDate and DATE_ADD(a.ArrivalDate, INTERVAL 100 DAY)) ;
    """
    dosqlexecute(cnxdict)
    return


def AssociateNonTreatment(cnxdict):
    printtext('Program: ')
    cnxdict['sql'] = """
        /*************************************************************************************
        Find all treatments where no treatment was given
        */
        DROP TABLE IF EXISTS Temp.NonTreatment ;
        CREATE TABLE Temp.NonTreatment
            SELECT * FROM caisis.vdatasetmedicaltherapy
                where (medtxdisease like '%aml%' or medtxdisease like '%mds%')
                and upper(medtxagent) in ('HU'
                    , 'HU/PALLIATIVE'
                    , 'PALLIATIVE CARE'
                    , 'NO TREATMENT'
                    , 'HOSPICE'
                    , 'UNKNOWN'
                    ) 
                    OR MedTxIntent IN ( 'Consolidation Regimen'
                    , 'Immunosuppressive Regimen'
                    , 'Maintenance Regimen'
                    , 'Other Regimen'
                    , 'Palliative Regimen'
                    , 'Consolidation'
                    , 'Immunosuppressive'
                    );
                    
        /*************************************************************************************
            Arrivals where no Treatment found
        */
        UPDATE caisis.`Playground` a, temp.NonTreatment b
            SET a.TreatmentStartDate = b.MedTxDate
                , a.Treatment = b.MedTxAgent
                , a.MedTxAgent = b.MedTxAgent
                , a.TreatmentIntent = concat('Non-Treatment: ',b.MedTxIntent)
                , a.TreatmentCount = a.TreatmentCount + 1
                , a.OtherTreatment = ''
            WHERE a.PatientId = b.PatientId
                AND a.TreatmentStartDate IS NULL
                AND a.TreatmentIntent IS NULL 
                AND a.MedTxAgent IS NULL
                AND ((b.MedTxDate BETWEEN a.ArrivalDate AND a.NextArrivalDate 
                      AND  b.MedTxDate BETWEEN a.ArrivalDate and DATE_ADD(a.ArrivalDate, INTERVAL 100 DAY))
                OR  b.MedTxDate BETWEEN a.ArrivalDate and DATE_ADD(a.ArrivalDate, INTERVAL 100 DAY)) ;
        
        UPDATE caisis.`Playground` a, temp.NonTreatment b
            SET a.TreatmentStartDate = b.MedTxDate 
                , a.Treatment = b.MedTxAgent
                , a.MedTxAgent = b.MedTxAgent
                , a.TreatmentIntent = concat('Non-Treatment: ',b.MedTxIntent)
                , a.TreatmentCount = a.TreatmentCount + 1
                , a.OtherTreatment = CONCAT(a.OtherTreatment, a.Treatment, ' on ', a.TreatmentStartDate, CHAR(13))
            WHERE a.PatientId = b.PatientId
                AND a.TreatmentStartDate IS NULL
                AND a.TreatmentIntent IS NULL 
                AND a.MedTxAgent IS NULL
                AND b.MedTxDate < a.TreatmentStartDate
                AND ((b.MedTxDate BETWEEN a.ArrivalDate AND a.NextArrivalDate 
                      AND  b.MedTxDate BETWEEN a.ArrivalDate and DATE_ADD(a.ArrivalDate, INTERVAL 100 DAY))
                OR  b.MedTxDate BETWEEN a.ArrivalDate and DATE_ADD(a.ArrivalDate, INTERVAL 100 DAY)) ;
        
        UPDATE caisis.`Playground` a, temp.NonTreatment b
            SET a.TreatmentStartDate = b.MedTxDate 
                , a.Treatment = b.MedTxAgent
                , a.MedTxAgent = b.MedTxAgent
                , a.TreatmentIntent = concat('Non-Treatment: ',b.MedTxIntent)
                , a.TreatmentCount = a.TreatmentCount + 1
                , a.OtherTreatment = CONCAT(a.OtherTreatment, a.Treatment, ' on ', a.TreatmentStartDate, CHAR(13))
            WHERE a.PatientId = b.PatientId
                AND a.TreatmentStartDate IS NULL
                AND a.TreatmentIntent IS NULL 
                AND a.MedTxAgent IS NULL
                AND b.MedTxDate < a.TreatmentStartDate
                AND ((b.MedTxDate BETWEEN a.ArrivalDate AND a.NextArrivalDate 
                      AND  b.MedTxDate BETWEEN a.ArrivalDate and DATE_ADD(a.ArrivalDate, INTERVAL 100 DAY))
                OR  b.MedTxDate BETWEEN a.ArrivalDate and DATE_ADD(a.ArrivalDate, INTERVAL 100 DAY)) ;
    """
    dosqlexecute(cnxdict)
    return


def RemoveParentheticalTreatmentStatement(cnxdict):
    printtext('Program: ')
    cnxdict['sql']="""
        /************************************************************************
            Making case variations and versions without the extra parens
        */
        UPDATE caisis.`Playground`
            SET MedTxAgent = LTRIM(RTRIM(UPPER(MedTxAgent)))
                , OriginalMedTxAgent = MedTxAgent ;
        UPDATE caisis.`Playground`
            SET MedTxAgentNoParen  = RTRIM(SUBSTRING(MedTxAgent,1,LOCATE('(',MedTxAgent)-1)) ;
    """
    dosqlexecute(cnxdict)
    return


def AssociateBackbone(cnxdict):
    printtext('Program: ')
    """
        In this section mapping MedTxAgent to the backbone, or common name
    """
    cnxdict['sql']="""
    /************************************************************************
        In this section mapping MedTxAgent to the backbone, or common name
    */
    
    /************************************************************************
        Join to OriginalProtocol in backbonemapping
    */
    UPDATE caisis.`Playground` a, caisis.backbonemapping b
        SET   a.BackBoneType       = left(b.BackBoneType,10)
            , a.BackBoneName       = b.BackBoneName
            , a.BackBoneAddOn      = b.BackBoneAddOn
        where a.MedTxAgent         = b.OriginalProtocol
            AND a.BackBoneName IS NULL 
            AND TreatmentIntent NOT LIKE '%Non-Treatment:%';
    
    UPDATE caisis.`Playground` a, caisis.backbonemapping b
        SET   a.BackBoneType       = left(b.BackBoneType,10)
            , a.BackBoneName       = b.BackBoneName
            , a.BackBoneAddOn      = b.BackBoneAddOn
        where a.OriginalMedTxAgent = b.OriginalProtocol
            AND a.BackBoneName IS NULL 
            AND TreatmentIntent NOT LIKE '%Non-Treatment:%';
    
    UPDATE caisis.`Playground` a, caisis.backbonemapping b
        SET   a.BackBoneType       = left(b.BackBoneType,10)
            , a.BackBoneName       = b.BackBoneName
            , a.BackBoneAddOn      = b.BackBoneAddOn
        where a.MedTxAgentNoParen  = b.OriginalProtocol
            AND a.BackBoneName IS NULL 
            AND TreatmentIntent NOT LIKE '%Non-Treatment:%';
    
    /************************************************************************
        Join to upper case version OriginalMedTxAgent in backbonemapping
    */
    UPDATE caisis.`Playground` a, caisis.backbonemapping b
        SET   a.BackBoneType       = left(b.BackBoneType,10)
            , a.BackBoneName       = b.BackBoneName
            , a.BackBoneAddOn      = b.BackBoneAddOn
        where a.MedTxAgent         = b.OriginalMedTxAgent
            AND a.BackBoneName IS NULL 
            AND TreatmentIntent NOT LIKE '%Non-Treatment:%';
    
    UPDATE caisis.`Playground` a, caisis.backbonemapping b
        SET   a.BackBoneType       = left(b.BackBoneType,10)
            , a.BackBoneName       = b.BackBoneName
            , a.BackBoneAddOn      = b.BackBoneAddOn
        where a.OriginalMedTxAgent = b.OriginalMedTxAgent
            AND a.BackBoneName IS NULL 
            AND TreatmentIntent NOT LIKE '%Non-Treatment:%';
    
    UPDATE caisis.`Playground` a, caisis.backbonemapping b
        SET   a.BackBoneType       = left(b.BackBoneType,10)
            , a.BackBoneName       = b.BackBoneName
            , a.BackBoneAddOn      = b.BackBoneAddOn
        where a.MedTxAgentNoParen  = b.OriginalMedTxAgent
            AND a.BackBoneName IS NULL 
            AND TreatmentIntent NOT LIKE '%Non-Treatment:%';
    
    /************************************************************************
        Join to upper case MedTxAgent
    */UPDATE caisis.`Playground` a, caisis.backbonemapping b
        SET   a.BackBoneType       = left(b.BackBoneType,10)
            , a.BackBoneName       = b.BackBoneName
            , a.BackBoneAddOn      = b.BackBoneAddOn
        where a.OriginalMedTxAgent = b.MedTxAgent
            AND a.BackBoneName IS NULL 
            AND TreatmentIntent NOT LIKE '%Non-Treatment:%';
    
    UPDATE caisis.`Playground` a, caisis.backbonemapping b
        SET   a.BackBoneType       = left(b.BackBoneType,10)
            , a.BackBoneName       = b.BackBoneName
            , a.BackBoneAddOn      = b.BackBoneAddOn
        where a.MedTxAgent         = b.MedTxAgent
            AND a.BackBoneName IS NULL 
            AND TreatmentIntent NOT LIKE '%Non-Treatment:%';
    
    UPDATE caisis.`Playground` a, caisis.backbonemapping b
        SET   a.BackBoneType       = left(b.BackBoneType,10)
            , a.BackBoneName       = b.BackBoneName
            , a.BackBoneAddOn      = b.BackBoneAddOn
        where a.MedTxAgentNoParen  = b.MedTxAgent
            AND a.BackBoneName IS NULL 
            AND TreatmentIntent NOT LIKE '%Non-Treatment:%';
    
    /************************************************************************
        Join to upper case MedTxAgentNoParen
    */UPDATE caisis.`Playground` a, caisis.backbonemapping b
        SET   a.BackBoneType       = left(b.BackBoneType,10)
            , a.BackBoneName       = b.BackBoneName
            , a.BackBoneAddOn      = b.BackBoneAddOn
        where a.OriginalMedTxAgent = b.MedTxAgentNoParen
            AND a.BackBoneName IS NULL 
            AND TreatmentIntent NOT LIKE '%Non-Treatment:%';
    
    UPDATE caisis.`Playground` a, caisis.backbonemapping b
        SET   a.BackBoneType       = left(b.BackBoneType,10)
            , a.BackBoneName       = b.BackBoneName
            , a.BackBoneAddOn      = b.BackBoneAddOn
        where a.MedTxAgent         = b.MedTxAgentNoParen
            AND a.BackBoneName IS NULL 
            AND TreatmentIntent NOT LIKE '%Non-Treatment:%';
    
    UPDATE caisis.`Playground` a, caisis.backbonemapping b
        SET   a.BackBoneType       = left(b.BackBoneType,10)
            , a.BackBoneName       = b.BackBoneName
            , a.BackBoneAddOn      = b.BackBoneAddOn
        where a.MedTxAgentNoParen  = b.MedTxAgentNoParen
            AND a.BackBoneName IS NULL 
            AND TreatmentIntent NOT LIKE '%Non-Treatment:%';
    """
    dosqlexecute(cnxdict)
    return


def AssociateResponse(cnxdict):
    printtext('Program: ')
    cnxdict['sql']="""
        /**********************************************************************************************************
            Find all responses entered that occur after a patient treatment
        */
        UPDATE caisis.Playground
            SET Response = ''
                , ResponseDate = NULL
                , DaysToResponse = NULL;
        
        DROP TABLE IF EXISTS temp.Response ;
        CREATE TABLE Temp.Response
            SELECT a.`PtMRN`
                , a.`PatientId`
                , b.ArrivalDate 
                , b.ArrivalDx
                , b.TreatmentStartDate
                , b.Treatment
                , b.BackBoneName
                , b.BackBoneAddOn
                , a.StatusDisease AS ResponseDisease
                , a.Status AS Response
                , a.StatusDate AS ResponseDate
                , b.NextArrivalDate
                , TIMESTAMPDIFF(DAY,b.TreatmentStartDate,a.StatusDate) AS DaysToResponse
                , a.StatusDate BETWEEN b.TreatmentStartDate and DATE_ADD(b.TreatmentStartDate, INTERVAL 100 DAY) AS ResponseWithin100Days
                , a.StatusDate < b.NextArrivalDate OR b.NextArrivalDate IS NULL AS ResponseBeforeNextArrival
                , 0 AS Used
                FROM caisis.vdatasetstatus a
                    LEFT JOIN Caisis.Playground b
                        ON a.PatientId = b.PatientId
                WHERE (a.StatusDisease LIKE '%aml%')
                    AND (a.Status like '%unk response%'
                    OR a.Status In (
                    '+CRp'
                    , 'CR'
                    , 'CR CYTO MRD'
                    , 'CR MRD'
                    , 'CR-MRD'
                    , 'Cri'
                    , 'CRi CYTO MRD'
                    , 'Cri MRD'
                    , 'CRi-MRD'
                    , 'CRp'
                    , 'CRp CYTO MRD'
                    , 'CRp EXD MRD'
                    , 'CRp MRD'
                    , 'PR'
                    , 'Resistant'
                    , 'Refractory'
                    , 'Death'
                    , 'Response:  Death'
                    , 'Response: Death'
                    , 'Persistent Disease'
                    , 'Dead of Disease'
                    , 'Response Not Categorized'
                )) 
                AND b.TreatmentStartDate <= a.StatusDate;
        
        /*
        SELECT * FROM temp.Response ;
        SELECT * FROM temp.Response WHERE DaysToResponse > 100;
        SELECT Response, LENGTH(Response), Count(*) FROM temp.Response GROUP BY Response;
        SELECT ArrivalDx,  Count(*) FROM temp.Response GROUP BY Response;
        */
           
        
        UPDATE caisis.Playground a, temp.response b
            SET a.Response = b.Response
                , a.ResponseDate = b.ResponseDate
                , a.DaysToResponse = b.DaysToResponse
                , b.Used = 1
                WHERE a.PtMRN       = b.PtMRN
                AND   a.ArrivalDate = b.ArrivalDate
                AND   a.ArrivalDx   = b.ArrivalDx 
                AND   a.BackBoneName IS NOT NULL
                AND   b.ResponseWithin100Days ;
        
        /*
        Do this 2 more times
        */
        UPDATE caisis.Playground a, temp.response b
            SET a.Response = b.Response
                , a.ResponseDate = b.ResponseDate
                , a.DaysToResponse = b.DaysToResponse
                , b.Used = 1
                WHERE a.PtMRN       = b.PtMRN
                AND   a.ArrivalDate = b.ArrivalDate
                AND   a.ArrivalDx   = b.ArrivalDx 
                AND   a.BackBoneName IS NOT NULL
                AND   b.DaysToResponse < a.DaysToResponse ;        
        
        UPDATE caisis.Playground a, temp.response b
            SET a.Response = b.Response
                , a.ResponseDate = b.ResponseDate
                , a.DaysToResponse = b.DaysToResponse
                , b.Used = 1
                WHERE a.PtMRN       = b.PtMRN
                AND   a.ArrivalDate = b.ArrivalDate
                AND   a.ArrivalDx   = b.ArrivalDx 
                AND   a.BackBoneName IS NOT NULL
                AND   b.DaysToResponse < a.DaysToResponse ;        
                    
        
        /*
            Now look to see if there is a response for all the orphaned AZA or DECICTABINE Treatments
        */
        UPDATE caisis.Playground a, temp.response b
            SET a.Response = b.Response
                , a.ResponseDate = b.ResponseDate
                , a.DaysToResponse = b.DaysToResponse
                , b.Used = 1
                WHERE a.PtMRN       = b.PtMRN
                AND   a.ArrivalDate = b.ArrivalDate
                AND   a.ArrivalDx   = b.ArrivalDx 
                AND   b.BackBoneName IN ('AZA','DECITABINE') 
                AND   a.ResponseDate IS NULL 
                AND   a.TreatmentStartDate < b.ResponseDate 
                AND   (b.ResponseDate < a.NextArrivalDate OR a.NextArrivalDate IS NULL);
        
        
        UPDATE caisis.Playground a, temp.response b
            SET a.Response = b.Response
                , a.ResponseDate = b.ResponseDate
                , a.DaysToResponse = b.DaysToResponse
                , b.Used = 1
                WHERE a.PtMRN       = b.PtMRN
                AND   a.ArrivalDate = b.ArrivalDate
                AND   a.ArrivalDx   = b.ArrivalDx 
                AND   b.BackBoneName IN ('AZA','DECITABINE') 
                AND   a.ResponseDate IS NULL 
                AND   a.TreatmentStartDate < b.ResponseDate 
                AND   (b.ResponseDate < a.NextArrivalDate OR a.NextArrivalDate IS NULL)
                AND   b.ResponseDate < a.ResponseDate 
                ;
        
        
        UPDATE caisis.Playground
            SET CRNumber = CASE
                WHEN Response IN ( 'CR'
                    , 'CR CYTO MRD'
                    , 'CR MRD'
                    , 'CR-MRD'
                    , 'Cri'
                    , 'CRi CYTO MRD'
                    , 'CRi MRD'
                    , 'CRp'
                    , 'CRp MRD'
                ) THEN CASE
                         WHEN ArrivalDx LIKE '%ND1%' AND ArrivalDx LIKE '%AML%' THEN '1'
                         WHEN ArrivalDx LIKE '%ND2%' AND ArrivalDx LIKE '%AML%' THEN '1'
                         WHEN ArrivalDx LIKE '%REF%' AND ArrivalDx LIKE '%AML%' THEN '1'
                         WHEN ArrivalDx LIKE '%REL%' AND ArrivalDx LIKE '%AML%' AND MedTxIntent LIKE '%1%' THEN '2'
                         WHEN ArrivalDx LIKE '%REL%' AND ArrivalDx LIKE '%AML%' THEN '1+'
                         ELSE 'Unknown'
                     END
                ELSE ''
             END ;
    """
    dosqlexecute(cnxdict)
    return


def AssociateResponseFlow(cnxdict):
    printtext('Program: ')
    cnxdict['sql'] = """
        /***********************************************************************************
            Get blasts by FLOW at response
        */
        DROP TABLE IF EXISTS temp.ResponseFlowblasts ;
        CREATE TABLE temp.ResponseFlowblasts
            SELECT b.*
                FROM caisis.vdatasetpathtest b join caisis.playground a
                on a.PtMRN = b.PtMRN
                AND timestampdiff(day,a.ResponseDate,b.DateObtained) between -7 and 7
                WHERE b.PathResult IS NOT NULL
                AND b.PathResult NOT IN ('ND', 'N/A')
                AND PathTest IN ( 'Blasts (FLOW POS/NEG)'
                    , 'Blasts (FLOW)'
                    , 'Blasts (PB FLOW)'
                    , 'Blasts (PBFLOW POS/NEG)'
                    , 'Blasts (PBFLOW)'
                    , 'Evidence of Disease (FLOW)'
                    , 'Evidence of Disease (PB FLOW)'
                    , 'PB Blast (FLOW)'
                );

        /*
        SELECT * FROM temp.ResponseFlowblasts ;
        */



        /***********************************************************************************
        Update the result table to contain the Flow blasts at response, when the flow date
        exactly matches response date


        Looking at flow not entered as a numeric value
        SELECT PathResult, count(*) FROM temp.ResponseFlowblasts where PathResult RLIKE "~|-|[?]|<|>"  GROUP BY 1;

        SELECT * FROM temp.ResponseFlowblasts where          
                CASE
                     WHEN PathResult IS NULL             THEN False  # Nothing to map
                     WHEN PathResult = ''                THEN False  # Nothing to map
                     WHEN PathResult RLIKE "~|-|[?]|<|>" THEN True  # a dash or approximate symbol
                     WHEN PathResult RLIKE '[a-zA-Z]'    THEN False  # contains letters
                     WHEN PathResult RLIKE '\d*[\.\d]?'  THEN False
                     ELSE False
                 END ;

        SELECT PathResult FROM temp.ResponseFlowblasts where PathResult RLIKE "~|-|[?]|<" ;

        */


        /***********************************************************************************
        Update the result table to contain the Flow blasts at response, when the flow date
        matches response date within 7 days
        AND timestampdiff(day,a.ResponseDate,b.DateObtained) between -7 and 7

        Note that blasts by flow can be recorded several ways in the database, as such 
        several updates are below in order to handle the different ways the data
        are entered.

        */

        UPDATE caisis.Playground a, temp.ResponseFlowblasts b
            SET a.ResponseFlowDate     = b.DateObtained
            , a.ResponseFlowSource     = b.PathTestCategory
            , a.ResponseFlowBlasts =
                 CASE
                     WHEN b.PathResult IS NULL             THEN NULL  # Nothing to map
                     WHEN b.PathResult = ''                THEN NULL  # Nothing to map
                     WHEN b.PathResult RLIKE "~|-|[?]|<|>" THEN NULL  # a dash or approximate symbol
                     WHEN b.PathResult RLIKE '[a-zA-Z]'    THEN NULL  # contains letters
                     WHEN b.PathResult RLIKE '\d*[\.\d]?'  THEN CAST(b.PathResult AS DECIMAL(7,3))  
                     ELSE CAST(b.PathResult AS DECIMAL(7,3))
                 END
            , a.ResponseFlowBlastsText =
                CASE
                    WHEN b.PathResult = '-' THEN 'negative'
                    WHEN b.PathResult = '+' THEN 'positive'
                    WHEN b.PathResult = '<' THEN 'decreased'
                    WHEN b.PathResult = '>' THEN 'increased'
                    WHEN b.PathResult = '~' THEN 'unchanged'
                    WHEN b.PathResult = '?' THEN 'unknown'
                    ELSE LEFT(b.PathResult,45)
                END
            WHERE b.PtMRN IS NOT NULL
                AND a.ResponseFlowBlastsText IS NULL
                AND a.PtMRN = b.PtMRN
                AND a.ResponseDate = b.DateObtained
                AND b.PathTest IN ('Blasts (FLOW)') ;

        /***********************************************************************************
        Update the result table to contain the Flow blasts at response, when the flow date
        matches response date within 7 days
        AND timestampdiff(day,a.ResponseDate,b.DateObtained) between -7 and 7

        SELECT timestampdiff(day,a.ResponseDate,b.DateObtained),
            a.PtMRN, a.ResponseDate, b.DateObtained, b.PathResult
            From temp.result2 a join temp.flowblasts b
                on a.PtMRN = b.PtMRN
                WHERE abs(timestampdiff(day,a.ResponseDate,b.DateObtained)) between 1 and 7;
        ***********************************************************************************/

        UPDATE caisis.Playground a, temp.ResponseFlowblasts b
            SET a.ResponseFlowDate     = b.DateObtained
            , a.ResponseFlowSource     = b.PathTestCategory
            , a.ResponseFlowBlasts =
                 CASE
                     WHEN b.PathResult IS NULL             THEN NULL  # Nothing to map
                     WHEN b.PathResult = ''                THEN NULL  # Nothing to map
                     WHEN b.PathResult RLIKE "~|-|[?]|<|>" THEN NULL  # a dash or approximate symbol
                     WHEN b.PathResult RLIKE '[a-zA-Z]'    THEN NULL  # contains letters
                     WHEN b.PathResult RLIKE '\d*[\.\d]?'  THEN CAST(b.PathResult AS DECIMAL(7,3))  
                     ELSE CAST(b.PathResult AS DECIMAL(7,3))
                 END
            , a.ResponseFlowBlastsText =
                CASE
                    WHEN b.PathResult = '-' THEN 'negative'
                    WHEN b.PathResult = '+' THEN 'positive'
                    WHEN b.PathResult = '<' THEN 'decreased'
                    WHEN b.PathResult = '>' THEN 'increased'
                    WHEN b.PathResult = '~' THEN 'unchanged'
                    WHEN b.PathResult = '?' THEN 'unknown'
                    ELSE LEFT(b.PathResult,45)
                END
            WHERE b.PtMRN IS NOT NULL
                AND a.ResponseFlowBlastsText IS NULL
                AND a.PtMRN = b.PtMRN
                AND timestampdiff(day,a.ResponseDate,b.DateObtained) between -7 and 7
                AND b.PathTest IN ('Blasts (FLOW)') ;


        /*
        Error Code: 1054. Unknown column 'a.FlowBlastsText' in 'field list'
        */



        /***********************************************************************************
        Update the result table to contain the Flow blasts at response, when the flow date
        matches response date exactly
        ***********************************************************************************/

        UPDATE caisis.Playground a, temp.ResponseFlowblasts b
            SET a.ResponseFlowDate     = b.DateObtained
            , a.ResponseFlowSource     = b.PathTestCategory
            , a.ResponseFlowBlasts =
                 CASE
                     WHEN b.PathResult IS NULL             THEN NULL  # Nothing to map
                     WHEN b.PathResult = ''                THEN NULL  # Nothing to map
                     WHEN b.PathResult RLIKE "~|-|[?]|<|>" THEN NULL  # a dash or approximate symbol
                     WHEN b.PathResult RLIKE '[a-zA-Z]'    THEN NULL  # contains letters
                     WHEN b.PathResult RLIKE '\d*[\.\d]?'  THEN CAST(b.PathResult AS DECIMAL(7,3))  
                     ELSE CAST(b.PathResult AS DECIMAL(7,3))
                 END
            , a.ResponseFlowBlastsText =
                CASE
                    WHEN b.PathResult = '-' THEN 'negative'
                    WHEN b.PathResult = '+' THEN 'positive'
                    WHEN b.PathResult = '<' THEN 'decreased'
                    WHEN b.PathResult = '>' THEN 'increased'
                    WHEN b.PathResult = '~' THEN 'unchanged'
                    WHEN b.PathResult = '?' THEN 'unknown'
                    ELSE LEFT(b.PathResult,45)
                END
            WHERE b.PtMRN IS NOT NULL
                AND a.ResponseFlowBlastsText IS NULL
                AND a.PtMRN = b.PtMRN
                AND a.ResponseDate = b.DateObtained
                AND b.PathTest IN ( 'Blasts (PB FLOW)'
                    , 'Blasts (PBFLOW)'
                    , 'PB Blast (FLOW)'
                    ) ;

        /***********************************************************************************
        Update the result table to contain the Flow blasts at response, when the flow date
        matches response date within 7 days
        ***********************************************************************************/

        UPDATE caisis.Playground a, temp.ResponseFlowblasts b
            SET a.ResponseFlowDate     = b.DateObtained
            , a.ResponseFlowSource     = b.PathTestCategory
            , a.ResponseFlowBlasts =
                 CASE
                     WHEN b.PathResult IS NULL             THEN NULL  # Nothing to map
                     WHEN b.PathResult = ''                THEN NULL  # Nothing to map
                     WHEN b.PathResult RLIKE "~|-|[?]|<|>" THEN NULL  # a dash or approximate symbol
                     WHEN b.PathResult RLIKE '[a-zA-Z]'    THEN NULL  # contains letters
                     WHEN b.PathResult RLIKE '\d*[\.\d]?'  THEN CAST(b.PathResult AS DECIMAL(7,3))  
                     ELSE CAST(b.PathResult AS DECIMAL(7,3))
                 END
            , a.ResponseFlowBlastsText =
                CASE
                    WHEN b.PathResult = '-' THEN 'negative'
                    WHEN b.PathResult = '+' THEN 'positive'
                    WHEN b.PathResult = '<' THEN 'decreased'
                    WHEN b.PathResult = '>' THEN 'increased'
                    WHEN b.PathResult = '~' THEN 'unchanged'
                    WHEN b.PathResult = '?' THEN 'unknown'
                    ELSE LEFT(b.PathResult,45)
                END
            WHERE b.PtMRN IS NOT NULL
                AND a.ResponseFlowBlastsText IS NULL
                AND a.PtMRN = b.PtMRN
                AND timestampdiff(day,a.ResponseDate,b.DateObtained) between -7 and 7
                AND b.PathTest IN ( 'Blasts (PB FLOW)'
                    , 'Blasts (PBFLOW)'
                    , 'PB Blast (FLOW)'
                    ) ;

        /***********************************************************************************
        Update the result table to contain the Flow blasts at response, when the flow date
        matches response date exactly
        ***********************************************************************************/

        UPDATE caisis.Playground a, temp.ResponseFlowblasts b
            SET a.ResponseFlowDate     = b.DateObtained
            , a.ResponseFlowSource     = b.PathTestCategory
            , a.ResponseFlowBlasts =
                 CASE
                     WHEN b.PathResult IS NULL             THEN NULL  # Nothing to map
                     WHEN b.PathResult = ''                THEN NULL  # Nothing to map
                     WHEN b.PathResult RLIKE "~|-|[?]|<|>" THEN NULL  # a dash or approximate symbol
                     WHEN b.PathResult RLIKE '[a-zA-Z]'    THEN NULL  # contains letters
                     WHEN b.PathResult RLIKE '\d*[\.\d]?'  THEN CAST(b.PathResult AS DECIMAL(7,3))  
                     ELSE CAST(b.PathResult AS DECIMAL(7,3))
                 END
            , a.ResponseFlowBlastsText =
                CASE
                    WHEN b.PathResult = '-' THEN 'negative'
                    WHEN b.PathResult = '+' THEN 'positive'
                    WHEN b.PathResult = '<' THEN 'decreased'
                    WHEN b.PathResult = '>' THEN 'increased'
                    WHEN b.PathResult = '~' THEN 'unchanged'
                    WHEN b.PathResult = '?' THEN 'unknown'
                    ELSE LEFT(b.PathResult,45)
                END
            WHERE b.PtMRN IS NOT NULL
                AND a.ResponseFlowBlastsText IS NULL
                AND a.PtMRN = b.PtMRN
                AND a.ResponseDate = b.DateObtained
                AND b.PathTest IN ( 'Blasts (FLOW POS/NEG)'
                    , 'Blasts (PBFLOW POS/NEG)'
                    , 'Evidence of Disease (FLOW)'
                    , 'Evidence of Disease (PB FLOW)'
                );

        /***********************************************************************************
        Update the result table to contain the Flow blasts at response, when the flow date
        matches response date within 7 days
        AND timestampdiff(day,a.ResponseDate,b.DateObtained) between -7 and 7
        ***********************************************************************************/


        UPDATE caisis.Playground a, temp.ResponseFlowblasts b
            SET a.ResponseFlowDate     = b.DateObtained
            , a.ResponseFlowSource     = b.PathTestCategory
            , a.ResponseFlowBlasts =
                 CASE
                     WHEN b.PathResult IS NULL             THEN NULL  # Nothing to map
                     WHEN b.PathResult = ''                THEN NULL  # Nothing to map
                     WHEN b.PathResult RLIKE "~|-|[?]|<|>" THEN NULL  # a dash or approximate symbol
                     WHEN b.PathResult RLIKE '[a-zA-Z]'    THEN NULL  # contains letters
                     WHEN b.PathResult RLIKE '\d*[\.\d]?'  THEN CAST(b.PathResult AS DECIMAL(7,3))  
                     ELSE CAST(b.PathResult AS DECIMAL(7,3))
                 END
            , a.ResponseFlowBlastsText =
                CASE
                    WHEN b.PathResult = '-' THEN 'negative'
                    WHEN b.PathResult = '+' THEN 'positive'
                    WHEN b.PathResult = '<' THEN 'decreased'
                    WHEN b.PathResult = '>' THEN 'increased'
                    WHEN b.PathResult = '~' THEN 'unchanged'
                    WHEN b.PathResult = '?' THEN 'unknown'
                    ELSE LEFT(b.PathResult,45)
                END
            WHERE b.PtMRN IS NOT NULL
                AND a.ResponseFlowBlastsText IS NULL
                AND a.PtMRN = b.PtMRN
                AND timestampdiff(day,a.ResponseDate,b.DateObtained) between -7 and 7
                AND b.PathTest IN ( 'Blasts (FLOW POS/NEG)'
                    , 'Blasts (PBFLOW POS/NEG)'
                    , 'Evidence of Disease (FLOW)'
                    , 'Evidence of Disease (PB FLOW)'
                );

        /***********************************************************************************
        SELECT count(*) FROM caisis.Playground WHERE NOT ResponseFlowBlasts IS NULL;
        SELECT * FROM caisis.Playground  WHERE ResponseFlowDate IS NOT NULL;
        */
    """
    dosqlexecute(cnxdict)
    return


def AssociateRelapse(cnxdict):
    printtext('Program: ')
    cnxdict['sql']="""
        /***********************************************************************************
            Figure out if patients have relapsed since treatment or between arrivals
        */
        SELECT * FROM caisis.Playground  ;
        SELECT Status, count(*) 
            FROM caisis.vdatasetstatus 
            WHERE status like '%relapse%' 
            GROUP BY status ;
        
        DROP TABLE IF EXISTS temp.RelapseStatus ;
        CREATE TABLE temp.RelapseStatus
            SELECT PatientId
                , Status AS RelapseType
                , StatusDate AS RelapseDate
                , StatusDisease AS RelapseDisease
                , StatusNotes AS RelapseNotes
                FROM caisis.vdatasetstatus a
                WHERE status like '%relapse%';
        
        
        DROP TABLE IF EXISTS temp.EarliestRelapse ;
        CREATE TABLE temp.EarliestRelapse
            SELECT PatientId, MIN(RelapseDate) AS EarliestRelapseDate
                FROM temp.RelapseStatus
                WHERE RelapseDate IS NOT NULL 
                GROUP BY PatientId ;
        
        /*
        SELECT * FROM temp.EarliestRelapse ;
        SELECT PtMRN FROM temp.RelapseStatus GROUP BY 1;
        
        UPDATE caisis.Playground a
            SET a.RelapseType  = NULL
            , a.RelapseDate    = NULL
            , a.RelapseDisease = NULL
            , a.RelapseNotes   = '' ;
        */
        
        UPDATE caisis.Playground  a, temp.RelapseStatus b
            SET a.RelapseType  = b.RelapseType
            , a.RelapseDate    = b.RelapseDate
            , a.RelapseDisease = b.RelapseDisease
            , a.RelapseNotes   = b.RelapseNotes
            WHERE a.PatientId = b.PatientId
                AND b.RelapseDate BETWEEN a.ResponseDate and a.NextArrivalDate ;
        
        UPDATE caisis.Playground  a, temp.RelapseStatus b
            SET a.RelapseType  = b.RelapseType
            , a.RelapseDate    = b.RelapseDate
            , a.RelapseDisease = b.RelapseDisease
            , a.RelapseNotes   = b.RelapseNotes
            WHERE a.PatientId = b.PatientId
                AND a.RelapseDate IS NULL
                AND a.NextArrivalDate IS NULL
                AND b.RelapseDate > a.ResponseDate ;
        
        # Is there an earlier relapse ?
        UPDATE caisis.Playground  a, temp.RelapseStatus b
            SET a.RelapseType  = b.RelapseType
            , a.RelapseDate    = b.RelapseDate
            , a.RelapseDisease = b.RelapseDisease
            , a.RelapseNotes   = b.RelapseNotes
            WHERE a.PatientId = b.PatientId
                AND b.RelapseDate < a.RelapseDate
                AND a.NextArrivalDate IS NULL
                AND b.RelapseDate > a.ResponseDate ;
        
        # Is there a relapse between when treatmentstarted and the next arrival (response may be missing)
        UPDATE caisis.Playground  a, temp.RelapseStatus b
            SET a.RelapseType  = b.RelapseType
            , a.RelapseDate    = b.RelapseDate
            , a.RelapseDisease = b.RelapseDisease
            , a.RelapseNotes   = b.RelapseNotes
            WHERE a.PatientId = b.PatientId
                AND a.RelapseType IS NULL
                AND b.RelapseDate BETWEEN a.TreatmentStartDate and a.NextArrivalDate ;
        
        # Is there a relapse after treatment start when there are not future arrivals (response may be missing)?
        UPDATE caisis.Playground  a, temp.RelapseStatus b
            SET a.RelapseType  = b.RelapseType
            , a.RelapseDate    = b.RelapseDate
            , a.RelapseDisease = b.RelapseDisease
            , a.RelapseNotes   = b.RelapseNotes
            WHERE a.PatientId = b.PatientId
                AND a.RelapseDate IS NULL
                AND a.NextArrivalDate IS NULL
                AND b.RelapseDate > a.TreatmentStartDate ;
        
        
        UPDATE caisis.Playground a, temp.EarliestRelapse b
            SET a.EarliestRelapseDate = b.EarliestRelapseDate 
            , a.Relapse = CASE 
                    WHEN RelapseDate IS NOT NULL THEN 'Y'
                    ELSE ''
                END 
            WHERE a.PatientId = b.PatientId ;
    
    """
    dosqlexecute(cnxdict)
    return


def AssociateLastUpdate(cnxdict):
    printtext('Program: ')
    cnxdict['sql']="""
        UPDATE caisis.Playground a, caisis.vdatasetlastvisit b
            SET a.LastInformationDate =
                CASE
                    WHEN a.LastInformationDate IS NULL THEN b.LastLabDate
                    ELSE a.LastInformationDate
                END
            WHERE a.PtMRN = b.PtMRN
                AND a.PtDeathDate IS NULL ;
                
        
        DROP TABLE IF EXISTS temp.LastStatus ;
        CREATE TABLE temp.LastStatus
            SELECT a.PtMRN
                , a.PatientId
                , a.Status
                , b.LastStatus
                FROM vdatasetstatus a
                    JOIN (SELECT PatientId, max(statusdate) AS LastStatus FROM vdatasetstatus GROUP BY PatientId) b
                ON a.PatientId = b.PatientId
                AND a.StatusDate = b.LastStatus ;
                
        select * from temp.LastStatus ;
        
        UPDATE Caisis.Playground
            SET LastInformationDate = NULL
            , LastStatusDate = NULL
            , LastStatusType = '' ;
        
            
        UPDATE Caisis.Playground a, temp.LastStatus b
            SET a.LastStatusDate = b.LastStatus
            , a.LastStatusType = CASE 
                WHEN b.Status IN ( 'CR'
                    , 'CR CYTO MRD'
                    , 'CR MRD'
                    , 'CR-MRD'
                    , 'CRi'
                    , 'CRi MRD'
                    , 'CRp'
                    , 'CRp MRD') THEN 'Remission'
                WHEN b.Status LIKE '%recover%' 
                    OR b.Status IN ( 'Recovery of ANC 1000'
                    , 'Recovery of ANC 500'
                    , 'Recovery of Counts'
                    , 'Recovery of Plts 100k'
                    , 'Recovery of Plts 50k') THEN 'Count Recovery'
                WHEN b.Status LIKE '%relapse%' OR b.Status IN ( 'CYTO Relapse'
                    , 'FLOW Relapse'
                    , 'PB Relapse') THEN 'Relapse'
        
                WHEN b.Status IN ( 'Persistent Disease'
                    , 'PR'
                    , 'Refractory'
                    , 'Resistant'
                    , 'Not Categorized') THEN 'No Remission'
        
                WHEN b.Status IN ( 'Diagnosis Date'
                    , 'Newly Diagnosed') THEN 'Diagnosis'
        
                WHEN b.Status LIKE '%unk%' THEN 'Unknown'
                
                WHEN b.Status IN ( 'Alive'
                    , 'Initial AHD Date' 
                    , 'Non-Heme Cancer Diagnosis'
                    , 'Arrival Work-up' ) THEN b.Status
        
                ELSE ''
            END
            WHERE a.PatientId = b.PatientId  ;
        
          
        UPDATE caisis.playground 
            SET lastinformationdate = CASE
                    WHEN LastInformationDate IS NULL AND LastStatusDate IS NOT NULL THEN LastStatusDate
                    WHEN LastStatusDate > LastInformationDate THEN LastStatusDate
                    ELSE lastinformationdate
                END ;
        
        
        UPDATE caisis.playground 
            SET LastInformationDate = CASE
                    WHEN PtDeathDate IS NOT NULL THEN PtDeathDate
                    ELSE LastInformationDate
                END
                , LastStatusDate = CASE
                    WHEN PtDeathDate IS NOT NULL THEN PtDeathDate
                    ELSE LastStatusDate
                END
                , LastStatusType = CASE
                    WHEN PtDeathDate IS NOT NULL THEN 'Death'
                    ELSE LastStatusType
                END;
        
        UPDATE caisis.playground
            SET FollowUpDays = timestampdiff(day,ArrivalDate,LastInformationDate)
            , FollowUpMonths = timestampdiff(day,ArrivalDate,LastInformationDate)/30.44  #  Days per month 365.25/12
            , FollowUpYears  = timestampdiff(month,ArrivalDate,LastInformationDate)/12 ;
        """
    dosqlexecute(cnxdict)
    return


def AssociateFirstArrival(cnxdict):
    printtext('Program: ')
    cnxdict['sql']="""
        /************************************************************************************************
            Update to show the first time the patient arrived, and what type of arrival the
            patient had then.  This is helpful when looking at stay vs go.
        */
        DROP TABLE IF EXISTS temp.FirstArrival ;
        CREATE TABLE temp.FirstArrival
            SELECT PtMRN
                        , PatientId
                        , MIN(ArrivalDate) AS FirstArrivalDate
                        FROM caisis.playground
                        GROUP BY PtMRN, PatientId ;
        
        UPDATE caisis.playground a
                , temp.firstarrival b
            SET a.ReturnPatient = CASE
                    WHEN a.ArrivalDate = b.FirstArrivalDate THEN 'No'
                    ELSE 'Yes'
                END
            WHERE a.PatientId = b.PatientId
            AND a.ArrivalDate = b.FirstArrivalDate ;
        
        UPDATE caisis.playground a
                , (SELECT b2.*
                    , a2.ArrivalDx AS FirstArrivalDx 
                    , a2.ArrivalType AS FirstArrivalType
                    FROM Caisis.Playground a2 
                    JOIN temp.FirstArrival b2
                    ON a2.PatientId = b2.PatientId AND a2.ArrivalDate = b2.FirstArrivalDate) b
            SET a.FirstArrivalDx = b.FirstArrivalDx
                , a.FirstArrivalType = b.FirstArrivalType
                , a.FirstArrivalDate = b.FirstArrivalDate
            WHERE a.PatientId = b.PatientId ;
    """
    dosqlexecute(cnxdict)
    return


def AssociateDiagnosisDate(cnxdict):
    printtext('Program: ')
    excludelist = """DEAD|PERSIST|REFRACT|NOT CATE|ALIVE|ANTECEDENT|CR|ARRIVAL|AHD|NON|PROGRESSIVE|RECOVERY|META|RESIST|UNKNOWN|PR|RECURR|RELAP|RESP|NO EVIDENCE"""

    cnxdict['sql'] = """

        /**********************************************************************************************************/
        DROP TABLE IF EXISTS temp.AMLLeukemias ;
        CREATE TABLE temp.AMLLeukemias
            SELECT StatusDisease, Status, COUNT(*) FROM caisis.vdatasetstatus 
                WHERE UPPER(Status) RLIKE 'DIAG'
                AND   UPPER(Status) 
                      NOT RLIKE '{0}'
                AND   UPPER(StatusDisease) RLIKE 'AML|APL|LEUK'
                AND   UPPER(StatusDisease) NOT RLIKE 'CANCER'
                GROUP BY StatusDisease, Status ;        
        
        DROP TABLE IF EXISTS temp.AMLDiagnosis ;
        CREATE TABLE temp.AMLDiagnosis
            SELECT    a.PatientId
                    , a.`Status`
                    , a.`StatusDisease`
                    , a.`StatusDate`
                FROM caisis.vdatasetstatus a
                JOIN temp.AMLLeukemias b
                ON a.Status = b.Status 
                AND a.StatusDisease = b.StatusDisease ;
        
        UPDATE caisis.playground a, temp.AMLDiagnosis b
            SET a.DiagnosisDate = b.StatusDate
            , a.Diagnosis = b.`StatusDisease`
            WHERE a.PatientId = b.PatientId
            AND   b.StatusDate <= a.FirstArrivalDate ;
       
        /**********************************************************************************************************/
        DROP TABLE IF EXISTS temp.MDSLeukemias ;
        CREATE TABLE temp.MDSLeukemias
            SELECT StatusDisease, Status, COUNT(*) FROM caisis.vdatasetstatus 
                WHERE UPPER(Status) RLIKE 'DIAG'
                AND   UPPER(Status) 
                      NOT RLIKE '{0}'
                AND   UPPER(StatusDisease) NOT RLIKE 'AML|APL|LEUK'
                AND   UPPER(StatusDisease) RLIKE 'MDS|RAEB|LEUK'
                AND   UPPER(StatusDisease) NOT RLIKE 'CANCER'
                GROUP BY StatusDisease, Status ;        
        
            
        DROP TABLE IF EXISTS temp.MDSDiagnosis ;
        CREATE TABLE temp.MDSDiagnosis
            SELECT    a.PatientId
                    , a.`Status`
                    , a.`StatusDisease`
                    , a.`StatusDate`
                FROM caisis.vdatasetstatus a
                JOIN temp.MDSLeukemias b
                ON a.Status = b.Status 
                AND a.StatusDisease = b.StatusDisease ;
            
        UPDATE caisis.playground a, temp.MDSDiagnosis b
            SET a.DiagnosisDate = b.StatusDate
            , a.Diagnosis = b.`StatusDisease`
            WHERE a.PatientId = b.PatientId
            AND   b.StatusDate <= a.FirstArrivalDate 
            AND   a.DiagnosisDate IS NULL ;
            
        DROP TABLE IF EXISTS temp.AMLLeukemias ;    
        DROP TABLE IF EXISTS temp.MDSLeukemias ;    
        DROP TABLE IF EXISTS temp.AMLDiagnosis ;    
        DROP TABLE IF EXISTS temp.MDSDiagnosis ;                
            
    """.format(excludelist)
    dosqlexecute(cnxdict)
    return


def AssociateArrivalKaryotype(cnxdict):
    printtext('Program: ')
    cnxdict['sql']="""
        /************************************************************************************************
            Update to find the karyotype closest to arrival
            UPDATE caisis.playground SET ArrivalKaryotype = NULL, ArrivalKaryotypeDate = NULL ;
        */
        
        DROP TABLE IF EXISTS temp.relevantkaryo ;
        CREATE TABLE temp.relevantkaryo
            SELECT a.ArrivalDate
                    , a.ArrivalDX
                    , a.TreatmentStartDate
                    , CASE
                        WHEN a.TreatmentStartDate IS NULL 
                            THEN timestampdiff(day,b.DateObtained,a.ArrivalDate)
                        ELSE timestampdiff(day,b.DateObtained,a.TreatmentStartDate)
                    END AS KaryotypeDaysBeforeTreatmentorArrival
                    , b.* FROM caisis.playground a
                JOIN caisis.allkaryo b
                ON a.PatientId = b.PatientId 
                WHERE b.PathResult <> ''
                    AND b.PathTest IN ('UW Cyto','SCCA Karyotype','Cyto Karyotype')
                    AND b.PathResult NOT RLIKE '^[a-zA-Z]+'
                    AND b.DateObtained BETWEEN date_add(a.ArrivalDate, INTERVAL -90 DAY) 
                    AND CASE 
                        WHEN a.TreatmentStartDate IS NULL THEN ArrivalDate
                        ELSE a.TreatmentStartDate
                    END
            ;
            
        UPDATE caisis.playground a, 
            (SELECT PatientId, ArrivalDate, PathResult, DateObtained, KaryotypeDaysBeforeTreatmentorArrival
                FROM temp.relevantkaryo 
                WHERE PathResult <> ''
                GROUP BY PatientId, ArrivalDate
                HAVING COUNT(*)=1 )  b
            SET a.ArrivalKaryotype = b.PathResult
                , a.ArrivalKaryotypeDate = b.DateObtained
            WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
            ;
        
        # Repeat until no earlier found
        UPDATE caisis.playground a, temp.relevantkaryo b
            SET a.ArrivalKaryotype = b.PathResult
                , a.ArrivalKaryotypeDate = b.DateObtained
            WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
                AND CASE 
                    WHEN a.ArrivalKaryotype IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
                    WHEN a.ArrivalKaryotypeDate < b.DateObtained THEN True
                    ELSE False
                END
            ;
        
        UPDATE caisis.playground a, temp.relevantkaryo b
            SET a.ArrivalKaryotype = b.PathResult
                , a.ArrivalKaryotypeDate = b.DateObtained
            WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
                AND CASE 
                    WHEN a.ArrivalKaryotype IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
                    WHEN a.ArrivalKaryotypeDate < b.DateObtained THEN True
                    ELSE False
                END
            ;
        
        UPDATE caisis.playground a, temp.relevantkaryo b
            SET a.ArrivalKaryotype = b.PathResult
                , a.ArrivalKaryotypeDate = b.DateObtained
            WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
                AND CASE 
                    WHEN a.ArrivalKaryotype IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
                    WHEN a.ArrivalKaryotypeDate < b.DateObtained THEN True
                    ELSE False
                END
            ;
        
        UPDATE caisis.playground a, temp.relevantkaryo b
            SET a.ArrivalKaryotype = b.PathResult
                , a.ArrivalKaryotypeDate = b.DateObtained
            WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
                AND CASE 
                    WHEN a.ArrivalKaryotype IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
                    WHEN a.ArrivalKaryotypeDate < b.DateObtained THEN True
                    ELSE False
                END
            ;
        
        UPDATE caisis.playground a, temp.relevantkaryo b
            SET a.ArrivalKaryotype = b.PathResult
                , a.ArrivalKaryotypeDate = b.DateObtained
            WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
                AND CASE 
                    WHEN a.ArrivalKaryotype IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
                    WHEN a.ArrivalKaryotypeDate < b.DateObtained THEN True
                    ELSE False
                END
            ;
        
        UPDATE caisis.playground a, temp.relevantkaryo b
            SET a.ArrivalKaryotype = b.PathResult
                , a.ArrivalKaryotypeDate = b.DateObtained
            WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
                AND CASE 
                    WHEN a.ArrivalKaryotype IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
                    WHEN a.ArrivalKaryotypeDate < b.DateObtained THEN True
                    ELSE False
                END
            ;
        
        UPDATE caisis.playground a, temp.relevantkaryo b
            SET a.ArrivalKaryotype = b.PathResult
                , a.ArrivalKaryotypeDate = b.DateObtained
            WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
                AND CASE 
                    WHEN a.ArrivalKaryotype IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
                    WHEN a.ArrivalKaryotypeDate < b.DateObtained THEN True
                    ELSE False
                END
            ;
   
    """
    dosqlexecute(cnxdict)
    return


def AssociateArrivalFISH(cnxdict):
    printtext('Program: ')
    cnxdict['sql']="""
        /************************************************************************************************
            Update to find the FISH closest to arrival
        */
        
        DROP TABLE IF EXISTS temp.relevantfish ;
        CREATE TABLE temp.relevantfish
            SELECT a.ArrivalDate
                    , a.ArrivalDX
                    , a.TreatmentStartDate
                    , CASE
                        WHEN a.TreatmentStartDate IS NULL 
                            THEN timestampdiff(day,b.DateObtained,a.ArrivalDate)
                        ELSE timestampdiff(day,b.DateObtained,a.TreatmentStartDate)
                    END AS FISHDaysBeforeTreatmentorArrival
                    , b.* FROM caisis.playground a
                JOIN caisis.allkaryo b
                ON a.PatientId = b.PatientId 
                WHERE b.PathResult <> ''
                    AND b.PathTest IN ('UW FISH','SCCA FISH','FISH Karyotype')
                    AND b.PathResult NOT IN (''
                        ,'Cancelled','No growth'
                        ,'FAILED TO IDENTIFY ICSN DIAGNOSIS'
                        ,'?'
                        ,'No growth or insufficient growth'
                        ,'Insufficient cells for analysis'
                        ,'VVVV'
                        ,'No IFISH','FAILED TO FIND END OF ICSN DIAGNOSIS'
                    )
                    AND LOCATE('ish',b.PathResult) <> 0
                    AND b.DateObtained BETWEEN date_add(a.ArrivalDate, INTERVAL -90 DAY) 
                        AND CASE 
                            WHEN a.TreatmentStartDate IS NULL THEN ArrivalDate
                            ELSE a.TreatmentStartDate
                        END
            ;
        
        UPDATE caisis.playground a, 
            (SELECT PatientId, ArrivalDate, PathResult, DateObtained, FISHDaysBeforeTreatmentorArrival
                FROM temp.relevantfish 
                WHERE PathResult <> ''
                GROUP BY PatientId, ArrivalDate
                HAVING COUNT(*)=1 )  b
            SET a.ArrivalFISH = b.PathResult
                , a.ArrivalFISHDate = b.DateObtained
            WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
            ;
        
        # Repeat until no earlier found
        UPDATE caisis.playground a, temp.relevantfish b
            SET a.ArrivalFISH = b.PathResult
                , a.ArrivalFISHDate = b.DateObtained
            WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
                AND CASE 
                    WHEN a.ArrivalFISH IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
                    WHEN a.ArrivalFISHDate < b.DateObtained THEN True
                    ELSE False
                END
            ;
        
        # Repeat until no earlier found
        UPDATE caisis.playground a, temp.relevantfish b
            SET a.ArrivalFISH = b.PathResult
                , a.ArrivalFISHDate = b.DateObtained
            WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
                AND CASE 
                    WHEN a.ArrivalFISH IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
                    WHEN a.ArrivalFISHDate < b.DateObtained THEN True
                    ELSE False
                END
            ;
            
        # Repeat until no earlier found
        UPDATE caisis.playground a, temp.relevantfish b
            SET a.ArrivalFISH = b.PathResult
                , a.ArrivalFISHDate = b.DateObtained
            WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
                AND CASE 
                    WHEN a.ArrivalFISH IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
                    WHEN a.ArrivalFISHDate < b.DateObtained THEN True
                    ELSE False
                END
            ;
            
        # Repeat until no earlier found
        UPDATE caisis.playground a, temp.relevantfish b
            SET a.ArrivalFISH = b.PathResult
                , a.ArrivalFISHDate = b.DateObtained
            WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
                AND CASE 
                    WHEN a.ArrivalFISH IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
                    WHEN a.ArrivalFISHDate < b.DateObtained THEN True
                    ELSE False
                END
            ;
            
        # Repeat until no earlier found
        UPDATE caisis.playground a, temp.relevantfish b
            SET a.ArrivalFISH = b.PathResult
                , a.ArrivalFISHDate = b.DateObtained
            WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
                AND CASE 
                    WHEN a.ArrivalFISH IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
                    WHEN a.ArrivalFISHDate < b.DateObtained THEN True
                    ELSE False
                END
            ;
            
        # Repeat until no earlier found
        UPDATE caisis.playground a, temp.relevantfish b
            SET a.ArrivalFISH = b.PathResult
                , a.ArrivalFISHDate = b.DateObtained
            WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
                AND CASE 
                    WHEN a.ArrivalFISH IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
                    WHEN a.ArrivalFISHDate < b.DateObtained THEN True
                    ELSE False
                END
            ;
            
        # Repeat until no earlier found
        UPDATE caisis.playground a, temp.relevantfish b
            SET a.ArrivalFISH = b.PathResult
                , a.ArrivalFISHDate = b.DateObtained
            WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
                AND CASE 
                    WHEN a.ArrivalFISH IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
                    WHEN a.ArrivalFISHDate < b.DateObtained THEN True
                    ELSE False
                END
            ;
    """
    dosqlexecute(cnxdict)
    return


def AssociateArrivalCGAT(cnxdict):
    printtext('Program: ')
    cnxdict['sql']="""
        /************************************************************************************************
            Update to find the CGAT closest to arrival
        */
        
        DROP TABLE IF EXISTS temp.relevantCGAT ;
        CREATE TABLE temp.relevantCGAT
            SELECT a.ArrivalDate
                    , a.ArrivalDX
                    , a.TreatmentStartDate
                    , CASE
                        WHEN a.TreatmentStartDate IS NULL 
                            THEN timestampdiff(day,b.DateObtained,a.ArrivalDate)
                        ELSE timestampdiff(day,b.DateObtained,a.TreatmentStartDate)
                    END AS CGATDaysBeforeTreatmentorArrival
                    , b.* FROM caisis.playground a
                JOIN caisis.allkaryo b
                ON a.PatientId = b.PatientId 
                WHERE b.PathResult <> ''
                    AND b.PathTest IN ('UW CGAT','SCCA CGAT')
                    AND b.PathResult NOT IN (''
                        ,'FAILED TO IDENTIFY ICSN DIAGNOSIS'
                        ,'See Table'
                        ,'Inadequate for Chromosome Genomic Array Testing (CGAT)'
                    )
                    AND b.DateObtained BETWEEN date_add(a.ArrivalDate, INTERVAL -90 DAY) 
                        AND CASE 
                            WHEN a.TreatmentStartDate IS NULL THEN ArrivalDate
                            ELSE a.TreatmentStartDate
                        END
            ;
        
        UPDATE caisis.playground a, 
            (SELECT PatientId, ArrivalDate, PathResult, DateObtained, CGATDaysBeforeTreatmentorArrival
                FROM temp.relevantCGAT
                WHERE PathResult <> ''
                GROUP BY PatientId, ArrivalDate
                HAVING COUNT(*)=1 )  b
            SET a.ArrivalCGAT = b.PathResult
                , a.ArrivalCGATDate = b.DateObtained
            WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
            ;
        
        # Repeat until no earlier found
        UPDATE caisis.playground a, temp.relevantCGAT b
            SET a.ArrivalCGAT = b.PathResult
                , a.ArrivalCGATDate = b.DateObtained
            WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
                AND CASE 
                    WHEN a.ArrivalCGAT IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
                    WHEN a.ArrivalCGATDate < b.DateObtained THEN True
                    ELSE False
                END
            ;
        
        # Repeat until no earlier found
        UPDATE caisis.playground a, temp.relevantCGAT b
            SET a.ArrivalCGAT = b.PathResult
                , a.ArrivalCGATDate = b.DateObtained
            WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
                AND CASE 
                    WHEN a.ArrivalCGAT IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
                    WHEN a.ArrivalCGATDate < b.DateObtained THEN True
                    ELSE False
                END
            ;
        
        # Repeat until no earlier found
        UPDATE caisis.playground a, temp.relevantCGAT b
            SET a.ArrivalCGAT = b.PathResult
                , a.ArrivalCGATDate = b.DateObtained
            WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
                AND CASE 
                    WHEN a.ArrivalCGAT IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
                    WHEN a.ArrivalCGATDate < b.DateObtained THEN True
                    ELSE False
                END
            ;
        
        # Repeat until no earlier found
        UPDATE caisis.playground a, temp.relevantCGAT b
            SET a.ArrivalCGAT = b.PathResult
                , a.ArrivalCGATDate = b.DateObtained
            WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
                AND CASE 
                    WHEN a.ArrivalCGAT IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
                    WHEN a.ArrivalCGATDate < b.DateObtained THEN True
                    ELSE False
                END
            ;
        
        # Repeat until no earlier found
        UPDATE caisis.playground a, temp.relevantCGAT b
            SET a.ArrivalCGAT = b.PathResult
                , a.ArrivalCGATDate = b.DateObtained
            WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
                AND CASE 
                    WHEN a.ArrivalCGAT IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
                    WHEN a.ArrivalCGATDate < b.DateObtained THEN True
                    ELSE False
                END
            ;
        
        # Repeat until no earlier found
        UPDATE caisis.playground a, temp.relevantCGAT b
            SET a.ArrivalCGAT = b.PathResult
                , a.ArrivalCGATDate = b.DateObtained
            WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
                AND CASE 
                    WHEN a.ArrivalCGAT IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
                    WHEN a.ArrivalCGATDate < b.DateObtained THEN True
                    ELSE False
                END
            ;
        
        # Repeat until no earlier found
        UPDATE caisis.playground a, temp.relevantCGAT b
            SET a.ArrivalCGAT = b.PathResult
                , a.ArrivalCGATDate = b.DateObtained
            WHERE a.PatientId = b.PatientId AND a.ArrivalDate = b.ArrivalDate
                AND CASE 
                    WHEN a.ArrivalCGAT IS NULL AND a.ArrivalKaryotypeDate IS NULL THEN True
                    WHEN a.ArrivalCGATDate < b.DateObtained THEN True
                    ELSE False
                END
            ;
    """
    dosqlexecute(cnxdict)
    return


def CreateEventDateRange(cnxdict):
    now = datetime.datetime.now()
    printtext('Program: ')
    cnxdict['sql'] = """
    DROP TABLE IF EXISTS caisis.vdataseteventdaterange ;
    CREATE TABLE caisis.vdataseteventdaterange
        SELECT Arrival_Id
            , CAST('DIAGNOSIS' AS CHAR(40)) AS Event
            , PtMRN
            , PatientId
            , DiagnosisDate
            , FirstArrivalDate
            , ArrivalDate
            , TreatmentStartDate
            , ResponseDate
            , RelapseDate
            , date_add(DiagnosisDate, INTERVAL -35 DAY) AS StartDateRange
            , DiagnosisDate AS TargetDate
            , CASE
                WHEN FirstArrivalDate IS NULL THEN NULL
                WHEN FirstArrivalDate >= DiagnosisDate THEN FirstArrivalDate
                ELSE DATE_ADD(DiagnosisDate, INTERVAL 1 DAY)
            END AS EndDateRange
            FROM Caisis.Playground 
            WHERE DiagnosisDate IS NOT NULL
        UNION
        SELECT Arrival_Id
            , CAST('ARRIVAL' AS CHAR(40)) AS Event
            , PtMRN
            , PatientId
            , DiagnosisDate
            , FirstArrivalDate
            , ArrivalDate
            , TreatmentStartDate
            , ResponseDate
            , RelapseDate
            , date_add(ArrivalDate, INTERVAL -35 DAY) AS StartDateRange
            , ArrivalDate AS TargetDate
            , CASE
                WHEN TreatmentStartDate IS NULL THEN NULL
                ELSE TreatmentStartDate
            END AS EndDateRange
            FROM Caisis.Playground 
            WHERE ArrivalDate IS NOT NULL
        UNION
        SELECT Arrival_Id
            , CAST('TREATMENT' AS CHAR(40)) AS Event
            , PtMRN
            , PatientId
            , DiagnosisDate
            , FirstArrivalDate
            , ArrivalDate
            , TreatmentStartDate
            , ResponseDate
            , RelapseDate
            , ArrivalDate AS StartDateRange
            , CASE
                WHEN TreatmentStartDate IS NULL THEN NULL
                ELSE date_add(TreatmentStartDate, INTERVAL -1 DAY)
            END AS TargetDate
            , CASE
                WHEN TreatmentStartDate IS NULL THEN NULL
                ELSE date_add(TreatmentStartDate, INTERVAL +2 DAY)
            END AS EndDateRange
            FROM Caisis.Playground 
            WHERE TreatmentStartDate IS NOT NULL
        UNION
        SELECT Arrival_Id
            , CAST('RESPONSE' AS CHAR(40)) AS Event
            , PtMRN
            , PatientId
            , DiagnosisDate
            , FirstArrivalDate
            , ArrivalDate
            , TreatmentStartDate
            , ResponseDate
            , RelapseDate
            , date_add(ResponseDate, INTERVAL -14 DAY) AS StartDateRange
            , ResponseDate AS TargetDate
            , date_add(ResponseDate, INTERVAL +14 DAY) AS EndDateRange
            FROM Caisis.Playground 
            WHERE ResponseDate IS NOT NULL
        UNION
        SELECT Arrival_Id
            , CAST('RELAPSE' AS CHAR(40)) AS Event
            , PtMRN
            , PatientId
            , DiagnosisDate
            , FirstArrivalDate
            , ArrivalDate
            , TreatmentStartDate
            , ResponseDate
            , RelapseDate
            , date_add(RelapseDate, INTERVAL -14 DAY) AS StartDateRange
            , RelapseDate AS TargetDate
            , RelapseDate AS EndDateRange
            FROM Caisis.Playground 
            WHERE RelapseDate IS NOT NULL
        ORDER BY PtMRN, ArrivalDate ;    
    
    ALTER TABLE `caisis`.`vdataseteventdaterange`
        ADD INDEX `Arrival_Id`  (`Arrival_Id` ASC),
        ADD INDEX `PatientId`   (`PatientId` ASC),
        ADD INDEX `ArrivalDate` (`ArrivalDate` ASC);
        
    UPDATE `caisis`.`vdataseteventdaterange` 
        SET EndDateRange = STR_TO_DATE(CURDATE(),'%Y-%m-%d')
        WHERE EndDateRange IS NULL ;
        
    """
    dosqlexecute(cnxdict)
    return


def CreateMolecularTable(cnxdict):
    printtext('Program: ')
    cnxdict['sql']="""
        /***********************************************************************************
            Create the npm1 table
        */
        
        DROP TABLE IF EXISTS molecular.`npm1` ;
        CREATE TABLE molecular.`npm1`
            SELECT 
                `LabTestCategory`,
                `LabGroupId`,
                `PatientId` AS `PatientId`,
                `PtMRN` AS `PtMRN`,
                `LabDate` AS `LabDate`,
                CAST(NULL AS CHAR(50)) AS SpecimenType,
                `LabTest` AS `LabTest`,
                `LabResult` AS `LabResult`,
                `LabUnits` AS `LabUnits`,
                CAST(NULL AS CHAR(50)) AS `BasesTest`,
                CAST(NULL AS CHAR(50)) AS `Bases`,
                CAST(NULL AS CHAR(50)) AS `RatioTest`,
                CAST(NULL AS CHAR(50)) AS `Ratio`,
                CAST(NULL AS CHAR(50)) AS Interpretation,
                CAST(NULL AS CHAR(50)) AS Summary,
                `LabTestId` AS `LabTestId`,
                `LabAccessionNum` AS `LabAccessionNum`
            FROM  `caisis`.`molecularlabs`
            WHERE `LabTestCategory` = 'NPM1'
                    AND `LabTest` LIKE '%result%'
                    AND UPPER(LabResult) NOT RLIKE 'DATA ENTRY CORRECTION|DUPLICATE|MISLOGGED|REORDER|WRONG|TEST NOT NEEDED'
            ORDER BY `PtMRN` , `LabDate`;


        DROP TABLE IF EXISTS molecular.npm1specimen ;
        CREATE TABLE molecular.npm1specimen
            SELECT LabGroupId, LabResult
                FROM  `caisis`.`molecularlabs`
                WHERE `LabTestCategory` = 'NPM1' 
                AND   `LabTest`   LIKE '%spec%'
                AND   `LabResult` NOT RLIKE 'DUPLICATE|REORDER|SEE|WRONG|NOT PROVIDED|MISLOGGED' ;


        DROP TABLE IF EXISTS molecular.npm1interpretation ;
        CREATE TABLE molecular.npm1interpretation
            SELECT LabGroupId, LabTest, LabResult
                FROM  `caisis`.`molecularlabs`
                WHERE `LabTestCategory` = 'NPM1' 
                AND   UPPER(`LabTest`) RLIKE 'INTERP'
                AND   `LabResult` NOT RLIKE 'DUPLICATE|REORDER|SEE|WRONG|MISLOGGED|NOT SUFFICIENT|TEST NOT NEEDED' ;
                

        UPDATE molecular.npm1 a, molecular.npm1specimen b
            SET a.SpecimenType = b.LabResult 
            WHERE a.LabGroupId = b.LabGroupId ;      
        
        UPDATE molecular.npm1 a, molecular.npm1interpretation b
            SET a.interpretation = b.LabResult 
            WHERE a.LabGroupId = b.LabGroupId ;     

        /*
            SELECT * FROM  molecular.`npm1`;
            SELECT * FROM caisis.molecularlabs ;
        */
                
        /***********************************************************************************
            Create the flt3 table
        */
        
        DROP TABLE IF EXISTS molecular.flt3ratio ;
        CREATE TABLE molecular.flt3ratio
            SELECT `LabTestCategory`,
                `LabGroupId`,
                `PatientId`,
                `LabTest`   AS `RatioTest`,
                `LabResult` AS `Ratio`
                FROM `caisis`.`molecularlabs` 
                WHERE `LabTest` LIKE '%ratio%'
                    AND `LabTestCategory` = 'FLT3' 
                    AND `LabResult` NOT LIKE '%see%' 
                    AND `LabResult` RLIKE '[0-9]' ;
        
        DROP TABLE IF EXISTS molecular.flt3base ;
        CREATE TABLE molecular.flt3base
            SELECT `LabTestCategory`,
                `LabGroupId`,
                `PatientId`,
                `LabTest` AS `BasesTest`,
                `LabResult` AS `Bases`
                FROM `caisis`.`molecularlabs` 
                    WHERE `LabTestCategory` = 'FLT3' 
                    AND `LabResult` LIKE '%base%';
        
        DROP TABLE IF EXISTS molecular.flt3result ;
        CREATE TABLE molecular.flt3result
            SELECT `result`.`LabTestCategory`,
                `result`.`LabGroupId`,
                `result`.`PatientId`,
                `result`.`PtMRN`,
                `result`.`LabDate`,
                CAST(NULL AS CHAR(50)) AS SpecimenType,
                `result`.`LabTest`,
                `result`.`LabResult`,
                `result`.`LabUnits`,
                CAST(NULL AS CHAR(50)) AS Interpretation,
                `result`.`LabTestId` AS `LabTestId`,
                `result`.`LabAccessionNum` AS `LabAccessionNum`
                FROM `caisis`.`molecularlabs` `result`
                WHERE `result`.`LabTestCategory` = 'FLT3' 
                AND UPPER(`result`.`LabTest`) RLIKE 'RESULT|ITD|TKD' 
                AND UPPER(`result`.`LabTest`) NOT RLIKE 'RATIO|LENGTH' 
                AND `result`.`LabResult` IN ('Alteration of uncertain significance'
                    ,'Alterations of uncertain significance'
                    ,'Negative'
                    ,'Positive') ;
        
        /*  I found one patient who had a Negative result overturned 
            to a Positive, this code corrects that ONE case
            that was fed twice with both a negative and positive result
        */
        DELETE FROM molecular.flt3result 
            WHERE LabGroupId = 157018142 AND LabResult LIKE '%neg%';
            
        DROP TABLE IF EXISTS molecular.flt3specimen ;
        CREATE TABLE molecular.flt3specimen
            SELECT LabGroupId, LabResult
                FROM  `caisis`.`molecularlabs`
                WHERE `LabTestCategory` = 'FLT3' 
                AND   `LabTest`   LIKE '%spec%'
                AND   `LabResult` NOT RLIKE 'DUPLICATE|REORDER|SEE|WRONG' ;


        DROP TABLE IF EXISTS molecular.flt3interpretation ;
        CREATE TABLE molecular.flt3interpretation
            SELECT LabGroupId, LabTest, LabResult
                FROM  `caisis`.`molecularlabs`
                WHERE `LabTestCategory` = 'FLT3' 
                AND   UPPER(`LabTest`) RLIKE 'INTERP'
                AND   `LabResult` NOT RLIKE 'DUPLICATE|REORDER|SEE|WRONG|MISLOGGED|NOT SUFFICIENT' ;
        
        DROP TABLE IF EXISTS molecular.flt3 ;
        CREATE TABLE molecular.flt3
            SELECT a.LabTestCategory
                , a.LabGroupId
                , a.PatientId
                , a.PtMRN
                , a.LabDate
                , a.SpecimenType
                , a.LabTest
                , a.LabResult
                , a.LabUnits
                , b.BasesTest
                , b.Bases
                , c.RatioTest
                , c.Ratio
                , a.Interpretation
                , GROUP_CONCAT(concat( a.`LabTest`, ' (', a.`LabResult`, '); ',
                    IF(b.BasesTest IS NOT NULL,concat(b.`BasesTest`, ' (', b.`Bases`, '); '),''),
                    IF(c.RatioTest IS NOT NULL,concat(c.`RatioTest`, ' (', c.`Ratio`, '); '),'')
                )) AS Summary
                , a.LabTestId
                , a.LabAccessionNum
                FROM molecular.flt3result a
                LEFT JOIN molecular.flt3base b  ON a.PatientId = b.PatientId and a.LabGroupId = b.LabGroupId
                LEFT JOIN molecular.flt3ratio c ON a.PatientId = c.PatientId and a.LabGroupId = c.LabGroupId 
                GROUP BY LabGroupId 
                ORDER BY PtMRN, LabDate ;

        UPDATE molecular.flt3 a, molecular.flt3specimen b
            SET a.SpecimenType = b.LabResult 
            WHERE a.LabGroupId = b.LabGroupId ;      
        
        UPDATE molecular.flt3 a, molecular.flt3interpretation b
            SET a.interpretation = b.LabResult 
            WHERE a.LabGroupId = b.LabGroupId ;      

        /*
            Check to see if there are any patients that are marked both neg and pos
            SELECT *
                FROM molecular.flt3
                WHERE Summary LIKE '%pos%' and Summary LIKE '%neg%'
                GROUP BY LabGroupId ;
        */

        
        /***********************************************************************************
            Create the cebpa table
        */
        
        DROP TABLE IF EXISTS molecular.`cebparesult` ;
        CREATE TABLE molecular.`cebparesult`
            SELECT 
                `LabTestCategory`,
                `LabGroupId`,
                `PatientId` AS `PatientId`,
                `PtMRN` AS `PtMRN`,
                `LabDate` AS `LabDate`,
                CAST(NULL AS CHAR(50)) AS SpecimenType,
                `LabTest` AS `LabTest`,
                `LabResult` AS `LabResult`,
                `LabUnits` AS `LabUnits`,
                CAST(NULL AS CHAR(50)) AS `BasesTest`,
                CAST(NULL AS CHAR(50)) AS `Bases`,
                CAST(NULL AS CHAR(50)) AS `RatioTest`,
                CAST(NULL AS CHAR(50)) AS `Ratio`,
                CAST(NULL AS CHAR(50)) AS Interpretation,
                CAST(NULL AS CHAR(50)) AS Summary,
                `LabTestId` AS `LabTestId`,
                `LabAccessionNum` AS `LabAccessionNum`
            FROM  `caisis`.`molecularlabs`
            WHERE `LabTestCategory` = 'CEBPA' 
                    AND UPPER(LabTest)   NOT RLIKE 'METHOD|REVIEWED BY'
                    AND (UPPER(LabTest)   RLIKE 'RESULT' OR LabTest = 'CEBPA')
                    AND UPPER(LabResult) NOT RLIKE 'BILLED|DATA ENTRY CORRECTION|DUPLICATE|MISLOGGED|REORDER|WRONG|TEST NOT NEEDED'
            ORDER BY `PtMRN` , `LabDate`;


        DROP TABLE IF EXISTS molecular.cebpaspecimen ;
        CREATE TABLE molecular.cebpaspecimen
            SELECT LabGroupId, LabResult
                FROM  `caisis`.`molecularlabs`
                WHERE `LabTestCategory` = 'CEBPA' 
                AND   `LabTest`   LIKE '%spec%'
                AND   UPPER(`LabResult`) NOT RLIKE 'DATA ENTRY CORRECTION|DUPLICATE|REORDER|SEE|WRONG|NOT PROVIDED|MISLOGGED' ;


        DROP TABLE IF EXISTS molecular.cebpainterpretation ;
        CREATE TABLE molecular.cebpainterpretation
            SELECT LabGroupId, LabTest, LabResult
                FROM  `caisis`.`molecularlabs`
                WHERE `LabTestCategory` = 'CEBPA' 
                AND   UPPER(`LabTest`) RLIKE 'INTERPR'
                AND   UPPER(`LabResult`) NOT RLIKE 'BILLED|DATA ENTRY CORRECTION|DUPLICATE|REORDER|WRONG|MISLOGGED|NOT SUFFICIENT|TEST NOT NEEDED' ;
                
        UPDATE molecular.cebpa a, molecular.cebpaspecimen b
            SET a.SpecimenType = b.LabResult 
            WHERE a.LabGroupId = b.LabGroupId ;      
        
        UPDATE molecular.cebpa a, molecular.cebpainterpretation b
            SET a.interpretation = b.LabResult 
            WHERE a.LabGroupId = b.LabGroupId ;  
        
            
        DROP TABLE IF EXISTS molecular.flt3ratio ;
        DROP TABLE IF EXISTS molecular.flt3base ;
        DROP TABLE IF EXISTS molecular.flt3result ;
        DROP TABLE IF EXISTS molecular.specimen ;
        DROP TABLE IF EXISTS molecular.interpretation ;
            
        DROP TABLE IF EXISTS caisis.vdatasetmolecular ;
        CREATE TABLE caisis.vdatasetmolecular
            SELECT * FROM molecular.npm1 
            UNION SELECT * FROM molecular.flt3 
            UNION SELECT * FROM molecular.cebpa
            ORDER BY PtMRN, LabDate, LabGroupId ;
    """
    dosqlexecute(cnxdict)
    return


def AssociateLabs(cnxdict):
    printtext('Program: ')
    timepointlist = ['Diagnosis','Arrival','Treatment','Response','Relapse']
    testlist = cnxdict['lablist'] + cnxdict['molelist']
        # ['ANC', 'ALB',  'BLAST', 'CREAT', 'FLUID', 'HCT', 'HGB', 'PLT', 'RBC', 'UNCLASS',
        #         'WBC', 'FLT3', 'NPM1',  'CEBPA', 'MUT', ]


    for testname in testlist:
        sourcetable        = 'vdatasetlabtests'
        var_interpretation = 'NULL'
        var_summary        = 'NULL'
        var_specimentype   = 'NULL'
        flt3fields         = ''

        ismolecular        = testname in cnxdict['molelist']
        for timepoint in timepointlist:
            targettable = 'Playground{0}Labs'.format(timepoint)
            if ismolecular:
                sourcetable        = 'vdatasetmolecular'
                targettable        = 'PlaygroundMolecular'
                var_interpretation = 'a.Interpretation'
                var_summary        = 'a.Summary'
                var_specimentype   = 'a.Interpretation'
                if testname == 'FLT3':
                    flt3fields = """
                        , a.BasesTest 
                        , a.Bases 
                        , a.RatioTest 
                        , a.Ratio  
                    """

            selectfieldlist = """a.LabTestCategory
                        , '{0}' AS timepoint
                        , MIN(ABS(TIMESTAMPDIFF(DAY,a.LabDate,b.TargetDate))) As DaysBetween
                        , 'Yes' AS PrecedeTargetDate
                        , b.arrival_id         
                        , b.PatientId         
                        , b.PtMRN         
                        , a.LabGroupId         
                        , a.LabDate         
                        , {1} AS SpecimenType
                        , a.LabTest         
                        , a.LabResult         
                        , a.LabUnits         
                        {2}         
                        , {3} AS Interpretation
                        , {4} AS Summary         
                        , b.StartDateRange         
                        , b.TargetDate         
                        , b.EndDateRange
            """.format(timepoint,var_specimentype,flt3fields,var_interpretation,var_summary)


            cnxdict['sql'] = """
                # on or before target date
                DROP TABLE IF EXISTS temp.beforetargetdate ;
                CREATE TABLE temp.beforetargetdate
                    SELECT {3} FROM caisis.{2} a
                    JOIN caisis.vdataseteventdaterange b ON a.PatientId = b.PatientId 
                    WHERE b.Event = '{0}' AND   a.LabTestCategory = '{1}' AND a.LabDate BETWEEN b.StartDateRange AND b.TargetDate
                    GROUP BY b.arrival_id, a.LabTest, ABS(TIMESTAMPDIFF(DAY,a.LabDate,b.TargetDate))
                    ORDER BY b.arrival_id, a.LabTest, ABS(TIMESTAMPDIFF(DAY,a.LabDate,b.TargetDate)) ; 
        
                # after target date
                DROP TABLE IF EXISTS temp.aftertargetdate ;
                CREATE TABLE temp.aftertargetdate
                    SELECT {3} FROM caisis.{2} a
                    JOIN caisis.vdataseteventdaterange b ON a.PatientId = b.PatientId 
                    WHERE b.Event = '{0}' AND   a.LabTestCategory = '{1}' AND   a.LabDate BETWEEN b.TargetDate AND b.EndDateRange
                    GROUP BY b.arrival_id, a.LabTest, ABS(TIMESTAMPDIFF(DAY,a.LabDate,b.TargetDate))
                    ORDER BY b.arrival_id, a.LabTest, ABS(TIMESTAMPDIFF(DAY,a.LabDate,b.TargetDate)) ; 
    
                DROP TABLE IF EXISTS temp.{0}relevantlab ;
                CREATE TABLE temp.{0}relevantlab    
                    SELECT * FROM temp.beforetargetdate 
                        UNION SELECT * FROM temp.aftertargetdate ;
                        
                ALTER TABLE temp.{0}relevantlab
                    ADD INDEX `Arrival_Id`  (`Arrival_Id` ASC);
                
                DROP TABLE IF EXISTS temp.beforetargetdate ;
                DROP TABLE IF EXISTS temp.aftertargetdate ;
            """.format(timepoint, testname, sourcetable,selectfieldlist)
            dosqlexecute(cnxdict)

            if testname == 'FLT3':
                flt3fields = """
                            , a.{0}{1}BasesTest       = b.BasesTest 
                            , a.{0}{1}Bases           = b.Bases 
                            , a.{0}{1}RatioTest       = b.RatioTest 
                            , a.{0}{1}Ratio           = b.Ratio  
                """.format(testname, timepoint)
            cnxdict['sql'] = """
                UPDATE caisis.{3} a, temp.{1}relevantlab b
                        SET   a.{0}{1}LabDate         = b.LabDate 
                            , a.{0}{1}LabResult       = b.LabResult
                            , a.{0}{1}LabGroupId      = b.LabGroupId 
                            , a.{0}{1}LabDate         = b.LabDate
                            , a.{0}{1}SpecimenType    = b.SpecimenType
                            , a.{0}{1}LabTest         = b.LabTest 
                            , a.{0}{1}LabResult       = b.LabResult
                            , a.{0}{1}LabUnits        = b.LabUnits
                            {2}
                            , a.{0}{1}Interpretation  = b.Interpretation 
                            , a.{0}{1}Summary         = b.Summary 
                        WHERE a.arrival_id = b.arrival_id ;
            """.format(testname, timepoint, flt3fields, targettable)
            dosqlexecute(cnxdict)


def PushPlaygroundTables(cnxdict,targetdatabase='jake_caisis'):
    printtext('Program: ')
    timestring = time.strftime("%Y%m%d")


    if targetdatabase != 'jake_caisis':
        cnxdict['sql'] = """
            DROP TABLE IF EXISTS {1}.playground ;
            CREATE TABLE {1}.playground
                SELECT * FROM caisis.playground ;


            DROP TABLE IF EXISTS {1}.playgroundmolecular ;
            CREATE TABLE {1}.playgroundmolecular
                SELECT * FROM caisis.playgroundmolecular ;

        """.format(timestring, targetdatabase)
        dosqlexecute(cnxdict)

        for timepoint in ['Diagnosis', 'Arrival', 'Treatment', 'Response', 'Relapse']:
            cnxdict['sql'] = """
                DROP TABLE IF EXISTS {1}.playground{2}labs ;
                CREATE TABLE {1}.playground{2}labs
                    SELECT * FROM caisis.playground{2}labs ;

            """.format(timestring, targetdatabase, timepoint)
            dosqlexecute(cnxdict)

    cnxdict['sql'] = """
        DROP TABLE IF EXISTS {1}.playground_{0} ;
        CREATE TABLE {1}.playground_{0}
            SELECT * FROM caisis.playground ;


        DROP TABLE IF EXISTS {1}.playgroundmolecular_{0} ;
        CREATE TABLE {1}.playgroundmolecular_{0}
            SELECT * FROM caisis.playgroundmolecular ;

    """.format(timestring, targetdatabase)
    dosqlexecute(cnxdict)

    for timepoint in ['Diagnosis', 'Arrival', 'Treatment', 'Response', 'Relapse']:
        cnxdict['sql'] = """
            DROP TABLE IF EXISTS {1}.playground{2}labs_{0} ;
            CREATE TABLE {1}.playground{2}labs_{0}
                SELECT * FROM caisis.playground{2}labs ;

        """.format(timestring, targetdatabase, timepoint)
        dosqlexecute(cnxdict)


    return


def BuildRedCapDictionary(cnxdict):
    cnxdict['sql'] = ''
    unioncmd  = ''
    schema = 'caisis'
    for tablename in ['playground'
                ,'playgrounddiagnosislabs'
                ,'playgroundarrivallabs'
                ,'playgroundtreatmentlabs'
                ,'playgroundresponselabs'
                ,'playgroundrelapselabs'
                ,'playgroundmolecular']:

        cmd = """
                SELECT 
                      lower(COLUMN_NAME) AS `Variable / Field Name`
                    , TABLE_NAME AS `Form Name`
                    , '' as `Section Header`
                    , 'text' AS `Field Type`
                    , COLUMN_NAME AS `Field Label`
                    , '' AS `Choices, Calculations, OR Slider Labels`
                    , '' AS `Field Note`
                    , CASE 
                        WHEN DATA_TYPE = 'datetime' THEN 'date_ymd'
                        ELSE ''
                    END AS `Text Validation Type OR Show Slider Number`
                    , '' AS `Text Validation Min`
                    , '' AS `Text Validation Max`
                    , '' AS `Identifier?`
                    , '' AS `Branching Logic (Show field only if...)`
                    , '' AS `Required Field?`
                    , '' AS `Custom Alignment`
                    , '' AS `Question Number (surveys only)`
                    , '' AS `Matrix Group Name`
                    , '' AS `Matrix Ranking?`
                    , '' AS `Field Annotation`
                  FROM INFORMATION_SCHEMA.COLUMNS
                  WHERE table_name = '{0}' 
                    AND table_schema = 'caisis' 
        """.format(tablename.lower(),schema)
        cnxdict['sql'] = cnxdict['sql']  + unioncmd + cmd
        unioncmd  = ' union '
    cnxdict['sql'] = """
        DROP TABLE IF EXISTS caisis.vdatasetRedCapStructure ;
        CREATE TABLE caisis.vdatasetRedCapStructure {0} ; 
        DELETE FROM caisis.vdatasetRedCapStructure WHERE `Variable / Field Name` = 'arrival_id' AND `Form Name` != 'playground';
    """.format(cnxdict['sql'] )
    dosqlexecute(cnxdict)

    filedescription = 'Playground Data Dictionary'
    filename='{}'.format(filedescription)[0:28]
    cnxdict['out_filepath'] = buildfilepath(cnxdict, filename='{}'.format(filename))
    cmd = 'SELECT * FROM caisis.vdatasetRedCapStructure;'
    df = pd.read_sql(cmd, cnxdict['cnx'])
    cnxdict['out_filepath'] = cnxdict['out_filepath'].replace('xlsx', 'csv')
    df.to_csv(cnxdict['out_filepath'], index=False, encoding='utf-8')
    print(cnxdict['out_filepath'])


def DownloadRelatedPlaygroundTablesForRedCapUpload(cnxdict):
    cnxdict['sql'] = ''
    unioncmd  = ''
    schema = 'caisis'
    for tablename in ['playgrounddiagnosislabs'
                ,'playgroundarrivallabs'
                ,'playgroundtreatmentlabs'
                ,'playgroundresponselabs'
                ,'playgroundrelapselabs'
                ,'playgroundmolecular']:

        cmd = """
            SELECT * FROM {} ORDER BY Arrival_Id;
        """.format(tablename)
        filedescription = '{}'.format(tablename)
        cnxdict['out_fileext'] = 'csv'
        cnxdict['out_filepath'] = buildfilepath(cnxdict, filename=filedescription[0:28])
        df = pd.read_sql(cmd, cnxdict['cnx'])
        df.to_csv(cnxdict['out_filepath'], index=False, encoding='utf-8')
        print(cnxdict['out_filepath'])


def DownloadPlaygroundForRedCapUpload(cnxdict):
    cnxdict['sql'] = ''
    unioncmd  = ''
    schema = 'caisis'
    for tablename in ['playground']:
        cmd = """
            SELECT * FROM {} ORDER BY Arrival_Id;
        """.format(tablename)
        filedescription = '{}'.format(tablename)
        cnxdict['out_fileext'] = 'xlsx'
        cnxdict['out_filepath'] = buildfilepath(cnxdict, filename=filedescription[0:28])
        writer = pd.ExcelWriter(cnxdict['out_filepath'],datetime_format='mm/dd/yyyy')  # datetime_format='mmm d yyyy hh:mm:ss'
        df = pd.read_sql(cmd, cnxdict['cnx'])
        df.to_excel(writer, sheet_name=tablename, index=False)
        dowritersave(writer, cnxdict)
        print(cnxdict['out_filepath'])


"""
    MAIN PROCEDURE CALLS
"""
def MainProcedureCalls(cnxdict):
    printtext('Program: ')
    """
        MAIN PROCEDURE CALLS
    """

    cnxdict['EchoSQL']=0  # no output

    # --------------------------------------------------------------------------------------------------------------
    # Call procedure to arrival with previous and next table used to link tables
    # CreatePrevNextArrivalTable(cnxdict)
    # --------------------------------------------------------------------------------------------------------------
    # Call procedure to build empty playground structure
    cnxdict['EchoSQL']=1
    CreatePlaygroundTemplate(cnxdict)
    CreatePlaygroundMolecularTemplate(cnxdict)
    CreatePlaygroundLabsTemplate(cnxdict)
    cnxdict['EchoSQL']=0
    # --------------------------------------------------------------------------------------------------------------
    # Call procedure to find all unique treatments for patient arrivals (GCLAM etc)
    AssociateTreatment(cnxdict)
    # --------------------------------------------------------------------------------------------------------------
    # Call procedure to find non-treatments for patient arrivals (palliative etc)
    AssociateNonTreatment(cnxdict)
    # --------------------------------------------------------------------------------------------------------------
    # Make a copy of the treatment name without the parenthetical statements to make it
    # easier to map
    RemoveParentheticalTreatmentStatement(cnxdict)
    # --------------------------------------------------------------------------------------------------------------
    # In this section mapping MedTxAgent to the backbone, or common name
    AssociateBackbone(cnxdict)
    # --------------------------------------------------------------------------------------------------------------
    # In this section response information added to the status screen is joined to find
    # response to induction treatment courses
    AssociateResponse(cnxdict)
    # --------------------------------------------------------------------------------------------------------------
    # Get blasts by FLOW at response
    AssociateResponseFlow(cnxdict)
    # --------------------------------------------------------------------------------------------------------------
    # Figure out if patients have relapsed since treatment or between arrivals
    AssociateRelapse(cnxdict)
    # --------------------------------------------------------------------------------------------------------------
    # Fill in fields that indicate the last time we have a status (abstraction) for the patient, last lab, and deceased.
    AssociateLastUpdate(cnxdict)
    # --------------------------------------------------------------------------------------------------------------
    # Update to show the first time the patient arrived, and what type of arrival the
    # patient had then.  This is helpful when looking at stay vs go.
    AssociateFirstArrival(cnxdict)
    # --------------------------------------------------------------------------------------------------------------
    # Update to find the karyotype closest to arrival
    AssociateArrivalKaryotype(cnxdict)
    # --------------------------------------------------------------------------------------------------------------
    # Update to find the FISH closest to arrival
    AssociateArrivalFISH(cnxdict)
    # --------------------------------------------------------------------------------------------------------------
    # Update to find the CGAT closest to arrival
    AssociateArrivalCGAT(cnxdict)
    # --------------------------------------------------------------------------------------------------------------
    # Update to find the CGAT closest to arrival
    AssociateDiagnosisDate(cnxdict)
    # --------------------------------------------------------------------------------------------------------------
    # Date ranges for finding information linked to key events:  Dx, Arrival, Rx, Response, Relapse
    CreateEventDateRange(cnxdict)
    # --------------------------------------------------------------------------------------------------------------
    # Query lab data to create vdatasetmolecular table
    CreateMolecularTable(cnxdict)
    # CreateCommonLabTables(cnxdict)
    # --------------------------------------------------------------------------------------------------------------
    # Associate the molecular lab data with each arrival timepoint -- tons of fields
    cnxdict['EchoSQL']=1
    AssociateLabs(cnxdict)
    # AssociateCommonLabs(cnxdict)
    cnxdict['EchoSQL']=0
    # --------------------------------------------------------------------------------------------------------------
    # Range tables contain the StartDateRange, TargetDate, and EndDateRange values which represent dates in
    # which to look for valid testing results for a patient in order for them to be relevant for a patient event
    # such as "diagnosis" or "treatment start".  Since the time range of valid tests varies with the particular
    # test or event, a new range table is created for dis-similar tests.
    #CreateMolecularTestRange(cnxdict)

    # --------------------------------------------------------------------------------------------------------------
    # Copy updated playground to Jake
    PushPlaygroundTables(cnxdict, 'jake_caisis')
    # --------------------------------------------------------------------------------------------------------------
    # Copy updated playground to playgrounddatabase
    PushPlaygroundTables(cnxdict, 'playgrounddatabase')
    BuildRedCapDictionary(cnxdict)
    DownloadPlaygroundForRedCapUpload(cnxdict)

# Depriciated
# Call sql script to build a table framework for the playground
# Modify this script to modify the structure of the playground table
# sqlfileexecute("BuildPlaygroundStructure.sql", cnxdict, 'newplayground')
# sqlfileexecute("NewPlayground_2.sql", cnxdict, 'newplayground')


"""
    OUTPUT PROCEDURES
"""
def OutputProcedures(cnxdict):
    printtext('Program: ')    
    """
        OUTPUT PROCEDURES
    """

    filedescription = 'Playground'
    filename='{} Workbook'.format(filedescription)[0:28]
    cnxdict['out_filepath'] = buildfilepath(cnxdict, filename='{} Workbook'.format(filedescription))
    writer = pd.ExcelWriter(cnxdict['out_filepath'], datetime_format='mm/dd/yyyy')  # datetime_format='mmm d yyyy hh:mm:ss'

    sqlcmd = """
        SELECT a.* FROM caisis.playground a ;
    """

    df = pd.read_sql(sqlcmd, cnxdict['cnx'])
    df.to_excel(writer, sheet_name=filename, index=False)
    dowritersave(writer, cnxdict)
    print(cnxdict['out_filepath'])


lablist = ['ANC', 'ALB', 'BLAST', 'CREAT', 'FLUID', 'HCT', 'HGB', 'PLT',
           'RBC', 'UNCLASS', 'WBC', 'BILI', 'GFRBL', 'GFRNB']
molelist = ['FLT3', 'NPM1', 'CEBPA', 'MUT']
cnxdict = connect_to_mysql_db_prod('newplayground',lablist=lablist, molelist=molelist)

now = datetime.datetime.now()
print(now.strftime("%Y-%m-%d"))

MainProcedureCalls(cnxdict)

# OutputProcedures(cnxdict)
# CreatePlaygroundLabsTemplate(cnxdict)
# CreatePlaygroundMolecularTemplate(cnxdict)
# AssociateLabs(cnxdict)
# BuildRedCapDictionary(cnxdict)
# DownloadPlaygroundForRedCapUpload(cnxdict)