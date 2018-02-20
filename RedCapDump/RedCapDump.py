from Utilities.MySQLdbUtils import *
reload(sys)

Identifiers = ('PtMRN','ptmrn','PtName','PtBirthdate','PtBirthDate','PtDeathDate')

def create_disease_features(cnxdict):
    print('creating disease features')

    cnxdict['sql'] = """

        DROP TABLE IF EXISTS temp.df_aml;
        CREATE TABLE temp.df_aml
            SELECT a.pt_no
                , a.PtMRN AS df_mrn    # Medical Record Number
                , 'disease_features' AS redcap_repeat_instrument
                , e.redcap_repeat_instance_NEW AS redcap_repeat_instance
                , e.redcap_repeat_instance_NEW
                , CASE
                    WHEN b.StatusDisease LIKE '%nd%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN b.statusdate # 'Diagnosis' really new diagnosis arrival date
                    WHEN b.StatusDisease LIKE '%nd%' AND b.Status LIKE '%newly%' AND b.Status LIKE '%diagnosed%' THEN b.statusdate # 'Diagnosis' really new diagnosis date
                    WHEN b.StatusDisease LIKE '%rel%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN b.statusdate # 'Diagnosis' really relapsed arrival date
                    WHEN b.StatusDisease LIKE '%ref%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN b.statusdate # 'Diagnosis' really refractory arrival date
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
                    WHEN c.SecondaryType LIKE '%de novo aml%'
                        AND (c.SecondaryType LIKE '%AHD (type%'
                        OR       c.SecondaryType LIKE '%Chemotherapy History%'
                        OR       c.SecondaryType LIKE '%Secondary AML%' )
                        THEN 198 # Inconsistent
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
                        THEN 150 # Both AHD and t-AML

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
                FROM caisis.redcapmapping a
                JOIN caisis.vdatasetstatus b
                    ON a.PtMRN = b.PtMRN
                LEFT JOIN caisis.secondarystatus c
                    ON a.PtMRN = c.PtMRN

                LEFT JOIN temp.temp_aml_from_redcap e on a.pt_no = e.pt_no and
                    CASE
                        WHEN b.StatusDisease LIKE '%nd%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN b.statusdate # 'Diagnosis' really new diagnosis arrival date
                        WHEN b.StatusDisease LIKE '%nd%' AND b.Status LIKE '%newly%' AND b.Status LIKE '%diagnosed%' THEN b.statusdate # 'Diagnosis' really new diagnosis date
                        WHEN b.StatusDisease LIKE '%rel%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN b.statusdate # 'Diagnosis' really relapsed arrival date
                        WHEN b.StatusDisease LIKE '%ref%' AND b.Status LIKE '%arrival%' AND b.Status LIKE '%work%' THEN b.statusdate # 'Diagnosis' really refractory arrival date
                        ELSE NULL
                    END = e.df_date
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
                ORDER BY a.PtMRN, redcap_repeat_instance_NEW;

        UPDATE temp.df_aml a, temp.temp_aml_from_redcap b
            SET a.redcap_repeat_instance = b.redcap_repeat_instance_NEW
            WHERE a.pt_no = b.pt_no and a.df_date = b.df_date ;

        # SET @instanceid=0;
        # UPDATE temp.df_aml
        #     SET redcap_repeat_instance=@instanceid:=@instanceid+1;

        DROP TABLE temp.ph_aml;
        CREATE TABLE temp.ph_aml
        SELECT `df_aml`.`df_mrn` as `ph_mrn`,
                    date_format(min(`df_aml`.`df_date`),'%Y-%m-%d') AS `ph_date`,
                    -9999 AS `pt_no`
                FROM `temp`.`df_aml`
                WHERE `df_aml`.`df_dx_aml1` <> 198
                GROUP BY `df_aml`.`df_mrn`
                ORDER BY 2;

        # SET @instance=5499;
        # UPDATE temp.ph_aml SET pt_no=@instance:=@instance+1;

        # UPDATE temp.df_aml, temp.ph_aml
        #     SET df_aml.pt_no = ph_aml.pt_no
        #     WHERE df_aml.df_mrn = ph_aml.ph_mrn;


    """
    dosqlexecute(cnxdict) # normally do not need to recreate views
    return


def hide_dataframe_identifier(HideIdentifiers=True, df=None, IdentifierList=None):
    if not HideIdentifiers:
        return df
    collist = ''
    for col in df.columns:
        collist = collist + ',' + col.lower()
    collist = collist[1:]
    for identifier in IdentifierList:
        if identifier in df.columns:
            if 'date' in identifier.lower():
                df[identifier] = df[identifier].dt.strftime('%m/%Y')
                df[identifier].replace(['NaT'], '', regex=True, inplace=True)  # horizontal ellipsis
            else:
                df[identifier]=''
    return df


def create_tracs_treatment(cnxdict):
    print('creating treatment')

    cnxdict['sql'] = """
        DROP TABLE IF EXISTS TEMP.CaisisMedTherapy ;

        CREATE TABLE TEMP.CaisisMedTherapy
                SELECT
                    a.PtMRN
                    , a.PatientId
                    , a.MedicalTherapyId
                    , -9 AS MedTxAdministrationId
                    , a.MedTxDate
                    , a.MedTxDateText
                    , a.MedTxDate AS CycleDate
                    , a.MedTxDateText AS CycleDateText
                    , a.MedTxType
                    , a.MedTxDisease
                    , concat(a.MedTxIntent,' Regimen') AS MedTxIntent
                    , a.MedTxAgent AS OriginalMedTxAgent
                    , LTRIM(RTRIM(UPPER(a.MedTxAgent))) AS MedTxAgent
                    , LTRIM(RTRIM(UPPER(a.MedTxAgent))) AS MedTxAgentNoParen
                    , LOCATE('(',LTRIM(RTRIM(UPPER(a.MedTxAgent)))) AS FirstParen
                    , LENGTH(LTRIM(RTRIM(UPPER(a.MedTxAgent)))) - LOCATE(')',LTRIM(RTRIM(REVERSE(UPPER(a.MedTxAgent)))))  AS LastParen
                    , a.MedTxCycle
                    , a.MedTxNotes
                    , 'Regimen' AS TreatmentType
                    , ' 1' AS DisplayOrder
                    , 'Therapy Table' AS SourceTable
                FROM
                    caisis.vdatasetmedicaltherapy a
                WHERE YEAR(MedTxDate) > 2007
            UNION
                SELECT
                    a.PtMRN
                    , a.PatientId
                    , a.MedicalTherapyId
                    , b.MedTxAdministrationId
                    , a.MedTxDate
                    , a.MedTxDateText
                    , b.MedTxAdminStartDate AS CycleDate
                    , b.MedTxAdminStartDateText AS CycleDateText
                    , a.MedTxType
                    , a.MedTxDisease
                    , CASE
                        WHEN a.MedTxDate = b.MedTxAdminStartDate THEN concat('First ',a.MedTxIntent,' Cycle')
                        ELSE 'Cycle'
                    END AS MedTxIntent
                    , b.MedTxAdminAgent AS OriginalMedTxAgent
                    , LTRIM(RTRIM(UPPER(b.MedTxAdminAgent))) AS MedTxAgent
                    , LTRIM(RTRIM(UPPER(b.MedTxAdminAgent))) AS MedTxAgentNoParen
                    , LOCATE('(',LTRIM(RTRIM(UPPER(b.MedTxAdminAgent)))) AS FirstParen
                    , LENGTH(LTRIM(RTRIM(UPPER(b.MedTxAdminAgent)))) - LOCATE(')',LTRIM(RTRIM(REVERSE(UPPER(b.MedTxAdminAgent)))))  AS LastParen
                    , a.MedTxCycle
                    , b.MedTxAdminNotes AS MedTxNotes
                    , 'Cycle' AS TreatmentType
                    , ' 2' AS DisplayOrder
                    , 'Admin Table' AS SourceTable
                FROM
                    caisis.vdatasetmedicaltherapy a
                    LEFT JOIN caisis.vdatasetmedtxadministration b ON a.MedicalTherapyId = b.MedicalTherapyId and a.MedTxDate <> b.MedTxAdminStartDate
                WHERE b.MedicalTherapyId IS NOT NULL
                AND YEAR(MedTxDate) > 2007
            ORDER BY PtMRN, MedTxDate;

        UPDATE TEMP.CaisisMedTherapy
            SET MedTxAgentNoParen = LTRIM(RTRIM(REPLACE(MedTxAgent, SUBSTRING(MedTxAgent,FirstParen,LastParen-FirstParen+2),''))),
            CycleDate = IF(DisplayOrder = ' 1',NULL,CycleDate),
            CycleDateText = IF(DisplayOrder = ' 1',NULL,CycleDateText);

        ALTER TABLE `temp`.`caisismedtherapy` DROP COLUMN `LastParen`, DROP COLUMN `FirstParen`;

        # /*
        # Looking at frequency of disease and treatments entered
        #
        # SELECT MedTxDisease, COUNT(*) FROM TEMP.CaisisMedTherapy GROUP BY MedTxDisease ;
        # SELECT MedTxIntent, COUNT(*) FROM TEMP.CaisisMedTherapy GROUP BY MedTxIntent ;
        # */

        # /*
        # Remove treatment records other diseases
        # since we don't need to map them.
        # When exporting these treatment we will map to "other"
        # */
        DELETE FROM TEMP.CaisisMedTherapy
            WHERE MedTxDisease NOT RLIKE 'A[MP]L'
                AND MedTxDisease NOT RLIKE 'MDS'
                AND MedTxDisease NOT RLIKE 'RAEB'
                AND MedTxDisease NOT RLIKE 'RCMD'
                AND MedTxDisease NOT RLIKE 'BPDCN'
                AND UPPER(MedTxDisease) NOT RLIKE 'REL';

        DELETE FROM TEMP.CaisisMedTherapy
            WHERE MedTxIntent IN ( 'Ablation Regimen'
                    , 'Ablative Regimen'
                    , 'Consolidation Regimen'
                    , 'Immunosuppressive Regimen'
                    , 'Maintenance Regimen'
                    , 'Other Regimen'
                    , 'Palliative Regimen'
                    );

        # /*
        # Looking at frequency of disease and treatments entered
        # SELECT MedTxDisease, COUNT(*) FROM TEMP.CaisisMedTherapy GROUP BY MedTxDisease ;
        # SELECT MedTxIntent, COUNT(*) FROM TEMP.CaisisMedTherapy GROUP BY MedTxIntent ;
        # */
        #

        ALTER TABLE `temp`.`caisismedtherapy`
            ADD COLUMN `Intensity` mediumtext NULL AFTER `SourceTable`
            , ADD COLUMN `AnthracyclinDose` DOUBLE NULL AFTER `SourceTable`
            , ADD COLUMN `Anthracyclin` VARCHAR(255) NULL AFTER `SourceTable`
            , ADD COLUMN `BackboneAddOn` VARCHAR(255) NULL AFTER `SourceTable`
            , ADD COLUMN `BackboneName` VARCHAR(225) NULL AFTER `SourceTable`
            , ADD COLUMN `BackboneType` VARCHAR(20) NULL AFTER `SourceTable`
            ;

        # /*
        # SELECT * FROM temp.caisismedtherapy;
        # */

        UPDATE temp.caisismedtherapy a, caisis.backbonemapping3 b
            SET a.BackBoneType = b.BackBoneType
                , a.BackBoneName = b.BackBoneName
                , a.BackBoneAddOn = b.BackBoneAddOn
            where a.OriginalMedTxAgent = b.OriginalMedTxAgent
                AND a.BackBoneName IS NULL ;

        UPDATE temp.caisismedtherapy a, caisis.backbonemapping3 b
            SET a.BackBoneType = b.BackBoneType
                , a.BackBoneName = b.BackBoneName
                , a.BackBoneAddOn = b.BackBoneAddOn
            where a.MedTxAgent = b.OriginalMedTxAgent
                AND a.BackBoneName IS NULL ;

        UPDATE temp.caisismedtherapy a, caisis.backbonemapping3 b
            SET a.BackBoneType = b.BackBoneType
                , a.BackBoneName = b.BackBoneName
                , a.BackBoneAddOn = b.BackBoneAddOn
            where a.MedTxAgent = b.MedTxAgent
                AND a.BackBoneName IS NULL ;

        UPDATE temp.caisismedtherapy a, caisis.backbonemapping3 b
            SET a.BackBoneType = b.BackBoneType
                , a.BackBoneName = b.BackBoneName
                , a.BackBoneAddOn = b.BackBoneAddOn
            where a.MedTxAgentNoParen = b.MedTxAgent
                AND a.BackBoneName IS NULL ;

        UPDATE temp.caisismedtherapy a, caisis.backbonemapping3 b
            SET a.BackBoneType = b.BackBoneType
                , a.BackBoneName = b.BackBoneName
                , a.BackBoneAddOn = b.BackBoneAddOn
            where a.OriginalMedTxAgent = b.OriginalProtocol
                AND a.BackBoneName IS NULL ;

        UPDATE temp.caisismedtherapy a, caisis.backbonemapping3 b
            SET a.BackBoneType = b.BackBoneType
                , a.BackBoneName = b.BackBoneName
                , a.BackBoneAddOn = b.BackBoneAddOn
            where a.MedTxAgent = b.OriginalProtocol
                AND a.BackBoneName IS NULL ;

        UPDATE temp.caisismedtherapy a, caisis.backbonemapping3 b
            SET a.BackBoneType = b.BackBoneType
                , a.BackBoneName = b.BackBoneName
                , a.BackBoneAddOn = b.BackBoneAddOn
            where a.MedTxAgentNoParen = b.OriginalProtocol
                AND a.BackBoneName IS NULL ;

        # SELECT * FROM temp.caisismedtherapy
        #     WHERE BackBoneName LIKE '%no backbone%' AND BackboneAddOn LIKE '%hydrox%';

        # SELECT PtMRN, COUNT(*)
        # FROM temp.caisismedtherapy
        # WHERE BackBoneName LIKE '7+3'
        #         AND SourceTable = 'Therapy Table'
        # GROUP BY 1;

        # /*
        # All Induction/Salvage Treatments
        #
        # By this point I should have the unique treatments, now how can I identify treatments
        # that were completed at our center?
        #
        # Problem:  Hydroxyurea given before treatment to control blast crisis and are
        # getting inappropriately marked as the induction when they are supportive.
        #
        # All induction and salvage should be in the main medical therapy table
        # but will also appear in the administrations table.
        # */

        DROP TABLE IF EXISTS temp.ArrivalTreatments_ ;
        CREATE TABLE temp.ArrivalTreatments_
            SELECT a.PtMRN
                , StatusDisease
                , Status
                , StatusDate
                , b.MedicalTherapyId
                , b.MedTxDate
                , b.MedTxIntent
                , b.MedTxAgent
                , b.MedTxAgentNoParen
                , b.OriginalMedTxAgent
                , b.BackBoneType
                , b.BackBoneName
                , b.BackBoneAddOn
                , CONCAT(CASE
                        WHEN b.BackBoneName RLIKE 'G?.?CLAM.*'
                            AND b.BackBoneAddOn LIKE '%,mitoxantrone%' THEN REPLACE(b.BackBoneAddOn,',mitoxantrone','') # remove implied as part of the regimen
                        ELSE b.BackBoneAddOn
                    END,CASE
                    WHEN b.BackBoneName = 'FLAG-IDA'
                        THEN CASE
                            WHEN b.BackBoneAddOn LIKE '%idarubicin%' THEN '' # already there
                            ELSE ',idarubicin'
                        END
                    WHEN b.BackBoneName = 'AZA+VORINOSTAT'
                        THEN CASE
                            WHEN b.BackBoneAddOn LIKE '%vorinostat%' THEN '' # already there
                            ELSE ',vorinostat'
                        END
                    WHEN b.BackBoneName = 'HAM'
                        THEN CASE
                            WHEN b.BackBoneAddOn LIKE '%mitoxantrone%' THEN '' # already there
                            ELSE ',mitoxantrone'
                        END
                    ELSE ''
                END) AS CalculatedAddOn
                , b.Anthracyclin
                , b.AnthracyclinDose
                , b.Intensity
                , timestampdiff(day,a.StatusDate,b.MedTxDate) AS DaysToTx
                , a.ArrivalDate
                , a.NextArrivalDate
                , CASE
                    WHEN b.MedTxDate BETWEEN date_add(a.StatusDate,  INTERVAL -7 DAY) AND a.NextArrivalDate   THEN 'a'
                    WHEN b.MedTxDate BETWEEN date_add(a.ArrivalDate, INTERVAL -7 DAY) AND a.NextArrivalDate   THEN 'b'
                    WHEN b.MedTxDate > date_add(a.StatusDate,  INTERVAL -7 DAY) AND a.NextArrivalDate IS NULL THEN 'c'
                    WHEN b.MedTxDate > date_add(a.ArrivalDate, INTERVAL -7 DAY) AND a.NextArrivalDate IS NULL THEN 'd'
                    ELSE 'f'
                END AS Fallout
                FROM temp.caisismedtherapy b
                LEFT JOIN ( SELECT a.*, b.ArrivalDate, b.NextArrivalDate
                            FROM caisis.vdatasetstatus a
                                LEFT JOIN `caisis`.`v_arrival_with_prev_next` b
                                    ON a.PtMRN = b.PtMRN AND a.StatusDate = b.ArrivalDate
                            WHERE a.Status LIKE '%work%') a
                    ON a.PtMRN = b.PtMRN
                WHERE CASE
                    WHEN b.MedTxDate BETWEEN date_add(a.StatusDate,  INTERVAL -7 DAY) AND a.NextArrivalDate   THEN 1
                    WHEN b.MedTxDate BETWEEN date_add(a.ArrivalDate, INTERVAL -7 DAY) AND a.NextArrivalDate   THEN 1
                    WHEN b.MedTxDate > date_add(a.StatusDate,  INTERVAL -7 DAY) AND a.NextArrivalDate IS NULL THEN 1
                    WHEN b.MedTxDate > date_add(a.ArrivalDate, INTERVAL -7 DAY) AND a.NextArrivalDate IS NULL THEN 1
                    ELSE 0
                END
                AND a.StatusDisease = b.MedTxDisease
                AND b.SourceTable = 'Therapy Table'
                AND NOT (BackBoneName LIKE '%no backbone%' AND BackboneAddOn LIKE '%hydrox%')
                GROUP BY a.PtMRN, b.MedicalTherapyId ;

        # SELECT * FROM temp.ArrivalTreatments_ GROUP BY PtMRN, ArrivalDate, MedTxDate;
        #
        #
        # SELECT IF(b.PtMRN IS NULL,'No',b.PtMRN) AS ResponseFound
        #         , a.PtMRN
        #         , a.ArrivalDate
        #         , a.MedTxDate
        #         , MIN(b.ResponseDate) AS FirstResponseDate
        #         , MAX(b.ResponseDate) AS LastResponseDate
        #         , a.NextArrivalDate
        #         , timestampdiff(day,a.ArrivalDate,a.MedTxDate) AS DaysArrivalToMedTx
        #         , timestampdiff(day,a.MedTxDate,MIN(b.ResponseDate)) AS DaysMedTxToFirstResponse
        #         , timestampdiff(day,a.MedTxDate,MAX(b.ResponseDate)) AS DaysMedTxToLastResponse
        #         , timestampdiff(day,MIN(b.ResponseDate),a.NextArrivalDate) AS DaysFirstArrivalToNextArrival
        #         , timestampdiff(day,MAX(b.ResponseDate),a.NextArrivalDate) AS DaysLastArrivalToNextArrival
        #         , a.*
        #         , GROUP_CONCAT(b.ResponseDescription), count(*) AS TotalResponseFound
        #         FROM temp.ArrivalTreatments_ a
        #     LEFT JOIN caisis.v_response b on a.PtMRN = b.PtMRN AND a.BackBoneName NOT IN ('PALLIATIVE/HOSPICE','Consult only, no treatment')
        #     WHERE (    (b.ResponseDate BETWEEN a.ArrivalDate AND a.NextArrivalDate)
        #             OR (b.ResponseDate > a.ArrivalDate and a.NextArrivalDate IS NULL)
        #             OR (b.ResponseDate) IS NULL
        #             -- OR (b.PtMRN IS NULL )
        #           )
        #     GROUP BY a.PtMRN, a.ArrivalDate, a.MedTxDate
        #     ORDER BY a.PtMRN, a.ArrivalDate, a.MedTxDate;

        # /*
        # Inductions/Salvage related to an Arrival
        # SELECT b.PtMRN, a.PtMRN, a.BackBoneName, a.BackBoneAddOn
        #     FROM temp.ArrivalTreatments_ a
        #     LEFT JOIN caisis.vdatasetpatients b on a.PtMRN = b.PtMRN
        #         WHERE (MedTxIntent IS NOT NULL OR (MedTxIntent IS NULL AND StatusDate IS NULL))
        #     GROUP BY a.BackBoneName, a.BackBoneAddOn;
        #
        #
        # SELECT b.PtMRN, a.PtMRN, a.BackBoneName, a.*
        #     FROM temp.ArrivalTreatments_ a
        #     LEFT JOIN caisis.vdatasetpatients b on a.PtMRN = b.PtMRN
        #         WHERE (MedTxIntent IS NOT NULL OR (MedTxIntent IS NULL AND StatusDate IS NULL));
        #
        # SELECT MedTxIntent, count(*) FROM temp.ArrivalTreatments_ group by 1;
        # */

        DROP TABLE IF EXISTS Temp.temp2 ;

        CREATE TABLE Temp.temp2
        SELECT -99999 AS pt_no
            , PtMRN AS UWID
            , 'tracs_treatment' AS redcap_repeat_instrument
            , -99 AS redcap_repeat_instance
            , StatusDate as rx_date
            , '' AS rx_date_estimated
            , '' AS rx_ecog
            , '' as rx_echo
            , '' as rx_lvef
            , CASE
                WHEN BackBoneName = 'PALLIATIVE/HOSPICE'  THEN 7 # Palliative/Supportive Care/Hospice
                WHEN BackBoneName = 'Consult only, no treatment' THEN 8
                WHEN MedTxIntent = 'Induction Regimen'    THEN 1 # Induction Chemo
                WHEN MedTxIntent = 'Re-induction Regimen' THEN 1 # Induction Chemo
                WHEN MedTxIntent = 'Salvage 1 Regimen'    THEN 3
                WHEN MedTxIntent = 'Salvage 2 Regimen'    THEN 4
                WHEN MedTxIntent = 'Salvage 3 Regimen'    THEN 5
                WHEN MedTxIntent = 'Salvage 4 Regimen'    THEN 5
                WHEN MedTxIntent = 'Salvage 5 Regimen'    THEN 5
                WHEN MedTxIntent = 'Salvage >5 Regimen'   THEN 5
                WHEN MedTxIntent = 'Salvage Regimen'      THEN 10
                WHEN MedTxIntent IS NULL                  THEN 6 # Unkown
                ELSE 6
            END as rx_stage
            , CASE
                WHEN BackBoneName = 'PALLIATIVE/HOSPICE'  THEN '(7) Palliative/Supportive Care/Hospice'
                WHEN BackBoneName = 'Consult only, no treatment' THEN '(8) Consult only, no treatment'
                WHEN MedTxIntent = 'Induction Regimen'    THEN '(1) Induction Chemo'
                WHEN MedTxIntent = 'Re-induction Regimen' THEN '(1) Induction Chemo'
                WHEN MedTxIntent = 'Salvage 1 Regimen'    THEN '(3) Salvage 1'
                WHEN MedTxIntent = 'Salvage 2 Regimen'    THEN '(4) Salvage 2'
                WHEN MedTxIntent = 'Salvage 3 Regimen'    THEN '(5) Salvage 3+'
                WHEN MedTxIntent = 'Salvage 4 Regimen'    THEN '(5) Salvage 3+'
                WHEN MedTxIntent = 'Salvage 5 Regimen'    THEN '(5) Salvage 3+'
                WHEN MedTxIntent = 'Salvage >5 Regimen'   THEN '(5) Salvage 3+'
                WHEN MedTxIntent = 'Salvage Regimen'      THEN '(10) Salvage -- number unknown'
                WHEN MedTxIntent IS NULL                  THEN '(6) Unknown'
                ELSE 6
            END as `(rx_stage_text)`
            , CASE
                WHEN MedTxIntent = 'Salvage Regimen'      THEN MedTxIntent
                ELSE ''
            END AS rx_stage_ot
            , CASE
                    WHEN BackBoneName = 'PALLIATIVE/HOSPICE'  THEN 31
                    WHEN BackBoneName = 'Consult only, no treatment' THEN 31
                    WHEN MedTxAgent   = '2566(TA)'   THEN 30
                    WHEN MedTxAgent   = '2566(TD)'   THEN 30
                    WHEN MedTxAgent   = 'DECITABINE+G-CLAM+SORAFENIB' THEN 13
                    WHEN BackBoneName RLIKE 'G.?CLAM' THEN
                        CASE
                            # GCLAM 2734
                            WHEN MedTxAgent RLIKE '2734'                  THEN 500
                            # GCLAM 9713
                            WHEN lower(BackBoneAddOn) RLIKE 'decitabine'  THEN 501
                            WHEN BackBoneName = 'D-GCLAM'                 THEN 501
                            WHEN MedTxAgent RLIKE '9713'                  THEN 501
                            # GCLAM 9510
                            WHEN MedTxAgent RLIKE '9510'                  THEN 502
                            WHEN MedTxAgent = 'G-CLAM+SORAFENIB'          THEN 502
                            WHEN MedTxAgent = 'G-CLAM+GO'                 THEN 504
                            # GCLAM 9759
                            WHEN MedTxAgent RLIKE '9759'                  THEN 503
                            # GCLAM GO
                            WHEN BackBoneAddOn RLIKE 'GO'                 THEN 504
                            WHEN MedTxAgent = 'G-CLAM+GO'                 THEN 504
                            ELSE 500
                        END
                    WHEN BackBoneName RLIKE 'DECITABINE+ARA-C'     THEN 19
                    WHEN BackBoneName = 'GCLA'                     THEN 2
                    WHEN BackBoneName = 'G-CLA'                    THEN 2
                    WHEN BackBoneName = 'HiDAC'                    THEN 3
                    WHEN BackBoneName = 'FLAG'                     THEN 4
                    WHEN BackBoneName = 'FLAG-IDA'                 THEN 4
                    WHEN BackBoneName = '7+3'                      THEN 5
                    WHEN BackBoneName = 'IAP'                      THEN 6
                    WHEN BackBoneName = 'MEC'                      THEN 7
                    WHEN BackBoneName RLIKE 'CPX'                  THEN 8
                    WHEN BackBoneName = 'AZA'                      THEN 9
                    WHEN BackBoneName = 'AZA+VORINOSTAT'           THEN 9
                    WHEN BackBoneName = 'DECITABINE+ARA-C'         THEN 19
                    WHEN BackBoneName = 'DECITABINE'               THEN 10
                    WHEN BackBoneName RLIKE 'ATRA'                 THEN 12
                    WHEN BackBoneName RLIKE 'ARA'                  THEN 11
                    WHEN BackBoneName = 'ACTIMAB'                  THEN 16
                    WHEN BackBoneName = 'ANTI CD25'                THEN 17
                    WHEN BackBoneName = 'AMG-330'                  THEN 18
                    WHEN BackBoneName = 'EPOCH'                    THEN 20
                    WHEN BackBoneName RLIKE 'H3B'                  THEN 21
                    WHEN BackBoneName = 'HYPERCVAD'                THEN 22
                    WHEN BackBoneName = 'JWP+Ara-C'                THEN 23
                    WHEN BackBoneName = 'MITOXANTRONE + ETOPOSIDE' THEN 24
                    WHEN BackBoneName = 'PickADrug'                THEN 25
                    WHEN BackBoneName = 'SGN Anti CD123'           THEN 26
                    WHEN BackBoneName = 'SGN-CD123A|SGN-CD123A'    THEN 26
                    WHEN BackBoneName = 'Stemline for MRD'         THEN 27
                    WHEN BackBoneName = 'Trillium Anti CD47'       THEN 28
                    WHEN BackBoneName = 'WT1 for HLA A2+'          THEN 29
                    WHEN BackBoneName IN ('ABT-199+ARA-C (FH9237)'
                      ,'ABT-199+AZA (UW14053)'
                      ,'ABT-199|ABT199'
                      ,'ABT-199+ARA-C (FH9237)'
                      ,'ABT-199'
                      ,'AC-225 (2572)'
                      ,'AC-225'
                      ,'actinomycin D (dactinomycin)'
                      ,'adriamycin'
                      ,'anti CXCR4 antibody (MDX1338)'
                      ,'MDX-1338'
                      ,'AMG-232 +/- tremetnib (UW13037)'
                      ,'AMG-232'
                      ,'AZA+GO(S0703)'
                      ,'bendamustine+idarubicin (FH2413)'
                      ,'BEND-IDA'
                      ,'BMN (UW11003)'
                      ,'CEP-701 FLT3 drug'
                      ,'CEP701 - FLT3 drug'
                      ,'cladribine'
                      ,'CLADRABINE'
                      ,'clofarabine'
                      ,'clofarabine+LDAC (FH2302)'
                      ,'CWP (2513)'
                      ,'CWP'
                      ,'FLAM (2315)'
                      ,'FLAM'
                      ,'FLX-925'
                      ,'FLX925'
                      ,'G-CLAC'
                      ,'GCLAC (clofarabine, FH6568)'
                      ,'CLAG'
                      ,'GCLAC+Magic Cells (FH2335)'
                      ,'gemcitabine+docetaxel'
                      ,'HAM+vincristine'
                      ,'HCT varied'
                      ,'HCT'
                      ,'hedgehog inhibitor + LDAC or DEC or ARA-C+Dauno (FH2592)'
                      ,'HEDGEHOG|HEDGEHOG'
                      ,'IGN-523'
                      ,'Immunosuppression withdrawal'
                      ,'MEK inhibitor'
                      ,'nalarabine'
                      ,'OSTEOK vitamin'
                      ,'ON 01910.Na (2534)|LDAC'
                      ,'PDL1'
                      ,'Plerixafor (UW09011)'
                      ,'PLX3397'
                      ,'PLX-3397 (plerixafor) (2532)'
                      ,'PR-104'
                      ,'pralatrexate'
                      ,'pravastatin'
                      ,'SB-1518 (FH2246)'
                      ,'SGN-CD123a (FH9653)'
                      ,'SGN-CD123A|SGN-CD123A'
                      ,'SGN-CD33a (FH2690)'
                      ,'SGN-CD33A'
                      ,'SGN-CD33A|7+3'
                      ,'temozolomid'
                      ,'thioguanine (6TG)'
                      ,'tosedostat + decitabine vs arac (FH2566)'
                      ,'vorinostat + GO (FH2200)'
                      ,'vorinostat + GO + AZA (FH2288)'
                      ,'AZA+GO+VORINOSTAT'
                      ,'VORINO+GO'
                      ,'vorinostat'
                    ) THEN 30
                    WHEN MedTxAgent = '2566' THEN 30
                    WHEN BackBoneName IN ('ADCT-301'
                      , 'BORTEZOMIB'
                      , 'CD8+ T Cells (2498)'
                      , 'DACTINOMYCIN'
                      , 'GEMCITABINE'
                      , 'GO'
                      , 'HAM'
                      , 'HDAC'
                      , 'HYDROXYUREA'
                      , 'JAK-2'
                      , 'IBRUTINIB'
                      , 'IDARUBICIN'
                      , 'IT'
                      , 'LDAC'
                      , 'LENALIDOMIDE'
                      , 'MITOXANTRONE'
                      , 'PR104'
                      , 'TEMOZOLOMIDE'
                      , 'REVLAMID'
                      , 'RUXOLITINIB'
                      , 'SB1518'
                      , 'SORAFENIB'
                      , 'Withdrawal Immunosuppression'
                      , 'XRT'
                    ) THEN 32
                WHEN BackBoneName = 'RANDOMIZE MDS VS AML TX	11' THEN 13
                # WHEN BackBoneName = 'TOSEDOSTAT'                    THEN 'Look up to finalize arac vs decit'
                ELSE 13
            END AS rx_backbone
            , space(200) as `(rx_backbone_text)`
            , CASE
                WHEN BackBoneName = 'GCLAC+Magic Cells (FH2335)' THEN 'GCLAC+Magic Cells (FH2335)'
                WHEN BackBoneName = 'AC-225'                     THEN 'AC-225 (2572)'
                WHEN BackBoneName = 'ABT-199'                    THEN 'ABT-199+ARA-C (FH9237)'
                WHEN BackBoneName = 'ABT-199|ABT199'             THEN 'ABT-199+ARA-C (FH9237)'
                WHEN BackBoneName = 'AMG-232'                    THEN 'AMG-232 +/- tremetnib (UW13037)'
                WHEN BackBoneName = 'MDX-1338'                   THEN 'anti CXCR4 antibody (MDX1338)'
                WHEN BackBoneName = 'BEND-IDA'                   THEN 'bendamustine+idarubicin (FH2413)'
                WHEN BackBoneName = 'BEND-IDA'                   THEN 'bendamustine+idarubicin (FH2413)'
                WHEN BackBoneName = 'BMN (UW11003)'              THEN 'BMN (UW11003)'
                WHEN BackBoneName = 'CLADRIBINE'                 THEN 'cladribine'
                WHEN BackBoneName = 'CLADRABINE'                 THEN 'cladribine'
                WHEN BackBoneName = 'CWP'                        THEN 'CWP (2513)'
                WHEN BackBoneName = 'FLAM'                       THEN 'FLAM (2315)'
                WHEN BackBoneName = 'FLX925'                     THEN 'FLX-925'
                WHEN BackBoneName = 'HCT'                        THEN 'HCT varied'
                WHEN BackBoneName = 'IGN-523'                    THEN 'IGN-523'
                WHEN BackBoneName = 'CEP701 FLT3 drug'           THEN 'CEP-701 FLT3 drug'
                WHEN BackBoneName = 'CEP701 - FLT3 drug'         THEN 'CEP-701 FLT3 drug'
                WHEN BackBoneName = 'MEK INHIBITOR'              THEN 'MEK inhibitor'
                WHEN BackBoneName LIKE '%2534%'                  THEN 'ON 01910.Na (FH2534)'
                WHEN BackBoneName LIKE '%01910.Na%'              THEN 'ON 01910.Na (FH2534)'
                WHEN BackBoneName = 'PRALATREXATE'               THEN 'pralatrexate'
                WHEN BackBoneName = 'CLAG'                       THEN 'GCLAC (clofarabine, FH6568)'
                WHEN BackBoneName = 'G-CLAC'                     THEN 'GCLAC (clofarabine, FH6568)'
                WHEN BackBoneName = 'AZA+GO+VORINOSTAT'          THEN 'vorinostat + GO + AZA (FH2288)'
                WHEN BackBoneName = 'VORINO+GO'                  THEN 'vorinostat + GO (FH2200)'
                WHEN MedTxAgent   LIKE '%2566%'                  THEN 'tosedostat + decitabine vs arac (FH2566)'
                WHEN BackBoneName LIKE '%2566%'                  THEN 'tosedostat + decitabine vs arac (FH2566)'
                WHEN BackBoneName = 'HEDGEHOG|HEDGEHOG'          THEN 'hedgehog inhibitor + LDAC or DEC or ARA-C+Dauno (FH2592)'
                WHEN BackBoneName = 'PLX3397'                    THEN 'PLX-3397 (plerixafor) (2532)'
                WHEN BackBoneName = 'SGN-CD33A'                  THEN 'SGN-CD33a (FH2690)'
                WHEN BackBoneName = 'SGN-CD33A|7+3'              THEN 'SGN-CD33a (FH2690)'
                ELSE space(100)
            END AS rx_historical
            , space(100) AS rx_ot_ot
            , CASE
                WHEN OriginalMedTxAgent LIKE '%off%' THEN 0
                ELSE 99
            END AS rx_wildcard
            , CASE
                WHEN BackBoneName = '7+3'
                    THEN CASE
                        WHEN MedTxAgent LIKE '%dauno%' THEN 2
                        WHEN MedTxAgent LIKE '%90%'    THEN 2
                        WHEN MedTxAgent LIKE '%60%'    THEN 2
                        WHEN MedTxAgent LIKE '%45%'    THEN 2
                        WHEN MedTxAgent LIKE '%ida%'   THEN 1
                        ELSE 99
                    END
                WHEN BackBoneName RLIKE 'G?.?CLAM' THEN 3
                ELSE 99
            END AS rx_7plus3_anthra
            , CASE
                WHEN BackBoneName = '7+3'
                    THEN CASE
                        WHEN MedTxAgent LIKE '%dauno%' THEN '(2) daunorubicin'
                        WHEN MedTxAgent LIKE '%90%'    THEN '(2) daunorubicin'
                        WHEN MedTxAgent LIKE '%60%'    THEN '(2) daunorubicin'
                        WHEN MedTxAgent LIKE '%45%'    THEN '(2) daunorubicin'
                        WHEN MedTxAgent LIKE '%ida%'   THEN '(1) idarubicin'
                        ELSE 99
                    END
                WHEN BackBoneName RLIKE 'G?.?CLAM' THEN '(3) mitoxantrone'
                WHEN BackBoneName = 'HAM' THEN '(3) mitoxantrone'
                ELSE 99
            END AS `(rx_7plus3_anthra_text)`
            , CASE
                WHEN BackBoneName = '7+3'
                    THEN CASE
                        WHEN MedTxAgent LIKE '%90%'    THEN 90
                        WHEN MedTxAgent LIKE '%60%'    THEN 60
                        WHEN MedTxAgent LIKE '%45%'    THEN 45
                        WHEN MedTxAgent LIKE '%dauno%' THEN 60 # assuming when not noted
                        ELSE 99
                    END
                ELSE 99
            END AS rx_7plus3_dauno_dose
            , 99 AS rx_7plus3_ida_dose
            , 99 AS rx_mitox_dose
            , CASE
                WHEN BackboneName = 'HDAC'                    THEN 1 # 1, High Dose
                WHEN BackboneName = 'HiDAC'                   THEN 1 # 1, High Dose
                WHEN MedTxAgent = 'DNR +IDAC'                 THEN 2 # 2, Intermediate Dose
                WHEN BackboneName = 'LDAC'                    THEN 3 # 3, Low Dose
                WHEN BackboneName = 'CLOFARABINE+LDAC'        THEN 3 # 3, Low Dose
                WHEN BackboneName = 'ON 01910.Na (2534)|LDAC' THEN 3 # 3, Low Dose
                ELSE 99 # '99, unknown'
            END AS rx_ara_c_dose # unknown
            , CASE
                WHEN BackboneName = 'HDAC'                    THEN '1, High Dose:    >= 500 mg/2    (>= 1000 mg)'
                WHEN BackboneName = 'HiDAC'                   THEN '1, High Dose:    >=  500 mg/2    (>= 1000 mg)'
                WHEN MedTxAgent = 'DNR +IDAC'                 THEN '2, Intermediate Dose:   between 21-499mg/m2    (40mg-999mg).'
                WHEN BackboneName = 'LDAC'                    THEN '3, Low Dose:   <= 20mg/m2     (40 mg or less)'
                WHEN BackboneName = 'CLOFARABINE+LDAC'        THEN '3, Low Dose:   <= 20mg/m2     (40 mg or less)'
                WHEN BackboneName = 'ON 01910.Na (2534)|LDAC' THEN '3, Low Dose:   <= 20mg/m2     (40 mg or less)'
                ELSE '99, unknown'
            END AS `(rx_ara_c_dose_text)`
            , '' as rx_ara_c_dose_days
            ,CASE # atra
                WHEN BackBoneName = 'ATRA' THEN 1
                ELSE 0 # not atra (this should not happen!
            END as `rx_apl_regimen(atra)`
            ,CASE # arsenic
                WHEN BackBoneName = 'ATRA' AND CalculatedAddOn LIKE '%arsenic%' THEN 1
                ELSE 0 # not arsenic
            END as `rx_apl_regimen(ato)`
            ,CASE # cytarabine (ARA-C)
                WHEN BackBoneName = 'ATRA' AND CalculatedAddOn = ',decitabine vs arac' THEN 0
                WHEN BackBoneName = 'ATRA' AND CalculatedAddOn LIKE '%cytarabine%' THEN 1
                ELSE 0 # not ARA-C
            END as `rx_apl_regimen(arac)`
            ,CASE # daunorubicin
                WHEN BackBoneName = 'ATRA' AND CalculatedAddOn LIKE '%daunorubicin%' THEN 1
                ELSE 0 # not daunorubicin
            END as `rx_apl_regimen(dauno)`
            ,CASE # idarubicin
                WHEN BackBoneName = 'ATRA' AND CalculatedAddOn LIKE '%idarubicin%' THEN 1
                ELSE 0 # not idarubicin
            END as `rx_apl_regimen(ida)`
            , 0 as `rx_apl_regimen(ot)`
            ,CASE # atra
                WHEN BackBoneName = 'ATRA' AND CalculatedAddOn LIKE '%,cytarabine,daunorubicin%'       THEN '(atra) ATRA and (arac) ARA-C and (dauno) daunorubicin'
                WHEN BackBoneName = 'ATRA' AND CalculatedAddOn LIKE '%,gemtuzumab ozogamicin,arsenic%' THEN '(atra) ATRA and (arse) arsenic and (ot) other (GO)'
                WHEN BackBoneName = 'ATRA' AND CalculatedAddOn LIKE '%,idarubicin,arsenic%'            THEN '(atra) ATRA and (arse) arsenic and (ida) idarubicin'
                WHEN BackBoneName = 'ATRA' AND CalculatedAddOn LIKE '%arsenic%'                        THEN '(atra) ATRA and (arse) arsenic'
                WHEN BackBoneName = 'ATRA' AND CalculatedAddOn LIKE '%cytarabine%'                     THEN '(atra) ATRA and (arac) ARA-C'
                WHEN BackBoneName = 'ATRA' AND CalculatedAddOn LIKE '%daunorubicin%'                   THEN '(atra) ATRA and (dauno) daunorubicin'
                WHEN BackBoneName = 'ATRA' AND CalculatedAddOn LIKE '%idarubicin%'                     THEN '(atra) ATRA and (ida) idarubicin'
                WHEN BackBoneName = 'ATRA' AND CalculatedAddOn LIKE '%,gemtuzumab ozogamicin%'         THEN '(atra) ATRA and (ot) other (GO)'
                WHEN BackBoneName = 'ATRA' AND CalculatedAddOn = '' THEN '(1) ATRA'
                WHEN BackBoneName = 'ATRA' AND CalculatedAddOn NOT IN (',idarubicin,arsenic'
                    , ',gemtuzumab ozogamicin,arsenic'
                    , ',gemtuzumab ozogamicinc'
                    , ',cytarabine,daunorubicin'
                    , ',arsenic'
                    )
                    THEN '() none'
                ELSE ''
            END AS `(rx_apl_regimen_text)`

            , CASE
                WHEN CalculatedAddOn LIKE '%,aza%'   THEN 1 # 1, azacitidine (Vidaza)
                WHEN CalculatedAddOn = ',decitabine vs arac' THEN 4 #unknown
                WHEN CalculatedAddOn LIKE '%,decit%' THEN 2 # 2, decitabine (Dacogen)
                ELSE ''
            END AS rx_hma
            , CASE
                WHEN CalculatedAddOn LIKE '%,aza%'   THEN '(1) azacitidine (Vidaza)'
                WHEN CalculatedAddOn = ',decitabine vs arac' THEN '(4) type unknown'
                WHEN CalculatedAddOn LIKE '%,decit%' THEN '(2) decitabine (Dacogen)'
                ELSE 'Nothing'
            END AS `(rx_hma_text)`

            # NOTE THAT THIS IS A CHECK ALL THAT APPLY
            ,CASE # GCSF
                WHEN CalculatedAddOn LIKE '%gcsf%' THEN 1
                ELSE 0 # not gcsf
            END as `rx_gf(gcsf)`
            ,CASE # erythropoietin
                WHEN CalculatedAddOn LIKE '%erythropoietin%' THEN 1
                ELSE 0 # not erythropoietin
            END as `rx_gf(ery)`
            ,CASE # darbepoietin
                WHEN CalculatedAddOn LIKE '%darbepoietin%' THEN 1
                ELSE 0 # not darbepoietin
            END as `rx_gf(dar)`
            ,CASE # romiplostim
                WHEN CalculatedAddOn LIKE '%romiplostim%' THEN 1
                ELSE 0 # not romiplostim
            END as `rx_gf(rom)`
            ,CASE # eltrombopag
                WHEN CalculatedAddOn LIKE '%eltrombopag%' THEN 1
                ELSE 0 # not eltrombopag
            END as `rx_gf(elt)`
            ,CASE # testosterone
                WHEN CalculatedAddOn LIKE '%testosterone%' THEN 1
                ELSE 0 # not testosterone
            END as `rx_gf(tes)`
            ,CASE # danazol
                WHEN CalculatedAddOn LIKE '%danazol%' THEN 1
                ELSE 0 # not danazol
            END as `rx_gf(dan)`
            , 0 as `rx_gf(ot)`
            , CASE
                WHEN CalculatedAddOn LIKE '%,GCSF%'           THEN '(gcsf) GCSF'
                WHEN CalculatedAddOn LIKE '%,erythropoietin%' THEN '(ery) erythropoietin'
                WHEN CalculatedAddOn LIKE '%,darbepoietin%'   THEN '(dar) darbepoietin'
                WHEN CalculatedAddOn LIKE '%,romiplostim%'    THEN '(rom) romiplostim'
                WHEN CalculatedAddOn LIKE '%,eltrombopag%'    THEN '(elt) eltrombopag'
                WHEN CalculatedAddOn LIKE '%,testosterone%'   THEN '(tes) testosterone'
                WHEN CalculatedAddOn LIKE '%,danazol%'        THEN '(dan) danazol'
                ELSE ''
            END AS `(rx_gf_text)`

            , CASE
                WHEN CalculatedAddOn LIKE '%CAR T-cells%'           THEN 1  # CAR T-cells
                WHEN CalculatedAddOn LIKE '%gemtuzumab ozogamicin%' THEN 2  # gemtuzumab ozogamicin (GO, mylotarg)
                WHEN CalculatedAddOn LIKE '%rituximab%'             THEN 3  # rituximab (Rituxan)
                WHEN CalculatedAddOn LIKE '%rituxan%'               THEN 3  # rituximab (Rituxan)
                ELSE ''
            END AS rx_antibodies
            , CASE
                WHEN CalculatedAddOn LIKE '%CAR T-cells%'           THEN '(1) CAR T-cells'
                WHEN CalculatedAddOn LIKE '%gemtuzumab ozogamicin%' THEN '(2) gemtuzumab ozogamicin (GO, mylotarg)'
                WHEN CalculatedAddOn LIKE '%rituximab%'             THEN '(3) rituximab (Rituxan)'
                WHEN CalculatedAddOn LIKE '%rituxan%'               THEN '(3) rituximab (Rituxan)'
                ELSE ''
            END AS `(rx_antibodies_text)`

            , CASE
                WHEN CalculatedAddOn LIKE '%bosutinib%'    THEN 1 # bosutinib'
                WHEN CalculatedAddOn LIKE '%dasatinib%'    THEN 2 # dasatinib'
                WHEN CalculatedAddOn LIKE '%enasidenib%'   THEN 3 # enasidenib'
                WHEN CalculatedAddOn LIKE '%imatinib%'     THEN 4 # imatinib'
                WHEN CalculatedAddOn LIKE '%lestaurtinib%' THEN 5 # lestaurtinib'
                WHEN CalculatedAddOn LIKE '%midostaurin%'  THEN 6 # midostaurin'
                WHEN CalculatedAddOn LIKE '%nilotinib%'    THEN 7 # nilotinib'
                WHEN CalculatedAddOn LIKE '%pacritinib%'   THEN 8 # pacritinib'
                WHEN CalculatedAddOn LIKE '%ponatinib%'    THEN 9 # ponatinib'
                WHEN CalculatedAddOn LIKE '%ruxolitinib%'  THEN 10 # ruxolitinib'
                WHEN CalculatedAddOn LIKE '%sorafenib%'    THEN 11 # sorafenib'
                ELSE ''
            END AS rx_inhibitors
            , CASE
                WHEN CalculatedAddOn LIKE '%bosutinib%'    THEN '1, bosutinib'
                WHEN CalculatedAddOn LIKE '%dasatinib%'    THEN '2, dasatinib'
                WHEN CalculatedAddOn LIKE '%enasidenib%'   THEN '3, enasidenib'
                WHEN CalculatedAddOn LIKE '%imatinib%'     THEN '4, imatinib'
                WHEN CalculatedAddOn LIKE '%lestaurtinib%' THEN '5, lestaurtinib'
                WHEN CalculatedAddOn LIKE '%midostaurin%'  THEN '6, midostaurin'
                WHEN CalculatedAddOn LIKE '%nilotinib%'    THEN '7, nilotinib'
                WHEN CalculatedAddOn LIKE '%pacritinib%'   THEN '8, pacritinib'
                WHEN CalculatedAddOn LIKE '%ponatinib%'    THEN '9, ponatinib'
                WHEN CalculatedAddOn LIKE '%ruxolitinib%'  THEN '10, ruxolitinib'
                WHEN CalculatedAddOn LIKE '%sorafenib%'    THEN '11, sorafenib'
                ELSE ''
            END AS `(rx_inhibitors_text)`

            # NOTE THAT THIS IS A CHECK ALL THAT APPLY
            ,CASE # arsenic
                WHEN CalculatedAddOn LIKE '%arsenic%' THEN 1
                ELSE 0 # not arsenic
            END as `rx_ot_nc(ato)`
            ,CASE # ATG-CSA (cyclosporine)
                WHEN CalculatedAddOn LIKE '%cyclosporine%' THEN 1
                WHEN CalculatedAddOn LIKE '%atg%' THEN 1
                WHEN CalculatedAddOn LIKE '%csa%' THEN 1
                ELSE 0 # not ATG-CSA (cyclosporine)
            END as `rx_ot_nc(csa)`
            ,CASE # atra
                WHEN CalculatedAddOn LIKE '%atra%' THEN 1
                ELSE 0 # not atra
            END as `rx_ot_nc(atra)`
            ,CASE # bortezomib
                WHEN CalculatedAddOn LIKE '%bortezomib%' THEN 1
                ELSE 0 # not bortezomib
            END as `rx_ot_nc(bor)`
            ,CASE # cyclosporine
                WHEN CalculatedAddOn LIKE '%cyclosporine%' THEN 1
                ELSE 0 # not cyclosporine
            END as `rx_ot_nc(cyc)`
            ,CASE # granulocytes
                WHEN CalculatedAddOn LIKE '%granulocytes%' THEN 1
                ELSE 0 # not granulocytes
            END as `rx_ot_nc(gran)`
            ,CASE # hydroxyurea
                WHEN CalculatedAddOn LIKE '%hydroxyurea%' THEN 1
                WHEN CalculatedAddOn = 'HU' THEN 1
                ELSE 0 # not hydroxyurea
            END as `rx_ot_nc(hyd)`
            ,CASE # lenalidomide (revlimid)
                WHEN CalculatedAddOn LIKE '%lenalidomide %' THEN 1
                WHEN CalculatedAddOn LIKE '%revlimid %' THEN 1
                ELSE 0 # not lenalidomide (revlimid)
            END as `rx_ot_nc(len)`
            ,CASE # Nohla "magic" cells
                WHEN CalculatedAddOn LIKE '%magic%' THEN 1
                WHEN CalculatedAddOn LIKE '%nohla%' THEN 1
                ELSE 0 # not Nohla "magic" cells
            END as `rx_ot_nc(mag)`
            ,CASE # steroids/prednisone/dexamethasone
                WHEN CalculatedAddOn LIKE '%steroid%' THEN 1
                WHEN CalculatedAddOn LIKE '%dexamethasone%' THEN 1
                WHEN CalculatedAddOn LIKE '%prednisone%' THEN 1
                ELSE 0 # not steroids/prednisone/dexamethasone
            END as `rx_ot_nc(ste)`
            ,CASE # XRT (radiation treatment)
                WHEN CalculatedAddOn LIKE '%radiation %' THEN 1
                WHEN CalculatedAddOn LIKE '%XRT %' THEN 1
                ELSE 0 # not XRT (radiation treatment)
            END as `rx_ot_nc(xrt)`
            , 0 as `rx_ot_nc(ot)`
            , CASE
                WHEN CalculatedAddOn LIKE '%arsenic%'       THEN 'ato, arsenic (ATO)'
                WHEN CalculatedAddOn LIKE '%cyclosporine%'  THEN 'cyc, ATG-CSA (cyclosporine)'
                WHEN CalculatedAddOn LIKE '%tretinoin%'     THEN 'atra, ATRA'
                WHEN CalculatedAddOn LIKE '%bortezomib%'    THEN 'bor, bortezomib'
                WHEN CalculatedAddOn LIKE '%cyclosporine%'  THEN 'cyc, cyclosporine'
                WHEN CalculatedAddOn LIKE '%granulocytes%'  THEN 'gran, granulocytes'
                WHEN CalculatedAddOn LIKE '%hydroxyurea%'   THEN 'hyd, hydroxyurea'
                WHEN CalculatedAddOn LIKE '%lenalidomide%'  THEN 'len, lenalidomide (revlimid)'
                WHEN CalculatedAddOn LIKE '%revlimid%'      THEN 'len, lenalidomide (revlimid)'
                WHEN CalculatedAddOn LIKE '%nohla%'         THEN 'mag, Nohla "magic" cells'
                WHEN CalculatedAddOn LIKE '%magic%'         THEN 'mag, Nohla "magic" cells'
                WHEN CalculatedAddOn LIKE '%prednisone%'    THEN 'ste, steroids/prednisone/dexamethasone'
                WHEN CalculatedAddOn LIKE '%dexamethasone%' THEN 'ste, steroids/prednisone/dexamethasone'
                WHEN CalculatedAddOn LIKE '%steroids%'      THEN 'ste, steroids/prednisone/dexamethasone'
                WHEN CalculatedAddOn LIKE '%xrt%'           THEN 'xrt, XRT (radiation treatment)'
                ELSE ''
            END AS `(rx_ot_nc_text)`

            # NOTE THAT THIS IS A CHECK ALL THAT APPLY
            ,CASE # cytarabine (ARA-C)
                WHEN CalculatedAddOn = ',decitabine vs arac' THEN 0
                WHEN CalculatedAddOn LIKE '%cytarabine%' THEN 1
                WHEN CalculatedAddOn LIKE '%ara%' THEN 1
                ELSE 0 # not cytarabine (ARA-C)
            END as `rx_ot(ara)`
            ,CASE # daunorubicin
                WHEN CalculatedAddOn LIKE '%dauno%' THEN 1
                ELSE 0 # not daunorubicin
            END as `rx_ot(dau)`
            ,CASE # etoposide (VP16)
                WHEN CalculatedAddOn LIKE '%etop%' THEN 1
                ELSE 0 # not etoposide (VP16)
            END as `rx_ot(eto)`
            ,CASE # idarubicin
                WHEN CalculatedAddOn LIKE '%idar%' THEN 1
                ELSE 0 # not idarubicin
            END as `rx_ot(ida)`
            ,CASE # mitoxantrone
                WHEN CalculatedAddOn LIKE '%mitox%' THEN 1
                ELSE 0 # not mitoxantrone
            END as `rx_ot(mit)`
            ,CASE # IT chemo (intrathecal)
                WHEN CalculatedAddOn LIKE '%intrathecal%' THEN 1
                ELSE 0 # not IT chemo (intrathecal)
            END as `rx_ot(int)`
            ,CASE # vincristine
                WHEN CalculatedAddOn LIKE '%vincristine%' THEN 1
                ELSE 0 # not vincristine
            END as `rx_ot(vin)`
            , 0 as `rx_ot(ot)`
            ,CASE # text description of other chemo
                WHEN CalculatedAddOn = ',decitabine vs arac' THEN ''
                WHEN CalculatedAddOn LIKE '%cytarabine%' THEN 'ara, cytarabine (ARA-C)'
                WHEN CalculatedAddOn LIKE '%ara%' THEN 'ara, cytarabine (ARA-C)'
                WHEN CalculatedAddOn LIKE '%dauno%' THEN 'dau, daunorubicin'
                WHEN CalculatedAddOn LIKE '%etop%' THEN 'etp. etoposide (VP16)'
                WHEN CalculatedAddOn LIKE '%idar%' THEN 'ida, idarubicin'
                WHEN CalculatedAddOn LIKE '%mitox%' THEN 'mit, mitoxantrone'
                WHEN CalculatedAddOn LIKE '%intrathecal%' THEN 'int, IT chemo (intrathecal)'
                WHEN CalculatedAddOn LIKE '%vincristine%' THEN 'vin, vincristine'
                ELSE ''
            END as `(rx_ot_text)`
            , CASE
                WHEN BackBoneName = '7+3'
                    THEN CASE
                        WHEN MedTxAgent LIKE '%dauno%' THEN 'daunorubicin'
                        WHEN MedTxAgent LIKE '%90%'    THEN 'daunorubicin'
                        WHEN MedTxAgent LIKE '%45%'    THEN 'daunorubicin'
                        WHEN MedTxAgent LIKE '%ida%'   THEN 'idarubicin'
                        ELSE ''
                    END
                ELSE ''
            END AS CalculatedAnthracyclin
            , CASE
                WHEN BackBoneName = '7+3'
                    THEN CASE
                        WHEN MedTxAgent LIKE '%90%'    THEN 90
                        WHEN MedTxAgent LIKE '%45%'    THEN 45
                        WHEN MedTxAgent LIKE '%dauno%' THEN 60 # assuming when not noted
                        ELSE ''
                    END
                ELSE ''
            END AS CalculatedAnthracyclinDose
            , space(100) as unmapped
            , ArrivalTreatments_.*
            FROM temp.ArrivalTreatments_
        ;

        ALTER TABLE `temp`.`temp2` ADD INDEX `PtMRN` (`PtMRN`(10) ASC);

        UPDATE Temp.Temp2 a, Caisis.`tracsids` b
            SET a.pt_no = b.pt_no
            WHERE a.PtMRN = b.ph_mrn_ee;

        UPDATE temp.temp2
            SET unmapped = rtrim(BackBoneName),
            rx_historical = ''
            WHERE rx_backbone = '13'
            AND rtrim(rx_historical) = '';

        UPDATE temp.temp2
            SET rx_historical = rtrim(rx_historical)
            , unmapped = rtrim(unmapped);


        UPDATE temp.temp2
            SET `(rx_backbone_text)` = CASE
                WHEN rx_backbone = 1   THEN '(1) GCLAM options:'
                WHEN rx_backbone = 500  THEN '(500) GCLAM (FH2734)'
                WHEN rx_backbone = 501  THEN '(501) Dec+GCLAM (FH9713)'
                WHEN rx_backbone = 502  THEN '(502) Sorafenib+GCLAM (FH9510)'
                WHEN rx_backbone = 503  THEN '(503) Mini-GCLAM (FH9759)'
                WHEN rx_backbone = 504  THEN '(504) GO-GCLAM'
                WHEN rx_backbone = 2    THEN '(2) GCLA'
                WHEN rx_backbone = 3    THEN '(3) HiDAC'WHEN rx_backbone = '4'    THEN '(4) FLAG'
                WHEN rx_backbone = 5    THEN '(5) 7+3'
                WHEN rx_backbone = 6    THEN '(6) IAP'
                WHEN rx_backbone = 7    THEN '(7) MEC'
                WHEN rx_backbone = 8    THEN '(8) Vyxeos (CPX-351)'
                WHEN rx_backbone = 9    THEN '(9) azacitadine'
                WHEN rx_backbone = 10   THEN '(10) decitabine'
                WHEN rx_backbone = 11   THEN '(11) ARA-C containing regimen -- but not GCLAM, GCLA, 7+3, dec+arac, FLAG, HiDAC, IAP, MEC, FLAM, etc'
                WHEN rx_backbone = 32   THEN '(32) Non ARA-C containing regimen -- select individual add-on drug(s)'
                WHEN rx_backbone = 12   THEN '(12) APL therapy ATRA + other'
                WHEN rx_backbone = 13   THEN '(13) Not Mapped/Abstractor unsure of regimen, insert other.'
                WHEN rx_backbone = 14   THEN '(14) Clinical Trial Regimen'
                WHEN rx_backbone = 100  THEN '(100) ** Less Common backbone'
                WHEN rx_backbone = 99   THEN  '(99) another example for @HIDECHOICE'
                WHEN rx_backbone = 16   THEN  '(16) Actimab (FH2572)'
                WHEN rx_backbone = 17   THEN  '(17) Anti CD25 (FH9513)'
                WHEN rx_backbone = 18   THEN  '(18) BiTE AMG330 (FH9382)'
                WHEN rx_backbone = 19   THEN  '(19) Decit+arac (UW09019)'
                WHEN rx_backbone = 20   THEN  '(20) EPOCH'
                WHEN rx_backbone = 21   THEN  '(21) H3B Splicing modulator (FH9781)'
                WHEN rx_backbone = 22   THEN  '(22) HyperCVAD'
                WHEN rx_backbone = 23   THEN  '(23) JWP+arac (UW17003)'
                WHEN rx_backbone = 24   THEN  '(24) Mitox+etop(VP16)'
                WHEN rx_backbone = 25   THEN  '(25) Pick-a-Drug (UW8003 or UW9226)'
                WHEN rx_backbone = 26   THEN  '(26) SGN Anti CD123 (FH9653)'
                WHEN rx_backbone = 27   THEN  '(27) Stemline for MRD (FH9498)'
                WHEN rx_backbone = 28   THEN  '(28) Trillium Anti CD47 (UW16064)'
                WHEN rx_backbone = 29   THEN  '(29) WT1 for HLA A2+ (FH9296)'
                WHEN rx_backbone = 30   THEN  '(30) Historic regimen'
                WHEN rx_backbone = 31   THEN  '(31) Not treated'

                ELSE rx_backbone
            END ;

        UPDATE Temp.temp2
            SET rx_ot_ot = rtrim(unmapped)
            WHERE ltrim(rx_ot_ot) = '';


        SET @id:=0;

        DROP TABLE IF EXISTS temp.temp2_b;
        CREATE TABLE temp.temp2_b
        SELECT `temp2`.`pt_no`
            , `temp2`.`redcap_repeat_instrument`
            , @id:=@id+1 AS `redcap_repeat_instance`
            , `temp2`.`rx_date`
            , `temp2`.`rx_stage`
            , `temp2`.`rx_stage_ot`
            , `temp2`.`rx_backbone`
            , `temp2`.`rx_historical`
            , `temp2`.`rx_ot_ot`
            , `temp2`.`rx_wildcard`
            , `temp2`.`rx_7plus3_anthra`
            , `temp2`.`rx_7plus3_dauno_dose`
            , `temp2`.`rx_7plus3_ida_dose`
            , `temp2`.`rx_mitox_dose`
            , `temp2`.`rx_ara_c_dose`
            # check boxes
            , `temp2`.`rx_apl_regimen(atra)`  AS rx_apl_regimen___atra
            , `temp2`.`rx_apl_regimen(ato)`   AS rx_apl_regimen___ato
            , `temp2`.`rx_apl_regimen(arac)`  AS rx_apl_regimen___arac
            , `temp2`.`rx_apl_regimen(dauno)` AS rx_apl_regimen___dauno
            , `temp2`.`rx_apl_regimen(ida)`   AS rx_apl_regimen___ida
            , `temp2`.`rx_apl_regimen(ot)`    AS rx_apl_regimen___ot
            , `temp2`.`rx_hma`
            # check boxes
            , `temp2`.`rx_gf(gcsf)`    AS rx_gf___gcsf
            , `temp2`.`rx_gf(ery)`     AS rx_gf___ery
            , `temp2`.`rx_gf(dar)`     AS rx_gf___dar
            , `temp2`.`rx_gf(rom)`     AS rx_gf___rom
            , `temp2`.`rx_gf(elt)`     AS rx_gf___elt
            , `temp2`.`rx_gf(tes)`     AS rx_gf___tes
            , `temp2`.`rx_gf(dan)`     AS rx_gf___dan
            , `temp2`.`rx_gf(ot)`      AS rx_gf___ot
            , `temp2`.`rx_antibodies`
            , `temp2`.`rx_inhibitors`
            # check boxes
            , `temp2`.`rx_ot_nc(ato)`  AS rx_ot_nc___ato
            , `temp2`.`rx_ot_nc(csa)`  AS rx_ot_nc___csa
            , `temp2`.`rx_ot_nc(atra)` AS rx_ot_nc___atra
            , `temp2`.`rx_ot_nc(bor)`  AS rx_ot_nc___bor
            , `temp2`.`rx_ot_nc(cyc)`  AS rx_ot_nc___cyc
            , `temp2`.`rx_ot_nc(gran)` AS rx_ot_nc___gran
            , `temp2`.`rx_ot_nc(hyd)`  AS rx_ot_nc___hyd
            , `temp2`.`rx_ot_nc(len)`  AS rx_ot_nc___len
            , `temp2`.`rx_ot_nc(mag)`  AS rx_ot_nc___mag
            , `temp2`.`rx_ot_nc(ste)`  AS rx_ot_nc___ste
            , `temp2`.`rx_ot_nc(xrt)`  AS rx_ot_nc___xrt
            , `temp2`.`rx_ot_nc(ot)`   AS rx_ot_nc___ot
            # check boxes
            , `temp2`.`rx_ot(ara)`     AS rx_ot___ara
            , `temp2`.`rx_ot(dau)`     AS rx_ot___dau
            , `temp2`.`rx_ot(eto)`     AS rx_ot___eto
            , `temp2`.`rx_ot(ida)`     AS rx_ot___ida
            , `temp2`.`rx_ot(mit)`     AS rx_ot___mit
            , `temp2`.`rx_ot(int)`     AS rx_ot___int
            , `temp2`.`rx_ot(vin)`     AS rx_ot___vin
            , `temp2`.`rx_ot(ot)`      AS rx_ot___ot
        FROM `temp`.`temp2`
        WHERE pt_no > 0
        ORDER BY pt_no, rx_date;

        UPDATE temp.temp2_b a, (SELECT pt_no, MIN(redcap_repeat_instance) AS min_redcap_repeat_instance FROM Temp.temp2_b group by pt_no) b
            SET a.redcap_repeat_instance = a.redcap_repeat_instance -b.min_redcap_repeat_instance + 1
            WHERE a.pt_no = b.pt_no;

        # SELECT * FROM temp.temp2_b;
    """
    dosqlexecute(cnxdict) # normally do not need to recreate views
    return


import datetime
print(datetime.datetime.now().strftime('%Y%m%d_%H%M'))


def output_redcap(cnxdict,population_description='AML and MDS',HideIdentifiers=True):
    # create_disease_features(cnxdict)
    create_tracs_treatment(cnxdict)

    timenow=datetime.datetime.now().strftime('_%H%M%S')

    cnxdict['out_filepath'] = cnxdict['out_filepath'].replace('.xlsx','{0}.xlsx'.format(timenow))
    writer = pd.ExcelWriter(cnxdict['out_filepath'], datetime_format='mm/dd/yyyy')

    # ----------------------------------------------------------------------------------
    if False:
        sheet = '{0} Disease Features'.format(population_description)
        print ('Creating: {0}'.format(sheet))
        cmd = """
            SELECT `df_aml`.`pt_no`,
                `df_aml`.`df_mrn` as `ph_mrn`,
                `df_aml`.`redcap_repeat_instrument`,
                `df_aml`.`redcap_repeat_instance`,
                date_format(`df_aml`.`df_date`,'%Y-%m-%d') AS `df_date`,
                `df_aml`.`df_phase`,
                `df_aml`.`df_dx`,
                `df_aml`.`df_dx_ot`,
                `df_aml`.`df_dx_aml1`,
                `df_aml`.`df_dx_aml2`
            FROM `temp`.`df_aml`
            WHERE `df_aml`.`df_dx_aml1` <> 198
            ORDER BY `df_aml`.`df_mrn`, `df_aml`.`df_date`;
        """
        # df = pd.read_sql(cmd, cnxdict['cnx'])
        df = dosqlread(cmd, cnxdict['cnx'])

        df = clean_common_df_error_columns(df, get_colnames(cnxdict, sch='temp', tbl='population'))  # removed uni
        hide_dataframe_identifier(HideIdentifiers=HideIdentifiers, df=df, IdentifierList=Identifiers)
        try:
            df.to_excel(writer, sheet_name=sheet, index=False)
        except:
            print("Failed to create {0}".format(sheet))

    # ----------------------------------------------------------------------------------
    if True:
        sheet = '{0} TRACS Treatment'.format(population_description)
        print ('Creating: {0}'.format(sheet))
        cmd = """
            SELECT * FROM temp.temp2_b;
        """
        # df = pd.read_sql(cmd, cnxdict['cnx'])
        df = dosqlread(cmd, cnxdict['cnx'])

        df = clean_common_df_error_columns(df, get_colnames(cnxdict, sch='temp', tbl='population'))  # removed uni
        hide_dataframe_identifier(HideIdentifiers=HideIdentifiers, df=df, IdentifierList=Identifiers)
        try:
            df.to_excel(writer, sheet_name=sheet[0:29], index=False)
        except:
            print("Failed to create {0}".format(sheet))

    # ----------------------------------------------------------------------------------
    if True:
        sheet = '{0} Patients'.format(population_description)
        print ('Creating: {0}'.format(sheet))
        cmd = """
            SELECT b.pt_no
                , b.PtFirstName AS ph_name_first
                , b.PtMiddleName AS ph_name_middle
                , b.PtLastName AS ph_name_last
                , a.ph_mrn
                , '' AS ph_upn
                , b.PatientId AS ph_other_id
                , b.PtBirthDate AS ph_dob
                , b.PtGender AS ph_sex
                , CASE
                    WHEN b.PtRace = 'American Indian or Alaska Native'  THEN 0
                    WHEN b.PtRace = 'Asian'                             THEN 1
                    WHEN b.PtRace = 'Black or African American'         THEN 2
                    WHEN b.PtRace = 'White'                             THEN 4
                    WHEN b.PtRace = 'Multiple'                          THEN 88
                    WHEN b.PtRace = 'Declined to answer'                THEN 99
                    WHEN b.PtRace = 'Other/Unknown'                     THEN 99
                    WHEN b.PtRace IS NULL                               THEN 99
                    ELSE 99
                END AS ph_race
                , CASE
                    WHEN b.PtEthnicity = 'Hispanic or Latino'     THEN 0
                    WHEN b.PtEthnicity = 'Not-Hispanic or Latino' THEN 1
                    WHEN b.PtEthnicity = 'Declined to answer'     THEN 99
                    WHEN b.PtEthnicity = 'Other/Unknown'          THEN 99
                    WHEN b.PtEthnicity IS NULL                    THEN 99
                    ELSE 99
                END AS ph_ethnicity
                FROM `temp`.`ph_aml` a
                LEFT JOIN caisis.redcapmapping b on a.ph_mrn = b.PtMRN
                ORDER BY pt_no;
        """
        # df = pd.read_sql(cmd, cnxdict['cnx'])
        df = dosqlread(cmd, cnxdict['cnx'])

        df = clean_common_df_error_columns(df, get_colnames(cnxdict, sch='temp', tbl='population'))  # removed uni
        hide_dataframe_identifier(HideIdentifiers=HideIdentifiers, df=df, IdentifierList=Identifiers)
        try:
            df.to_excel(writer, sheet_name=sheet, index=False)
        except:
            print("Failed to create {0}".format(sheet))


    dowritersave(writer, cnxdict)
    return None


def MainRoutine(population,IncludePatientSourceTable=False,HideIdentifiers=True):
    cnxdict = connect_to_mysql_db_prod('utility')
    cnxdict['out_filepath'] = buildfilepath(cnxdict, DisplayPath=True)

    output_redcap(
        cnxdict
        ,population
        ,HideIdentifiers=HideIdentifiers)

    return None


MainRoutine('RedCap',HideIdentifiers=False)
