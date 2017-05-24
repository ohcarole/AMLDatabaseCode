from MySQLdbUtils import *

"""
J:\Estey_AML\AML Programming\Python\sharedUtils\CategorizeProtocol.py
These procedures expect a database connection to a specific schema
"""


def create_protocol_and_intensity_mapping_table(cnxdict):
    cnxdict['sql']="""
        SET SQL_SAFE_UPDATES = 0;
        DROP TABLE IF EXISTS protocolmapping;

        SET @rownum = 0;
        CREATE TABLE protocolmapping
            SELECT @rownum := @rownum+1 as pk
                , space(40) as UpdateItem
                , protocol as OriginalProtocol
                , CONCAT(' ',UCASE(a.protocol),' ') as protocol
                , space(50) as shortname
                , space(200) as `chemo note`
                , space(200) as `chemo add on`
                , space(200) as `other add on`
                , space(10) as wildcard
                , space(50) as intensity
                , `total use`
            FROM (select protocol, count(*) as `total use` from amldata group by protocol) a
            WHERE
                a.protocol IS NOT NULL
                AND a.protocol > '';
    """
    dosqlexecute(cnxdict)
    cleanup_protocol(cnxdict)
    categorize_protocol(cnxdict)
    categorize_chemotype(cnxdict)
    categorize_wildcard_protocol(cnxdict)
    categorize_chemo_regimen_additions(cnxdict)
    categorize_other_medications_added_to_chemo_regimens(cnxdict)
    categorize_based_on_add_on(cnxdict)
    categorize_other_and_non_treatment(cnxdict)
    return None


def cleanup_protocol(cnxdict):
    cnxdict['sql'] = """
        /*********************************************************************************
        # Table clean up
        *********************************************************************************/
        ALTER TABLE `protocolmapping`
            CHANGE COLUMN `pk` `pk` INT NOT NULL ,
            ADD PRIMARY KEY (`pk`);
        UPDATE `protocolmapping`
            SET `UpdateItem` = ''
            , `chemo note` = ''
            , `chemo add on` = ''
            , `other add on` = ''
            , `shortname`    = ''
            , `wildcard`     = ''
            , `intensity`    = '';
        UPDATE `protocolmapping`
            SET   `protocol` = REPLACE(`protocol`,' AND ','+');
        UPDATE `protocolmapping`
            SET   `protocol` = REPLACE(`protocol`,'&','+');
        UPDATE `protocolmapping`
            SET   `protocol` = REPLACE(`protocol`,'+ ','+');
        UPDATE `protocolmapping`
            SET   `UpdateItem` = CONCAT(',69',UpdateItem)
            , `protocol` = REPLACE(`protocol`,'I.V.','IV ')
            WHERE `protocol` LIKE 'I.V.';
        UPDATE `protocolmapping`
            SET   `UpdateItem` = CONCAT(',70',UpdateItem)
            , `protocol` = REPLACE(`protocol`,'IV-','IV ')
            WHERE `protocol` LIKE 'IV-';
        # UPDATE `protocolmapping`
        #     SET   `UpdateItem` = CONCAT(',71',UpdateItem)
        #     , `protocol` = REPLACE(`protocol`,'ORAL-CLO','ORAL CLO')
        #     WHERE `protocol` LIKE 'ORAL-';
    """
    return dosqlexecute(cnxdict)


def categorize_protocol(cnxdict):
    cnxdict['sql'] = """
        /*********************************************************************************
        # Protocol mapping
        *********************************************************************************/
        #   MICE
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',130',UpdateItem),
            shortname = CONCAT(',MICE',shortname)
            WHERE protocol RLIKE 'MICE';

        #  HAM-S
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',HAM-S',`shortname`)
            WHERE TRIM(protocol) = 'HAM-S';
        #   PR104
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',130',UpdateItem),
            shortname = CONCAT(',PR104',shortname)
            WHERE TRIM(protocol) = 'PR104';
        #   AC220
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',130',UpdateItem),
            shortname = CONCAT(',AC220',shortname)
            WHERE TRIM(protocol) = 'AC220';
        #   Rituximab-CHOP and Bortezomib (a.k.a. SWOG 0601
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',130',UpdateItem),
            shortname = CONCAT(',RITUXIMAB-CHOP + BORTEZOMIB',shortname)
            WHERE TRIM(protocol) = 'SWOG 0601';
        #   ADCT-301
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',130',UpdateItem),
            shortname = CONCAT(',ADCT-301',shortname)
            WHERE TRIM(protocol) = 'ADCT-301';
        #   FLX
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',130',UpdateItem),
            shortname = CONCAT(',FLX',shortname)
            WHERE TRIM(protocol) = 'FLX';
        #   TEMOZOLOMIDE
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',130',UpdateItem),
            shortname = CONCAT(',TEMOZOLOMIDE',shortname)
            WHERE TRIM(protocol) = 'TEMOZOLOMIDE';
        #   PRALATREXATE
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',130',UpdateItem),
            shortname = CONCAT(',PRALATREXATE',shortname)
            WHERE TRIM(protocol) = 'PRALATREXATE';
        #   MEK
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',130',UpdateItem),
            shortname = CONCAT(',MEK',shortname)
            WHERE TRIM(protocol) = 'MEK';
        #   MEC
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',130',UpdateItem),
            shortname = CONCAT(',MEC',shortname)
            WHERE TRIM(protocol) = 'MEC'
            OR protocol LIKE ' MEC %'
            AND shortname = '';
        #   FLAM
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',130',UpdateItem),
            shortname = CONCAT(',FLAM',shortname)
            WHERE TRIM(protocol) = 'FLAM';
        #  CLOFARABINE
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',130',UpdateItem),
            shortname = CONCAT(',CLOFARABINE',shortname)
            WHERE TRIM(protocol) = 'CLOFARABINE';
        # HYPERCVAD
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',130',UpdateItem),
            shortname = CONCAT(',HYPERCVAD',shortname)
            WHERE protocol RLIKE 'CVAD';
        # WIS
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',130',UpdateItem),
            shortname = CONCAT(',WIS',shortname)
            WHERE protocol RLIKE 'WIS';
        # DEC ARA-C (9019)
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',130',UpdateItem),
            shortname = CONCAT(',DEC ARA-C (9019)',shortname)
            WHERE protocol RLIKE '9019';
        # PAD-1 (8003)
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',129',UpdateItem),
            shortname = CONCAT(',PAD-1 (8003)',shortname)
            WHERE protocol RLIKE '8003';
        # PAD-2 (9226)
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',128',UpdateItem),
            shortname = CONCAT(',PAD-2 (9226)',shortname)
            WHERE protocol RLIKE '9226';
        # SGN-CD123 (9653)
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',73',UpdateItem),
            shortname = CONCAT(',SGN-CD123 (9653)',shortname)
            WHERE protocol RLIKE '9653'
            OR    protocol RLIKE 'SGN\.*CD123';
        # SGN-CD33 (2690)
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',73',UpdateItem),
            shortname = CONCAT(',SGN-CD33 (2690)',shortname)
            WHERE protocol RLIKE '2690'
            OR    protocol RLIKE 'SGN-CD33';
        # 7+3 SGN (9233)
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',130',UpdateItem),
            shortname = CONCAT(',SGN+7+3',shortname)
            WHERE (protocol RLIKE '9233'
            OR     protocol RLIKE 'SGN\.*[37]\.*[37]')
            AND    protocol NOT RLIKE '33A';
        # OUTPATIENT INDUCTION (7901)
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',73',UpdateItem),
            shortname = CONCAT(',OUTPATIENT INDUCTION (7901)',shortname)
            WHERE protocol RLIKE '7901';
        #SWOG 0301 (CSA, DAUNORUBICIN, ARA-C, G-CSF)'
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',73',UpdateItem),
            shortname = CONCAT(',SWOG 0301',shortname)
            WHERE protocol RLIKE 'SWOG 0301';
        # 2409/CME-P
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',97',UpdateItem),
            shortname = CONCAT(',CME-P (2409)',shortname)
            WHERE protocol RLIKE '2409';
        # 2413/BEND-IDA
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',98',UpdateItem),
            shortname = CONCAT(',BEND-IDA (2413)',shortname)
            WHERE protocol RLIKE '2413';
        # 2466/MK-2206
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',99',UpdateItem),
            shortname = CONCAT(',MK-2206 (2466)',shortname)
            WHERE protocol RLIKE '2466';
        # 2468/Y-DOTA
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',100',UpdateItem),
            shortname = CONCAT(',Y-DOTA (2468)',shortname)
            WHERE protocol RLIKE '2468'
            OR    protocol RLIKE 'Y\.*DOTA'
            OR    protocol like '%ydota%';
        # 2498/CD8 T-CELL
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',101',UpdateItem),
            shortname = CONCAT(',CD8 T-CELL (2498)',shortname)
            WHERE protocol RLIKE '2498';
        # 2513/CWP
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',102',UpdateItem),
            shortname = CONCAT(',CWP (2513)',shortname)
            WHERE protocol RLIKE '2513';
        # 2532/PLX108-05
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',103',UpdateItem),
            shortname = CONCAT(',PLX108-05 (2532)',shortname)
            WHERE protocol RLIKE '2532';
        # 2534/ON 01910
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',104',UpdateItem),
            shortname = CONCAT(',ON 01910 (2534)',shortname)
            WHERE protocol RLIKE '2534';
        # LIST
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',105',UpdateItem),
            shortname = CONCAT(',LIST',shortname)
            WHERE protocol RLIKE 'LIST';
        # 2566 TA/Tosedostat
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',105',UpdateItem),
            shortname = CONCAT(',TOSEDOSTAT (2566-TA)',shortname)
            WHERE protocol RLIKE '2566'
            AND   protocol RLIKE 'TA';
        # GO (7971)
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',126',UpdateItem),
            shortname = CONCAT(',GO (7971)',shortname)
            WHERE protocol RLIKE '7971';
        # GO (Expanded Access?)
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',127',UpdateItem),
            shortname = CONCAT(',GO (Expanded Access?)',shortname)
            WHERE protocol = ' GO ';
        # 2566 TD/Tosedostat
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',106',UpdateItem),
            shortname = CONCAT(',TOSEDOSTAT (2566-TD)',shortname)
            WHERE protocol RLIKE '2566'
            AND   protocol RLIKE 'TD';
        # 2572/AC-225
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',107',UpdateItem),
            shortname = CONCAT(',AC-225 (2572)',shortname)
            WHERE protocol RLIKE '2572';
        # 2566/Tosedostat
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',113',UpdateItem),
            shortname = CONCAT(',TOSEDOSTAT (2566)',shortname)
            WHERE protocol RLIKE '2566'
            AND   protocol NOT RLIKE '(TD|TA)';
        # MEC GO CSA
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',105',UpdateItem),
            shortname = CONCAT(',MEC GO CSA',shortname)
            WHERE protocol RLIKE 'MEC'
            AND   protocol RLIKE 'GO'
            AND   protocol RLIKE 'CSA' ;
        # Hedgehog
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',38',UpdateItem),
            `shortname` = CONCAT(',HEDGEHOG (2592)',`shortname`)
            WHERE protocol LIKE '%hedge%'
            OR    protocol RLIKE '2592'
            OR    protocol LIKE '%hog%';
        # D-GCLAM
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',2',UpdateItem),
            shortname = CONCAT(',D-GCLAM',shortname)
            WHERE protocol RLIKE 'D\.*G\.*(CLAM|CALM)';
        # GCLAM
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',1',UpdateItem),
            shortname = CONCAT(',G-CLAM',shortname)
            WHERE (protocol RLIKE 'G\.*(CLAM|CALM)'
            OR     protocol RLIKE '2734')
            AND   shortname NOT RLIKE 'D-GCLAM';
        # EPI PRIME (2588)
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',105',UpdateItem),
            shortname = CONCAT(',7+3 OR EPI PRIME (2588)',shortname)
            WHERE protocol RLIKE '2588';
        # UW09036/MDX
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',78',UpdateItem),
            shortname = CONCAT(',MDX',shortname)
            WHERE protocol LIKE '%9036%';
        # UW11003/BMN
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',79',UpdateItem),
            shortname = CONCAT(',UW11003',shortname)
            WHERE protocol RLIKE '11003';
        # UW13037/AMG
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',80',UpdateItem),
            shortname = CONCAT(',AMG 232',shortname)
            WHERE protocol RLIKE '13037'
            OR    protocol RLIKE 'AMG\.*232';
        # ABT-199
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',131',UpdateItem),
            shortname = CONCAT(',ABT-199 (unspecified)',shortname)
            WHERE protocol NOT RLIKE '14053'
            AND   protocol NOT RLIKE '9237'
            AND   protocol RLIKE 'ABT';
        # ABT-199 (9237)
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',132',UpdateItem),
            shortname = CONCAT(',ABT-199 (9237)',shortname)
            WHERE protocol RLIKE '9237';
        # ABT-199 (14053)
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',133',UpdateItem),
            shortname = CONCAT(',ABT-199 (14053)',shortname)
            WHERE protocol RLIKE '14053';
        # UW14070/E-MEC
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',83',UpdateItem),
            shortname = CONCAT(',E-MEC',shortname)
            WHERE protocol RLIKE '14070'
            OR    protocol RLIKE 'E[-]*MEC';
        # UW13049/IGN-523
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',81',UpdateItem),
            shortname = CONCAT(',IGN-523',shortname)
            WHERE protocol RLIKE '13049'
            OR    protocol RLIKE 'IGN\.*523';
        # IAP
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',67',UpdateItem),
            shortname = CONCAT(',IAP',shortname)
            WHERE protocol RLIKE 'IAP'
            OR    protocol RLIKE '2674'
            OR    protocol RLIKE 'IA PRAVA';
        # IA
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',77',UpdateItem),
            shortname = CONCAT(',IA',shortname)
            WHERE protocol LIKE ' ia%'
            AND   protocol NOT RLIKE 'IAP';
        # IT
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',75',UpdateItem),
            shortname = CONCAT(',IT',shortname)
            WHERE protocol RLIKE 'IT '
            AND   protocol NOT RLIKE 'IT MTX';
        # XRT
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',74',UpdateItem),
            shortname = CONCAT(',XRT',shortname)
            WHERE protocol RLIKE 'XRT'
            OR    protocol RLIKE 'RADIATION';
        # CLAM
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',4',UpdateItem),
            shortname = CONCAT(',CLAM',shortname)
            WHERE protocol RLIKE '.*(CLAM|CALM)'
            AND   protocol NOT RLIKE 'G\.*(CLAM|CALM)';
        # CLAG
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',5',UpdateItem),
            shortname = CONCAT(',CLAG',shortname)
            WHERE protocol RLIKE '.*(CLAG|CALG)';
        # G-CLAC
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',121',UpdateItem),
            shortname = CONCAT(',G-CLAC',shortname)
            WHERE protocol RLIKE 'G.{0,1}CLAC'
            AND   protocol NOT RLIKE '(6562|2335|7144)';
        # G-CLAC (2335)
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',123',UpdateItem),
            shortname = CONCAT(',G-CLAC (2335)',shortname)
            WHERE protocol RLIKE '2335';
         # G-CLAC (6562)
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',123',UpdateItem),
            shortname = CONCAT(',G-CLAC (6562)',shortname)
            WHERE protocol RLIKE '6562';
        # G-CLAC (7144)
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',124',UpdateItem),
            shortname = CONCAT(',G-CLAC (7144)',shortname)
            WHERE protocol RLIKE '7144';
        # G-CLA
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',3',UpdateItem),
            shortname = CONCAT(',G-CLA',shortname)
            WHERE protocol RLIKE 'G\.*(CLA|CAL)'
            AND   protocol NOT RLIKE 'D\.*G\.*(CLAM|CALM)'
            AND   protocol NOT RLIKE 'G\.*(CLAM|CALM)'
            AND   shortname NOT LIKE '%G-CLAC%';
        # HiDAC/High Dose Ara-C
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',87',UpdateItem),
            shortname = CONCAT(',HIDAC',shortname)
            WHERE protocol RLIKE 'H\.*DAC';
        # IDAC/Intermediate Dose Ara-C, a.k.a. Medium-Dose Ara-C
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',88',UpdateItem),
            shortname = CONCAT(',IDAC',shortname)
            WHERE protocol RLIKE 'I[ ]*DAC'
            AND   protocol NOT RLIKE 'H\.*DAC';
        # LDAC/Intermediate Dose Ara-C
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',89',UpdateItem),
            shortname = CONCAT(',LDAC',shortname)
            WHERE protocol RLIKE 'L[ ]*DAC';
        # FELDMAN
        UPDATE protocolmapping
            SET `UpdateItem` = CONCAT(',66',UpdateItem),
            shortname = CONCAT(',PRIMED 7+3',shortname)
            WHERE protocol RLIKE 'FELDMAN'
            AND   protocol RLIKE '[37].*[+].*[37]';
        # 7+3 or 3+7
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',7',UpdateItem),
            shortname = CONCAT(',7+3',shortname)
            WHERE shortname NOT RLIKE 'PRIMED'
            AND  (protocol RLIKE '[37].*[+].*[37]'
            OR    protocol RLIKE 'SWOG.*(301|106)'
            OR    protocol RLIKE '3+ARAC(2)'
            OR    protocol RLIKE '9011'
            OR    protocol RLIKE 'SO301'
            OR    protocol RLIKE 'SO106'
            OR    protocol RLIKE 'SO.*(301|106)')
            AND   NOT (protocol LIKE '%tki%'
            OR    protocol LIKE '%sgn%'
            OR    protocol LIKE '%cren%');
        # 5+2 or 2+5
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',8',UpdateItem),
            shortname = CONCAT(',5+2',shortname)
            WHERE shortname NOT RLIKE 'PRIMED'
            AND   protocol RLIKE '[25].*[+].*[52]';
        # HCT
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',9',UpdateItem),
            shortname = CONCAT(',HCT',shortname)
            WHERE protocol LIKE '%131%'
            OR    protocol LIKE '%739%'
            OR    protocol LIKE '%1432%'
            OR    protocol LIKE '%1641%'
            OR    protocol LIKE '%1931%'
            OR    protocol LIKE '%1938%'
            OR    protocol LIKE '%2010%'
            OR    protocol LIKE '%2044%'
            OR    protocol LIKE '%2130%'
            OR    protocol LIKE '%2186%'
            OR    protocol LIKE '%2309%'
            OR    protocol LIKE '%2524%'
            OR    protocol LIKE '%7617%'
            OR    protocol LIKE '%tbi%'
            OR    protocol RLIKE 'FLU[.]*TREO'
            OR    protocol RLIKE 'CD45I131'
            OR    protocol RLIKE 'CY\.*TBI'
            OR    protocol RLIKE 'FLU TREO'
            OR    protocol RLIKE 'FLU/BY '
            OR    protocol RLIKE 'BU-CY'
            OR    protocol RLIKE 'HCT'
            OR    protocol RLIKE 'PBMTC GVH 1201'
            OR    protocol RLIKE 'SCT';
        # Prepatory for HCT
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',84',UpdateItem),
            shortname = CONCAT(',HCT PREP',shortname)
            WHERE TRIM(protocol) IN ('2206'
                , '2222');
        # EPOCH
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',10',UpdateItem),
            shortname = CONCAT(',EPOCH',shortname)
            WHERE protocol RLIKE 'EPOCH';
        # AZA+GO+VORINO
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',90',UpdateItem),
            shortname = CONCAT(',AZA+GO+VORINO',shortname)
            WHERE (protocol RLIKE 'AZA'
            AND    protocol RLIKE '(VORINO|SAHA)'
            AND    protocol RLIKE 'GO')
            OR    protocol LIKE '%2288%';
        # GO+VORINO
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',91',UpdateItem),
            shortname = CONCAT(',GO+VORINO',shortname)
            WHERE (protocol RLIKE '(GO|(VORINO|SAHA))\.*(GO|(VORINO|SAHA))'
            AND    protocol NOT RLIKE 'AZA')
            OR     protocol LIKE '%2200%';
        # SB1518
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',92',UpdateItem),
            shortname = CONCAT(',SB1518',shortname)
            WHERE protocol RLIKE 'SB1518'
            OR    protocol LIKE '%2246%';
        # ORAL CLOF
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',93',UpdateItem),
            shortname = CONCAT(',ORAL CLOF',shortname)
            WHERE protocol RLIKE 'ORAL CLO'
            OR    protocol RLIKE 'ORAL-CLO'
            OR    protocol LIKE '%2302%';
        # FLAG IDA
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',94',UpdateItem),
            shortname = CONCAT(',FLAG IDA',shortname)
            WHERE (protocol RLIKE 'FLAG'
            AND    protocol RLIKE 'IDA');
        # FLAG
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',95',UpdateItem),
            shortname = CONCAT(',FLAG',shortname)
            WHERE (protocol RLIKE 'FLAG'
            AND    protocol NOT RLIKE 'IDA')
            OR    protocol LIKE '%2315%';
        # E-Selectin
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',11',UpdateItem),
            shortname = CONCAT(',E-MEC',shortname)
            WHERE protocol RLIKE 'E\.*SEL'
            AND   protocol RLIKE 'MEC';
        # E-Selectin
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',119',UpdateItem),
            shortname = CONCAT(',E-SELECTIN',shortname)
            WHERE protocol RLIKE 'E\.*SEL'
            AND   protocol NOT RLIKE 'MEC';
        # 7+3 Cren
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',12',UpdateItem),
            shortname = CONCAT(',7+3 CREN',shortname)
            WHERE  protocol RLIKE '9351'
            or    (protocol RLIKE '[37].*[+].*[37]'
            AND    (protocol LIKE '%tki%'
            OR      protocol LIKE '%cren%'));
        # MOM vs RUX
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',65',UpdateItem),
            shortname = CONCAT(',MOM VS RUX',shortname)
            WHERE protocol RLIKE 'MOM';
        # CPX-351 (2642)
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',110',UpdateItem),
            `shortname` = CONCAT(',CPX-351 (2642)',`shortname`)
            WHERE protocol RLIKE '2642';
        # CPX-351 (2651)
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',111',UpdateItem),
            `shortname` = CONCAT(',CPX-351 (2651)',`shortname`)
            WHERE protocol RLIKE '2651';
        # CPX-351 (?)
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',111',UpdateItem),
            `shortname` = CONCAT(',CPX-351 (?)',`shortname`)
            WHERE protocol RLIKE 'CPX'
            AND   protocol NOT RLIKE '(2651|2642)';
        # D-MEC
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',112',UpdateItem),
            `shortname` = CONCAT(',D-MEC',`shortname`)
            WHERE protocol RLIKE '(D.*MEC|MEC.*D)'
            OR    protocol RLIKE '2652';

    """
    return dosqlexecute(cnxdict)


def categorize_chemotype(cnxdict):
    cnxdict['sql'] = """
        /*********************************************************************************
        Chemo type note
        *********************************************************************************/
        # Cytarabine/Ara-C
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',13',UpdateItem),
            `chemo note` = CONCAT(',cytarabine',`chemo note`)
            WHERE ( protocol RLIKE 'ARA[ ]*C'
            OR      protocol RLIKE 'ARA[ ]*\-[ ]*C'
            OR      protocol LIKE '%cyta%')
            AND   ( protocol RLIKE '[37].*[+].*[37]'
            OR      protocol RLIKE 'SWOG.*106');
            OR      protocol RLIKE 'SWOG.*301');
        # Idarubicin/Ida
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',14',UpdateItem),
            `chemo note` = CONCAT(',idarubicin',`chemo note`)
            WHERE (protocol RLIKE 'IDA'
            AND    protocol NOT RLIKE 'FLAG.IDA'
            AND    protocol NOT RLIKE 'IDAC'
            AND    protocol NOT RLIKE 'VIDA'
            AND    protocol NOT RLIKE 'HIDA')
            OR    (protocol RLIKE 'IDA'
            AND    (protocol RLIKE '[37].*[+].*[37]'
            OR      protocol RLIKE 'SWOG.*(301|106)'));
        # Daunorubicin
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',15',UpdateItem),
            `chemo note` = CONCAT(',daunorubicin',`chemo note`)
            WHERE protocol LIKE '%dauno%'
            AND   ( protocol RLIKE '[37].*[+].*[37]'
            OR    protocol RLIKE 'SWOG.*(301|106)');
    """
    return dosqlexecute(cnxdict)


def categorize_wildcard_protocol(cnxdict):
    cnxdict['sql'] = """
        /*********************************************************************************
        Wildcard mapping
        *********************************************************************************/
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',16',UpdateItem),
            wildcard = 'Yes'
            WHERE protocol RLIKE 'OFF';
    """
    return dosqlexecute(cnxdict)


def categorize_chemo_regimen_additions(cnxdict):
    cnxdict['sql'] = """
        /*********************************************************************************
        Chemo add on mapping
        *********************************************************************************/
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',17',UpdateItem),
            `chemo add on` = TRIM(`chemo add on`);
        # daunorubicin
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',120',UpdateItem),
            `chemo add on` = CONCAT(',daunorubicin',`chemo add on`)
            WHERE (protocol RLIKE 'DNR'
            OR     protocol RLIKE '[+]\.*DAUNO')
            AND    protocol NOT RLIKE '[(]\.*DAUNO';
        # DOCETAXEL
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',125',UpdateItem),
            `chemo add on` = CONCAT(',docetaxel',`chemo add on`)
            WHERE protocol RLIKE 'DOCET';
        # Asparaganase
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',125',UpdateItem),
            `chemo add on` = CONCAT(',asparaginase',`chemo add on`)
            WHERE protocol RLIKE 'ASPA';
        # Nelarabine
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',125',UpdateItem),
            `chemo add on` = CONCAT(',nelarabine',`chemo add on`)
            WHERE protocol RLIKE 'NELA'
            OR    protocol RLIKE 'NALA';
        # MEK inhibitor
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',135',UpdateItem),
            `chemo add on` = CONCAT(',mek-inhibitor',`chemo add on`)
            WHERE protocol RLIKE 'MEK INHIBITOR';
        # Gemcitabine
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',125',UpdateItem),
            `chemo add on` = CONCAT(',gemcitabine',`chemo add on`)
            WHERE protocol RLIKE 'GEMCITABINE';
        # idarubicin
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',120',UpdateItem),
            `chemo add on` = CONCAT(',idarubicin',`chemo add on`)
            WHERE  protocol RLIKE 'IDA'
            AND    protocol NOT RLIKE 'FLAG.IDA'
            AND    protocol NOT RLIKE 'IDAC'
            AND    protocol NOT RLIKE 'VIDA'
            AND    protocol NOT RLIKE 'HIDA'
            AND    protocol NOT RLIKE '[37].*[+].*[37]'
            AND    protocol NOT RLIKE 'SWOG.*(301|106)';

        # SGN-CD33 (2690)
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',120',UpdateItem),
            `chemo add on` = CONCAT(',sgn antibody (type unclear)',`chemo add on`)
            WHERE protocol RLIKE 'SGN'
            AND   protocol NOT RLIKE 'CD(33|123)';
        # Sorafenib/Sora/Sor
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',18',UpdateItem),
            `chemo add on` = CONCAT(',sorafenib',`chemo add on`)
            WHERE protocol LIKE '%sor%';
        # vincristine/VP (NOT VP16 which is Etoposide)
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',19',UpdateItem),
            `chemo add on` = CONCAT(',vincristine',`chemo add on`)
            WHERE (protocol RLIKE 'VP'
            OR     protocol LIKE  '%vin%'
            OR     protocol RLIKE 'MVP'
            OR     protocol RLIKE 'EPOCH')
            AND   protocol NOT RLIKE 'VP[ ]*16';
        # Doxorubicin
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',20',UpdateItem),
            `chemo add on` = CONCAT(',doxorubicin',`chemo add on`)
            WHERE protocol RLIKE 'DOXO'
            OR    protocol RLIKE 'EPOCH';
        # Cyclophosphamide
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',21',UpdateItem),
            `chemo add on` = CONCAT(',cylophosphamide',`chemo add on`)
            WHERE protocol RLIKE 'DOXO'
            OR    protocol RLIKE 'EPOCH';
        # IT MTX, Intrathecal methotrexate
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',22',UpdateItem),
            `chemo add on` = CONCAT(',intrathecal methotrexate',`chemo add on`)
            WHERE protocol RLIKE 'IT[ ]*MTX';
        # MTX, methotrexate without IT/Intrathecal
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',23',UpdateItem),
            `chemo add on` = CONCAT(',methotrexate',`chemo add on`)
            WHERE protocol LIKE '%mtx%'
            AND   protocol NOT RLIKE 'IT[ ]*MTX';
        # Azacitidine
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',24',UpdateItem),
            `chemo add on` = CONCAT(',azacitidine',`chemo add on`)
            WHERE protocol LIKE '%aza%';
        # Myelotarg/GO/(MYLOTARG;
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',25',UpdateItem),
            `chemo add on` = CONCAT(',gemtuzumab ozogamicin',`chemo add on`)
            WHERE   (protocol LIKE '%go%'
            OR		 protocol RLIKE 'GO'
            OR		 protocol RLIKE 'SO106'
            OR       protocol LIKE '%mylotarg%'
            OR       protocol LIKE '%myelotarg%')
            AND 	protocol NOT LIKE '%goc%'
            AND 	protocol NOT LIKE '%without go%';
        # Cytarabine/Ara-C
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',26',UpdateItem),
            `chemo add on` = CONCAT(',cytarabine',`chemo add on`)
            WHERE ( protocol RLIKE 'ARA[ ]*C'
            OR      protocol RLIKE 'ARA[ ]*\-[ ]*C'
            OR      protocol RLIKE '[+]{1}[ ]*HIDAC'
            OR      protocol RLIKE 'DECI [+]*EC'
            OR      protocol RLIKE 'MEC'
            OR      protocol LIKE  '%ARA-C + ABT%'
            OR      protocol LIKE '%cyta%'
            OR      shortname LIKE '%7+3%')
            AND     protocol NOT RLIKE '2588'
            AND     protocol NOT LIKE '%mecgocsa%'
            AND     protocol NOT RLIKE 'SO106'
            AND     protocol NOT RLIKE 'S0301'
            AND     protocol NOT RLIKE '9011'
            AND     protocol NOT RLIKE '2562'
            AND     protocol NOT RLIKE '(D.*MEC|MEC.*D)'
            AND     protocol NOT RLIKE '[37][[:space:]]*\\\+[[:space:]]*[37]'
            AND    (protocol NOT RLIKE 'E\.*SEL' AND protocol NOT RLIKE 'MEC');

        # Low Dose Cytarabine/Ara-C
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',27',UpdateItem),
            `chemo add on` = CONCAT(',intermediate dose cytarabine',`chemo add on`)
            WHERE protocol RLIKE '[+][ ]{0,1}IDAC';
        # Low Dose Cytarbine/Ara-C
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',27',UpdateItem),
            `chemo add on` = CONCAT(',low dose cytarabine',`chemo add on`)
            WHERE protocol LIKE '%ldac%';
        # Bendamustine hydrochloride
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',28',UpdateItem),
            `chemo add on` = CONCAT(',bendamustine hydrochloride',`chemo add on`)
            WHERE protocol LIKE '%benda%';
        # Cladrabine/Clad
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',29',UpdateItem),
            `chemo add on` = CONCAT(',cladrabine',`chemo add on`)
            WHERE protocol LIKE '%clad%';
        # Rituximab/Rituxan
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',30',UpdateItem),
            `chemo add on` = CONCAT(',rituximab',`chemo add on`)
            WHERE protocol LIKE '%ritux%';
        # Fludarabine
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',31',UpdateItem),
            `chemo add on` = CONCAT(',fludarabine',`chemo add on`)
            WHERE protocol LIKE '%flud%';
        # I-CD45/ICD45/CD45I
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',32',UpdateItem),
            `chemo add on` = CONCAT(',i-cd45',`chemo add on`)
            WHERE protocol LIKE '%cd45%';
        # ATRA
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',33',UpdateItem),
            `chemo add on` = CONCAT(',tretinoin',`chemo add on`)
            WHERE protocol RLIKE 'ATRA'
            OR    protocol LIKE '%+ATRA%'
            OR    protocol LIKE '%all-trans%'
            OR    protocol LIKE '%tretinoin%'
            OR    protocol LIKE '%AA%';
        # Arsenic Trioxide
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',34',UpdateItem),
            `chemo add on` = CONCAT(',arsenic trioxide',`chemo add on`)
            WHERE protocol RLIKE 'ATO'
            OR    protocol LIKE '%+ato%'
            OR    protocol LIKE '%arsenic%'
            OR    protocol LIKE '%AA%';
        # Mitoxantrone
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',35',UpdateItem),
            `chemo add on` = CONCAT(',mitoxantrone',`chemo add on`)
            WHERE (protocol LIKE '%mito%'
            OR    protocol RLIKE 'MVP'
            OR    protocol RLIKE 'ME '
            OR    protocol RLIKE 'MVCSAP'
            OR    protocol RLIKE 'MEC')
            AND   protocol NOT RLIKE '2562'
            AND   protocol NOT RLIKE '(D.*MEC|MEC.*D)'
            AND   protocol NOT LIKE '%mecgocsa%'
            AND    (protocol NOT RLIKE 'E\.*SEL' AND protocol NOT RLIKE 'MEC');
        # Etoposide
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',36',UpdateItem),
            `chemo add on` = CONCAT(',etoposide',`chemo add on`)
            WHERE (protocol LIKE '%etop%'
            OR     protocol RLIKE 'ME '
            OR     protocol RLIKE 'MVCSAP'
            OR     protocol RLIKE 'MEC'
            OR     protocol RLIKE 'DECI [+]*EC'
            OR     protocol RLIKE 'VP[ ]*16'
            OR     protocol RLIKE 'EPOCH')
            AND    protocol NOT RLIKE '2562'
            AND    protocol NOT RLIKE '(D.*MEC|MEC.*D)'
            AND    protocol NOT LIKE '%mecgocsa%'
            AND    (protocol NOT RLIKE 'E\.*SEL' AND protocol NOT RLIKE 'MEC');
        # I-CD45/ICD45/CD45I
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',37',UpdateItem),
            `chemo add on` = CONCAT(',dotabiotin',`chemo add on`)
            WHERE protocol LIKE '%dota%';
        # Hedgehog
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',38',UpdateItem),
            `chemo add on` = CONCAT(',hedgehog',`chemo add on`)
            WHERE protocol LIKE '%hedge%'
            OR    protocol RLIKE '2592'
            OR    protocol LIKE '%hog%';

        # decitabine
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',114',UpdateItem),
            `chemo add on` = CONCAT(',decitabine',`chemo add on`)
            WHERE (protocol RLIKE 'DECI'
            OR    protocol RLIKE 'DECT'
            OR   (protocol RLIKE 'DAC' AND protocol NOT RLIKE 'DACT')
            OR    protocol RLIKE '2588')
            AND   (protocol NOT RLIKE 'H\.*DAC'
            AND    protocol NOT RLIKE 'I\.*DAC'
            AND    protocol NOT RLIKE 'L[ ]*DAC'
            AND    protocol NOT RLIKE 'LDAC'
            AND    protocol NOT RLIKE 'IDAC'
            AND    protocol NOT RLIKE 'HDAC'
            AND    protocol NOT RLIKE '2562'
            AND    protocol NOT RLIKE '(D.*MEC|MEC.*D)'
            AND    protocol NOT RLIKE 'DACT');
        # Bortezomib
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',40',UpdateItem),
            `chemo add on` = CONCAT(',bortezomib',`chemo add on`)
            WHERE protocol LIKE '%bortez%'
            OR    protocol LIKE '%velcade%';
        # Hydroxyurea
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',41',UpdateItem),
            `chemo add on` = CONCAT(',hydroxyurea',`chemo add on`)
            WHERE protocol LIKE '%hu%'
            OR    protocol LIKE '%hydrox%';
        # Crenolanib
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',42',UpdateItem),
            `chemo add on` = CONCAT(',crenolanib',`chemo add on`)
            WHERE protocol LIKE '%cren%'
            OR    protocol LIKE '%tki%';
        # Dactinomycin
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',43',UpdateItem),
            `chemo add on` = CONCAT(',dactinomycin',`chemo add on`)
            WHERE protocol RLIKE '.ACT.{1}D'
            OR    protocol RLIKE 'DACT';
        # Actinomycin
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',44',UpdateItem),
            `chemo add on` = CONCAT(',actinomycin',`chemo add on`)
            WHERE protocol RLIKE 'ACT'
            AND   protocol NOT RLIKE 'DACT'
            AND   protocol NOT RLIKE '.ACT.{1}D';
        # Trametinib
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',45',UpdateItem),
            `chemo add on` = CONCAT(',trametinib',`chemo add on`)
            WHERE protocol RLIKE 'TR[EA]M';
        # THIOGUANINE
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',46',UpdateItem),
            `chemo add on` = CONCAT(',thioguanine',`chemo add on`)
            WHERE protocol RLIKE '6\.*TG'
            OR    protocol RLIKE 'THIO';
        # Vorinostat
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',47',UpdateItem),
            `chemo add on` = CONCAT(',vorinostat',`chemo add on`)
            WHERE protocol RLIKE 'VORINO'
            OR    protocol RLIKE 'SAHA'
            OR    protocol RLIKE 'ZOLINZA';

        # Pembrolizumab
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',48',UpdateItem),
            `chemo add on` = CONCAT(',pembrolizumab ',`chemo add on`)
            WHERE protocol RLIKE 'PDL';
        # Ruxolitinib
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',49',UpdateItem),
            `chemo add on` = CONCAT(',ruxolitinib ',`chemo add on`)
            WHERE (protocol LIKE '%rux%'
            OR    protocol LIKE '%jak%');
        # Lenalidomide
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',50',UpdateItem),
            `chemo add on` = CONCAT(',lenalidomide',`chemo add on`)
            WHERE protocol LIKE '%lenal%'
            OR    protocol LIKE '%revl%';
        # Romiplostim
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',51',UpdateItem),
            `chemo add on` = CONCAT(',romiplostim ',`chemo add on`)
            WHERE protocol LIKE '%romi%';
        # Imatinib
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',52',UpdateItem),
            `chemo add on` = CONCAT(',imatinib mesylate',`chemo add on`)
            WHERE protocol LIKE '%imat%'
            OR    protocol LIKE '%glee%';
        # momelotinib
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',64',UpdateItem),
            `chemo add on` = CONCAT(',momelotinib',`chemo add on`)
            WHERE protocol LIKE '%mom%';
        # Ibrutinib
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',53',UpdateItem),
            `chemo add on` = CONCAT(',ibrutinib',`chemo add on`)
            WHERE protocol LIKE '%ibru%'
            OR    protocol LIKE '%ibur%'
            OR    protocol LIKE '%imbru%';
    """
    return dosqlexecute(cnxdict)


def map_first_agent(cnxdict):
    cnxdict['sql'] = """
        /*********************************************************************************
        Map using chemo add on information when no protocol shortname has been assigned
        *********************************************************************************/

        # PALLIATIVE
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',PALLIATIVE',`shortname`)
            WHERE shortname = ''
            AND   protocol RLIKE 'PALL'
            OR    protocol RLIKE 'HOSP';

        # ARA-C
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',ARA-C',`shortname`)
            WHERE shortname = ''
            AND   `chemo add on` = ',cytarabine'
            AND   `other add on` = '';

        # IDA
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',IDA',`shortname`)
            WHERE `chemo add on` = ',idarubicin';


        # AMG 330
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',AMG 330',`shortname`)
            WHERE shortname = ''
            AND   protocol RLIKE 'AMG'
            AND   protocol RLIKE '330';

        # ATRA
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',ATRA',`shortname`)
            WHERE shortname = ''
            AND   `chemo add on` = ',tretinoin'
            AND   `other add on` = '';

        # ATO
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',ATO',`shortname`)
            WHERE shortname = ''
            AND   `chemo add on` = ',arsenic trioxide'
            AND   `other add on` = '';

        # AZA
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',AZA',`shortname`)
            WHERE shortname = ''
            AND   `chemo add on` = ',azacitidine'
            AND   `other add on` = '';

        # ASPARAGINASE
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',ASPARAGINASE',`shortname`)
            WHERE `chemo add on` = ',asparaginase';

        # BORTEZOMIB
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',BORTEZOMIB',`shortname`)
            WHERE `chemo add on` = ',bortezomib';

        # CEP 701
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',CEP 701',`shortname`)
            WHERE `chemo add on` = ',lestaurtinib';

        # CLAD
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',CLAD',`shortname`)
            WHERE `chemo add on` = ',cladrabine';

        # DACT
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',DACT',`shortname`)
            WHERE `chemo add on` = ',dactinomycin';

        # DECI
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',DECI',`shortname`)
            WHERE `chemo add on` = ',decitabine';

        # DEXAMETHASONE
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',DEXAMETHASONE',`shortname`)
            WHERE `chemo add on` = ',dexamethasone';

        # DOCETAXEL
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',DOCETAXEL',`shortname`)
            WHERE `chemo add on` = ',docetaxel';

        # ETOPOSIDE
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',ETOPOSIDE',`shortname`)
            WHERE `chemo add on` = ',etoposide';


        # GEMCITABINE
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',GEMCITABINE',`shortname`)
            WHERE `chemo add on` = ',gemcitabine';

        # GO
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',GO',`shortname`)
            WHERE `chemo add on` = ',gemtuzumab ozogamicin';

        # HU
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',HU',`shortname`)
            WHERE `chemo add on` = ',hydroxyurea';

        # IBRUTINIB
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',IBRUTINIB',`shortname`)
            WHERE `chemo add on` = ',ibrutinib';

        # IMATINIB
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',IMATINIB',`shortname`)
            WHERE `chemo add on` = ',imatinib';

        # IT MTX
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',IT MTX',`shortname`)
            WHERE `chemo add on` = ',intrathecal methotrexate';

        # LENALIDOMIDE
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',LENALIDOMIDE',`shortname`)
            WHERE `chemo add on` = ',lenalidomide';

        # MEK INHIBITOR
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',MEK INHIBITOR',`shortname`)
            WHERE `chemo add on` = ',mek-inhibitor';

        # MTX
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',MTX',`shortname`)
            WHERE `chemo add on` = ',methotrexate';

        # MITO
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',MITO',`shortname`)
            WHERE `chemo add on` = ',mitoxantrone';

        # NELARABINE
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',NELARABINE',`shortname`)
            WHERE `chemo add on` = ',nelarabine';

        # OSTEO-K
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',OSTEO-K',`shortname`)
            WHERE `chemo add on` = ',osteo-k';

        # PDL1
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',PDL1',`shortname`)
            WHERE `chemo add on` = ',pembrolizumab';

        # RITUXIMAB
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',RITUXIMAB',`shortname`)
            WHERE `chemo add on` = ',rituximab';

        # ROMIPLOSTIM
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',ROMIPLOSTIM',`shortname`)
            WHERE `chemo add on` = ',romiplostim';

        # +RUXOLITINIB
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +RUXOLITINIB')
            WHERE shortname <> ''
            AND   shortname <> ',RUXOLITINIB'
            AND   `chemo add on` LIKE '%,ruxolitinib%';

        # SORA
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',SORA',`shortname`)
            WHERE `chemo add on` = ',sorafenib';

        # THIOGUANINE
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',THIOGUANINE',`shortname`)
            WHERE `chemo add on` = ',thioguanine';

        # VINCRISTINE
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',VINCRISTINE',`shortname`)
            WHERE `chemo add on` = ',vincristine';

        # VORINOSTAT
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',VORINOSTAT',`shortname`)
            WHERE `chemo add on` = ',vorinostat';



    """
    return dosqlexecute(cnxdict)


def map_using_muliple_agent(cnxdict):
    cnxdict['sql'] = """
        /*********************************************************************************
        Map using chemo add on information when no protocol shortname has been assigned
        *********************************************************************************/

        SET SQL_SAFE_UPDATES = 0;

        # AA
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',AA',`shortname`)
            WHERE `chemo add on` = ',arsenic trioxide,tretinoin';

        # EC
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',EC',`shortname`)
            WHERE `chemo add on` LIKE '%,etoposide,cytarabine%';

        # ME
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',ME',`shortname`)
            WHERE `chemo add on` = ',etoposide,mitoxantrone';

        # MVP
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',MVP',`shortname`)
            WHERE `chemo add on` = ',mitoxantrone,vincristine';

        # MVCSAP (Mito Etop Cyclasporine & pravastatin)
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',MV-CSA-P',`shortname`)
            WHERE `chemo add on` = ',etoposide,mitoxantrone'
            AND   `other add on` = ',cyclosporine,pravastatin';

    """
    return dosqlexecute(cnxdict)


def categorize_based_on_add_on(cnxdict):
    map_first_agent(cnxdict)
    map_using_muliple_agent(cnxdict)
    cnxdict['sql'] = """
        /*********************************************************************************
        Map using chemo add on information when a shortname has been assigned
        *********************************************************************************/

        # +ARA-C
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +ARA-C')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',ARA-C'
            AND   `chemo add on` LIKE '%,cytarabine'
            AND   `other add on` = '';

        # +ATRA
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +ATRA')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',ATRA'
            AND   `chemo add on` LIKE '%,tretinoin%';

        # +AZA
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +AZA')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',AZA'
            AND   `other add on` LIKE '%,azacitidine%';

        # +ASPARAGINASE
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +ASPARAGINASE')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',ASPARAGINASE'
            AND   `chemo add on` LIKE '%,asparaginase%';

        # +BORTEZOMIB
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +BORTEZOMIB')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',BORTEZOMIB'
            AND   `chemo add on` LIKE '%,bortezomib%';

        # +CEP 701
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +CEP 701')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',CEP 701'
            AND   `chemo add on` LIKE '%,lestaurtinib%';

        # +CLAD
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +CLAD')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',CLAD'
            AND   `chemo add on` = '%,cladrabine%'
            AND   `other add on` = '';

        # +DACT
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +DACT')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',DACT'
            AND   `chemo add on` LIKE '%,dactinomycin%';

        # +DECI
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +DECI')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',DECI'
            AND   `chemo add on` LIKE '%,decitabine%';

        # +DEXAMETHASONE
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +DEXAMETHASONE',)
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',DEXAMETHASONE'
            AND   `other add on` LIKE '%,dexamethasone%';

        # +DOCETAXEL
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +DOCETAXEL')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',DOCETAXEL'
            AND   `chemo add on` LIKE '%,docetaxel%';

        # +ETOPOSIDE
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +ETOPOSIDE')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',ETOPOSIDE'
            AND   `chemo add on` LIKE '%,etoposide%';

        # +GEMCITABINE
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +GEMCITABINE')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',GEMCITABINE'
            AND   `chemo add on` LIKE '%,gemcitabine%';

        # +GO
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +GO')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',GO'
            AND   `chemo add on` LIKE '%,gemtuzumab ozogamicin%'
            AND   `other add on` = '';

        # +HU
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +HU')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',HU'
            AND   `chemo add on` LIKE '%,hydroxyurea%';

        # +IBRUTINIB
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +IBRUTINIB')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',IBRUTINIB'
            AND   `chemo add on` LIKE '%,ibrutinib%';

        # +IDA
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +IDA')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',IDA'
            AND   `chemo add on` LIKE '%,idarubicin%';

        # +IMATINIB
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +IMATINIB')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',IMATINIB'
            AND   `chemo add on` LIKE '%,imatinib%';

        # +IT MTX
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +IT MTX')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',IT MTX'
            AND   `chemo add on` LIKE '%,intrathecal methotrexate%';

        # +LENALIDOMIDE
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +LENALIDOMIDE')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',LENALIDOMIDE'
            AND   `chemo add on` LIKE '%,lenalidomide%';

        # +MEK INHIBITOR
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +MEK INHIBITOR')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',MEK INHIBITOR'
            AND   `chemo add on` LIKE '%mek-inhibitor%';

        # +MTX
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +MTX')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',MTX'
            AND   `chemo add on` LIKE '%,methotrexate%';

        # +MITO
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +MITO')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',MITO'
            AND   `chemo add on` LIKE '%,mitoxantrone%';

        # +NELARABINE
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +NELARABINE')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',NELARABINE'
            AND   `chemo add on` LIKE '%nelarabine%';

         # +OSTEO-K
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +OSTEO-K')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',OSTEO-K'
            AND   `other add on` LIKE '%,osteo-k%';

        # +PDL1
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +PDL1')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',PDL1'
            AND   `chemo add on` LIKE '%,pembrolizumab%';

        # +PRAV
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +PRAV')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',PRAV'
            AND   `other add on` LIKE '%,pravastatin%';

        # +RITUXIMAB
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +RITUXIMAB')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',RITUXIMAB'
            AND   `chemo add on` LIKE '%,rituximab%';

         # +ROMIPLOSTIM
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +ROMIPLOSTIM')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',ROMIPLOSTIM'
            AND   `chemo add on` LIKE '%,romiplostim%';

        # RUXOLITINIB
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',RUXOLITINIB',`shortname`)
            WHERE `chemo add on` = ',ruxolitinib';

        # +SORA
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +SORA')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',SORA'
            AND   `chemo add on` LIKE '%,sorafenib%';

        # +THIOGUANINE
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +THIOGUANINE')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',THIOGUANINE'
            AND   `chemo add on` LIKE '%,thioguanine%';

        # +VINCRISTINE
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +VINCRISTINE')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',VINCRISTINE'
            AND   `chemo add on` LIKE '%,vincristine%';

        # +VORINOSTAT
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +VORINOSTAT')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',VORINOSTAT'
            AND   `chemo add on` LIKE '%,vorinostat%';

        /* ------------------------------------------------------------------------------------------------ */

        # RUXOLITINIB
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',RUXOLITINIB',`shortname`)
            WHERE `chemo add on` = ',ruxolitinib';


        # +RUXOLITINIB
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(`shortname`,' +RUXOLITINIB')
            WHERE shortname <> ''
            AND   shortname NOT RLIKE ',RUXOLITINIB'
            AND   `chemo add on` LIKE '%,ruxolitinib%';

        # MEC
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',MEC',`shortname`)
            WHERE (protocol LIKE ' MEC%'
            OR     protocol LIKE '%XRT +MEC%')
            AND    protocol NOT RLIKE 'MECGOCSA';

        """
    return dosqlexecute(cnxdict)


def categorize_other_and_non_treatment(cnxdict):
    cnxdict['sql'] = """
        /*********************************************************************************
        Palliative/Hospice/No Treatment/Treatment Unknown
        *********************************************************************************/
        # PALLIATIVE/HOSPICE
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',PALLIATIVE/HOSPICE',`shortname`)
            WHERE protocol RLIKE 'PALL'
            OR    protocol RLIKE 'HOS';

        # OUTSIDE
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',OUTSIDE',`shortname`)
            WHERE protocol RLIKE 'OUTSIDE';

        # NO TREATMENT
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',NO TREATMENT',`shortname`)
            WHERE protocol RLIKE 'NO TREATMENT';

        # UNKNOWN
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',UNKNOWN',`shortname`)
            WHERE protocol RLIKE 'UNK'
            OR    protocol RLIKE 'UKN';

        # CONSULT
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',200',UpdateItem),
            `shortname` = CONCAT(',CONSULT',`shortname`)
            WHERE protocol RLIKE 'CON';

    """
    return dosqlexecute(cnxdict)


def categorize_other_medications_added_to_chemo_regimens(cnxdict):
    cnxdict['sql'] = """
        /*********************************************************************************
        Other add on mapping
        *********************************************************************************/
        # Osteo-K
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',125',UpdateItem),
            `chemo add on` = CONCAT(',osteo-k',`other add on`)
            WHERE protocol RLIKE 'OSTEO';
        # G-CSF, Filgrastim
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',72',UpdateItem),
            `other add on` = CONCAT(',filgrastim',`other add on`)
            WHERE protocol RLIKE 'G-CSF'
            OR    protocol LIKE '%filgr%';
        # prednisone
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',54',UpdateItem),
            `other add on` = CONCAT(',prednisone',`other add on`)
            WHERE protocol RLIKE 'EPOCH'
            OR    protocol LIKE '%pred%';
        # pravastatin
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',55',UpdateItem),
            `other add on` = CONCAT(',pravastatin',`other add on`)
            WHERE protocol LIKE '%prav%'
            OR    protocol RLIKE 'MVCSAP';
        # cranial radiation treatment
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',56',UpdateItem),
            `other add on` = CONCAT(',cranial radiation',`other add on`)
            WHERE protocol LIKE '%cranial xrt%';
        # radiation treatment
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',57',UpdateItem),
            `other add on` = CONCAT(',radiation',`other add on`)
            WHERE protocol LIKE '%xrt%'
            OR    protocol LIKE '%radiation%'
            AND   protocol NOT LIKE '%cranial xrt%';
        # Dexamethasone
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',58',UpdateItem),
            `other add on` = CONCAT(',dexamethasone',`other add on`)
            WHERE protocol LIKE '%dexa%';
        # intrathecal treatment
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',59',UpdateItem),
            `other add on` = CONCAT(',intrathecal',`other add on`)
            WHERE protocol RLIKE '[ ]IT[ ]';
            AND   protocol NOT RLIKE 'IT[ ]*MTX';
        # plerixafor
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',60',UpdateItem),
            `other add on` = CONCAT(',plerixafor',`other add on`)
            WHERE protocol RLIKE 'PLERIX';
        # Lestaurtinib
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',61',UpdateItem),
            `other add on` = CONCAT(',lestaurtinib',`other add on`)
            WHERE protocol RLIKE 'CEP';
        -- # Lestaurtinib
        -- UPDATE protocolmapping
        --     SET   `UpdateItem` = CONCAT(',62',UpdateItem),
        -- `other add on` = CONCAT(',lestaurtinib',`chemo add on`)
        -- 	WHERE protocol RLIKE 'PDL'
        --     OR protocol RLIKE 'CEP';
        -- # Cyclosporine
        UPDATE protocolmapping
            SET   `UpdateItem` = CONCAT(',63',UpdateItem),
            `other add on` = CONCAT(',cyclosporine',`other add on`)
            WHERE (protocol RLIKE 'CSA'
            OR    protocol RLIKE 'SAND')
            OR    protocol RLIKE 'MVCSAP';

    """
    return dosqlexecute(cnxdict)
