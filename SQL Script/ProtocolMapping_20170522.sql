SET SQL_SAFE_UPDATES = 0;

/**********************************************************************************************
Build protocol list table
**********************************************************************************************/

DROP TABLE IF EXISTS protocollist;

SET @rownum = 0;

CREATE TABLE protocollist
    SELECT @rownum := @rownum+1             AS pk
        , SPACE(40)                         AS UpdateItem
        , protocol                          AS OriginalProtocol
        , CONCAT(' ',UCASE(a.protocol),' ') AS protocol
        , SPACE(50)                         AS mapto
        , SPACE(50)                         AS noninduction
        , SPACE(50)                         AS singleregimen
        , SPACE(50)                         AS multiregimen
        , SPACE(200)                        AS druglist
        , SPACE(10)                         AS wildcard
        , SPACE(50)                         AS intensity
        , SPACE(200)                        AS treatmenttypes
        , totaluse
    FROM (SELECT protocol, COUNT(*) AS totaluse FROM amldata GROUP BY protocol) a
    WHERE
        a.protocol IS NOT NULL
        AND a.protocol > '';

/**********************************************************************************************
Format data
**********************************************************************************************/
ALTER TABLE protocollist
    CHANGE COLUMN pk pk INT NOT NULL ,
    ADD PRIMARY KEY (pk);

UPDATE protocollist
    SET updateitem   = ''
    , mapto          = ''
    , noninduction   = ''
    , singleregimen  = ''
    , multiregimen   = ''
    , druglist       = ''
    , wildcard       = ''
    , intensity      = ''
    , treatmenttypes = '';
    

/**********************************************************************************************
Standardize data if needed
**********************************************************************************************/
UPDATE protocollist SET protocol = REPLACE(protocol,'ACTINOMYCIN','DACTINOMYCIN');
UPDATE protocollist SET protocol = REPLACE(protocol,'DACTINOMYCIN','ACT-D');
UPDATE protocollist SET protocol = REPLACE(protocol,'6TG','THIOGUANINE');
UPDATE protocollist SET protocol = REPLACE(protocol,'DACOGEN','DECITABINE');
UPDATE protocollist SET protocol = REPLACE(protocol,' DAC ','DECITABINE');
UPDATE protocollist SET protocol = REPLACE(protocol,'PRAVASTATIN, MITOXANTRONE AND ETOPOSIDE','PRAVA +MITO +ETOP');
UPDATE protocollist SET protocol = '7+3 +SGN-CD33A' WHERE protocol = ' SGN + 7 + 3 ' OR protocol = ' SGN+7+3 ';

/**********************************************************************************************
Set wildcard flag
**********************************************************************************************/
UPDATE protocollist SET wildcard = 'Yes' WHERE protocol RLIKE 'OFF';
UPDATE protocollist SET protocol = REPLACE(protocol,'OFF','');
   
/**********************************************************************************************
HCT Regimens
**********************************************************************************************/
# HCT / HCT PREP
UPDATE protocollist SET mapto = CASE
        WHEN protocol RLIKE 'HCT' THEN CONCAT(mapto,',HCT')
        WHEN protocol RLIKE 'SCT' THEN CONCAT(mapto,',HCT')
        WHEN protocol RLIKE '[^0-9]131[^0-9]' THEN CONCAT(mapto,',HCT')
        WHEN protocol RLIKE '[^0-9]739[^0-9]' THEN CONCAT(mapto,',HCT')
        WHEN protocol RLIKE '[^0-9]1432[^0-9]' THEN CONCAT(mapto,',HCT')
        WHEN protocol RLIKE '[^0-9]1931[^0-9]' THEN CONCAT(mapto,',HCT')
        WHEN protocol RLIKE '[^0-9]2010[^0-9]' THEN CONCAT(mapto,',HCT')
        WHEN protocol RLIKE '[^0-9]2044[^0-9]' THEN CONCAT(mapto,',HCT')
        WHEN protocol RLIKE '[^0-9]2130[^0-9]' THEN CONCAT(mapto,',HCT')
        WHEN protocol RLIKE '[^0-9]2186[^0-9]' THEN CONCAT(mapto,',HCT')
        WHEN protocol RLIKE '[^0-9]2309[^0-9]' THEN CONCAT(mapto,',HCT')
        WHEN protocol RLIKE '[^0-9]2335[^0-9]' THEN CONCAT(mapto,',HCT')
        WHEN protocol RLIKE '[^0-9]2524[^0-9]' THEN CONCAT(mapto,',HCT')
        WHEN protocol RLIKE '[^0-9]7617[^0-9]' THEN CONCAT(mapto,',HCT')
        WHEN protocol RLIKE '[^0-9]1201[^0-9]' THEN CONCAT(mapto,',HCT')
        WHEN protocol RLIKE 'TBI' THEN CONCAT(mapto,',HCT')
        WHEN protocol RLIKE 'FLU' THEN CONCAT(mapto,',HCT')
        WHEN protocol RLIKE 'TREO' THEN CONCAT(mapto,',HCT')
        WHEN protocol RLIKE 'BU.CY' THEN CONCAT(mapto,',HCT')
        WHEN protocol RLIKE 'PBMTC' THEN CONCAT(mapto,',HCT')
        WHEN protocol RLIKE '[^0-9]2206[^0-9]' THEN CONCAT(mapto,',HCT PREP')
        WHEN protocol RLIKE '[^0-9]2222[^0-9]' THEN CONCAT(mapto,',HCT PREP')
        WHEN protocol RLIKE 'DOTA' THEN CONCAT(mapto,',HCT')
        WHEN protocol RLIKE '[^0-9]2468[^0-9]' THEN CONCAT(mapto,',HCT')
        WHEN protocol RLIKE 'AC220' THEN CONCAT(mapto,',HCT')
        ELSE mapto
    END;

/**********************************************************************************************
No Regimen
**********************************************************************************************/
# PALLIATIVE/HOSPICE/CONSULT/NO TREATMENT
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE 'PALL'         THEN CONCAT(mapto,',PALLIATIVE/HOSPICE')
            WHEN protocol RLIKE 'HOSP'         THEN CONCAT(mapto,',PALLIATIVE/HOSPICE')
            WHEN protocol RLIKE 'CONS'         THEN CONCAT(mapto,',CONSULT')
            WHEN protocol RLIKE 'NO TREATMENT' THEN CONCAT(mapto,',NO TREATMENT')
            WHEN protocol RLIKE 'OUTSIDE'      THEN CONCAT(mapto,',OUTSIDE TREATMENT')
            WHEN protocol RLIKE 'UNK'          THEN CONCAT(mapto,',UNKNOWN')
            WHEN protocol RLIKE 'UKN'          THEN CONCAT(mapto,',UNKNOWN')
            WHEN protocol = ''                 THEN CONCAT(mapto,',MISSING')
        ELSE mapto
    END;

/**********************************************************************************************
Radiation therapy
**********************************************************************************************/
# XRT
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE 'XRT' THEN CONCAT(mapto,',XRT')
        ELSE mapto
        END;

/**********************************************************************************************
Combo Regimens
**********************************************************************************************/
# 7+3 -- cytarabine and one of these:  idarubicin/daunorubicin/doxorubicin/mitoxantrone
UPDATE protocollist SET mapto = CASE
        WHEN protocol RLIKE '3.*[+].*7|7.*[+].*3' THEN CONCAT(mapto,',7+3')
        WHEN protocol RLIKE 'IA[+]{1}|IA[ ]{1}'   THEN CONCAT(mapto,',7+3')
        WHEN protocol RLIKE 'SO106'               THEN CONCAT(mapto,',7+3')
        WHEN protocol RLIKE 'SO301'               THEN CONCAT(mapto,',7+3')
        WHEN protocol RLIKE '[^0-9]9011[^0-9]'    THEN CONCAT(mapto,',7+3')
        WHEN protocol RLIKE '3+ARAC(2)'           THEN CONCAT(mapto,',7+3')
        WHEN protocol RLIKE 'SWOG.*(301|106)'     THEN CONCAT(mapto,',7+3')
        ELSE mapto
    END;
# 5+2
UPDATE protocollist SET mapto = CASE 
    WHEN protocol RLIKE '2.*[+].*5|5.*[+]\.*2' THEN CONCAT(mapto,',5+2') 
    ELSE mapto
    END;
/* D-GCLAM            -- decitabine, filgrastim (G-CSF granulocyte colony-stimulating factor), cladribine, cytarabine, mitoxantrone 
   G-CLAM (2734)      -- filgrastim (G-CSF granulocyte colony-stimulating factor), cladribine, cytarabine, mitoxantrone
   CLAM               -- cladribine, cytarabine, mitoxantrone
   G-CLAC (6562/7144) -- filgrastim (G-CSF granulocyte colony-stimulating factor), clofarabine, cytarabine
   G-CLA              -- filgrastim (G-CSF granulocyte colony-stimulating factor), cladribine, cytarabine
   CLAG               -- decitabine, idarubicin, and cytarabine 
   FLAM               -- fLavopiridol, cytarabine, mitoxantrone
  */
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE 'D.{0,1}G.*C[LAM]{3}' THEN CONCAT(mapto,',D-GCLAM')
            WHEN protocol RLIKE 'G.*C[LAM]{3}'        THEN CONCAT(mapto,',G-CLAM')
            WHEN protocol RLIKE '[^0-9]2734[^0-9]'    THEN CONCAT(mapto,',G-CLAM')
            WHEN protocol RLIKE 'F[LAM]{3}'           THEN CONCAT(mapto,',FLAM')
            WHEN protocol RLIKE '[^0-9]2315[^0-9]'    THEN CONCAT(mapto,',FLAM')
            WHEN protocol RLIKE 'C[LAM]{3}'           THEN CONCAT(mapto,',CLAM')
            WHEN protocol RLIKE 'G.*C[LAC]{3}'        THEN CONCAT(mapto,',G-CLAC')
            WHEN protocol RLIKE '[^0-9]6562[^0-9]'    THEN CONCAT(mapto,',G-CLAC')
            WHEN protocol RLIKE '[^0-9]7144[^0-9]'    THEN CONCAT(mapto,',G-CLAC')
            WHEN protocol RLIKE 'G.*CLA'              THEN CONCAT(mapto,',G-CLA')
            WHEN protocol RLIKE 'C[LAG]{3}'           THEN CONCAT(mapto,',CLAG')
        ELSE mapto
    END;
# D-MEC -- decitabine, mitoxantrone, etoposide, and cytarabine
# E-MEC -- e-selectin, mitoxantrone, etoposide, and cytarabine
# MEC -- mitoxantrone, etoposide, and cytarabine
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE 'D.{0,1}[MEC]{3}'         THEN CONCAT(mapto,',D-MEC')
            WHEN protocol RLIKE 'DECI.*[MEC]{3}'          THEN CONCAT(mapto,',D-MEC')
            WHEN protocol RLIKE '[MEC]{3}\.*DECI'         THEN CONCAT(mapto,',D-MEC')
            WHEN protocol RLIKE '[^0-9]2652[^0-9]'        THEN CONCAT(mapto,',D-MEC')
            WHEN protocol RLIKE 'E.{0,1}[MEC]{3}'         THEN CONCAT(mapto,',E-MEC')
            WHEN protocol RLIKE 'E.{0,1}SELECT.*[MEC]{3}' THEN CONCAT(mapto,',E-MEC')
            WHEN protocol RLIKE '[MEC]{3}.*E.{0,1}SELECT' THEN CONCAT(mapto,',E-MEC')
            WHEN protocol RLIKE '[^0-9]14070[^0-9]'       THEN CONCAT(mapto,',E-MEC')
            WHEN protocol RLIKE '[MEC]{3}'                THEN CONCAT(mapto,',MEC')
        ELSE mapto
    END;
# MICE -- cytarabine, etoposide, gemtuzumab ozogamicin, idarubicin, mitoxantrone hydrochloride
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE 'MICE' THEN CONCAT(mapto,',MICE')
        ELSE mapto
    END;
# FLAG  -- fludarabine, cytarabine, G-CSF (granulocyte colony-stimulating factor)
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE 'FLAG.?IDA' THEN CONCAT(mapto,',FLAG-IDA')
            WHEN protocol RLIKE 'FLAG' THEN CONCAT(mapto,',FLAG')
        ELSE mapto
    END;

# ATRA+ATO  -- arsenic, tretinoin
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE 'ATRA.*ATO'  THEN CONCAT(mapto,',ATRA+ATO')
            WHEN protocol RLIKE 'ATO.*ATRA'  THEN CONCAT(mapto,',ATRA+ATO')
            WHEN protocol RLIKE 'ATRA.*ARSE' THEN CONCAT(mapto,',ATRA+ATO')
            WHEN protocol RLIKE 'ARSE.*ATRA' THEN CONCAT(mapto,',ATRA+ATO')
            WHEN protocol RLIKE 'AA'         THEN CONCAT(mapto,',ATRA+ATO')
        ELSE mapto
    END;
/* IAP (2674) --  Idarubicin, Ara-C (cytarabine), Pravastatin
   IAVP  -- cytarabin, idarubicin, vincristine
*/
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE 'IAVP'              THEN CONCAT(mapto,',IAVP')
            WHEN protocol RLIKE 'IA.{1}PRAV'       THEN CONCAT(mapto,',IAP')
            WHEN protocol RLIKE 'IAP'               THEN CONCAT(mapto,',IAP')
            WHEN protocol RLIKE '[^0-9]2674[^0-9]'  THEN CONCAT(mapto,',IAP')
        ELSE mapto
    END;
/* HEDGEHOG 2592  -- Oral Hedgehog Inhibitor, in Combination with Intensive Chemotherapy, Low Dose Ara-C or Decitabine
   HEDGEHOG 09021 -- Oral Hedgehog Inhibitor, Administered as a Single Agent
*/
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE 'HEDGE'                    THEN CONCAT(mapto,',HEDGEHOG')
            WHEN protocol RLIKE '[^0-9]2592[^0-9]'         THEN CONCAT(mapto,',HEDGEHOG')
            WHEN protocol RLIKE '[^0-9][0]{0,1}9021[^0-9]' THEN CONCAT(mapto,',HEDGEHOG')
        ELSE mapto
    END;
# EPI PRIME (2588) -- decitabine, idarubicin, and cytarabine
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE 'EPI'              THEN CONCAT(mapto,',EPI PRIME (2588)')
            WHEN protocol RLIKE '[^0-9]2588[^0-9]' THEN CONCAT(mapto,',EPI PRIME (2588)')
        ELSE mapto
    END;
# EPOCH --etoposide-prednisone-Oncovin-cyclophosphamide-hydroxydaunorubicin regimen
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE 'EPOCH'   THEN CONCAT(mapto,',EPOCH')
        ELSE mapto
    END;
# ABT-199 (9237/UW14053) combined with Aza or Decitabine
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE ' ABT[ ]{0,1}[+]{0,1}' THEN CONCAT(mapto,',ABT-199 (9237/UW14053)')
            WHEN protocol RLIKE 'ABT-199'              THEN CONCAT(mapto,',ABT-199 (9237/UW14053)')
            WHEN protocol RLIKE '[^0-9]14053[^0-9]'    THEN CONCAT(mapto,',ABT-199 (9237/UW14053)')
            WHEN protocol RLIKE '[^0-9]9237[^0-9]'     THEN CONCAT(mapto,',ABT-199 (9237/UW14053)')
        ELSE mapto
    END;
# TOSEDOSTAT (2566) -- tosedostat with either cytarabine or decitabine
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE ' TOSE'                     THEN CONCAT(mapto,',TOSEDOSTAT (2566)')
            WHEN protocol RLIKE '[^0-9]2566[^0-9][ ]*[(]TA' THEN CONCAT(mapto,',TOSEDOSTAT AZA (2566)')
            WHEN protocol RLIKE '[^0-9]2566[^0-9][ ]*[(]TD' THEN CONCAT(mapto,',TOSEDOSTAT AZA (2566)')
            WHEN protocol RLIKE '[^0-9]2566[^0-9]'          THEN CONCAT(mapto,',TOSEDOSTAT (2566)')
        ELSE mapto
    END;
/* VORINOSTAT (2288) -- vorinostat, gemtuzumab ozogamicin, azacitidine
   VORINO+GO (2200)  -- vorinostat, gemtuzumab ozogamicin */
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE 'AZA[+]GO[+]VORINO'    THEN CONCAT(mapto,',AZA+GO+VORINO (2288)')
            WHEN protocol RLIKE '[^0-9]2288[^0-9]'     THEN CONCAT(mapto,',AZA+GO+VORINO (2288)')
            WHEN protocol RLIKE 'VORINO[+]GO'          THEN CONCAT(mapto,',VORINO+GO (2200)')
            WHEN protocol RLIKE 'GO [+]SAHA'           THEN CONCAT(mapto,',VORINO+GO (2200)')
            WHEN protocol RLIKE '[^0-9]2200[^0-9]'     THEN CONCAT(mapto,',VORINO+GO (2200)')
            WHEN protocol RLIKE 'AZA[ ]*[+][ ]*VORINO' THEN CONCAT(mapto,',AZA+VORINO')
        ELSE mapto
    END;
# CPX-351 -- Liposomal Cytarabine and Daunorubicin (CPX-351)
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE 'CPX'              THEN CONCAT(mapto,',CPX-351 (2642/2651)')
            WHEN protocol RLIKE '[^0-9]2642[^0-9]' THEN CONCAT(mapto,',CPX-351 (2642)')
            WHEN protocol RLIKE '[^0-9]2651[^0-9]' THEN CONCAT(mapto,',CPX-351 (2651)')
        ELSE mapto
    END;
# BEND-IDA -- bendamustine HCL, idarubicin 
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE 'BEND'             THEN CONCAT(mapto,',BEND-IDA (2413)')
            WHEN protocol RLIKE '[^0-9]2413[^0-9]' THEN CONCAT(mapto,',BEND-IDA (2413)')
        ELSE mapto
    END;
# DECI+ARA-C -- decitabine, cytarabine
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE ' DEC ARA[-]C'     THEN CONCAT(mapto,',DECI+ARA-C')
            WHEN protocol RLIKE '[^0-9]9019[^0-9]' THEN CONCAT(mapto,',DECI+ARA-C')
        ELSE mapto
    END;
# CLOFARABOME+LDAC (2302) -- Oral clofarabine, low-dose cytarabine
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE 'CLOF.*LDAC'      THEN CONCAT(mapto,',CLOFARABINE+LDAC (2302)')
            WHEN protocol RLIKE '[^0-9]2302[^0-9]' THEN CONCAT(mapto,',CLOFARABINE+LDAC (2302)')
        ELSE mapto
    END;
# MITO+VP16 -- mitoxantrone, etoposide
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE 'MITO\.*[+]\.*VP16' THEN CONCAT(mapto,',MITO+VP16')
        ELSE mapto
    END;
# 2409 (CSA/PRAVA/MITO/VP16) -- Cyclosporine Modulation of Drug Resistance in Combination with Pravastatin, Mitoxantrone, and Etoposide 
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE '[^0-9]2409[^0-9]' THEN CONCAT(mapto,',2409 (CSA/PRAVA/MITO/VP16)')
        ELSE mapto
    END;
# MVP16 -- mitoxantrone, etoposide 
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE 'MVP[ ]*16' THEN CONCAT(mapto,',MVP16')
        ELSE mapto
    END;
# MVP -- mitomycin-C, vinblastine, cisplatin
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE ' MVP ' THEN CONCAT(mapto,',MVP')
        ELSE mapto
    END;
# MV CSA P -- mitoxantrone, etoposide, cyclosporine, and pravastatin
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE ' MVCSAP ' THEN CONCAT(mapto,',MV CSA P')
        ELSE mapto
    END;
    
/**********************************************************************************************
Single Agent Regimens
**********************************************************************************************/
# SGN/SGN-CD123A/SGN-CD33A
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE 'SGN\.*CD123'       THEN CONCAT(mapto,',SGN-CD123A')
            WHEN protocol RLIKE 'SGN\.*CD33'        THEN CONCAT(mapto,',SGN-CD33A')
            WHEN protocol RLIKE '[^0-9]2690[^0-9]'  THEN CONCAT(mapto,',SGN-CD33A')
            WHEN protocol RLIKE 'SGN'               THEN CONCAT(mapto,',SGN')
        ELSE mapto
    END;
# MDX (UW09036) 
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE 'MDX'                    THEN CONCAT(mapto,',MDX (UW09036)')
            WHEN protocol RLIKE '[^0-9]0{0,1}9036[^0-9]' THEN CONCAT(mapto,',MDX (UW09036)')
        ELSE mapto
    END;
# BMN (UW11003)
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE 'BMN'               THEN CONCAT(mapto,',BMN (UW11003)')
            WHEN protocol RLIKE '[^0-9]11003[^0-9]' THEN CONCAT(mapto,',BMN (UW11003)')
        ELSE mapto
    END;
# ARA-C/HiDAC/LDAC/IDAC -- varying doses of cytarabine (ARA-C)
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE 'H\.{1}DAC' THEN CONCAT(mapto,',HiDAC')
            WHEN protocol RLIKE 'LDAC'      THEN CONCAT(mapto,',LDAC')
            WHEN protocol RLIKE 'IDAC'      THEN CONCAT(mapto,',IDAC')
            WHEN mapto    RLIKE '7[+]3'     THEN mapto
            WHEN protocol RLIKE 'ARA.{1}C'  THEN CONCAT(mapto,',ARA-C')
            WHEN protocol RLIKE 'CYTARA'    THEN CONCAT(mapto,',ARA-C')
        ELSE mapto
    END;
# AMG-232/AMG-330/ADCT-301 -- 
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE 'AMG\.{1}232'       THEN CONCAT(mapto,',AMG-232 (UW13037)')
            WHEN protocol RLIKE '[^0-9]13037[^0-9]' THEN CONCAT(mapto,',AMG-232 (UW13037)')
            WHEN protocol RLIKE 'AMG\.{1}330'       THEN CONCAT(mapto,',AMG-330 (9382)')
            WHEN protocol RLIKE '[^0-9]9382[^0-9]'  THEN CONCAT(mapto,',AMG-330 (9382)')
            WHEN protocol RLIKE 'ADCT\.{1}301'      THEN CONCAT(mapto,',ADCT-301 (9513)')
            WHEN protocol RLIKE '[^0-9]9513[^0-9]'  THEN CONCAT(mapto,',ADCT-301 (9513')
        ELSE mapto
    END;
/* SEVERAL SINGLE AGENTS IN ONE UPDATE STATEMENT
   IGN-523 (UW13049) -- Study drug:  IGN-523
   PR104 -- Study drug:  PR104
   MK-2206 (2466)
   CD8+ T Cells (2498)
   CWP (2513) 
   ON 01910.Na (2534)
   AC-225 (2572)
   GO (7971)
   SB1518 (2246)
   ATRA
   HU
   SORAFINIB
   LENALIDOMIDE
   CLADRABINE
   CLOFARABINE
   IDARUBICIN
   NALARABINE
   REVLAMID
   TEMOZOLOMIDE
   IBRUTINIB
   BORTEZOMIB
   PRALATREXATE
   WIS -- withdrawl of immunosuppression
   OSTEOK
   DACTINOMYCIN
   CEP701 -- lestaurtinib*/
UPDATE protocollist SET mapto = CASE 
            WHEN protocol RLIKE 'IGN'               THEN CONCAT(mapto,',IGN-523 (UW13049)')
            WHEN protocol RLIKE 'PR104'             THEN CONCAT(mapto,',PR104')
            WHEN protocol RLIKE 'FLX'               THEN CONCAT(mapto,',FLX925')
            WHEN protocol RLIKE 'MEK'               THEN CONCAT(mapto,',MEK INHIBITOR')
            WHEN protocol RLIKE '[^0-9]2246[^0-9]'  THEN CONCAT(mapto,',SB1518 (2246)')
            WHEN protocol RLIKE '[^0-9]2513[^0-9]'  THEN CONCAT(mapto,',CWP (2513)')
            WHEN protocol RLIKE '[^0-9]2534[^0-9]'  THEN CONCAT(mapto,',ON 01910.Na (2534)')
            WHEN protocol RLIKE '[^0-9]2572[^0-9]'  THEN CONCAT(mapto,',AC-225 (2572)')
            WHEN protocol RLIKE '[^0-9]13049[^0-9]' THEN CONCAT(mapto,',IGN-523 (UW13049)')
            WHEN protocol RLIKE '[^0-9]15067[^0-9]' THEN CONCAT(mapto,',FLX925')
            WHEN protocol RLIKE '[^0-9]2466[^0-9]'  THEN CONCAT(mapto,',MK-2206 (2466)')
            WHEN protocol RLIKE '[^0-9]2498[^0-9]'  THEN CONCAT(mapto,',CD8+ T Cells (2498)')
            WHEN protocol RLIKE '[^0-9]2532[^0-9]'  THEN CONCAT(mapto,',PLX3397 (2532)')
            WHEN protocol RLIKE '[^0-9]7971[^0-9]'  THEN CONCAT(mapto,',GO (7971)')
            WHEN protocol = ' GO '                  THEN CONCAT(mapto,',GO (7971)')
            WHEN protocol = ' SORAFINIB '           THEN CONCAT(mapto,',SORA')
            WHEN protocol = ' LENALIDOMIDE '        THEN CONCAT(mapto,',LENALIDOMIDE')
            WHEN protocol = ' ATRA '                THEN CONCAT(mapto,',ATRA')
            WHEN protocol = ' HU '                  THEN CONCAT(mapto,',HU')
            WHEN protocol = ' CLADRABINE '          THEN CONCAT(mapto,',CLADRABINE')
            WHEN protocol = ' CLOFARABINE '         THEN CONCAT(mapto,',CLOFARABINE')
            WHEN protocol = ' DACOGEN '             THEN CONCAT(mapto,',DECI')
            WHEN protocol = ' IDARUBICIN '          THEN CONCAT(mapto,',IDARUBICIN')
            WHEN protocol = ' NALARABINE '          THEN CONCAT(mapto,',NALARABINE')
            WHEN protocol = ' REVLAMID '            THEN CONCAT(mapto,',REVLAMID')
            WHEN protocol = ' TEMOZOLOMIDE '        THEN CONCAT(mapto,',TEMOZOLOMIDE')
            WHEN protocol = ' IBURTINIB '           THEN CONCAT(mapto,',IBRUTINIB')
            WHEN protocol = ' IBRUTINIB '           THEN CONCAT(mapto,',IBRUTINIB')
            WHEN protocol = ' BORTEZOMIB '          THEN CONCAT(mapto,',BORTEZOMIB')
            WHEN protocol = ' PRALATREXATE '        THEN CONCAT(mapto,',PRALATREXATE')
            WHEN protocol = ' WIS '                 THEN CONCAT(mapto,',WIS')
            WHEN protocol = ' OSTEOK '              THEN CONCAT(mapto,',OSTEOK')
            WHEN protocol = ' CEP701 '              THEN CONCAT(mapto,',CEP701')
            WHEN protocol = ' DACTINOMYCIN '        THEN CONCAT(mapto,',DACTINOMYCIN')
            WHEN protocol = ' ACT-D '               THEN CONCAT(mapto,',DACTINOMYCIN')
            WHEN protocol = ' IT MTX '              THEN CONCAT(mapto,',IT MTX')
        ELSE mapto
    END;  --    FLX925      


/**********************************************************************************************
HMA Agent given alone
**********************************************************************************************/
# AZA
UPDATE protocollist SET mapto = CASE 
            WHEN protocol = ' AZA '                   THEN CONCAT(mapto,',AZA')
            WHEN protocol = ' VIDAZA '                THEN CONCAT(mapto,',AZA')
            WHEN protocol = ' AZACITIDINE '           THEN CONCAT(mapto,',AZA')
            WHEN protocol RLIKE ' AZA[ ]{0,1}[+]{1}'  THEN CONCAT(mapto,',AZA')
        ELSE mapto
    END
    WHERE mapto='';


# DECI
UPDATE protocollist SET mapto = CASE 
            WHEN protocol = ' DACOGEN '            THEN CONCAT(mapto,',DECI')
            WHEN protocol = ' DAC '                THEN CONCAT(mapto,',DECI')
            WHEN protocol RLIKE ' DECI.{0,6}[ ]*'  THEN CONCAT(mapto,',DECI')
            WHEN protocol RLIKE '^ DECI.{0,6}[ ]*' THEN CONCAT(mapto,',DECI')
        ELSE mapto
    END
    WHERE mapto='';
 

/**********************************************************************************************
Assign Regimen
**********************************************************************************************/
# Non-Induction
UPDATE protocollist SET noninduction =  CASE
        WHEN mapto RLIKE 'PALLIATIVE[/]HOSPICE' THEN 'NO TREATMENT (PALLIATIVE/HOSPICE)'
        WHEN mapto RLIKE 'NO TREATMENT'         THEN 'NO TREATMENT'
        WHEN mapto RLIKE 'CONSULT'              THEN 'NO TREATMENT (CONSULT)'
        WHEN mapto RLIKE 'XRT'                  THEN 'XRT'
        ELSE noninduction
    END;
# Multi-Agent Induction Regimen
UPDATE protocollist SET multiregimen =  CASE
        WHEN mapto RLIKE '5[+]2'                     THEN '5+2'
        WHEN mapto RLIKE '7[+]3'                     THEN '7+3'
        WHEN mapto RLIKE 'ATRA[+]ATO'                THEN 'ATRA+ATO'
        WHEN mapto RLIKE 'D[-]GCLAM'                 THEN 'D-GCLAM'
        WHEN mapto RLIKE 'G[-]CLAM'                  THEN 'G-CLAM'
        WHEN mapto RLIKE 'G[-]CLAC'                  THEN 'G-CLAC'
        WHEN mapto RLIKE 'G[-]CLA'                   THEN 'G-CLA'
        WHEN mapto RLIKE 'D[-]MEC'                   THEN 'D-MEC'
        WHEN mapto RLIKE 'E[-]MEC'                   THEN 'E-MEC'
        WHEN mapto RLIKE 'MEC'                       THEN 'MEC'
        WHEN mapto RLIKE 'CLAM'                      THEN 'CLAM'
        WHEN mapto RLIKE 'CLAG'                      THEN 'CLAG'
        WHEN mapto RLIKE 'EPI PRIME [(]2588[)]'      THEN 'CLAG'
        WHEN mapto RLIKE 'EPOCH'                     THEN 'EPOCH'
        WHEN mapto RLIKE 'FLAG[-]IDA'                THEN 'FLAG-IDA'
        WHEN mapto RLIKE 'FLAG'                      THEN 'FLAG'
        WHEN mapto RLIKE 'IAP'                       THEN 'IAP'
        WHEN mapto RLIKE 'MICE'                      THEN 'MICE'
        WHEN mapto RLIKE 'TOSE'                      THEN 'TOSEDOSTAT'
        WHEN mapto RLIKE '2288'                      THEN 'AZA+GO+VORINO'
        WHEN mapto RLIKE '2200'                      THEN 'VORINO+GO'
        WHEN mapto RLIKE 'AZA[+]VORINO'              THEN 'AZA+VORINO'
        WHEN mapto RLIKE 'CPX'                       THEN 'CPX-351'
        WHEN mapto RLIKE 'BEND'                      THEN 'BEND-IDA'
        WHEN mapto RLIKE 'DECI ARA-C'                THEN 'DECI+ARA-C'
        WHEN mapto RLIKE 'CLOFARABINE[+]LDAC'        THEN 'CLOF+LDAC'
        WHEN mapto RLIKE 'MITO[+]VP16'               THEN 'MITO+VP16'
        WHEN mapto RLIKE '2409'                      THEN '2409 (CSA/PRAVA/MITO/VP16)'
        WHEN mapto RLIKE '2534'                      THEN 'ON 01910.Na (2534)'
        WHEN mapto RLIKE '2572'                      THEN 'AC-225 (2572)'
        WHEN mapto RLIKE 'MVP16'                     THEN 'MVP16'
        WHEN mapto RLIKE 'MVP'                       THEN 'MVP'
        ELSE multiregimen
    END;
# Single-Agent Induction Regimen
UPDATE protocollist SET singleregimen =  CASE
        WHEN mapto RLIKE 'ABT[-]199'            THEN 'ABT-199'
        WHEN mapto RLIKE 'ADCT[-]301'           THEN 'ADCT-301'
        WHEN mapto RLIKE 'AMG[-]232'            THEN 'AMG-232'
        WHEN mapto RLIKE 'AMG[-]330'            THEN 'AMG-330'
        WHEN mapto RLIKE 'BMN [(]UW11003[)]'    THEN 'BMN (UW11003)'
        WHEN mapto RLIKE 'MDX [(]UW09036[)]'    THEN 'MDX (UW09036)'
        WHEN mapto RLIKE 'SGN[-]CD33A'          THEN 'SGN-CD33A'
        WHEN mapto RLIKE 'SGN[-]CD123A'         THEN 'SGN-CD123A'
        WHEN mapto RLIKE '2513'                 THEN 'CWP (2513)'
        WHEN mapto RLIKE 'PR104'                THEN 'PR104'
        WHEN mapto RLIKE 'IGN[-]523'            THEN 'IGN-523'
        WHEN mapto RLIKE '2534'                 THEN 'ON 01910.Na (2534)'
        ELSE singleregimen
    END;



UPDATE protocollist SET singleregimen =  CASE WHEN singleregimen = '' AND multiagent = '' AND mapto RLIKE 'HCT' THEN
            CASE 
                WHEN mapto RLIKE 'MEK INHIBITOR'  THEN 'MEK INHIBITOR'
                WHEN mapto RLIKE 'ARA-C'          THEN 'ARA-C'
                WHEN mapto RLIKE 'AZA'            THEN 'AZA'
                WHEN mapto RLIKE 'DECI'           THEN 'DECI'
                WHEN mapto RLIKE 'GO'             THEN 'GO'
                WHEN mapto RLIKE 'CLOFARABINE'    THEN 'CLOFARABINE'
                ELSE singleregime
            END
        ELSE singleregimen
    END;
    
/**********************************************************************************************
Single Agent given alone
**********************************************************************************************/


/**********************************************************************************************
Agents combined (added on to) other regimens or single agents
**********************************************************************************************/
UPDATE protocollist SET druglist = CASE # SORAFENIB
        WHEN CONCAT('+',protocol) RLIKE '[+].?SORA' THEN 
            CASE 
                WHEN singleregimen NOT RLIKE 'SORA' AND multiregimen  NOT RLIKE 'SORA' THEN concat(druglist,',sorafenib')
                ELSE druglist
            END
        ELSE druglist[+].?SORA
    END;

UPDATE protocollist SET druglist = CASE
        WHEN CONCAT('+',protocol) RLIKE '[+].?GO' THEN
            CASE 
                WHEN singleregimen NOT RLIKE 'GO'   AND multiregimen  NOT RLIKE 'GO'   THEN concat(druglist,',gemtuzumab ozogamicin')
                ELSE druglist
            END
        ELSE druglist
    END;

UPDATE protocollist SET druglist = CASE
        WHEN CONCAT('+',protocol) RLIKE '[+].?VP16' THEN
            CASE 
                WHEN singleregimen NOT RLIKE 'VP16' AND multiregimen  NOT RLIKE 'VP16' THEN concat(druglist,',etoposide')
                ELSE druglist
            END
        ELSE druglist
    END;

UPDATE protocollist SET druglist = CASE
        WHEN CONCAT('+',protocol) RLIKE '[+].?VP' THEN
            CASE 
                WHEN singleregimen NOT RLIKE 'VP' AND multiregimen  NOT RLIKE 'VP' THEN concat(druglist,',vincristine')
                ELSE druglist
            END
        ELSE druglist
    END;

UPDATE protocollist SET druglist = CASE
        WHEN CONCAT('+',protocol) RLIKE '[+].?CLAD' THEN
            CASE 
                WHEN singleregimen NOT RLIKE 'CLAD' AND multiregimen  NOT RLIKE 'CLAD' THEN concat(druglist,',cladribine')
                ELSE druglist
            END
        ELSE druglist
    END;

UPDATE protocollist SET druglist = CASE
        WHEN CONCAT('+',protocol) RLIKE '[+].?BORT' THEN
            CASE 
                WHEN singleregimen NOT RLIKE 'BORT' AND multiregimen  NOT RLIKE 'BORT' THEN concat(druglist,',bortezomib')
                ELSE druglist
            END
        ELSE druglist
    END;   

UPDATE protocollist SET druglist = CASE
        WHEN CONCAT('+',protocol) RLIKE '[+].?PRAVA' THEN
            CASE 
                WHEN singleregimen NOT RLIKE 'PRAVA' AND multiregimen  NOT RLIKE 'PRAVA' THEN concat(druglist,',pravastatin')
                ELSE druglist
            END
        ELSE druglist
    END;
    
    
UPDATE protocollist SET druglist = CASE
        WHEN CONCAT('+',protocol) RLIKE '[+].?DECI' THEN
            CASE 
                WHEN singleregimen NOT RLIKE 'DECI' AND multiregimen  NOT RLIKE 'DECI' THEN concat(druglist,',decitabine')
                ELSE druglist
            END
        ELSE druglist
    END;

UPDATE protocollist SET druglist = CASE
        WHEN CONCAT('+',protocol) RLIKE '[+].?CEP' THEN
            CASE 
                WHEN singleregimen NOT RLIKE 'CEP' AND multiregimen  NOT RLIKE 'CEP' THEN concat(druglist,',ibrutinib')
                ELSE druglist
            END
        ELSE druglist
    END;    
    

UPDATE protocollist SET druglist = CASE
        WHEN CONCAT('+',protocol) RLIKE '[+].?IB(UR){2}' THEN
            CASE 
                WHEN singleregimen NOT RLIKE 'IB(UR){2}' AND multiregimen  NOT RLIKE 'IB(UR){2}' THEN concat(druglist,',lestaurtinib')
                ELSE druglist
            END
        ELSE druglist
    END;    

UPDATE protocollist SET druglist = CASE
        WHEN CONCAT('+',protocol) RLIKE '[+].?RITUX' THEN
            CASE 
                WHEN singleregimen NOT RLIKE 'RITUX' AND multiregimen  NOT RLIKE 'RITUX' THEN concat(druglist,',rituximab')
                ELSE druglist
            END
        ELSE druglist
    END;

UPDATE protocollist SET druglist = CASE
        WHEN CONCAT('+',protocol) RLIKE '[+].?ETOP' THEN
            CASE 
                WHEN singleregimen NOT RLIKE 'ETOP' AND multiregimen  NOT RLIKE 'ETOP' THEN concat(druglist,',etoposide')
                ELSE druglist
            END
        ELSE druglist
    END;

UPDATE protocollist SET druglist = CASE
        WHEN CONCAT('+',protocol) RLIKE '[+].?MITO' THEN
            CASE 
                WHEN singleregimen NOT RLIKE 'MITO' AND multiregimen  NOT RLIKE 'MITO' THEN concat(druglist,',mitoxantrone')
                ELSE druglist
            END
        ELSE druglist
    END;

UPDATE protocollist SET druglist = CASE
        WHEN CONCAT('+',protocol) RLIKE '[+].?CLO' THEN
            CASE 
                WHEN singleregimen NOT RLIKE 'CLO' AND multiregimen  NOT RLIKE 'CLO' THEN concat(druglist,',clorafabine')
                ELSE druglist
            END
        ELSE druglist
    END;

UPDATE protocollist SET druglist = CASE
        WHEN CONCAT('+',protocol) RLIKE '[+].?HYDROX' THEN concat(druglist,',hydroxyurea')
        WHEN CONCAT('+',protocol) RLIKE '[+].?HU'     THEN concat(druglist,',hydroxyurea')
        ELSE druglist
    END;

UPDATE protocollist SET druglist = CASE
        WHEN CONCAT('+',protocol) RLIKE '[+].?TREMETNIB'  THEN concat(druglist,',MEK')
        WHEN CONCAT('+',protocol) RLIKE '[+].?TRAMETINIB' THEN concat(druglist,',MEK')
        WHEN CONCAT('+',protocol) RLIKE '[+].?MEK'        THEN concat(druglist,',MEK')
        ELSE druglist
    END;

UPDATE protocollist SET druglist = CASE
        WHEN CONCAT('+',protocol) RLIKE '[+].?ACT[-]D'  THEN concat(druglist,',actinomycin-d')

        ELSE druglist
    END;
    
UPDATE protocollist SET druglist = CASE
        WHEN CONCAT('+',protocol) RLIKE '[+].?LENA'  THEN concat(druglist,',lenalidomide')
        ELSE druglist
    END;

UPDATE protocollist SET druglist = CASE
        WHEN CONCAT('+',protocol) RLIKE '[+].?ROMI'  THEN concat(druglist,',romiplastin')
        ELSE druglist
    END;

UPDATE protocollist SET druglist = CASE
        WHEN CONCAT('+',protocol) RLIKE '[+].?PDL1'  THEN concat(druglist,',PDL1')
        ELSE druglist
    END;


# MV, DAC/DACOGEN, plerixifor, tki, vincristine, dexametha

SELECT * FROM protocollist WHERE protocol RLIKE '[+].?[A-Z]'
    AND protocol NOT RLIKE '[+].?SORA'
    AND protocol NOT RLIKE '[+].?VP'
    AND protocol NOT RLIKE '[+].?GO';

SELECT * FROM protocollist WHERE protocol RLIKE '[+].?DECI' AND singleregimen NOT RLIKE 'DECI' AND multiregimen  NOT RLIKE 'DECI' ;
SELECT * FROM protocollist WHERE protocol RLIKE '[+]';


/**********************************************************************************************
Queries for analysis of mapping results
**********************************************************************************************/
SELECT 'Mapped'   AS type, mapto    AS item, SUM(totaluse) AS totaluse FROM protocollist WHERE mapto <> '' GROUP BY item 
    UNION 
SELECT 'Protocol' AS type, protocol AS item, totaluse                  FROM protocollist WHERE mapto = '' GROUP BY item;

SELECT 'Protocol' AS type, protocol AS item, totaluse FROM protocollist WHERE mapto = '' GROUP BY item;

SELECT * FROM protocollist
WHERE protocol 
        RLIKE '[+]'
        AND protocol RLIKE '[+][^7]'
        AND protocol RLIKE '[+][^3]'
        AND druglist = '';

SELECT protocol, mapto, noninduction, singleregimen, multiregimen, druglist, totaluse
FROM protocollist
WHERE       singleregimen = ''
        AND multiregimen = '' 
        AND mapto NOT RLIKE 'HCT'
        AND mapto NOT RLIKE 'OUTSIDE'
        AND noninduction NOT RLIKE 'NO TREAT';


SELECT UWID, ArrivalDate, ArrivalDx, year(ArrivalDate) as ArrivalYear, a.* 
    FROM protocollist a left join amldata b on a.OriginalProtocol = b.protocol;

SELECT * FROM protocollist;





























