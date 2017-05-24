from MySQLdbUtils import *


def standardizeprotocollist(cnxdict):
    """
    Standardize data if needed
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET protocol = REPLACE(protocol,'ACTINOMYCIN','DACTINOMYCIN');
        UPDATE protocollist SET protocol = REPLACE(protocol,'DACTINOMYCIN','ACT-D');
        UPDATE protocollist SET protocol = REPLACE(protocol,'6TG','THIOGUANINE');
        UPDATE protocollist SET protocol = REPLACE(protocol,'DACOGEN','DECITABINE');
        UPDATE protocollist SET protocol = REPLACE(protocol,' DAC ','DECITABINE');
        UPDATE protocollist SET protocol = REPLACE(protocol,'PRAVASTATIN, MITOXANTRONE AND ETOPOSIDE','PRAVA +MITO +ETOP');
        UPDATE protocollist SET protocol = '7+3 +SGN-CD33A' WHERE protocol = ' SGN + 7 + 3 ' OR protocol = ' SGN+7+3 ';
    """
    return None


def formatprotocollist(cnxdict):
    """
    Formats table protocollist.
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
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
    """
    dosqlexecute(cnxdict)
    return None


def build_protocol_list(cnxdict):
    """
    Creates table protocollist
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
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
    """
    dosqlexecute(cnxdict)
    formatprotocollist(cnxdict)
    standardizeprotocollist(cnxdict)
    return None


def set_wildcard_flag(cnxdict):
    """
    Set wildcard flag
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET wildcard = 'Yes' WHERE protocol RLIKE 'OFF';
        UPDATE protocollist SET protocol = REPLACE(protocol,'OFF','');
    """
    dosqlexecute(cnxdict)
    return None


def map_hct(cnxdict):
    """
    Map studies getting HCT for their treatment rather than chemotherapy
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
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
    """
    dosqlexecute(cnxdict)
    return None


def set_no_regimen(cnxdict):
    """
    Set Regimen for patient who got:  PALLIATIVE/HOSPICE/CONSULT/NO TREATMENT
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
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
    """
    dosqlexecute(cnxdict)
    return None


def set_radiation(cnxdict):
    """
    Set Regimen for patient who got:  PALLIATIVE/HOSPICE/CONSULT/NO TREATMENT
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET mapto = CASE
                    WHEN protocol RLIKE 'XRT' THEN CONCAT(mapto,',XRT')
                ELSE mapto
                END;
    """
    dosqlexecute(cnxdict)
    return None


def set_combo_regimen(cnxdict):
    set_7_plus_3(cnxdict)
    set_5_plus_2(cnxdict)
    set_gclam_variant(cnxdict)
    set_mec_variant(cnxdict)
    set_mice(cnxdict)
    set_flag(cnxdict)
    set_atra_ato(cnxdict)
    set_iap_iavp(cnxdict)
    set_hedgehog(cnxdict)
    set_epi_prime_2588(cnxdict)
    set_epoch(cnxdict)
    set_abt_199(cnxdict)
    set_tosedostat(cnxdict)
    set_vorino_2288_2200(cnxdict)
    set_cpx_351(cnxdict)
    set_bend_ida(cnxdict)
    set_deci_arac(cnxdict)
    set_clof_ldac(cnxdict)
    set_mito_vp16(cnxdict)
    set_csa_prava_mito_vp16_2409(cnxdict)
    set_mvp16(cnxdict)
    set_mv_csa_prav(cnxdict)
    set_mvp16(cnxdict)
    return None


def set_7_plus_3(cnxdict):
    """
    Set Regimen for patients on a 7+3 protocol
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
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
    """
    dosqlexecute(cnxdict)
    return None


def set_5_plus_2(cnxdict):
    """
    Set Regimen for patients on a 5+2 protocol
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET mapto = CASE
            WHEN protocol RLIKE '2.*[+].*5|5.*[+]\.*2' THEN CONCAT(mapto,',5+2')
            ELSE mapto
            END;
    """
    dosqlexecute(cnxdict)
    return None


def set_gclam_variant(cnxdict):
    """
        D-GCLAM            -- decitabine, filgrastim (G-CSF granulocyte colony-stimulating factor), cladribine, cytarabine, mitoxantrone
        G-CLAM (2734)      -- filgrastim (G-CSF granulocyte colony-stimulating factor), cladribine, cytarabine, mitoxantrone
        CLAM               -- cladribine, cytarabine, mitoxantrone
        G-CLAC (6562/7144) -- filgrastim (G-CSF granulocyte colony-stimulating factor), clofarabine, cytarabine
        G-CLA              -- filgrastim (G-CSF granulocyte colony-stimulating factor), cladribine, cytarabine
        CLAG               -- decitabine, idarubicin, and cytarabine
        FLAM               -- fLavopiridol, cytarabine, mitoxantrone
        :param cnxdict: data dictionary object
        :return: None
    """
    cnxdict['sql'] = """
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
    """
    dosqlexecute(cnxdict)
    return None


def set_mec_variant(cnxdict):
    """
        D-MEC -- decitabine, mitoxantrone, etoposide, and cytarabine
        E-MEC -- e-selectin, mitoxantrone, etoposide, and cytarabine
        MEC -- mitoxantrone, etoposide, and cytarabine
        :param cnxdict: data dictionary object
        :return: None
    """
    cnxdict['sql'] = """
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
    """
    dosqlexecute(cnxdict)
    return None


def set_mice(cnxdict):
    """
    MICE -- cytarabine, etoposide, gemtuzumab ozogamicin, idarubicin, mitoxantrone hydrochloride
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET mapto = CASE
                    WHEN protocol RLIKE 'MICE' THEN CONCAT(mapto,',MICE')
                ELSE mapto
            END;    """
    dosqlexecute(cnxdict)
    return None


def set_flag(cnxdict):
    """
    FLAG  -- fludarabine, cytarabine, G-CSF (granulocyte colony-stimulating factor)
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET mapto = CASE
                    WHEN protocol RLIKE 'FLAG.?IDA' THEN CONCAT(mapto,',FLAG-IDA')
                    WHEN protocol RLIKE 'FLAG' THEN CONCAT(mapto,',FLAG')
                ELSE mapto
            END;
    """
    dosqlexecute(cnxdict)
    return None


def set_atra_ato(cnxdict):
    """
    ATRA+ATO  -- arsenic, tretinoin
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
    UPDATE protocollist SET mapto = CASE 
                WHEN protocol RLIKE 'ATRA.*ATO'  THEN CONCAT(mapto,',ATRA+ATO')
                WHEN protocol RLIKE 'ATO.*ATRA'  THEN CONCAT(mapto,',ATRA+ATO')
                WHEN protocol RLIKE 'ATRA.*ARSE' THEN CONCAT(mapto,',ATRA+ATO')
                WHEN protocol RLIKE 'ARSE.*ATRA' THEN CONCAT(mapto,',ATRA+ATO')
                WHEN protocol RLIKE 'AA'         THEN CONCAT(mapto,',ATRA+ATO')
            ELSE mapto
        END;
    """
    dosqlexecute(cnxdict)
    return None


def set_iap_iavp(cnxdict):
    """
    IAP (2674) --  Idarubicin, Ara-C (cytarabine), Pravastatin
    IAVP  -- cytarabin, idarubicin, vincristine
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET mapto = CASE
                    WHEN protocol RLIKE 'IAVP'              THEN CONCAT(mapto,',IAVP')
                    WHEN protocol RLIKE 'IA\.{1}PRAV'       THEN CONCAT(mapto,',IAP')
                    WHEN protocol RLIKE 'IAP'               THEN CONCAT(mapto,',IAP')
                    WHEN protocol RLIKE '[^0-9]2674[^0-9]'  THEN CONCAT(mapto,',IAP')
                ELSE mapto
            END;
    """
    dosqlexecute(cnxdict)
    return None


def set_hedgehog(cnxdict):
    """
    HEDGEHOG 2592  -- Oral Hedgehog Inhibitor, in Combination with Intensive Chemotherapy, Low Dose Ara-C or Decitabine
    HEDGEHOG 09021 -- Oral Hedgehog Inhibitor, Administered as a Single Agent
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET mapto = CASE
                    WHEN protocol RLIKE 'HEDGE'                    THEN CONCAT(mapto,',HEDGEHOG')
                    WHEN protocol RLIKE '[^0-9]2592[^0-9]'         THEN CONCAT(mapto,',HEDGEHOG')
                    WHEN protocol RLIKE '[^0-9][0]{0,1}9021[^0-9]' THEN CONCAT(mapto,',HEDGEHOG')
                ELSE mapto
            END;
    """
    dosqlexecute(cnxdict)
    return None


def set_epi_prime_2588(cnxdict):
    """
    EPI PRIME (2588) -- decitabine, idarubicin, and cytarabine
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET mapto = CASE
                    WHEN protocol RLIKE 'EPI'              THEN CONCAT(mapto,',EPI PRIME (2588)')
                    WHEN protocol RLIKE '[^0-9]2588[^0-9]' THEN CONCAT(mapto,',EPI PRIME (2588)')
                ELSE mapto
            END;
    """
    dosqlexecute(cnxdict)
    return None


def set_epoch(cnxdict):
    """
    EPOCH --etoposide-prednisone-Oncovin-cyclophosphamide-hydroxydaunorubicin regimen
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET mapto = CASE
                    WHEN protocol RLIKE 'EPOCH'   THEN CONCAT(mapto,',EPOCH')
                ELSE mapto
            END;
    """
    dosqlexecute(cnxdict)
    return None


def set_abt_199(cnxdict):
    """
    ABT-199 (9237/UW14053) combined with Aza or Decitabine
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET mapto = CASE
                    WHEN protocol RLIKE ' ABT[ ]{0,1}[+]{0,1}' THEN CONCAT(mapto,',ABT-199 (9237/UW14053)')
                    WHEN protocol RLIKE 'ABT-199'              THEN CONCAT(mapto,',ABT-199 (9237/UW14053)')
                    WHEN protocol RLIKE '[^0-9]14053[^0-9]'    THEN CONCAT(mapto,',ABT-199 (9237/UW14053)')
                    WHEN protocol RLIKE '[^0-9]9237[^0-9]'     THEN CONCAT(mapto,',ABT-199 (9237/UW14053)')
                ELSE mapto
            END;
    """
    dosqlexecute(cnxdict)
    return None


def set_tosedostat(cnxdict):
    """
    TOSEDOSTAT (2566) -- tosedostat with either cytarabine or decitabine
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET mapto = CASE
                    WHEN protocol RLIKE ' TOSE'                     THEN CONCAT(mapto,',TOSEDOSTAT (2566)')
                    WHEN protocol RLIKE '[^0-9]2566[^0-9][ ]*[(]TA' THEN CONCAT(mapto,',TOSEDOSTAT AZA (2566)')
                    WHEN protocol RLIKE '[^0-9]2566[^0-9][ ]*[(]TD' THEN CONCAT(mapto,',TOSEDOSTAT AZA (2566)')
                    WHEN protocol RLIKE '[^0-9]2566[^0-9]'          THEN CONCAT(mapto,',TOSEDOSTAT (2566)')
                ELSE mapto
            END;
    """
    dosqlexecute(cnxdict)
    return None


def set_vorino_2288_2200(cnxdict):
    """
    VORINOSTAT (2288) -- vorinostat, gemtuzumab ozogamicin, azacitidine
    VORINO+GO (2200)  -- vorinostat, gemtuzumab ozogamicin
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET mapto = CASE
                    WHEN protocol RLIKE 'AZA[+]GO[+]VORINO'    THEN CONCAT(mapto,',AZA+GO+VORINO (2288)')
                    WHEN protocol RLIKE '[^0-9]2288[^0-9]'     THEN CONCAT(mapto,',AZA+GO+VORINO (2288)')
                    WHEN protocol RLIKE 'VORINO[+]GO'          THEN CONCAT(mapto,',VORINO+GO (2200)')
                    WHEN protocol RLIKE 'GO [+]SAHA'           THEN CONCAT(mapto,',VORINO+GO (2200)')
                    WHEN protocol RLIKE '[^0-9]2200[^0-9]'     THEN CONCAT(mapto,',VORINO+GO (2200)')
                    WHEN protocol RLIKE 'AZA[ ]*[+][ ]*VORINO' THEN CONCAT(mapto,',AZA+VORINO')
                ELSE mapto
            END;
    """
    dosqlexecute(cnxdict)
    return None


def set_cpx_351(cnxdict):
    """
    CPX-351 -- Liposomal Cytarabine and Daunorubicin (CPX-351)
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET mapto = CASE
                WHEN protocol RLIKE 'CPX'              THEN CONCAT(mapto,',CPX-351 (2642/2651)')
                WHEN protocol RLIKE '[^0-9]2642[^0-9]' THEN CONCAT(mapto,',CPX-351 (2642)')
                WHEN protocol RLIKE '[^0-9]2651[^0-9]' THEN CONCAT(mapto,',CPX-351 (2651)')
            ELSE mapto
        END;
    """
    dosqlexecute(cnxdict)
    return None


def set_bend_ida(cnxdict):
    """

    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET mapto = CASE
                WHEN protocol RLIKE 'BEND'             THEN CONCAT(mapto,',BEND-IDA (2413)')
                WHEN protocol RLIKE '[^0-9]2413[^0-9]' THEN CONCAT(mapto,',BEND-IDA (2413)')
            ELSE mapto
        END;
    """
    dosqlexecute(cnxdict)
    return None


def set_deci_arac(cnxdict):
    """
    DECI+ARA-C -- decitabine, cytarabine
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET mapto = CASE
                    WHEN protocol RLIKE ' DEC ARA[-]C'     THEN CONCAT(mapto,',DECI+ARA-C')
                    WHEN protocol RLIKE '[^0-9]9019[^0-9]' THEN CONCAT(mapto,',DECI+ARA-C')
                ELSE mapto
            END;
    """
    dosqlexecute(cnxdict)
    return None


def set_clof_ldac(cnxdict):
    """
    CLOFARABOME+LDAC (2302) -- Oral clofarabine, low-dose cytarabine
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET mapto = CASE
                    WHEN protocol RLIKE 'CLOF.*LDAC'      THEN CONCAT(mapto,',CLOFARABINE+LDAC (2302)')
                    WHEN protocol RLIKE '[^0-9]2302[^0-9]' THEN CONCAT(mapto,',CLOFARABINE+LDAC (2302)')
                ELSE mapto
            END;
    """
    dosqlexecute(cnxdict)
    return None


def set_mito_vp16(cnxdict):
    """
    MITO+VP16 -- mitoxantrone, etoposide
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET mapto = CASE
                    WHEN protocol RLIKE 'MITO\.*[+]\.*VP16' THEN CONCAT(mapto,',MITO+VP16')
                ELSE mapto
            END;
    """
    dosqlexecute(cnxdict)
    return None


def set_csa_prava_mito_vp16_2409(cnxdict):
    """
    2409 (CSA/PRAVA/MITO/VP16) -- Cyclosporine Modulation of Drug Resistance in Combination with Pravastatin, Mitoxantrone, and Etoposide
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET mapto = CASE
                    WHEN protocol RLIKE '[^0-9]2409[^0-9]' THEN CONCAT(mapto,',2409 (CSA/PRAVA/MITO/VP16)')
                ELSE mapto
            END;
    """
    dosqlexecute(cnxdict)
    return None


def set_mvp16(cnxdict):
    """
    MVP16 -- mitoxantrone, etoposide
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET mapto = CASE
                    WHEN protocol RLIKE 'MVP *16' THEN CONCAT(mapto,',MVP16')
                ELSE mapto
            END;
    """
    dosqlexecute(cnxdict)
    return None


def set_mv_csa_prav(cnxdict):
    """
    MV CSA P -- mitoxantrone, etoposide, cyclosporine, and pravastatin
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET mapto = CASE
                    WHEN protocol RLIKE ' MVCSAP ' THEN CONCAT(mapto,',MV CSA PRAV')
                ELSE mapto
            END;
    """
    dosqlexecute(cnxdict)
    return None


def set_sgn(cnxdict):
    """
    SGN/SGN-CD123A/SGN-CD33A
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET mapto = CASE 
                    WHEN protocol RLIKE 'SGN\.*CD123'       THEN CONCAT(mapto,',SGN-CD123A')
                    WHEN protocol RLIKE 'SGN\.*CD33'        THEN CONCAT(mapto,',SGN-CD33A')
                    WHEN protocol RLIKE '[^0-9]2690[^0-9]'  THEN CONCAT(mapto,',SGN-CD33A')
                    WHEN protocol RLIKE 'SGN'               THEN CONCAT(mapto,',SGN')
                ELSE mapto
            END;
    """
    dosqlexecute(cnxdict)
    return None


def set_mdx(cnxdict):
    """
    MDX (UW09036) 
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET mapto = CASE 
                    WHEN protocol RLIKE 'MDX'                    THEN CONCAT(mapto,',MDX (UW09036)')
                    WHEN protocol RLIKE '[^0-9]0{0,1}9036[^0-9]' THEN CONCAT(mapto,',MDX (UW09036)')
                ELSE mapto
            END;
    """
    dosqlexecute(cnxdict)
    return None


def set_bmn(cnxdict):
    """
    BMN (UW11003)
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET mapto = CASE 
                    WHEN protocol RLIKE 'BMN'               THEN CONCAT(mapto,',BMN (UW11003)')
                    WHEN protocol RLIKE '[^0-9]11003[^0-9]' THEN CONCAT(mapto,',BMN (UW11003)')
                ELSE mapto
            END;
    """
    dosqlexecute(cnxdict)
    return None


def set_arac(cnxdict):
    """
    ARA-C/HiDAC/LDAC/IDAC -- varying doses of cytarabine (ARA-C)
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET mapto = CASE 
                    WHEN protocol RLIKE 'H.{1}DAC' THEN CONCAT(mapto,',HiDAC')
                    WHEN protocol RLIKE 'LDAC'      THEN CONCAT(mapto,',LDAC')
                    WHEN protocol RLIKE 'IDAC'      THEN CONCAT(mapto,',IDAC')
                    WHEN mapto    RLIKE '7[+]3'     THEN CONCAT(mapto,',7+3')
                    WHEN protocol RLIKE 'ARA.{1}C'  THEN CONCAT(mapto,',ARA-C')
                    WHEN protocol RLIKE 'CYTARA'    THEN CONCAT(mapto,',ARA-C')
                ELSE mapto
        END;
    """
    dosqlexecute(cnxdict)
    return None


def set_amg(cnxdict):
    """
    AMG-232/AMG-330/ADCT-301 
    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        UPDATE protocollist SET mapto = CASE 
                    WHEN protocol RLIKE 'AMG\.{1}232'       THEN CONCAT(mapto,',AMG-232 (UW13037)')
                    WHEN protocol RLIKE '[^0-9]13037[^0-9]' THEN CONCAT(mapto,',AMG-232 (UW13037)')
                    WHEN protocol RLIKE 'AMG\.{1}330'       THEN CONCAT(mapto,',AMG-330 (9382)')
                    WHEN protocol RLIKE '[^0-9]9382[^0-9]'  THEN CONCAT(mapto,',AMG-330 (9382)')
                    WHEN protocol RLIKE 'ADCT\.{1}301'      THEN CONCAT(mapto,',ADCT-301 (9513)')
                    WHEN protocol RLIKE '[^0-9]9513[^0-9]'  THEN CONCAT(mapto,',ADCT-301 (9513')
                ELSE mapto
            END;
    """
    dosqlexecute(cnxdict)
    return None


def skeleton(cnxdict):
    """

    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        <SQL>
    """
    dosqlexecute(cnxdict)
    return None


def set_single_agent_regimen(cnxdict=None):
    if cnxdict is None:
        cnxdict = getconnection('hma')  
    set_sgn(cnxdict)
    set_mdx(cnxdict)
    set_bmn(cnxdict)
    set_arac(cnxdict)
    set_amg(cnxdict)
    druglist = []
    # druglist =   ([['inclusionpattern'  ,   'regimen'              ,   'description'                               ]])
    druglist.extend([['IGN'               ,   'IGN-523'              ,   'IGN-523 (UW13049) -- Study drug:  IGN-523' ]])
    druglist.extend([['[^0-9]13049[^0-9]' ,   'IGN-523 (UW13049)'    ,   'IGN-523 (UW13049) -- Study drug:  IGN-523' ]])
    druglist.extend([['PR104'             ,   'PR104'                ,   'PR104 -- Study drug:  PR104'               ]])
    druglist.extend([['FLX'               ,   'FLX925'               ,   'FLX925'                                    ]])
    druglist.extend([['[^0-9]15067[^0-9]' ,   'FLX925'               ,   'FLX925'                                    ]])
    druglist.extend([['MEK'               ,   'MEK INHIBITOR'        ,   'MK-2206 (2466)'                            ]])
    druglist.extend([['[^0-9]2466[^0-9]'  ,   'MEK INHIBITOR'        ,   'MK-2206 (2466)'                            ]])
    druglist.extend([['[^0-9]2498[^0-9]'  ,   'CD8+ T Cells (2498)'  ,   'CD8+ T Cells (2498)'                       ]])
    druglist.extend([['[^0-9]2246[^0-9]'  ,   'SB1518 (2246)'        ,   'SB1518 (2246)'                             ]])
    druglist.extend([['[^0-9]2513[^0-9]'  ,   'CWP (2513)'           ,   'CWP (2513)'                                ]])
    druglist.extend([['[^0-9]2534[^0-9]'  ,   'ON 01910.Na (2534)'   ,   'ON 01910.Na (2534)'                        ]])
    druglist.extend([['[^0-9]2572[^0-9]'  ,   'AC-225 (2572)'        ,   'AC-225 (2572)'                             ]])
    druglist.extend([['[^0-9]2532[^0-9]'  ,   'PLX3397 (2532)'       ,   'PLX3397 (2532)'                            ]])
    druglist.extend([['[^0-9]7971[^0-9]'  ,   'GO (7971)'            ,   'GO (7971)'                                 ]])
    druglist.extend([[' GO '              ,   'GO (7971)'            ,   'GO (7971)'                                 ]])
    druglist.extend([[' SORA '            ,   'SORA'                 ,   'SORA'                                      ]])
    druglist.extend([[' SORAFINIB '       ,   'SORA'                 ,   'SORA'                                      ]])
    druglist.extend([[' LENALIDOMIDE '    ,   'LENALIDOMIDE'         ,   'LENALIDOMIDE'                              ]])
    druglist.extend([[' ATRA '            ,   'ATRA'                 ,   'ATRA'                                      ]])
    druglist.extend([[' HU '              ,   'HU'                   ,   'HU'                                        ]])
    druglist.extend([[' CLADRABINE '      ,   'CLADRABINE'           ,   'CLADRABINE'                                ]])
    druglist.extend([[' CLOFARABINE '     ,   'CLOFARABINE'          ,   'CLOFARABINE'                               ]])
    druglist.extend([[' DACOGEN '         ,   'DECI'                 ,   'DECI'                                      ]])
    druglist.extend([[' IDARUBICIN '      ,   'IDARUBICIN'           ,   'IDARUBICIN'                                ]])
    druglist.extend([[' NALARABINE '      ,   'NALARABINE'           ,   'NALARABINE'                                ]])
    druglist.extend([[' REVLAMID '        ,   'REVLAMID'             ,   'REVLAMID'                                  ]])
    druglist.extend([[' TEMOZOLOMIDE '    ,   'TEMOZOLOMIDE'         ,   'TEMOZOLOMIDE'                              ]])
    druglist.extend([[' IBURTINIB '       ,   'IBRUTINIB'            ,   'IBRUTINIB'                                 ]])
    druglist.extend([[' IBRUTINIB '       ,   'IBRUTINIB'            ,   'IBRUTINIB'                                 ]])
    druglist.extend([[' BORTEZOMIB '      ,   'BORTEZOMIB'           ,   'BORTEZOMIB'                                ]])
    druglist.extend([[' PRALATREXATE '    ,  'PRALATREXATE'          ,   'PRALATREXATE'                              ]])
    druglist.extend([[' WIS '             ,   'WIS'                  ,   'WIS -- withdrawl of immunosuppression'     ]])
    druglist.extend([[' OSTEOK '          ,   'OSTEOK'               ,   'OSTEOK'                                    ]])
    druglist.extend([[' CEP701 '          ,   'CEP701'               ,   'CEP701 -- lestaurtinib'                    ]])
    druglist.extend([[' DACTINOMYCIN '    ,   'DACTINOMYCIN'         ,   'DACTINOMYCIN'                              ]])
    druglist.extend([[' ACT-D '           ,   'DACTINOMYCIN'         ,   'DACTINOMYCIN'                              ]])
    druglist.extend([[' IT MTX '          ,   'IT MTX'               ,   'IT MTX'                                    ]])
    
    stmt =''
    
    for pat,drug,desc in druglist:
        stmt = stmt + 'WHEN protocol = "{0}" THEN CONCAT(mapto,",{1}") '.format(pat,drug)
    
    cnxdict['sql'] = """
        UPDATE protocollist SET mapto = CASE
                {0}
            ELSE mapto
        END;
    """.format( stmt )

    print cnxdict['sql']
    
    dosqlexecute(cnxdict)
    return None


def skeleton(cnxdict):
    """

    :param cnxdict: data dictionary object
    :return: None
    """
    cnxdict['sql'] = """
        <SQL>
    """
    dosqlexecute(cnxdict)
    return None


def set_add_on_agent(cnxdict=None):
    if cnxdict is None:
        cnxdict = getconnection('hma')  # get a connection to the hma section for an example
    allitem =     ([['exclusionpattern',   'inclusionpattern',   'agent'                ]])
    allitem.extend([['SORA'            ,   'SORA'            ,   'sorafenib'            ]])
    allitem.extend([['GO'              ,   '[+].?GO'         ,   'gemtuzumab ozogamicin']])
    allitem.extend([['VP16'            ,   '[+].?VP16'       ,   'etoposide'            ]])
    allitem.extend([['VP'              ,   '[+].?VP'         ,   'vincristine'          ]])
    allitem.extend([[''                ,   ''                ,   ''                     ]])
    return None


def build_all(cnxdict=None):
    if cnxdict is None:
        cnxdict = getconnection('hma')  # get a connection to the hma section for an example
    build_protocol_list(cnxdict)
    set_wildcard_flag(cnxdict)
    map_hct(cnxdict)
    set_no_regimen(cnxdict)
    set_radiation(cnxdict)
    set_combo_regimen(cnxdict)
    set_single_agent_regimen(cnxdict)
    set_add_on_agent(cnxdict)
    return None