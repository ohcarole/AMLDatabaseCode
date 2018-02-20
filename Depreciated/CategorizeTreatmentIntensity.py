from Depreciated.CategorizeProtocol_ import *


def assign_intensity(cnxdict):
    #create_protocol_and_intensity_mapping_table(cnxdict)

    cnxdict['sql'] = """
        /*********************************************************************************
        # High Intensity
        *********************************************************************************/
        # G-CLAM/CLAM
        UPDATE protocolmapping
            SET intensity = 'High Intensity'
            WHERE (shortname RLIKE 'G-CLAM')
            AND   (shortname NOT RLIKE 'PALLIATIVE');
        # G-CLAC, CLAC
        UPDATE protocolmapping
            SET intensity = 'High Intensity'
            WHERE (shortname RLIKE 'CLAC')
            AND   (shortname NOT RLIKE 'PALLIATIVE');
        # FLAG, FLAG-IDA
        UPDATE protocolmapping
            SET intensity = 'High Intensity'
            WHERE (shortname RLIKE 'FLAG')
            AND   (shortname NOT RLIKE 'PALLIATIVE');
        # D-MEC
        UPDATE protocolmapping
            SET intensity = 'High Intensity'
            WHERE (shortname RLIKE 'D-MEC')
            AND   (shortname NOT RLIKE 'PALLIATIVE');

        /*********************************************************************************
        # Medium Intensity
        *********************************************************************************/
        # 7+3
        UPDATE protocolmapping
            SET intensity = 'Medium Intensity'
            WHERE (shortname RLIKE '7+3')
            AND   (shortname NOT RLIKE 'PALLIATIVE'
            AND    intensity = '' );
        # CSA
        UPDATE protocolmapping
            SET intensity = 'Medium Intensity'
            WHERE (shortname RLIKE 'CSA')
            AND   (shortname NOT RLIKE 'PALLIATIVE'
            AND    intensity = '' );
        # HiDAC
        UPDATE protocolmapping
            SET intensity = 'Medium Intensity'
            WHERE (shortname RLIKE 'HIDAC')
            AND   (shortname NOT RLIKE 'PALLIATIVE'
            AND    intensity = '' );
        # IA/IAP
        UPDATE protocolmapping
            SET intensity = 'Medium Intensity'
            WHERE (shortname RLIKE 'IA')
            AND   (shortname NOT RLIKE 'PALLIATIVE'
            AND    intensity = '' );

        /*********************************************************************************
        # Medium Intensity
        *********************************************************************************/
        # 7+3
        UPDATE protocolmapping
            SET intensity = 'Medium Intensity'
            WHERE (shortname RLIKE '7+3')
            AND   (shortname NOT RLIKE 'PALLIATIVE'
            AND    intensity = '' );
        # CSA
        UPDATE protocolmapping
            SET intensity = 'Medium Intensity'
            WHERE (shortname RLIKE 'CSA')
            AND   (shortname NOT RLIKE 'PALLIATIVE'
            AND    intensity = '' );
        # HiDAC
        UPDATE protocolmapping
            SET intensity = 'Medium Intensity'
            WHERE (shortname RLIKE 'HIDAC')
            AND   (shortname NOT RLIKE 'PALLIATIVE'
            AND    intensity = '' );
        # IA/IAP
        UPDATE protocolmapping
            SET intensity = 'Medium Intensity'
            WHERE (shortname RLIKE 'IA')
            AND   (shortname NOT RLIKE 'PALLIATIVE'
            AND    intensity = '' );

        /*********************************************************************************
        # Low Intensity
        *********************************************************************************/
        # AZA
        UPDATE protocolmapping
            SET intensity = 'Low Intensity'
            WHERE (shortname RLIKE 'AZA')
            AND   (shortname NOT RLIKE 'PALLIATIVE'
            AND    intensity = '' );
        # DECI
        UPDATE protocolmapping
            SET intensity = 'Low Intensity'
            WHERE (shortname RLIKE 'DECI')
            AND   (shortname NOT RLIKE 'PALLIATIVE'
            AND    intensity = '' );

    """
    return dosqlexecute(cnxdict)
