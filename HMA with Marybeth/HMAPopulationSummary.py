from Utilities.MySQLdbUtils import *
# import os
# print(os.path.dirname(os.path.realpath(__file__)))
# reload(sys)

Identifiers = ('PtMRN', 'ptmrn', 'PtName', 'PtBirthdate', 'PtBirthDate', 'PtDeathDate')


def create_population(t1_select, cnxdict):
    """
    :param t1_select:
    :param cnxdict:
    :return:
    The criteria used to set up the temp.t1 table defines the population, t1 needs only the column of ptmrn's
    """

    print('creating population')


    cnxdict['sql'] = """
       DROP TABLE IF EXISTS temp.t1;
    """
    dosqlexecute(cnxdict)  # normally do not need to recreate views

    cnxdict['sql'] = """
        CREATE TABLE temp.t1 {0} ;
    """.format(t1_select)
    dosqlexecute(cnxdict, Single=True)  # normally do not need to recreate views

    cnxdict['sql'] = """
        DROP TABLE IF EXISTS temp.population;
        CREATE TABLE temp.population
            SELECT
                a.PtMRN
                , a.PatientID
                , concat(
                    CASE
                        WHEN PtLastName IS NULL THEN ''
                        ELSE concat(UPPER(PtLastName),', ')
                    END,
                    CASE
                        WHEN PtFirstName IS NULL THEN ''
                        ELSE concat(UPPER(PtFirstName),' ')
                    END,
                    CASE
                        WHEN PtMiddleName IS NULL THEN ''
                        ELSE concat(UPPER(PtMiddleName),'.')
                    END) AS PtName
                , a.PtBirthDate
            FROM caisis.vdatasetpatients a
            LEFT JOIN temp.t1 b
                ON a.PtMRN = b.PtMRN
            WHERE b.PtMRN IS NOT NULL
            GROUP BY a.PtMRN
            ORDER BY a.PtMRN;
    """
    dosqlexecute(cnxdict)  # normally do not need to recreate views
    return


def create_patientsource(t1_select, cnxdict):
    """
    :param t1_select:
    :param cnxdict:
    The criteria used to set up the temp.t1 table defines the population, t1 needs only the column of ptmrn's
    """
    print('creating patient source')

    cnxdict['sql'] = """
       DROP TABLE IF EXISTS temp.patientsource;
    """
    dosqlexecute(cnxdict)  # normally do not need to recreate views

    cnxdict['sql'] = """
        CREATE TABLE temp.patientsource {0} ;
    """.format(t1_select)
    dosqlexecute(cnxdict, Single=True)  # normally do not need to recreate views

    return


def hide_dataframe_identifier(hide_identifiers=True, df=None, identifier_list=None):
    if not hide_identifiers:
        return df
    # collist = ''
    # for col in df.columns:
    #     collist = collist + ',' + col.lower()
    # collist = collist[1:]
    for identifier in identifier_list:
        if identifier in df.columns:
            if 'date' in identifier.lower():
                df[identifier] = df[identifier].dt.strftime('%m/%Y')
                df[identifier].replace(['NaT'], '', regex=True, inplace=True)  # horizontal ellipsis
            else:
                df[identifier] = ''
    return df


def output_playground_and_timeline(cmd
                                   , cnxdict
                                   , population_description='AML and MDS'
                                   , include_patient_source_table=False
                                   , hide_identifiers=True):
    # create_population(cmd, cnxdict)
    create_patientsource(cmd, cnxdict)

    timenow = datetime.datetime.now().strftime('_%H%M%S')

    cnxdict['out_filepath'] = cnxdict['out_filepath'].replace('.xlsx', '{0}.xlsx'.format(timenow))
    print(cnxdict['out_filepath'])
    writer = pd.ExcelWriter(cnxdict['out_filepath'], datetime_format='mm/dd/yyyy')

    # ----------------------------------------------------------------------------------
    if True:
        sheet = '{0} Patient List'.format(population_description)
        print ('Creating: {0}'.format(sheet))
        cmd = """
            SELECT * FROM temp.population
        """
        # df = pd.read_sql(cmd, cnxdict['cnx'])
        df = dosqlread(cmd, cnxdict['cnx'])

        df = clean_common_df_error_columns(df, get_colnames(cnxdict, sch='temp', tbl='population'))  # removed uni
        hide_dataframe_identifier(hide_identifiers=hide_identifiers, df=df, identifier_list=Identifiers)
        try:
            df.to_excel(writer, sheet_name=sheet, index=False)
        except:
            print("Failed to create {0}".format(sheet))

    # ----------------------------------------------------------------------------------
    if include_patient_source_table:
        sheet = '{0} Patient Source List'.format(population_description)
        print ('Creating: {0}'.format(sheet))
        df = dosqlread("""
            SELECT * FROM temp.patientsource ;
        """, cnxdict['cnx'])

        df = clean_common_df_error_columns(df, get_colnames(cnxdict, sch='temp', tbl='patientsource'))  # removed uni
        hide_dataframe_identifier(hide_identifiers=hide_identifiers, df=df, identifier_list=Identifiers)
        try:
            df.to_excel(writer, sheet_name=sheet, index=False)
        except:
            print("Failed to create {0}".format(sheet))

    # ----------------------------------------------------------------------------------
    if True:
        sheet = '{0} Playground'.format(population_description)
        print ('Creating: {0}'.format(sheet))

        cmd = """
            SELECT a.*
                FROM temp.population b
                LEFT JOIN caisis.playground a on b.PatientId = a.PatientId
            ORDER BY b.PtMRN, a.ArrivalDate;
        """
        # df = pd.read_sql(cmd, cnxdict['cnx'])
        # df = clean_common_df_error_columns(df,get_colnames(cnxdict, sch='caisis', tbl='patienttimeline'))  # removed uni
        df = dosqlread(cmd, cnxdict['cnx'])
        hide_dataframe_identifier(hide_identifiers=hide_identifiers, df=df, identifier_list=Identifiers)
        try:
            df.to_excel(writer, sheet_name=sheet, index=False)
        except:
            print("Failed to create {0}".format(sheet))

        try:
            df.to_excel(writer, sheet_name=sheet, index=False)
        except:
            print("Failed to create playground worksheet")

    # # ----------------------------------------------------------------------------------
    # if False:
    #     sheet = '{0} Playground and Secondary'.format(population_description)
    #     print ('Creating: {0}'.format(sheet))
    #
    #     cmd = """
    #         SELECT c.SecondaryType, a.*
    #             FROM temp.population b
    #             LEFT JOIN caisis.secondarystatus c ON b.PatientId = c.PatientId
    #             LEFT JOIN caisis.playground a ON b.PatientId = a.PatientId
    #             ORDER BY b.PtMRN, a.ArrivalDate;
    #     """
    #     # df = pd.read_sql(cmd, cnxdict['cnx'])
    #     # df = clean_common_df_error_columns(df
    #     #                                    , get_colnames(cnxdict
    #     #                                    , sch='caisis'
    #     #                                    , tbl='patienttimeline'))  # removed uni
    #     df = dosqlread(cmd, cnxdict['cnx'])
    #     hide_dataframe_identifier(hide_identifiers=hide_identifiers, df=df, identifier_list=Identifiers)
    #     try:
    #         df.to_excel(writer, sheet_name=sheet, index=False)
    #     except:
    #         print("Failed to create {0}".format(sheet))
    #
    #     try:
    #         df.to_excel(writer, sheet_name=sheet, index=False)
    #     except:
    #         print("Failed to create playground worksheet")
    #
    # # ----------------------------------------------------------------------------------
    # if True:
    #     sheet = '{0} Timeline'.format(population_description)
    #     print ('Creating: {0}'.format(sheet))
    #     cmd = """
    #         SELECT a.*
    #             FROM temp.population b
    #             LEFT JOIN caisis.patienttimeline a on b.PtMRN = a.PtMRN
    #             ORDER BY b.PtMRN, a.EventDate ;
    #     """
    #     # df = pd.read_sql(cmd, cnxdict['cnx'])
    #     # df = clean_common_df_error_columns(df
    #     #                                    , get_colnames(cnxdict
    #     #                                    , sch='caisis'
    #     #                                    , tbl='patienttimeline'))  # removed uni
    #     df = dosqlread(cmd, cnxdict['cnx'])
    #     hide_dataframe_identifier(hide_identifiers=hide_identifiers, df=df, identifier_list=Identifiers)
    #     try:
    #         df.to_excel(writer, sheet_name=sheet, index=False)
    #     except:
    #         print("Failed to create {0}".format(sheet))
    #
    # # ----------------------------------------------------------------------------------
    # if True:
    #     sheet = '{0} Treatments'.format(population_description)
    #     print ('Creating: {0}'.format(sheet))
    #     cmd = """
    #         SELECT b.PatientId, a.*
    #             FROM temp.population b
    #             LEFT JOIN caisis.induction a on b.PtMRN = a.PtMRN
    #             ORDER BY b.PtMRN;
    #     """
    #     df = pd.read_sql(cmd, cnxdict['cnx'])
    #     hide_dataframe_identifier(hide_identifiers=hide_identifiers, df=df, identifier_list=Identifiers)
    #     # df = dosqlread(cmd, cnxdict['cnx'])
    #     df = clean_common_df_error_columns(df,get_colnames( cnxdict, sch='caisis', tbl='molecular')) # removed uni
    #     try:
    #         df.to_excel(writer, sheet_name=sheet, index=False)
    #     except:
    #         print("Failed to create {0}".format(sheet))
    #
    # # ----------------------------------------------------------------------------------
    # if True:
    #     sheet = '{0} Molecular'.format(population_description)
    #     print ('Creating: {0}'.format(sheet))
    #     cmd = """
    #         SELECT b.PatientId, a.*
    #             FROM temp.population b
    #             LEFT JOIN caisis.molecular a on b.PtMRN = a.PtMRN
    #             ORDER BY b.PtMRN, a.LabDate;
    #     """
    #     df = pd.read_sql(cmd, cnxdict['cnx'])
    #     # df = dosqlread(cmd, cnxdict['cnx'])
    #     df = clean_common_df_error_columns(df,get_colnames( cnxdict, sch='caisis', tbl='molecular'))  # removed uni
    #     hide_dataframe_identifier(hide_identifiers=hide_identifiers, df=df, identifier_list=Identifiers)
    #     try:
    #         df.to_excel(writer, sheet_name=sheet, index=False)
    #     except:
    #         print("Failed to create {0}".format(sheet))
    #
    # # ----------------------------------------------------------------------------------
    # if True:
    #     sheet = '{0} Lab Results'.format(population_description)
    #     print ('Creating: {0}'.format(sheet))
    #     # Run J:\Estey_AML\AML Programming\Python\sharedUtils\RelevantTest_from_caisis.py first
    #     cmd = """
    #         SELECT b.PatientId, a.*
    #             FROM temp.population b
    #             LEFT JOIN caisis.labsummary a on b.PtMRN = a.PtMRN
    #             ORDER BY b.PtMRN, a.ArrivalDate;
    #     """
    #     df = pd.read_sql(cmd, cnxdict['cnx'])
    #     hide_dataframe_identifier(hide_identifiers=hide_identifiers, df=df, identifier_list=Identifiers)
    #     # df = dosqlread(cmd, cnxdict['cnx'])
    #     df = clean_common_df_error_columns(df, get_colnames(cnxdict, sch='caisis', tbl='labsummary'))  # removed uni
    #     try:
    #         df.to_excel(writer, sheet_name=sheet, index=False)
    #     except:
    #         print("Failed to create {0}".format(sheet))
    #
    # # ----------------------------------------------------------------------------------
    # if False:
    #     sheet = '{0} Secondary'.format(population_description)
    #     print ('Creating: {0}'.format(sheet))
    #     # Run J:\Estey_AML\AML Programming\Python\sharedUtils\RelevantTest_from_caisis.py first
    #     cmd = """
    #         SELECT b.PatientId, a.*
    #             FROM temp.population b
    #             LEFT JOIN caisis.labsummary a on b.PtMRN = a.PtMRN
    #             ORDER BY b.PtMRN, a.ArrivalDate;
    #     """
    #     df = pd.read_sql(cmd, cnxdict['cnx'])
    #     # df = dosqlread(cmd, cnxdict['cnx'])
    #     df = clean_common_df_error_columns(df, get_colnames(cnxdict, sch='caisis', tbl='labsummary'))  # removed uni
    #     hide_dataframe_identifier(hide_identifiers=hide_identifiers, df=df, identifier_list=Identifiers)
    #     try:
    #         df.to_excel(writer, sheet_name=sheet, index=False)
    #     except:
    #         print("Failed to create {0}".format(sheet))
    #
    # # ----------------------------------------------------------------------------------
    # if True:
    #     sheet = '{0} Previous Cancer'.format(population_description)
    #     print ('Creating: {0}'.format(sheet))
    #     # Run J:\Estey_AML\AML Programming\Python\sharedUtils\RelevantTest_from_caisis.py first
    #     cmd = """
    #         SELECT b.PatientId, a.*
    #             FROM temp.population b
    #             LEFT JOIN caisis.v_nonheme a on b.PtMRN = a.PtMRN
    #             WHERE a.PtMRN IS NOT NULL
    #             ORDER BY b.PtMRN, a.StatusDate ;
    #     """
    #     df = pd.read_sql(cmd, cnxdict['cnx'])
    #     # df = dosqlread(cmd, cnxdict['cnx'])
    #     df = clean_common_df_error_columns(df, get_colnames(cnxdict, sch='caisis', tbl='labsummary'))  # removed uni
    #     hide_dataframe_identifier(hide_identifiers=hide_identifiers, df=df, identifier_list=Identifiers)
    #     try:
    #         df.to_excel(writer, sheet_name=sheet, index=False)
    #     except:
    #         print("Failed to create {0}".format(sheet))
    #
    # # ----------------------------------------------------------------------------------
    # if True:
    #     sheet = '{0} Previous Heme Diagnosis'.format(population_description)
    #     print ('Creating: {0}'.format(sheet))
    #     # Run J:\Estey_AML\AML Programming\Python\sharedUtils\RelevantTest_from_caisis.py first
    #     cmd = """
    #         SELECT b.PatientId, a.*
    #             FROM temp.population b
    #             LEFT JOIN caisis.v_nonamlheme a on b.PatientId = a.PatientId
    #             WHERE a.PatientId IS NOT NULL
    #             ORDER BY b.PtName, a.StatusDate ;
    #     """
    #     df = pd.read_sql(cmd, cnxdict['cnx'])
    #     # df = dosqlread(cmd, cnxdict['cnx'])
    #     df = clean_common_df_error_columns(df, get_colnames(cnxdict, sch='caisis', tbl='labsummary'))  # removed uni
    #     hide_dataframe_identifier(hide_identifiers=hide_identifiers, df=df, identifier_list=Identifiers)
    #     try:
    #         df.to_excel(writer, sheet_name=sheet, index=False)
    #     except:
    #         print("Failed to create {0}".format(sheet))
    #
    # # ----------------------------------------------------------------------------------
    # if True:
    #     sheet = '{0} Patient TRM'.format(population_description)
    #     print ('Creating: {0}'.format(sheet))
    #     # Run J:\Estey_AML\AML Programming\Python\sharedUtils\RelevantTest_from_caisis.py first
    #     cmd = """
    #         SELECT a.PatientId,
    #             b.*
    #         FROM temp.population a
    #         LEFT JOIN caisis.arrivaltrm b
    #         ON a.PatientId = b.PatientId
    #         WHERE a.PtMRN IS NOT NULL;
    #     """
    #     df = pd.read_sql(cmd, cnxdict['cnx'])
    #     # df = dosqlread(cmd, cnxdict['cnx'])
    #     df = clean_common_df_error_columns(df, get_colnames(cnxdict, sch='caisis', tbl='labsummary'))  # removed uni
    #     hide_dataframe_identifier(hide_identifiers=hide_identifiers, df=df, identifier_list=Identifiers)
    #     try:
    #         df.to_excel(writer, sheet_name=sheet, index=False)
    #     except:
    #         print("Failed to create {0}".format(sheet))

    dowritersave(writer, cnxdict)
    return None


def main_routine(population, include_patient_source_table=False, hide_identifiers=True):
    cnxdict = connect_to_mysql_db_prod('utility')
    cnxdict['out_filepath'] = buildfilepath(cnxdict, DisplayPath=True)
    cmd = ''

    # Example Command for HMA's population
    if population == 'HMA':
        cmd = r"SELECT *, uwid as PtMRN FROM hma_20170721.hmapatients_20171030"

    # Example Command for HMA's population
    if population == 'HMA CONTROL':
        cmd = r"SELECT * FROM hma_20170721.control"

    """
        SELECT DISTINCT uwid as PtMRN, `HMA`, `HMA Start`,
            CASE
                WHEN a.VidazaStop IS NULL  THEN a.DacogenStop
                WHEN a.DacogenStop IS NULL THEN a.VidazaStop
                WHEN a.VidazaStop > a.DacogenStop THEN a.VidazaStop
                WHEN a.VidazaStop < a.DacogenStop THEN a.DacogenStop
                ELSE cast(null as datetime)
            END AS `HMA Stop`
            ,CASE
                WHEN a.VidazaCycles IS NULL  THEN a.DacogenCycles
                WHEN a.DacogenCycles IS NULL THEN a.VidazaCycles
                ELSE a.VidazaCycles + a.DacogenCycles
            END as `HMA Cycles`
            , CASE
                WHEN `HMA` = 'Vidaza' AND a.VidazaCycles IS NULL THEN 'Vidaza Cycles Missing'
                WHEN `HMA` = 'Both'   AND a.VidazaCycles IS NULL THEN 'Vidaza Cycles Missing'
                ELSE ''
            END AS VidazaCycleNote
            , a.VidazaCycles
            , a.DacogenCycles
            , a.`treatment startdate` AS InductionStart
            , b.treatmentdate
            , timestampdiff(day,CASE
                WHEN a.VidazaStop IS NULL  THEN a.DacogenStop
                WHEN a.DacogenStop IS NULL THEN a.VidazaStop
                WHEN a.VidazaStop > a.DacogenStop THEN a.VidazaStop
                WHEN a.VidazaStop < a.DacogenStop THEN a.DacogenStop
                ELSE cast(null as datetime)
            END,a.`treatment startdate`) AS `Days HMA to Induction`
            , a.protocol, b.protocol, c.originalprotocol
            , c.backbonename
            , c.anthracyclin
            , c.anthracyclindose
            , c.backboneaddon
            , c.intensity
            FROM hma_20170721.hmapatients_20171030 a
            LEFT JOIN caisis.treatment b on a.uwid = b.ptmrn
                AND year(a.`treatment startdate`) = year(b.treatmentdate)
                AND month(a.`treatment startdate`) = month(b.treatmentdate)
            LEFT JOIN caisis.backbonemapping3 c on (a.protocol = c.originalprotocol);
        """

    # During this create playground run, users can say whether or not they want supporting tables recreated.
    # Supporting tables actually take quite a bit of time to recreate, if this is not the first run you don't
    # lose anything by skipping the recreation and you will save a bunch of time.
    # create_playground()

    output_playground_and_timeline(cmd
        , cnxdict
        , population
        , include_patient_source_table=include_patient_source_table
        , hide_identifiers=hide_identifiers)

    return None


# main_routine('HMA', include_patient_source_table=True, hide_identifiers=False)
# main_routine('HMA CONTROL', include_patient_source_table=True, hide_identifiers=False)
# main_routine('All Caisis', include_patient_source_table=True, hide_identifiers=False)


cnxdict = connect_to_mysql_db_prod('utility')
cnxdict['out_filepath'] = buildfilepath(cnxdict, DisplayPath=True)
writer = pd.ExcelWriter(cnxdict['out_filepath'], datetime_format='mm/dd/yyyy')

cnxdict['sql'] = """
        DROP TABLE IF EXISTS temp.temp1;
        CREATE TABLE temp.temp1
            SELECT a.`id`
                , b.`arrival_id`
                , a.uwid                         AS ptmrn_mbp
                , a.`Include`                    AS include_mbp
                , a.`HMA Start`                  AS hmastart_mbp
                , a.`arrival date`               AS arrivaldate_mbp
                , a.`Days from HMA to Induction` AS dayshmatoinduction_mbp
                , a.`treatment startdate`        AS treatmentstartdate_mbp
                , a.`protocol`                   AS protocolmbp
                , a.`Protocol Mapped To`         AS protocolmappedto_mbp
                , a.`External Induction`         AS externalinduction_mbp
                , a.`Response to Induction at Outside Location` 
                                                 AS outinductionresp_mbp
                , a.`arrivaldx`                  AS arrivaldx_mbp
                , a.`Intensity of first induction after HMA` 
                                                 AS posthmainductionintensity_mbp
                , a.`HMA`                        AS hma_mbp
                , a.`Response to HMA`            AS hmaresponse_mbp
                , a.`VidazaStart`                AS vidazastart_mbp
                , a.`VidazaStop`                 AS vidazastop_mbp
                , a.`VidazaCycles`               AS vidazacycles_mbp
                , a.`DacogenStart`               AS dacogenstart_mbp
                , a.`DacogenStop`                AS dacogenstop_mbp
                , a.`DacogenCycles`              AS dacogencycles_mbp
                FROM hma_20170721.hmapatients_20171030 a
                left join caisis.playground b on a.uwid = b.PtMRN 
                AND a.`arrival date` = b.`ArrivalDate` ;
                
                
        DROP TABLE IF EXISTS temp.temp2;
        CREATE TABLE temp.temp2 
            SELECT a.`id`
                , b.`arrival_id`
                , a.uwid                         AS ptmrn_mbp
                , a.`Include`                    AS include_mbp
                , a.`HMA Start`                  AS hmastart_mbp
                , a.`arrival date`               AS arrivaldate_mbp
                , a.`Days from HMA to Induction` AS dayshmatoinduction_mbp
                , a.`treatment startdate`        AS treatmentstartdate_mbp
                , a.`protocol`                   AS protocolmbp
                , a.`Protocol Mapped To`         AS protocolmappedto_mbp
                , a.`External Induction`         AS externalinduction_mbp
                , a.`Response to Induction at Outside Location` 
                                                 AS outinductionresp_mbp
                , a.`arrivaldx`                  AS arrivaldx_mbp
                , a.`Intensity of first induction after HMA` 
                                                 AS posthmainductionintensity_mbp
                , a.`HMA`                        AS hma_mbp
                , a.`Response to HMA`            AS hmaresponse_mbp
                , a.`VidazaStart`                AS vidazastart_mbp
                , a.`VidazaStop`                 AS vidazastop_mbp
                , a.`VidazaCycles`               AS vidazacycles_mbp
                , a.`DacogenStart`               AS dacogenstart_mbp
                , a.`DacogenStop`                AS dacogenstop_mbp
                , a.`DacogenCycles`              AS dacogencycles_mbp
                FROM hma_20170721.hmapatients_20171030 a
                join caisis.playground b on a.uwid = b.PtMRN 
                AND a.`arrival date` != b.`ArrivalDate` 
                WHERE b.PtMRN in (SELECT PtMRN FROM temp.temp1 WHERE PatientId IS NULL);

        DROP TABLE IF EXISTS hma_20170721.hma_playground ;
        CREATE TABLE hma_20170721.hma_playground
            SELECT * FROM temp.temp1 
                UNION 
            SELECT * FROM temp.temp2 
                ORDER BY id;

    ALTER TABLE hma_20170721.hma_playground DROP COLUMN id ;
    
    DROP TABLE IF EXISTS `hma_20170721`.`hma_playgroundcontrol` ;
    CREATE TABLE `hma_20170721`.`hma_playgroundcontrol`
        SELECT b.arrival_id
        , b.`PtMRN`                 AS ptmrn_ctrlmbp
        , a.`ArrivalDx`             AS arrivaldx_ctrlmbp
        , a.`Treatmentstartdate`    AS treatmentstartdate_ctrlmbp
        , a.`Protocol`              AS protocol_ctrlmbp
        , a.`WildCard`              AS wildcard_ctrlmbp
        , a.`WildCardDate`          AS wildcarddate_ctrlmbp
        , a.`MedTxDate`             AS medtxdate_ctrlmbp
        , a.`MedTxAgent`            AS medtxagent_ctrlmbp    
        , a.`Categorized`           AS categorized_ctrlmbp
        , a.`Intensity`             AS intensity_ctrlmbp
        FROM `hma_20170721`.`control` a
            JOIN caisis.playground b 
            ON a.PtMRN = b.PtMRN and a.ArrivalDate = b.ArrivalDate ;
    
    """
dosqlexecute(cnxdict)

# -----------
sheet = 'HMA Patient List'
cmd = """
    SELECT * FROM hma_20170721.hmapatients_20171030 ;
"""
df = dosqlread(cmd, cnxdict['cnx'])
try:
    df.to_excel(writer, sheet_name=sheet, index=False)
except:
    print("Failed to create {0}".format(sheet))

# -----------
sheet = 'HMA Playground'
cmd = """
    SELECT * FROM hma_20170721.hma_playground WHERE arrival_id > 1000;
"""
df = dosqlread(cmd, cnxdict['cnx'])
try:
    df.to_excel(writer, sheet_name=sheet, index=False)
except:
    print("Failed to create {0}".format(sheet))

# -----------
sheet = 'HMA Playground Controls'
cmd = """
    SELECT a.* 
        FROM hma_20170721.hma_playgroundcontrol a
        LEFT JOIN hma_20170721.hma_playground b 
            ON a.ptmrn_ctrlmbp = b.ptmrn_mbp
        LEFT JOIN caisis.playground c
            on a.arrival_id = c.arrival_id
        WHERE b.arrival_id is null;"""
df = dosqlread(cmd, cnxdict['cnx'])
try:
    df.to_excel(writer, sheet_name=sheet, index=False)
except:
    print("Failed to create {0}".format(sheet))

# -----------
sheet = 'Patients since June 2016'
cmd = """
    SELECT a.*
        FROM caisis.playground a
        LEFT JOIN hma_20170721.hma_playgroundcontrol b
            ON a.`ptmrn` = b.`ptmrn_ctrlmbp` 
        LEFT JOIN hma_20170721.hma_playground c
            ON a.`ptmrn` = c.`ptmrn_mbp`
        WHERE b.ptmrn_ctrlmbp IS NULL 
        AND c.ptmrn_mbp IS NULL 
        AND a.arrivaldate > '2016-06-30'
        AND a.ReturnPatient = 'No';
"""
df = dosqlread(cmd, cnxdict['cnx'])
try:
    df.to_excel(writer, sheet_name=sheet, index=False)
except:
    print("Failed to create {0}".format(sheet))


dowritersave(writer, cnxdict)
