# import time
from MySQLdbUtils import *
from SQLServerUtils import *
from Connection import *
# from SQLServer_pyodbc import *
from SendNote import mail
from sqlalchemy import create_engine
from MessageBox import *

chunkamount = 30000

# engine = create_engine('mysql+mysqldb://carole_shaw:1UglyBunnyHop%%%@MYSQL-DB-PRD/caisis')


def putinmysql(df, sql, tbl, engine, mode='append'):
    try:
        df.to_sql(tbl.lower(), engine, chunksize=500, if_exists=mode)
    except:
        df.to_sql(tbl.lower(), engine, chunksize=500, if_exists=mode)
        print('/*\nFailed to save data to MySQL\n {}\n*/'.format(sql))


def get_sccacyto(cnxdict,engine):
    tbl = 'sccacyto'
    key = 'None'
    tempsql = """
        SELECT b.[PatientId], a.*
            FROM [AML].[vCytoDiagnosis] a
            JOIN WorkDBProd..vDatabasePatients b ON a.[PtMRN] = b.[PtMRN];
    """
    df = dosqlread(tempsql, cnxdict)
    putinmysql(df, tempsql, tbl.lower(), engine)
    indexcmd = """
        ALTER TABLE `caisis`.`{0}`0
            ADD INDEX `PatientId` (`PatientId`(8) ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `PtMRN` (`PtMRN`(8) ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `EncDate` (`EncDate` ASC);
        ALTER TABLE `caisis`.`{0}`
            CHANGE COLUMN `SpecDate` `SpecDate` DATETIME NULL DEFAULT NULL ;
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `SpecDate` (`SpecDate` ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `Accession` (`Accession`(12) ASC);
    """.format(tbl,key)
    index_mysql(indexcmd)


def get_uwcyto(cnxdict,engine):
    tbl = 'uwcyto'
    key = 'None'
    tempsql = """
        SELECT * FROM #UWCYTO ;
    """
    df = dosqlread(tempsql, cnxdict)
    putinmysql(df, tempsql, tbl.lower(), engine)
    # indexcmd = """
    #     ALTER TABLE `caisis`.`{0}`0
    #         ADD INDEX `PatientId` (`PatientId`(8) ASC);
    #     ALTER TABLE `caisis`.`{0}`
    #         ADD INDEX `PtMRN` (`PtMRN`(8) ASC);
    #     ALTER TABLE `caisis`.`{0}`
    #         ADD INDEX `EncDate` (`EncDate` ASC);
    #     ALTER TABLE `caisis`.`{0}`
    #         CHANGE COLUMN `SpecDate` `SpecDate` DATETIME NULL DEFAULT NULL ;
    #     ALTER TABLE `caisis`.`{0}`
    #         ADD INDEX `SpecDate` (`SpecDate` ASC);
    #     ALTER TABLE `caisis`.`{0}`
    #         ADD INDEX `Accession` (`Accession`(12) ASC);
    # """.format(tbl,key)
    # index_mysql(indexcmd)


def get_abscyto(cnxdict, engine):
    tbl = 'abscyto'
    key = 'None'
    tempsql = """
        SELECT * FROM #ABSCYTO ;
    """
    print('-- Get SQL Server {} data'.format(tbl))
    df = dosqlread(tempsql, cnxdict)
    # Add a test here to see if we need to keep going, if 0 records QUIT!  :)
    print('-- Store SQL Server {} data'.format(tbl))
    putinmysql(df, tempsql, tbl.lower(), engine)


def get_allcyto(cnxdict,engine):
    tbl = 'allcyto'
    key = 'None'
    tempsql = "SELECT * FROM #ALLCYTO;"
    df = dosqlread(tempsql, cnxdict)
    putinmysql(df, tempsql, tbl.lower(), engine)
    indexcmd = """
        ALTER TABLE `caisis`.`{0}`0
            ADD INDEX `PatientId` (`PatientId`(8) ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `PtMRN` (`PtMRN`(8) ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `EncDate` (`EncDate` ASC);
        ALTER TABLE `caisis`.`{0}`
            CHANGE COLUMN `SpecDate` `PathDate` DATETIME NULL DEFAULT NULL ;
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `SpecDate` (`SpecDate` ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `Accession` (`Accession`(12) ASC);
    """.format(tbl,key)
    index_mysql(indexcmd)


def call_stored_procedure(procname):
    mycnx = connect_to_mysql_db_prod('caisismysql')
    mycnx['crs'].callproc(procname)
    mycnx.close()
    return None


def get_caisis_tables():

    cnxdict = read_db_config('caisiswork')
    engine = create_engine('mysql+mysqldb://{0}:{1}@MYSQL-DB-PRD/caisis'.format(cnxdict['mysqluser'],cnxdict['mysqlpwd'],collation='Latin1_General'))
    cnxdict = connect_to_caisisprod(cnxdict)

    tbllist = """
        AllKaryo
        Albumin
        Blast
        Creatinine
        Hematocrit
        Hemoglobin
        HotSpot
        LabTestIndex
        Mutation
        Neutrophil
        Platelet
        RBC
        WBC
        MostRecentLab
        MostRecentPath
        MolecularLabs

        PatientsAccessed
        NewPatientsAccessed

        anc1000counts
        anc500counts
        anccounts
        ancnadir
        HotSpot
        LabPrefix
        MasumiPopulation
        MolecularLabs
        MostRecentLab
        MostRecentPath
        Mutation
        nadir
        nadirtemp
        NewPatientsAccessed
        PatientsAccessed
        Platelet
        plt100counts
        plt20counts
        plt50counts
        pltcounts
        pltnadir
        vDatasetAllLabTests
        vDatasetCategories
        vDatasetComorbidities
        vDatasetDiagnostics
        vDatasetEncounters
        vDatasetEncReviewOfSystems
        vDatasetHCTProc
        vDatasetLabTests
        vDatasetMedicalTherapy
        vDatasetMedTxAdministration
        vDatasetPathology
        vDatasetPathTest
        vDatasetPatientProtocols
        vDatasetPatientProtocolStatus
        vDatasetPatients
        vDatasetPatientsNoNames
        vDatasetProcedures
        vDatasetProtocols
        vDatasetRadiationTherapy
        vDatasetStatus
    """

    tbllist = """
        vDatasetAllLabTests
        vDatasetCategories
        vDatasetComorbidities
        vDatasetDiagnostics
        vDatasetEncounters
        vDatasetEncReviewOfSystems
        vDatasetHCTProc
        vDatasetLabTests
        vDatasetMedicalTherapy
        vDatasetMedTxAdministration
        vDatasetPathology
        vDatasetPathTest
        vDatasetPatientProtocols
        vDatasetPatientProtocolStatus
        vDatasetPatients
        vDatasetPatientsNoNames
        vDatasetProcedures
        vDatasetProtocols
        vDatasetRadiationTherapy
        vDatasetStatus
        vDatasetLastVisit
    """

    tbllist = """
        vDatasetStatus
        vDatasetCategories
        vDatasetComorbidities
        vDatasetEncounters
        vDatasetEncReviewOfSystems
        vDatasetHCTProc
        vDatasetMedicalTherapy
        vDatasetMedTxAdministration
        vDatasetPathology
        vDatasetPathTest
        vDatasetPatientProtocols
        vDatasetPatientProtocolStatus
        vDatasetPatients
        vDatasetProcedures
        vDatasetProtocols
        vDatasetRadiationTherapy
        vDatasetDiagnostics
    """


    print('-- Moving Caisis tables to MySQL')
    for tbl in tbllist.split('\n'):
        tbl = tbl.strip().replace(' ', '_').lower()
        if tbl == '':
            pass

        else:
            if tbl == 'vdatasetlabtests':
                tempsql = 'SELECT * FROM [WorkDBProd]..[{}] WHERE LabTestCategory IS NOT NULL; '.format(tbl)
            else:
                tempsql = 'SELECT * FROM [WorkDBProd]..[{}] '.format(tbl)
            print ('-- Create dataframe from caisis table {0}'.format(tbl))

            # for chunk in dosqlread(tempsql, cnxdict):
            #     putinmysql(df, tempsql, tbl, engine)

            # df = pd.read_sql(cmd, con)

            mode = 'replace'
            print(sql)
            print('Moving to MySQL'),
            loopsymbol = '.'
            recordsmoved = 0
            for chunk in pd.read_sql_query(tempsql, cnxdict, chunksize = chunkamount):
                print(loopsymbol),
                chunk = clean_common_df_error_columns(chunk, chunk.columns)
                putinmysql(chunk, tempsql, tbl, engine, mode) # need to append after first one
                recordsmoved = recordsmoved + len(chunk)
                mode = 'append'
            print

            print ('-- Dataframe complete, copied {1} records to MySQL table Caisis.{0}'.format(tbl,recordsmoved))

            # Add code to alter table adding index for columns
            # Just add new columns to index in the list of fields for each if statement
            # For instance if a new key were added 'patientindex' of type integer, you would add it to the
            # list of columns with if statement #2 since it is an integer field

            cnx = connect_to_mysql_db_prod('caisis_to_mysql')
            curcolnames = get_colnames(cnx, 'caisis', tbl)
            cnx['sql'] = None
            for colname in curcolnames:  # chunk.columns:
                # 1 -- Id fields stored as text (Note I think this will work for all fields, but might give some problems
                #      if indexing a short field, one with fewer than 10 characters.
                if colname.lower() in ['patientid','labgroupid']:
                    cnx['sql'] = "ALTER TABLE `caisis`.`{0}` ADD INDEX `{1}` (`{1}`(8) ASC);".format(tbl,colname)
                if colname.lower() in ['ptmrn',]:
                    cnx['sql'] = "ALTER TABLE `caisis`.`{0}` ADD INDEX `{1}` (`{1}`(10) ASC);".format(tbl,colname)
                # 2 -- Dates and integers ( the syntax is the same)
                if colname.lower() in ['labdate','pathdate','procdate','encdate','specdate','statusdate', 'pathologyid']:
                    cnx['sql'] = "ALTER TABLE `caisis`.`{0}` ADD INDEX `{1}` (`{1}` ASC);".format(tbl,colname)
                # 3 -- Free form text (keyword FULLTEXT added)
                if colname.lower() in ['pathnotes','pathkaryotype']:
                    cnx['sql'] = "ALTER TABLE `caisis`.`{0}` ADD FULLTEXT INDEX `{1}`(`{1}` ASC);".format(tbl, colname)
                # 4 -- Text labele
                if colname.lower() in ['pathtest','labtestcategory', 'labtest']:
                    cnx['sql'] = "ALTER TABLE `caisis`.`{0}` ADD INDEX `{1}` (`{1}`(100) ASC);".format(tbl,colname)
                if cnx['sql'] is not None:
                    print('Indexing on {0}'.format(colname))
                    dosqlexecute(cnx)
                    cnx['sql'] = None

    print('-- Done moving Caisis tables to MySQL')


def get_patient_list_():
    cnxdict = read_db_config('caisiswork')
    filepath = cnxdict['out_filedir'] + '\\' + \
        cnxdict['out_filename'] + '_' + time.strftime('%Y%m%d') + '.' + \
        cnxdict['out_fileext']
    mappedpath = filepath.replace('\\\\cs.fhcrc.org\crtprojects\CRI\CaisisDataRequests\AML Downloads\\','g:\\')
    print (filepath)
    print (mappedpath)
    cnxdict = connect_to_caisisprod(cnxdict)
    writer = pd.ExcelWriter(mappedpath, datetime_format='mm/dd/yyyy')

    pi        = 'Elihu Estey'
    piemail   = 'eestey@seattlecca.org'
    piaddress = 'Seattle Cancer Care Alliance; 825 Eastlake Ave E; Box G3200'
    phidownloaded = 'Name,DOB,' \
                    'Diagnoses and Dates,' \
                    'Procedures and Dates,' \
                    'Clinical Protocols and Dates,' \
                    'Pathology and Dates,' \
                    'Therapy and Dates,' \
                    'Lab Tests and Dates,' \
                    'Comorbidities and Dates,' \
                    'Encounters and Dates'

    contemail = 'cmshaw@fredhutch.org, gardnerk@seattlecca.org'

    phipurpose = 'AML database'

    sqlstmt = """
            SELECT [PtMRN]      AS [Patient MRN]
                , [PatientId]   AS [Patient Id]
                , [PtFirstName] AS [Patinet Firstname]
                , [PtLastName]  AS [Patient Lastname]
                , [PtBirthDate] AS [Patient Birthdate]
                , '{0}'         AS [PI]
                , '{1}'         AS [PI Email]
                , '{2}'         AS [Recipient Address]
                , '{3}'         AS [Contact Email]
                , '{4}'         AS [PHI Description]
                , '{5}'         AS [PHI purpose]
                , GETDATE()     AS [Date Downloaded]
                FROM [WorkDBProd]..[PatientsAccessed] ;
        """.format(pi,piemail, piaddress, contemail,phidownloaded, phipurpose)
    # print (sqlstmt)
    df = dosqlread(sqlstmt, cnxdict )
    # FAILS HERE
    df.to_excel(writer, sheet_name='Sheet1', index=False)
    dowritersave(writer, cnxdict)

    # df = dosqlread('SELECT [PtBirthDate] FROM [WorkDBProd]..[PatientsAccessed]', cnxdict)
    # try:
    # except:
    filepath = 'File could not be created'
    # return filepath


def get_patient_list():
    # [caisiswork]
    # database = WorkDBProd
    # trusted_connection = yes
    # driver = {SQL
    # Server}
    # ini_section = caisiswork
    # server = CONGO - H\H
    # itemnum = 0
    # mysqldb = MYSQL - DB - PROD
    # mysqlschema = caisis
    # mysqluser = carole_shaw
    # mysqlpwd = 1
    # UglyBunnyHop % % %
    # out_filedir = \\cs.fhcrc.org\crtprojects\CRI\CaisisDataRequests\AML Downloads
    # out_filename = AMLShaw
    # out_fileext = xlsx
    #
    cnxdict       = connect_to_mysql_db_prod('caisis_to_mysql')
    filepath      = cnxdict['out_filedir'] + '\\' + \
                    cnxdict['out_filename'] + '_' + time.strftime('%Y%m%d') + '.' + \
                    cnxdict['out_fileext']
    mappedpath    = filepath.replace('\\\\cs.fhcrc.org\crtprojects\CRI\CaisisDataRequests\AML Downloads\\','g:\\')
    writer        = pd.ExcelWriter(mappedpath, datetime_format='mm/dd/yyyy')
    contemail     = 'cmshaw@fredhutch.org, gardnerk@seattlecca.org'
    phipurpose    = 'AML database'
    pi            = 'Elihu Estey'
    piemail       = 'eestey@seattlecca.org'
    piaddress     = 'Seattle Cancer Care Alliance; 825 Eastlake Ave E; Box G3200'
    phidownloaded = 'Name,DOB,' \
                    'Diagnoses and Dates,' \
                    'Procedures and Dates,' \
                    'Clinical Protocols and Dates,' \
                    'Pathology and Dates,' \
                    'Therapy and Dates,' \
                    'Lab Tests and Dates,' \
                    'Comorbidities and Dates,' \
                    'Encounters and Dates'

    cmd = """
        SELECT PtMRN      AS `Patient MRN`
            , PatientId   AS `Patient Id`
            , PtFirstName AS `Patinet Firstname`
            , PtLastName  AS `Patient Lastname`
            , PtBirthDate AS `Patient Birthdate`
            , '{0}'         AS `PI`
            , '{1}'         AS `PI Email`
            , '{2}'         AS `Recipient Address`
            , '{3}'         AS `Contact Email`
            , '{4}'         AS `PHI Description`
            , '{5}'         AS `PHI purpose`
            , curdate()     AS `Date Downloaded`
            FROM caisis.vdatasetpatients ;
    """.format(pi,piemail, piaddress, contemail,phidownloaded, phipurpose)
    try:
        df = dosqlread(cmd, cnxdict['cnx'])
        df.to_excel(writer, sheet_name='Sheet1', index=False)
    except:
        filepath = 'File could not be created'

    if filepath <> 'File could not be created':
        dowritersave(writer, cnxdict)
    return filepath


def build_mysql_caisis():
    """

    :return:
    """
    MsgResp = tkMessageBox.showinfo(title="Email Download Log"
                                    , message="Send Download Log to Hutch Data Commonwealth (HDC)?"
                                    , type="yesno")
    window.wm_withdraw()

    get_caisis_tables()

    # call_stored_procedure('index_tables')
    filepath = get_patient_list()

    if MsgResp == 'yes':
        addresslist = ['cmshaw@fhcrc.org','sgglick@fredhutch.org']
    else:
        addresslist = ['cmshaw@fhcrc.org']
    mail(addresslist,
         'Patients Downloaded Today',
         "Patients downloaded today are available on the shared drive at the following location:",
         filepath=filepath)


build_mysql_caisis()
# get_patient_list()
# cnxdict = read_db_config('caisisprod')
# engine = create_engine('mysql+mysqldb://{0}:{1}@MYSQL-DB-PRD/caisis'.format(cnxdict['mysqluser'], cnxdict['mysqlpwd']))
# cnxdict = connect_to_caisisprod(cnxdict)
# create_temp_population_table(cnxdict)
# create_temp_pathology_table(cnxdict)
# create_temp_pathtest_table(cnxdict)
# create_temp_uwcyto_table(cnxdict)