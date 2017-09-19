import time
from MessageBox import *

from MySQLdbUtils import *
from SQLServerUtils import *
from Connection import *
from SQLServer_pyodbc import *
from SendNote import mail
from sqlalchemy import create_engine

# engine = create_engine('mysql+mysqldb://carole_shaw:1UglyBunnyHop%%%@MYSQL-DB-PRD/caisis')


def putinmysql(df, sql, tbl, engine):
    print(sql)
    # df.to_sql(tbl.lower(), engine, chunksize=1000, if_exists='replace')
    try:
        df.to_sql(tbl.lower(), engine, chunksize=1000, if_exists='replace')
    except:
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

        PatientsAccessed
        NewPatientsAccessed

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
        vDatasetPatientsNoNames
        vDatasetProcedures
        vDatasetRadiationTherapy
        vDatasetStatus
    """

    # vDatasetCategories

    tbllist = """
        HotSpot
    """

    print('-- Moving Caisis tables to MySQL')
    for tbl in tbllist.split('\n'):
        tbl = tbl.strip().replace(' ', '_').lower()
        if tbl == 'vdatasetlabtests':
            print('-- The table {} is not small enough for transfer to caisis.'.format(tbl))
        elif tbl == '':
            pass
        else:
            tbl = tbl.strip().replace(' ', '_').lower()
            tempsql = 'SELECT * FROM [WorkDBProd]..[{}] '.format(tbl)
            # tempsql = """
            # SELECT b.RowNum
            #         , a.[PatientId]
            #         , [PtMRN]
            #         , [PathologyId]
            #         , a.PathTestId
            #         , [PathDateText]
            #         , [PathDate]
            #         , [PathNum]
            #         , [PathNotes]
            #         , DateObtained
            #         , DateObtainedText
            #         , PathTime
            #         , PathTest
            #         , PathResult
            #         , PathUnits
            #         , PathNormalRange
            #         , PathTestNotes
            #         , PathDataSource
            #         , PathQuality
            #         , EnteredBy
            #         , EnteredTime
            #         , UpdatedBy
            #         , UpdatedTime
            #         , PathMethod
            #         , PathKaryotype
            #     FROM [WorkDBProd]..[{}] a
            #         LEFT JOIN (
            #             SELECT ROW_NUMBER() OVER(ORDER BY PathTestId ASC) AS RowNum, PathTestId
            #                 FROM [WorkDBProd]..[vDatasetPathTest] ) b
            #         ON a.PathTestId = b.PathTestId ;
            # """.format(tbl)
            print ('-- Create dataframe from caisis table {0}'.format(tbl))
            df = dosqlread(tempsql, cnxdict)
            # read_clean_sql(cmd, cnx)
            # BEGIN Candidate for refactoring and also for a called procedure
            """

            Some of the tables we are importing from SQL server may contain characters in the latin dataset that
            cause movement to utf8 in MySQL to fail.  I have added some corrections of the data below for specific
            fields where that has been an issue.

            This could be refactored at some point to check all characters in all fields in every table for
            latin to utf8 issues.  For now I am just catching the most common character discrepancies in the
            fields where they occur.
            """
            # try:
            #     df = cleandataframe(df,'PathResult')
            # except: pass
            #
            # try:
            #     df = cleandataframe(df,'PathKaryotype')
            # except: pass
            #
            # try:
            #     df = cleandataframe(df,'MedTxNotes')
            # except: pass
            #
            # try:
            #     df = cleandataframe(df,'PathNotes')
            # except: pass
            #
            # try:
            #     df = cleandataframe(df,'StatusNotes')
            # except: pass

            # END Candidate for refactoring and also for a called procedure

            print ('-- Dataframe complete, copy dataframe to MySQL table Caisis.{0}'.format(tbl))
            putinmysql(df, tempsql, tbl, engine)

    print('-- Done moving Caisis tables to MySQL')


def get_patient_list():
    cnxdict = read_db_config('caisiswork')
    filepath = cnxdict['out_filedir'] + '\\' + \
        cnxdict['out_filename'] + '_' + time.strftime('%Y%m%d') + '.' + \
        cnxdict['out_fileext']
    mappedpath = filepath.replace('\\\\cs.fhcrc.org\crtprojects\CRI\CaisisDataRequests\AML Downloads\\','g:\\')
    print (filepath)
    print (mappedpath)
    cnxdict = connect_to_caisisprod(cnxdict)
    writer = pd.ExcelWriter(mappedpath)

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

    # df = dosqlread('SELECT [PtBirthDate] FROM [WorkDBProd]..[PatientsAccessed]', cnxdict)
    try:
        df.to_excel(writer, sheet_name='Sheet1',index=False)
        dowritersave(writer, cnxdict)
    except:
        filepath = 'File could not be created'
    return filepath


def build_mysql_caisis():
    """

    :return:
    """
    MsgResp = tkMessageBox.showinfo(title="Email Download Log"
                                    , message="Send Download Log to Hutch Data Commonwealth (HDC)?"
                                    , type="yesno")
    window.wm_withdraw()

    # get_caisis_tables()
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

# cnxdict = read_db_config('caisisprod')
# engine = create_engine('mysql+mysqldb://{0}:{1}@MYSQL-DB-PRD/caisis'.format(cnxdict['mysqluser'], cnxdict['mysqlpwd']))
# cnxdict = connect_to_caisisprod(cnxdict)
# create_temp_population_table(cnxdict)
# create_temp_pathology_table(cnxdict)
# create_temp_pathtest_table(cnxdict)
# create_temp_uwcyto_table(cnxdict)