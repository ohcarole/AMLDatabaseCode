import Tkinter
import time
import tkMessageBox

from SQLServer_pyodbc import *
from SendNote import mail
from sqlalchemy import create_engine

window = Tkinter.Tk()
window.wm_withdraw()


# engine = create_engine('mysql+mysqldb://carole_shaw:1UglyBunnyHop%%%@MYSQL-DB-PRD/caisis')


def putinmysql(df, sql, tbl, engine):
    print(sql)
    df.to_sql(tbl.lower(), engine, chunksize=1000, if_exists='replace')
    try:
        df.to_sql(tbl.lower(), engine, chunksize=1000, if_exists='replace')
    except:
        print('/*\nSQL Failed\n {}\n*/'.format(sql))


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
    engine = create_engine('mysql+mysqldb://{0}:{1}@MYSQL-DB-PRD/caisis'.format(cnxdict['mysqluser'],cnxdict['mysqlpwd']))
    cnxdict = connect_to_caisisprod(cnxdict)

    tbllist = """
        AllKaryo
        Albumin
        Blast
        Creatinine
        Hematocrit
        Hemoglobin
        LabTestIndex
        Mutation
        Neutrophil
        Platelet
        RBC
        WBC
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
        vDatasetRadiationTherapy
        vDatasetStatus
    """
    tbllist = """
        Platelet
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
            tempsql = 'SELECT * FROM [WorkDBProd]..[{}]'.format(tbl)
            print ('-- Create dataframe from caisis table {0}'.format(tbl))
            df = dosqlread(tempsql, cnxdict)
            try:
                df['PathResult'].replace([u'\u2026'],  "...", regex=True, inplace=True)
            except:
                pass

            try:
                df['MedTxNotes'].replace([u'\u2013'],  "...", regex=True, inplace=True)
                df['MedTxNotes'].replace([u'\u2019'],  "...", regex=True, inplace=True)
                df['MedTxNotes'].replace([u'\u2022'],  "...", regex=True, inplace=True)
                df['MedTxNotes'].replace([u'\u2026'],  "...", regex=True, inplace=True)
            except: pass
            try:
                df['PathNotes'].replace([u'\u2019'],   "'",   regex=True, inplace=True)
            except: pass
            try:
                df['StatusNotes'].replace([u'\u2026'], "...", regex=True, inplace=True)
            except: pass

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
    df = dosqlread('SELECT [PtMRN], [PtLastName], [PtFirstName], [PatientId], [PtBirthDate] FROM [WorkDBProd]..[PatientsAccessed]', cnxdict)
    # df = dosqlread('SELECT [PtBirthDate] FROM [WorkDBProd]..[PatientsAccessed]', cnxdict)
    try:
        df.to_excel(writer, sheet_name='Sheet1',index=False)
        writer.save()
    except:
        filepath = 'File could not be created'
    return filepath


def build_mysql_caisis():
    """

    :return:
    """
    get_caisis_tables()
    # call_stored_procedure('index_tables')
    filepath = get_patient_list()

    MsgResp = tkMessageBox.showinfo(title="Email Download Log"
                                    , message="Send Download Log to Center IT?"
                                    , type="yesno")
    window.wm_withdraw()
    if MsgResp == 'yes':
        addresslist = ['cmshaw@fhcrc.org','sglick@fredhutch.org']
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