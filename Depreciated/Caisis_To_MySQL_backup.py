from sqlalchemy import create_engine

from SQLServer_pyodbc import *


# engine = create_engine('mysql+mysqldb://carole_shaw:1UglyBunnyHop%%%@MYSQL-DB-PRD/caisis')



def create_temp_population_table(cnxdict):
    tempsql = """
        SELECT TOP (10000) 999999 as [OrderId]
            , [PtMRN]
            , [vDatasetPatients].[PatientId]
            , [PtLastName]
            , [PtFirstName]
            , [PtMiddleName]
            , [PtBirthDate]
            , [PtBirthDateText]
            , [PtGender]
            , [PtDeathDate]
            , [PtDeathDateText]
            , [PtDeathType]
            , [PtDeathCause]
            , [CategoryId]
            , [Category]
          INTO #POPULATION
          FROM [dbo].[vDatasetPatients]
          LEFT JOIN [dbo].[vDatasetCategories] on [vDatasetPatients].[PatientId] = [vDatasetCategories].[PatientId]
          WHERE [Category] LIKE '%AML%'
          ORDER BY [vDatasetPatients].[PtMRN];
    """
    dosqlread(tempsql, cnxdict)
    tempsql = "CREATE INDEX PatientId on #POPULATION (PatientId);"
    dosqlread(tempsql, cnxdict)


def create_temp_pathology_table(cnxdict):
    tempsql = """
        SELECT b.[PtMRN], a.*
            INTO #AMLPATHOLOGY
            FROM [dbo].[vDatasetPathology] a
            LEFT JOIN #POPULATION b ON a.[PatientId] = b.[PatientId]
            WHERE a.[PathologyId] IS NOT NULL;
    """
    dosqlread(tempsql, cnxdict)
    tempsql = "CREATE INDEX [PathologyId] on #AMLPATHOLOGY ([PathologyId]);"
    dosqlread(tempsql, cnxdict)
    tempsql = "CREATE INDEX PatientId on #AMLPATHOLOGY (PatientId);"
    dosqlread(tempsql, cnxdict)


def create_temp_labtest_table(cnxdict):
    tempsql = """
        SELECT b.[PtMRN], a.*
            INTO #AMLLABTEST
            FROM [dbo].vDatasetLabTests a
            JOIN #POPULATION b ON a.[PatientId] = b.[PatientId]
            WHERE a.[LabTestId] IS NOT NULL AND YEAR(a.[LabDate])>2000 ;
    """
    dosqlread(tempsql, cnxdict)
    tempsql = "CREATE INDEX [LabTestId] on #AMLLABTEST ([LabTestId]);"
    dosqlread(tempsql, cnxdict)
    tempsql = "CREATE INDEX [PatientId] on #AMLLABTEST (PatientId);"
    dosqlread(tempsql, cnxdict)
    tempsql = "CREATE CLUSTERED INDEX [LabTest] on #AMLLABTEST (LabTest);"
    dosqlread(tempsql, cnxdict)


def create_temp_molecular(cnxdict):
    tempsql = """
        SELECT CASE
                WHEN CHARINDEX('NPM1 ',UPPER(a.LabTest),1)>0 THEN 'NPM1'
                WHEN CHARINDEX('CEBPA',UPPER(a.LabTest),1)>0 THEN 'CEPBA'
                WHEN CHARINDEX('FLT3 ',UPPER(a.LabTest),1)>0 THEN 'FLT3'
                ELSE ''
            END AS MutationType,
            b.[PtMRN], a.*
            INTO #AMLMOLECULAR
            FROM [dbo].[vDatasetLabTests] a
            JOIN #POPULATION b ON a.[PatientId] = b.[PatientId]
            WHERE b.[PatientId] IS NOT NULL
            AND ( (a.LabTest LIKE '%result%'
            AND   (a.LabTest LIKE '%npm1%'
            OR     a.LabTest LIKE '%cebpa%'))
            OR    ((a.LabTest LIKE 'flt3%' and a.LabTest LIKE '%itd%')
            OR     (a.LabTest LIKE 'flt3%' and a.LabTest LIKE '%tkd%')
            OR     (a.LabTest LIKE 'flt3%' and a.LabTest LIKE '%result%')
            OR     (a.LabTest LIKE 'flt3%' and a.LabTest LIKE '%length%')
            OR     (a.LabTest LIKE 'flt3%' and a.LabTest LIKE '%ratio%'))
            );
    """
    dosqlread(tempsql, cnxdict)
    tempsql = "CREATE INDEX [LabTestId] on #AMLMOLECULAR ([LabTestId]);"
    dosqlread(tempsql, cnxdict)
    tempsql = "CREATE INDEX [PatientId] on #AMLMOLECULAR (PatientId);"
    dosqlread(tempsql, cnxdict)
    tempsql = "CREATE CLUSTERED INDEX [LabTest] on #AMLMOLECULAR (LabTest);"
    dosqlread(tempsql, cnxdict)


def putinmysql(df, sql, tbl, engine):
    try:
        df.to_sql(tbl, engine, chunksize=1000, if_exists='replace')
    except:
        print(sql)


def get_patient(cnxdict,engine):
    tempsql = """
        SELECT * FROM #POPULATION;
    """
    df = dosqlread(tempsql, cnxdict)
    # df.to_sql('patient', engine, chunksize=1000, if_exists='replace')
    putinmysql(df, tempsql, 'patient', engine)


def get_encounter(cnxdict,engine):
    tempsql = """
        SELECT b.[PtMRN], a.* FROM [dbo].[vDatasetEncounters] a
            JOIN #POPULATION b ON a.[PatientId] = b.[PatientId];
    """
    df = dosqlread(tempsql, cnxdict)
    putinmysql(df, tempsql, 'encounter', engine)


def get_sccacyto(cnxdict,engine):
    tempsql = """
        SELECT c.[PatientId], a.*
            FROM [AML].[vCytoDiagnosis] a
            JOIN #POPULATION b ON a.[PtMRN] = b.[PtMRN];
    """
    df = dosqlread(tempsql, cnxdict)
    putinmysql(df, tempsql, 'sccacyto', engine)


def get_review_of_system(cnxdict,engine):
    tempsql = """
        SELECT a.[PtMRN], a.[PatientId], d.*
            FROM #POPULATION a
            JOIN [dbo].[vDatasetEncounters] c ON a.[PatientId] = c.[PatientId]
            JOIN [dbo].[vDatasetEncReviewOfSystems] d ON c.[EncounterId] = d.[EncounterId];
    """
    df = dosqlread(tempsql, cnxdict)
    putinmysql(df, tempsql, 'review_of_system', engine)
    # df.to_sql('encreviwofsystem',engine,chunksize=1000,if_exists='replace')


def get_molecular(cnxdict, engine):
    tempsql = """
        SELECT 'RESULT  ' AS MutationInfo, a.* from #AMLMOLECULAR a
            WHERE ((a.LabTest LIKE '%result%'
            AND    (a.LabTest LIKE '%npm1%' OR  a.LabTest LIKE '%cebpa%'))
            OR    ((a.LabTest LIKE 'flt3%'  AND a.LabTest LIKE '%itd%')
            OR     (a.LabTest LIKE 'flt3%'  AND a.LabTest LIKE '%tkd%')
            OR     (a.LabTest LIKE 'flt3%'  AND a.LabTest LIKE '%result%')))
            AND NOT (
                (a.Labtest LIKE '%length%' OR  a.LabResult LIKE '%bases%')
                OR
                (a.LabTest LIKE '%ratio%'))
        UNION SELECT 'BASES   ' AS MutationInfo, * FROM #AMLMOLECULAR a
            WHERE (a.Labtest LIKE '%length%'
            OR    a.LabResult LIKE '%bases%')
            AND   a.LabResult NOT IN ('Test not applicable','Duplicate request','Duplicate order')
        UNION SELECT 'RATIO   ' AS MutationInfo, a.* FROM #AMLMOLECULAR a
            WHERE a.labtest LIKE '%ratio%'
            AND   a.LabResult NOT IN ('Test not applicable','Duplicate request','Duplicate order')
        ORDER BY a.PtMRN, a.MutationType, a.LabAccessionNum;
    """
    df = dosqlread(tempsql, cnxdict)
    putinmysql(df, tempsql, 'molecular', engine)
    # df.to_sql('molecular', engine, chunksize=1000, if_exists='replace')
    index_labtests(cnxdict, engine, ('molecular'))


def get_labtests(cnxdict, engine,labtestlist):
    for currtbl in labtestlist:
        print(currtbl)
        tempsql = """
            SELECT b.[PtMRN], a.*
                        FROM #AMLLABTEST  AS  a
                        JOIN #POPULATION b ON a.[PatientId] = b.[PatientId]
                        WHERE a.[LabTestId] IS NOT NULL AND a.[LabTest] LIKE '%{0}%';
        """.format(currtbl)
        df = dosqlread(tempsql, cnxdict)
        print(currtbl)
        putinmysql(df, tempsql, '{0}'.format(currtbl), engine)
        # df.to_sql('{0}'.format(currtbl), engine, chunksize=1000, if_exists='replace')


def index_labtests(cnxdict, engine, labtestlist):
    """
    Move lab data read from caisis into MySQL and index
    """
    for currtbl in labtestlist:
        currtbl = currtbl.replace(' ', '_')
        mycnx = connect_to_mysql_db_prod('caisismysql')
        mycnx['sql'] = """
            ALTER TABLE `caisis`.`{0}`
                CHANGE COLUMN `LabTestId` `LabTestId` BIGINT(20) NOT NULL ,
                ADD PRIMARY KEY (`LabTestId`);
            ALTER TABLE `caisis`.`{0}`
                ADD INDEX `patientid` (`PatientId` ASC);
            ALTER TABLE `caisis`.`{0}`
                ADD INDEX `labdate` (`LabDate` ASC);
            ALTER TABLE `caisis`.`{0}`
                ADD FULLTEXT INDEX `labtest` (`LabTest` ASC);
        """.format(currtbl)
        dosqlexecute(mycnx)


def get_labtest_tables(cnxdict, engine):
    labtestlist = ('platelet count', 'neutrophil', 'wbc', 'albumin', 'unclassified cell', 'blast')
    get_labtests(cnxdict, engine,labtestlist)
    index_labtests(cnxdict, engine, labtestlist)


def get_procedure(cnxdict,engine):
    tempsql = """
        SELECT TOP (20000) b.[PtMRN], a.*
          FROM [CaisisProd].[dbo].[vDatasetProcedures] a
          JOIN #POPULATION b ON a.[PatientId] = b.[PatientId];
    """
    df = dosqlread(tempsql, cnxdict)
    putinmysql(df, tempsql, 'procedure', engine)
    # df.to_sql('procedure',engine,chunksize=1000,if_exists='replace')


def get_pathology(cnxdict,engine):
    tempsql = """
        SELECT * FROM #AMLPATHOLOGY;
    """
    # cnxdict['db'].set_character_set('utf8')
    # cnxdict['db'].execute('SET NAMES utf8;')
    # cnxdict['db'].execute('SET CHARACTER SET utf8;')
    # cnxdict['db'].execute('SET character_set_connection=utf8;')
    print('Get SQL Server pathology data')
    df = dosqlread(tempsql, cnxdict)
    df['PathNotes'].replace([u'\u2019'],"'",regex=True,inplace=True)
    print('Store SQL Server pathology data')
    putinmysql(df, tempsql, 'pathology', engine)
    # df.to_sql('pathology',engine,chunksize=1000,if_exists='replace')


def get_pathtest(cnxdict,engine):
    tempsql = """
        SELECT a.[PtMRN], a.[PatientId], b.*
            FROM #AMLPATHOLOGY a
            LEFT JOIN [dbo].[vDatasetPathTest] b ON a.[PathologyId] = b.[PathologyId]
            WHERE b.[PathTestId] IS NOT NULL;
	"""
    print('Get SQL Server pathology test data')
    df = dosqlread(tempsql, cnxdict)
    print('Store SQL Server pathology test data')
    putinmysql(df, tempsql, 'pathtest', engine)
    # df.to_sql('pathtest',engine,chunksize=1000,if_exists='replace')


def get_status(cnxdict,engine):
    tempsql = """
        SELECT [PtMRN], a.*
            FROM [dbo].[vDatasetStatus] a
            JOIN #POPULATION b ON a.[PatientId] = b.[PatientId];
    """
    df = dosqlread(tempsql, cnxdict)
    df['StatusNotes'].replace([u'\u2026'],"...",regex=True,inplace=True)
    putinmysql(df, tempsql, 'status', engine)
    # df.to_sql('status', engine, chunksize=1000, if_exists='replace')


def get_medicaltherapy(cnxdict, engine):
    tempsql = """
        SELECT b.[PtMRN], a.*
            FROM [dbo].[vDatasetMedicalTherapy] a
            JOIN #POPULATION b ON a.[PatientId] = b.[PatientId];
    """
    df = dosqlread(tempsql, cnxdict)
    df['MedTxNotes'].replace([u'\u2013'],"...",regex=True,inplace=True)
    df['MedTxNotes'].replace([u'\u2019'],"...",regex=True,inplace=True)
    df['MedTxNotes'].replace([u'\u2022'],"...",regex=True,inplace=True)
    df['MedTxNotes'].replace([u'\u2026'],"...",regex=True,inplace=True)
    putinmysql(df, tempsql, 'medicaltherapy', engine)


def get_medicaltherapy_administration_(cnxdict, engine):
    tempsql = """
        SELECT TOP (10) *
          FROM [CaisisProd].[dbo].[vDatasetMedTxAdministration];
    """
    df = dosqlread(tempsql, cnxdict)
    putinmysql(df, tempsql, 'medicaltherapyadmin', engine)


def get_medicaltherapy_administration(cnxdict, engine):
    tempsql = """
        SELECT c.PtMRN, c.PatientId, b.*
            FROM [dbo].[vDatasetMedicalTherapy] a
            JOIN [dbo].[vDatasetMedTxAdministration] b ON a.MedicalTherapyId = b.MedicalTherapyId
            JOIN #POPULATION c ON a.PatientId = c.Patientid;
    """
    df = dosqlread(tempsql, cnxdict)
    putinmysql(df, tempsql, 'medicaltherapyadmin', engine)


def build_mysql_caisis():
    cnxdict = read_db_config('caisisprod')
    engine = create_engine('mysql+mysqldb://{0}:{1}@MYSQL-DB-PRD/caisis'.format(cnxdict['mysqluser'],cnxdict['mysqlpwd']))
    cnxdict = connect_to_caisisprod(cnxdict)
    create_temp_population_table(cnxdict)
    create_temp_pathology_table(cnxdict)
    create_temp_labtest_table(cnxdict)
    create_temp_molecular(cnxdict)
    get_patient(cnxdict,engine)
    get_encounter(cnxdict,engine)
    get_review_of_system(cnxdict,engine)
    get_labtests(cnxdict,engine)
    get_procedure(cnxdict,engine)
    get_sccacyto(cnxdict,engine)
    get_pathology(cnxdict,engine)
    get_pathtest(cnxdict,engine)
    get_status(cnxdict,engine)
    get_medicaltherapy(cnxdict,engine)
    get_medicaltherapy_administration(cnxdict,engine)
    get_molecular(cnxdict,engine)
    cnxdict.close()


# cnxdict = read_db_config('caisisprod')
# engine = create_engine('mysql+mysqldb://{0}:{1}@MYSQL-DB-PRD/caisis'.format(cnxdict['mysqluser'], cnxdict['mysqlpwd']))
# cnxdict = connect_to_caisisprod(cnxdict)
build_mysql_caisis()


