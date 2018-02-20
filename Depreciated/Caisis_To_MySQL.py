from sqlalchemy import create_engine

from Examples.SendNote import *
from Utilities.SQLServer_pyodbc import *


# engine = create_engine('mysql+mysqldb://carole_shaw:1UglyBunnyHop%%%@MYSQL-DB-PRD/caisis')


def create_temp_population_table(cnxdict):
    tempsql = """
        SELECT [vDatasetPatients].[PtMRN]
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
          INTO #POPTEMP
          FROM [dbo].[vDatasetPatients]
          LEFT JOIN [dbo].[vDatasetCategories]
            ON [vDatasetPatients].[PatientId] = [vDatasetCategories].[PatientId]
          WHERE [vDatasetCategories].[Category] LIKE '%AML%'
          ORDER BY [vDatasetPatients].[PtMRN];
    """
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "CREATE INDEX PatientId on #POPTEMP (PatientId);"
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "ALTER TABLE #POPTEMP ADD OrderId INT IDENTITY(1,1) NOT NULL;"
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = """
        SELECT [OrderId]
            , [PtMRN]
            , [PatientId]
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
          FROM #POPTEMP
          ORDER BY [OrderId];
    """
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "CREATE INDEX [PatientId] on #POPULATION ([PatientId]);"
    create_temporary_caisis_table(tempsql, cnxdict)


def create_temp_pathology_table(cnxdict):
    tempsql = """
        SELECT b.[PtMRN], a.*
            INTO #AMLPATHOLOGY
            FROM [dbo].[vDatasetPathology] a
            JOIN #POPULATION b ON a.[PatientId] = b.[PatientId]
            WHERE a.[PathologyId] IS NOT NULL;
    """
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "CREATE INDEX [PathologyId] on #AMLPATHOLOGY ([PathologyId]);"
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "CREATE INDEX [PatientId] on #AMLPATHOLOGY (PatientId);"
    create_temporary_caisis_table(tempsql, cnxdict)


def create_temp_labtest_table(cnxdict):
    tempsql = """
        SELECT b.[PtMRN], a.*
            INTO #AMLLABTEST
            FROM [dbo].vDatasetLabTests a
            JOIN #POPULATION b ON a.[PatientId] = b.[PatientId]
            WHERE a.[LabTestId] IS NOT NULL AND YEAR(a.[LabDate])>2000 ;
    """
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "CREATE INDEX [LabTestId] on #AMLLABTEST ([LabTestId]);"
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "CREATE INDEX [PatientId] on #AMLLABTEST (PatientId);"
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "CREATE CLUSTERED INDEX [LabTest] on #AMLLABTEST (LabTest);"
    create_temporary_caisis_table(tempsql, cnxdict)


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
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "CREATE INDEX [LabTestId] on #AMLMOLECULAR ([LabTestId]);"
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "CREATE INDEX [PatientId] on #AMLMOLECULAR (PatientId);"
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "CREATE CLUSTERED INDEX [LabTest] on #AMLMOLECULAR (LabTest);"
    create_temporary_caisis_table(tempsql, cnxdict)


def putinmysql(df, sql, tbl, engine):
    try:
        df.to_sql(tbl, engine, chunksize=1000, if_exists='replace')
    except:
        print(sql)


def get_patient(cnxdict,engine):
    tbl = 'patient'
    key = 'PatientId'
    tempsql = """
        SELECT * FROM #POPULATION;
    """
    df = dosqlread(tempsql, cnxdict)
    putinmysql(df, tempsql, tbl, engine)
    indexcmd = """
        ALTER TABLE `caisis`.`{0}`
            CHANGE COLUMN `{1}` `{1}` BIGINT(20) NOT NULL ,
            ADD PRIMARY KEY (`{1}`);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `PtMRN` (`PtMRN`(8) ASC);
    """.format(tbl,key)
    index_mysql(indexcmd)


def get_encounter(cnxdict,engine):
    tbl = 'encounter'
    key = 'EncounterId'
    tempsql = """
        SELECT b.[PtMRN], a.* FROM [dbo].[vDatasetEncounters] a
            JOIN #POPULATION b ON a.[PatientId] = b.[PatientId];
    """
    df = dosqlread(tempsql, cnxdict)
    putinmysql(df, tempsql, tbl, engine)
    indexcmd = """
        ALTER TABLE `caisis`.`{0}`
            CHANGE COLUMN `{1}` `{1}` BIGINT(20) NOT NULL ,
            ADD PRIMARY KEY (`{1}`);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `PatientId` (`PatientId`(8) ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `PtMRN` (`PtMRN`(8) ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `EncDate` (`EncDate` ASC);
    """.format(tbl,key)
    index_mysql(indexcmd)


def get_sccacyto(cnxdict,engine):
    tbl = 'sccacyto'
    key = 'None'
    tempsql = """
        SELECT b.[PatientId], a.*
            FROM [AML].[vCytoDiagnosis] a
            JOIN #POPULATION b ON a.[PtMRN] = b.[PtMRN];
    """
    df = dosqlread(tempsql, cnxdict)
    putinmysql(df, tempsql, tbl, engine)
    indexcmd = """
        ALTER TABLE `caisis`.`{0}`
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


def get_review_of_system(cnxdict,engine):
    tbl = 'review_of_system'
    key = 'EncReviewOfSystemId'
    tempsql = """
        SELECT a.[PtMRN], a.[PatientId], d.*
            FROM #POPULATION a
            JOIN [dbo].[vDatasetEncounters] c ON a.[PatientId] = c.[PatientId]
            JOIN [dbo].[vDatasetEncReviewOfSystems] d ON c.[EncounterId] = d.[EncounterId];    """
    df = dosqlread(tempsql, cnxdict)
    putinmysql(df, tempsql, tbl, engine)
    indexcmd = """
        ALTER TABLE `caisis`.`{0}`
            CHANGE COLUMN `{1}` `{1}` BIGINT(20) NOT NULL ,
            ADD PRIMARY KEY (`{1}`);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `EncounterId` (`EncounterId`(8) ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `PatientId` (`PatientId`(8) ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `PtMRN` (`PtMRN`(8) ASC);
    """.format(tbl,key)
    index_mysql(indexcmd)


def get_molecular(cnxdict, engine):
    tbl = 'molecular'
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
    putinmysql(df, tempsql, tbl, engine)
    index_labtests(cnxdict, engine, (tbl))
    indexcmd = """
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `MutationInfo` (`MutationInfo`(6) ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `MutationType` (`MutationType`(6) ASC);
    """.format(tbl)
    index_mysql(indexcmd)


def get_labtests(cnxdict, engine,labtestlist):
    for tbl in labtestlist:
        tbl = tbl.replace(' ', '_')
        print(tbl)
        tempsql = """
            SELECT b.[PtMRN], a.*
                        FROM #AMLLABTEST  AS  a
                        JOIN #POPULATION b ON a.[PatientId] = b.[PatientId]
                        WHERE a.[LabTestId] IS NOT NULL AND a.[LabTest] LIKE '%{0}%';
        """.format(tbl)
        df = dosqlread(tempsql, cnxdict)
        putinmysql(df, tempsql, tbl, engine)


def index_mysql(indexcmd):
    mycnx = connect_to_mysql_db_prod('caisismysql')
    mycnx['sql'] = indexcmd
    dosqlexecute(mycnx)


def index_labtests_(cnxdict, engine, labtestlist):
    """
    Move lab data read from caisis into MySQL and index
    """
    for tbl in labtestlist:
        tbl = tbl.replace(' ', '_')
        indexcmd = """
            ALTER TABLE `caisis`.`{0}`
                CHANGE COLUMN `LabTestId` `LabTestId` BIGINT(20) NOT NULL ,
                ADD PRIMARY KEY (`LabTestId`);
        """.format(tbl)
        index_mysql(indexcmd)


def index_labtests(cnxdict, engine, labtestlist):
    """
    Move lab data read from caisis into MySQL and index
    """
    for tbl in labtestlist:
        tbl = tbl.replace(' ', '_')
        indexcmd = """
            ALTER TABLE `caisis`.`{0}`
                CHANGE COLUMN `LabTestId` `LabTestId` BIGINT(20) NOT NULL ,
                ADD PRIMARY KEY (`LabTestId`);
            ALTER TABLE `caisis`.`{0}`
                ADD INDEX `patientid` (`PatientId` ASC);
            ALTER TABLE `caisis`.`{0}`
                ADD INDEX `labdate` (`LabDate` ASC);
            ALTER TABLE `caisis`.`{0}`
                ADD FULLTEXT INDEX `labtest` (`LabTest` ASC);
            ALTER TABLE `caisis`.`{0}`
                ADD INDEX `PtMRN` (`PtMRN`(8) ASC);
        """.format(tbl)
        index_mysql(indexcmd)


def get_labtest_tables(cnxdict, engine):
    labtestlist = ('platelet count', 'neutrophil', 'wbc', 'albumin', 'unclassified cell', 'blast')
    # get_labtests(cnxdict, engine,labtestlist)
    index_labtests(cnxdict, engine,labtestlist)


def get_procedure(cnxdict,engine):
    tbl = 'procedure'
    key = 'ProcedureId'
    tempsql = """
        SELECT TOP (20000) b.[PtMRN], a.*
          FROM [CaisisProd].[dbo].[vDatasetProcedures] a
          JOIN #POPULATION b ON a.[PatientId] = b.[PatientId];
    """
    df = dosqlread(tempsql, cnxdict)
    putinmysql(df, tempsql, tbl, engine)
    indexcmd = """
        ALTER TABLE `caisis`.`{0}`
            CHANGE COLUMN `{1}` `{1}` BIGINT(20) NOT NULL ,
            ADD PRIMARY KEY (`{1}`);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `PatientId` (`PatientId`(8) ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `PtMRN` (`PtMRN`(8) ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `ProcDate` (`ProcDate` ASC);
    """.format(tbl,key)
    index_mysql(indexcmd)


def get_pathology(cnxdict,engine):
    tbl = 'pathology'
    key = 'PathologyId'
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
    putinmysql(df, tempsql, tbl, engine)
    indexcmd = """
        ALTER TABLE `caisis`.`{0}`
            CHANGE COLUMN `{1}` `{1}` BIGINT(20) NOT NULL ,
            ADD PRIMARY KEY (`{1}`);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `PatientId` (`PatientId`(8) ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `PtMRN` (`PtMRN`(8) ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `PathDate` (`PathDate` ASC);
    """.format(tbl,key)
    index_mysql(indexcmd)


def get_pathtest(cnxdict,engine):
    tbl='pathtest'
    key='PathTestId'
    tempsql = """
        SELECT a.[PtMRN], a.[PatientId], b.*
            FROM #AMLPATHOLOGY a
            LEFT JOIN [dbo].[vDatasetPathTest] b ON a.[PathologyId] = b.[PathologyId]
            WHERE b.[PathTestId] IS NOT NULL;
	"""
    print('Get SQL Server pathology test data')
    df = dosqlread(tempsql, cnxdict)
    print('Store SQL Server pathology test data')
    putinmysql(df, tempsql, tbl, engine)
    indexcmd = """
        ALTER TABLE `caisis`.`{0}`
            CHANGE COLUMN `{1}` `{1}` BIGINT(20) NOT NULL ,
            ADD PRIMARY KEY (`{1}`);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `PatientId` (`PatientId`(8) ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `PtMRN` (`PtMRN`(8) ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `PathDate` (`PathDate` ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD FULLTEXT INDEX `PathTest` (`PathTest` ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD FULLTEXT INDEX `PathResult` (`PathResult` ASC);
        """.format(tbl, key)
    index_mysql(indexcmd)


def get_status(cnxdict,engine):
    tbl='status'
    key='StatusId'
    tempsql = """
        SELECT [PtMRN], a.*
            FROM [dbo].[vDatasetStatus] a
            JOIN #POPULATION b ON a.[PatientId] = b.[PatientId];
    """
    df = dosqlread(tempsql, cnxdict)
    df['StatusNotes'].replace([u'\u2026'],"...",regex=True,inplace=True)
    putinmysql(df, tempsql, tbl, engine)
    indexcmd = """
        ALTER TABLE `caisis`.`{0}`
            CHANGE COLUMN `{1}` `{1}` BIGINT(20) NOT NULL ,
            ADD PRIMARY KEY (`{1}`);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `PatientId` (`PatientId`(8) ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `PtMRN` (`PtMRN`(8) ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `StatusDate` (`StatusDate` ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD FULLTEXT INDEX `Status` (`Status` ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD FULLTEXT INDEX `PathResult` (`PathResult` ASC);
        """.format(tbl, key)
    index_mysql(indexcmd)


def get_medicaltherapy(cnxdict, engine):
    tbl='medicaltherapy'
    key='MedicalTherapyId'
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
    putinmysql(df, tempsql, tbl, engine)
    indexcmd = """
        ALTER TABLE `caisis`.`{0}`
            CHANGE COLUMN `{1}` `{1}` BIGINT(20) NOT NULL ,
            ADD PRIMARY KEY (`{1}`);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `PatientId` (`PatientId`(8) ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `PtMRN` (`PtMRN`(8) ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `MedTxDate` (`MedTxDate` ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD FULLTEXT INDEX `MedTxAgent` (`MedTxAgent` ASC);
        """.format(tbl, key)
    index_mysql(indexcmd)


def get_medicaltherapy_administration(cnxdict, engine):
    tbl='medicaltherapyadmin'
    key='MedTxAdministrationId'
    tempsql = """
        SELECT c.PtMRN, c.PatientId, b.*
            FROM [dbo].[vDatasetMedicalTherapy] a
            JOIN [dbo].[vDatasetMedTxAdministration] b ON a.MedicalTherapyId = b.MedicalTherapyId
            JOIN #POPULATION c ON a.PatientId = c.Patientid;
    """
    df = dosqlread(tempsql, cnxdict)
    putinmysql(df, tempsql, tbl, engine)
    indexcmd = """
        ALTER TABLE `caisis`.`{0}`
            CHANGE COLUMN `{1}` `{1}` BIGINT(20) NOT NULL ,
            ADD PRIMARY KEY (`{1}`);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `PatientId` (`PatientId`(8) ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `PtMRN` (`PtMRN`(8) ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD INDEX `MedTxAdminStartDate` (`MedTxAdminStartDate` ASC);
        ALTER TABLE `caisis`.`{0}`
            ADD FULLTEXT INDEX `MedTxAdminAgent` (`MedTxAdminAgent` ASC);
        """.format(tbl, key)
    index_mysql(indexcmd)


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
    get_labtest_tables(cnxdict,engine)
    get_procedure(cnxdict,engine)
    get_status(cnxdict,engine)
    get_medicaltherapy(cnxdict,engine)
    get_medicaltherapy_administration(cnxdict,engine)
    get_sccacyto(cnxdict,engine)
    get_pathology(cnxdict,engine)
    get_pathtest(cnxdict,engine)
    get_molecular(cnxdict,engine)
    send_note()
    cnxdict.close()


# cnxdict = read_db_config('caisisprod')
# engine = create_engine('mysql+mysqldb://{0}:{1}@MYSQL-DB-PRD/caisis'.format(cnxdict['mysqluser'], cnxdict['mysqlpwd']))
# cnxdict = connect_to_caisisprod(cnxdict)
build_mysql_caisis()


