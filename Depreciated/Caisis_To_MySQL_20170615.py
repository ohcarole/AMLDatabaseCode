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
    create_temporary_caisis_table(tempsql,cnxdict)
    tempsql = "ALTER TABLE #POPTEMP ADD OrderId INT IDENTITY(1,1) NOT NULL;"
    create_temporary_caisis_table(tempsql,cnxdict)
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
    create_temporary_caisis_table(tempsql,cnxdict)
    tempsql = "CREATE INDEX [PatientId] on #POPULATION ([PatientId]);"
    create_temporary_caisis_table(tempsql,cnxdict)


def create_temp_pathology_table(cnxdict):
    tempsql = """
        SELECT b.[PtMRN], a.*
                ,CASE
                    WHEN a.PathologyId IS NULL THEN NULL
                ELSE -1
                END AS HasPathTest
            INTO #AMLPATHOLOGY
            FROM [dbo].[vDatasetPathology] a
            JOIN WorkDBProd..vDatabasePatients b ON a.[PatientId] = b.[PatientId]
            WHERE a.[PathologyId] IS NOT NULL;
    """
    create_temporary_caisis_table(tempsql, cnxdict)


def update_index_caisis_pathology(cnxdict):
    tempsql = "ALTER TABLE #AMLPATHOLOGY ALTER COLUMN PathDate DATETIME ;"
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "UPDATE #AMLPATHOLOGY SET PathNotes = REPLACE(PathNotes, CHAR(9),' ');"
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "UPDATE #AMLPATHOLOGY SET PathNotes = REPLACE(PathNotes, CHAR(10),' ');"
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "UPDATE #AMLPATHOLOGY SET PathNotes = REPLACE(PathNotes, CHAR(13),' ');"
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = """
        UPDATE #AMLPATHOLOGY SET PathNotes = REPLACE(PathNotes, '"', CHAR(39));
    """
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = """
    UPDATE #AMLPATHOLOGY
        SET HasPathTest = (
            SELECT CASE
                WHEN #AMLPATHOLOGY.PathologyId    IS NULL THEN 0
                WHEN vDatasetPathTest.PathologyId IS NULL THEN 0
                WHEN vDatasetPathTest.PathologyId IS NOT NULL AND #AMLPATHOLOGY.PathNum         IS NOT NULL THEN 2
                WHEN vDatasetPathTest.PathologyId IS NOT NULL THEN 1
                ELSE 0
            END AS Result
            FROM dbo.vDatasetPathTest
            WHERE #AMLPATHOLOGY.PathologyId = vDatasetPathTest.PathologyId
            GROUP BY vDatasetPathTest.PathologyId);
    """
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "DELETE #AMLPATHOLOGY WHERE PathNum IS NULL AND HasPathTest IS NULL;"
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
            JOIN WorkDBProd..vDatabasePatients b ON a.[PatientId] = b.[PatientId]
            WHERE a.[LabTestId] IS NOT NULL AND YEAR(a.[LabDate])>2000 ;
    """
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "CREATE INDEX [LabTestId] on #AMLLABTEST ([LabTestId]);"
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "CREATE INDEX [PatientId] on #AMLLABTEST (PatientId);"
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "CREATE CLUSTERED INDEX [LabTest] on #AMLLABTEST (LabTest);"
    create_temporary_caisis_table(tempsql, cnxdict)


def create_temp_pathtest_table(cnxdict):

    # tempsql = """
    #     SELECT #AMLPATHOLOGY.[PatientId]
    #         ,#AMLPATHOLOGY.[PtMRN]
    #         ,#AMLPATHOLOGY.[PathologyId]
    #         ,#AMLPATHOLOGY.[PathDateText]
    #         ,#AMLPATHOLOGY.[PathDate]
    #         ,#AMLPATHOLOGY.[PathNum]
    #         ,#AMLPATHOLOGY.[PathNotes]
    #         ,#AMLPATHOLOGY.[HasPathTest]
    #         ,#AMLPATHOLOGY.[EnteredBy]
    #         , t.PathTestId
    #         , t.PathDate AS DateObtained
    #         , space(1) as DateObtainedText
    #         , t.PathTest
    #         , t.PathResult
    #         , t.PathUnits
    #     INTO #AMLPATHTEST
    #     FROM #AMLPATHOLOGY
    #     LEFT JOIN dbo.vDatasetPathTest t on #AMLPATHOLOGY.PathologyId = t.PathologyId
    #     WHERE t.PathResult IS NOT NULL;
    # """
    tempsql = """
        SELECT #AMLPATHOLOGY.[PatientId]
            ,#AMLPATHOLOGY.[PtMRN]
            ,#AMLPATHOLOGY.[PathologyId]
            ,#AMLPATHOLOGY.[PathDateText]
            ,#AMLPATHOLOGY.[PathDate]
            ,#AMLPATHOLOGY.[PathNum]
            ,#AMLPATHOLOGY.[PathNotes]
            ,#AMLPATHOLOGY.[HasPathTest]
            , t.PathTestId
            , t.PathDate AS DateObtained
            , space(1) as DateObtainedText
            , t.PathTime
            , t.PathTest
            , t.PathResult
            , t.PathUnits
            , t.PathNormalRange
            , t.PathNotes AS PathTestNotes
            , t.PathDataSource
            , t.PathQuality
            , t.EnteredBy
            , t.EnteredTime
            , t.UpdatedBy
            , t.UpdatedTime
            , t.PathMethod
        INTO #AMLPATHTEST
        FROM #AMLPATHOLOGY
        LEFT JOIN dbo.vDatasetPathTest t on #AMLPATHOLOGY.PathologyId = t.PathologyId
        WHERE t.PathResult IS NOT NULL
        ORDER BY t.PathTestId;
	"""
    create_temporary_caisis_table(tempsql, cnxdict)


def update_index_caisis_pathtest(cnxdict):
    tempsql = "CREATE INDEX [PathTestId] on #AMLPATHTEST ([PathTestId]);"
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "ALTER TABLE #AMLPATHTEST ADD OrderId INT IDENTITY(1,1) NOT NULL;"
    create_temporary_caisis_table(tempsql, cnxdict)

    tempsql = "UPDATE #AMLPATHTEST SET PathTestNotes = REPLACE(PathTestNotes, CHAR(9),' ');"
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "UPDATE #AMLPATHTEST SET PathTestNotes = REPLACE(PathTestNotes, CHAR(10),' ');"
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "UPDATE #AMLPATHTEST SET PathTestNotes = REPLACE(PathTestNotes, CHAR(13),' ');"
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = """
        UPDATE #AMLPATHOLOGY SET PathNotes = REPLACE(PathNotes, '"', CHAR(39));
    """
    create_temporary_caisis_table(tempsql, cnxdict)

    tempsql = "ALTER TABLE #AMLPATHTEST ALTER COLUMN DateObtained DATETIME ;"
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "CREATE INDEX [PathologyId] on #AMLPATHTEST ([PathologyId]);"
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "CREATE INDEX [PatientId] on #AMLPATHTEST (PatientId);"
    create_temporary_caisis_table(tempsql, cnxdict)


def create_temp_uwcyto_table(cnxdict):
    tempsql = """
        SELECT #AMLPATHOLOGY.*
            INTO #UWCYTO_1
            FROM #AMLPATHOLOGY
            LEFT JOIN WorkDBProd..vDatabasePatients on #AMLPATHOLOGY.Patientid = WorkDBProd..vDatabasePatients.PatientId
            WHERE NOT WorkDBProd..vDatabasePatients.PatientId IS NULL
            AND ( PathNum LIKE '%NE%'
            OR    PathNum LIKE '%NF%'
            OR    PathNum LIKE '%MN%' );
        """
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "CREATE INDEX [PathologyId] on #UWCYTO_1 ([PathologyId]);"
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "CREATE INDEX [PatientId] on #UWCYTO_1 (PatientId);"
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = """
        SELECT TOP (20000) a.[Patientid]
            , CHARINDEX('Date Obtained:',PathNotes,1) AS StartDateObtained
            , CHARINDEX('Age:',PathNotes,1) AS AgeStart
            , CHARINDEX('Sex:',PathNotes,1) AS SexStart
            , CHARINDEX('Report Status:',PathNotes,1) AS ReportStatusStart
            , CHARINDEX('Sample Type:',PathNotes,1) AS SampleTypeStart
            , CHARINDEX('ISCN Diagnosis:',PathNotes,1) as StartISCNDiagnosis
            , CHARINDEX('Summary:',PathNotes,1) as SummaryStart
            , CHARINDEX('Diagnosis and Comments:',PathNotes,1) as DiagnosisAndCommentsStart
            , CHARINDEX('Reported By:',PathNotes,1) as ReportedByStart
            , CHARINDEX('Note:',PathNotes,1) as NoteStart
            , CHARINDEX('Results of this IFISH study:',PathNotes,1) as ResultsStart
            , [PathologyId]
        INTO #NOTEINFO
        FROM #UWCYTO_1 a
            GROUP BY a.[Patientid]
                , [PathologyId]
                , [PathDate]
                , [PathDateText]
                , [PathNum]
                , [PathNotes]
            ORDER BY [Patientid]
    """
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "CREATE INDEX [PathologyId] on #NOTEINFO ([PathologyId]);"
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql="""
        SELECT TOP 20000
              CASE
                WHEN a.[PathNum] LIKE '%NE%' THEN 'CYTO'
                WHEN a.[PathNum] LIKE '%NF%' THEN 'FISH'
                WHEN a.[PathNum] LIKE '%MN%' THEN 'CGAT'
                ELSE ''
              END AS Type
              , a.PatientId
              , a.PtMRN
              , a.PathologyId
              , CASE
                WHEN PathNotes IS NULL OR StartDateObtained IS NULL OR StartDateObtained < 1 THEN ''
                WHEN StartDateObtained > 0 AND AgeStart          - (StartDateObtained + 14) > 0 THEN RTRIM(LTRIM(SUBSTRING(a.PathNotes,StartDateObtained + 14, AgeStart          - (StartDateObtained + 14))))
                WHEN StartDateObtained > 0 AND SexStart          - (StartDateObtained + 14) > 0 THEN RTRIM(LTRIM(SUBSTRING(a.PathNotes,StartDateObtained + 14, SexStart          - (StartDateObtained + 14))))
                WHEN StartDateObtained > 0 AND ReportStatusStart - (StartDateObtained + 14) > 0 THEN RTRIM(LTRIM(SUBSTRING(a.PathNotes,StartDateObtained + 14, ReportStatusStart - (StartDateObtained + 14))))
                WHEN StartDateObtained > 0 AND SampleTypeStart   - (StartDateObtained + 14) > 0 THEN RTRIM(LTRIM(SUBSTRING(a.PathNotes,StartDateObtained + 14, SampleTypeStart   - (StartDateObtained + 14))))
                ELSE ''
              END AS DateObtainedText
              , CONVERT(date,'1/1/1900',101) AS DateObtained
              , a.PathDate
              , a.PathDateText
              , CASE
                WHEN StartISCNDiagnosis <= 0 THEN 'FAILED TO IDENTIFY ICSN DIAGNOSIS'
                WHEN StartISCNDiagnosis > 0 AND SummaryStart              - (StartISCNDiagnosis + 15) > 0 THEN RTRIM(LTRIM(SUBSTRING(a.PathNotes,StartISCNDiagnosis+15, SummaryStart              - (StartISCNDiagnosis + 15))))
                WHEN StartISCNDiagnosis > 0 AND DiagnosisAndCommentsStart - (StartISCNDiagnosis + 15) > 0 THEN RTRIM(LTRIM(SUBSTRING(a.PathNotes,StartISCNDiagnosis+15, DiagnosisAndCommentsStart - (StartISCNDiagnosis + 15))))
                WHEN StartISCNDiagnosis > 0 AND ReportedByStart           - (StartISCNDiagnosis + 15) > 0 THEN RTRIM(LTRIM(SUBSTRING(a.PathNotes,StartISCNDiagnosis+15, ReportedByStart           - (StartISCNDiagnosis + 15))))
                WHEN StartISCNDiagnosis > 0 AND ResultsStart              - (StartISCNDiagnosis + 15) > 0 THEN RTRIM(LTRIM(SUBSTRING(a.PathNotes,StartISCNDiagnosis+15, ResultsStart              - (StartISCNDiagnosis + 15))))
                ELSE 'FAILED TO FIND END OF ICSN DIAGNOSIS'
              END AS Karyo
            , a.PathNum
            , a.PathNotes
            INTO #UWCYTO
            FROM #UWCYTO_1 a
            LEFT JOIN #NOTEINFO   ON a.PathologyId = #NOTEINFO.PathologyId
            LEFT JOIN WorkDBProd..vDatabasePatients ON a.PatientId = WorkDBProd..vDatabasePatients.[PatientId];
        """
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = """
            UPDATE #UWCYTO SET DateObtained = NULL WHERE DateObtainedText = '';
        """
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = """
            UPDATE #UWCYTO SET DateObtained = CONVERT(date,DateObtainedText,101)
                WHERE LTRIM(RTRIM(DateObtainedText)) LIKE '%201[0-7]'
                OR    RTRIM(DateObtainedText) LIKE '%200[0-9]';
	    """
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "CREATE INDEX [PathologyId] on #UWCYTO ([PathologyId]);"
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "CREATE INDEX [PatientId] on #UWCYTO (PatientId);"
    create_temporary_caisis_table(tempsql, cnxdict)


def create_temp_sccacyto_table(cnxdict):
    tempsql = """
        SELECT WorkDBProd..vDatabasePatients.PatientId, vCytoDiagnosis.*
            INTO #SCCACYTO
            FROM WorkDBProd..vDatabasePatients
            LEFT JOIN AML.vCytoDiagnosis on WorkDBProd..vDatabasePatients.PtMRN = vCytoDiagnosis.ptmrn
            WHERE CKARYO IS NOT NULL and CKARYO > '';
        """
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "CREATE INDEX [PatientId] on #SCCACYTO (PatientId);"
    create_temporary_caisis_table(tempsql, cnxdict)


def create_temp_abscyto_table(cnxdict):
    """
    Sometimes karyotype data is entered into caisis from outside or other sources
    :param cnxdict:
    :return:
    """
    tempsql="""
        SELECT #AMLPATHTEST.*
            INTO #ABSCYTO
            FROM #AMLPATHTEST
                LEFT JOIN #AMLPATHOLOGY ON #AMLPATHTEST.[PathologyId] = #AMLPATHOLOGY.[PathologyId]
            WHERE #AMLPATHOLOGY.HasPathTest > 0
            AND #AMLPATHTEST.[PathTest] LIKE '%karyotype%';
    """
    create_temporary_caisis_table(tempsql, cnxdict)
    # tempsql = """
    #     UPDATE #ABSCYTO SET PathResult = REPLACE(PathResult, CHAR(9),' ')
    #         WHERE  CHARINDEX(CHAR(9), PathResult,1)>0 ;
    # """
    # create_temporary_caisis_table(tempsql, cnxdict)
    # tempsql = """
    #     UPDATE #ABSCYTO SET PathResult = REPLACE(PathResult, CHAR(10),' ')
    #         WHERE  CHARINDEX(CHAR(10),PathResult,1)>0 ;
    # """
    # create_temporary_caisis_table(tempsql, cnxdict)
    # tempsql = """
    #     UPDATE #ABSCYTO SET PathResult = REPLACE(PathResult, CHAR(13),' ')
    #         WHERE  CHARINDEX(CHAR(13),PathResult,1)>0 ;
    # """
    # create_temporary_caisis_table(tempsql, cnxdict)
    # tempsql = """
    #     UPDATE #ABSCYTO SET PathResult = REPLACE(PathResult, '"', CHAR(39))
    #         WHERE  CHARINDEX('"',PathResult,1)>0 ;
    # """
    # create_temporary_caisis_table(tempsql, cnxdict)
    # tempsql = "CREATE INDEX [PathologyId] on #ABSCYTO ([PathologyId]);"
    # create_temporary_caisis_table(tempsql, cnxdict)
    # tempsql = "CREATE INDEX [PathTestId] on #ABSCYTO ([PathTestId]);"
    # create_temporary_caisis_table(tempsql, cnxdict)
    # tempsql = "CREATE INDEX [PatientId] on #ABSCYTO (PatientId);"
    # create_temporary_caisis_table(tempsql, cnxdict)


def create_temp_allcyto_table(cnxdict):
    #  Put together karyotype data from UW Path, SCCA in Gateway and Migrated AML data
    tempsql = """
        SELECT
                -1 as ALLKaryoId
                ,Type
                ,PatientId
                ,PtMRN
                ,PathologyId
                ,DateObtained
                ,PathDate
                ,Karyo
                ,PathNum
                ,PathNotes
                ,'' AS PathTestId
                ,'' AS PathTest
                ,'' AS PathResult
            INTO #ALLCYTO
            FROM #UWCYTO
        UNION
        SELECT
                -1 as ALLKaryoId
                ,'MIGRATE' AS Type
                ,PatientId
                ,PtMRN
                ,PathologyId
                ,DateObtained
                ,PathDate
                ,'' AS Karyo
                ,PathNum
                ,PathNotes
                ,PathTestId
                ,PathTest
                ,PathResult
            FROM #ABSCYTO
        UNION
        SELECT
                -1 as ALLKaryoId
                ,'SCCA' AS Type
                ,PatientId
                ,PtMRN
                ,-1 AS PathologyId
                ,SpecDate as DateObtained
                ,NULL as PathDate
                ,CKARYO as Karyo
                ,Accession as PathNum
                ,'' as PathNotes
                ,-1 as PathTestid
                ,'' as PathTest
                ,'' As PathResult
            FROM #SCCACYTO
            ORDER BY PatientId, DateObtained, PathDate;
	    """
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = """
        UPDATE #ALLCYTO SET PathNotes = ''  WHERE PathNotes IS NULL;
        """
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = """
        UPDATE #ALLCYTO SET PathNum = ''  WHERE PathNum IS NULL;
        """
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = """
        UPDATE #ALLCYTO SET PathTestId = -1  WHERE PathTestId = 0;
        """
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "CREATE INDEX [PathologyId] on #ALLCYTO ([PathologyId]);"
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "CREATE INDEX [PathTestId] on #ALLCYTO ([PathTestId]);"
    create_temporary_caisis_table(tempsql, cnxdict)
    tempsql = "CREATE INDEX [PatientId] on #ALLCYTO (PatientId);"
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
            JOIN WorkDBProd..vDatabasePatients b ON a.[PatientId] = b.[PatientId]
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
            JOIN WorkDBProd..vDatabasePatients b ON a.[PatientId] = b.[PatientId];
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
            JOIN WorkDBProd..vDatabasePatients b ON a.[PtMRN] = b.[PtMRN];
    """
    df = dosqlread(tempsql, cnxdict)
    putinmysql(df, tempsql, tbl, engine)
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
    putinmysql(df, tempsql, tbl, engine)
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
    putinmysql(df, tempsql, tbl, engine)
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
    putinmysql(df, tempsql, tbl, engine)
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


def get_review_of_system(cnxdict,engine):
    tbl = 'review_of_system'
    key = 'EncReviewOfSystemId'
    tempsql = """
        SELECT a.[PtMRN], a.[PatientId], d.*
            FROM WorkDBProd..vDatabasePatients a
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
                        JOIN WorkDBProd..vDatabasePatients b ON a.[PatientId] = b.[PatientId]
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
          JOIN WorkDBProd..vDatabasePatients b ON a.[PatientId] = b.[PatientId];
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
    print('-- Get SQL Server pathology data')
    df = dosqlread(tempsql, cnxdict)
    df['PathNotes'].replace([u'\u2019'],"'",regex=True,inplace=True)
    print('-- Store SQL Server pathology data')
    putinmysql(df, tempsql, tbl, engine)


def update_index_mysql_pathology(
    tbl='pathology',
    key = 'PathologyId'):
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
    # tempsql = """
    #         SELECT a.[PtMRN], a.[PatientId], b.*
    #             FROM #AMLPATHOLOGY a
    #             LEFT JOIN [dbo].[vDatasetPathTest] b ON a.[PathologyId] = b.[PathologyId]
    #             WHERE b.[PathTestId] IS NOT NULL;
    # 	"""
    for i in range(1,5):
        startrec = (i - 1) * 20000 + 1
        endrec   = i * 20000
        subtbl = tbl + '_' + str(i)
        tempsql = """
            SELECT * FROM #AMLPATHTEST WHERE OrderId BETWEEN {} AND {};
        """.format(startrec,endrec)
        print('-- Get SQL Server pathology test data')
        df = dosqlread(tempsql, cnxdict)
        # Add a test here to see if we need to keep going, if 0 records QUIT!  :)
        print('-- Store SQL Server {} data'.format(subtbl))
        putinmysql(df, tempsql, subtbl, engine)


def update_index_mysql_pathtest(tbl='pathtest',key='PathTestId'):
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
            JOIN WorkDBProd..vDatabasePatients b ON a.[PatientId] = b.[PatientId];
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
            JOIN WorkDBProd..vDatabasePatients b ON a.[PatientId] = b.[PatientId];
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
            JOIN WorkDBProd..vDatabasePatients c ON a.PatientId = c.Patientid;
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
    get_patient(cnxdict,engine)

    create_temp_pathology_table(cnxdict)
    update_index_caisis_pathology(cnxdict)
    get_pathology(cnxdict,engine)
    update_index_mysql_pathology()

    create_temp_pathtest_table(cnxdict)
    update_index_caisis_pathtest(cnxdict)
    get_pathtest(cnxdict,engine)
    # update_index_mysql_pathtest()

    create_temp_uwcyto_table(cnxdict)
    get_uwcyto(cnxdict, engine)

    create_temp_sccacyto_table(cnxdict)
    get_sccacyto(cnxdict, engine)

    create_temp_abscyto_table(cnxdict)
    get_abscyto(cnxdict, engine)

    # create_temp_allcyto_table(cnxdict)
    # get_allcyto(cnxdict, engine)
    #
    create_temp_labtest_table(cnxdict)
    create_temp_molecular(cnxdict)

    get_encounter(cnxdict,engine)
    get_review_of_system(cnxdict,engine)
    # get_labtest_tables(cnxdict,engine)
    get_procedure(cnxdict,engine)
    get_status(cnxdict,engine)
    get_medicaltherapy(cnxdict,engine)
    get_medicaltherapy_administration(cnxdict,engine)
    get_sccacyto(cnxdict,engine)
    get_pathtest(cnxdict,engine)
    get_molecular(cnxdict,engine)
    send_note()
    cnxdict.close()

#
# cnxdict = read_db_config('caisisprod')
# engine = create_engine('mysql+mysqldb://{0}:{1}@MYSQL-DB-PRD/caisis'.format(cnxdict['mysqluser'], cnxdict['mysqlpwd']))
# cnxdict = connect_to_caisisprod(cnxdict)

# This fails, and I DON'T KNOW WHY
# mycnx = connect_to_mysql_db_prod('caisismysql')
# mycnx['sql'] = """
#     INSERT INTO caisis.pathtest
#         SELECT `pathtest`.`index`+30000 as `index`,
#             `pathtest`.`PatientId`,
#             `pathtest`.`PtMRN`,
#             `pathtest`.`PathologyId`,
#             `pathtest`.`PathDateText`,
#             `pathtest`.`PathDate`,
#             `pathtest`.`PathNum`,
#             `pathtest`.`PathNotes`,
#             `pathtest`.`HasPathTest`,
#             `pathtest`.`PathTestId`,
#             `pathtest`.`DateObtained`,
#             `pathtest`.`DateObtainedText`,
#             `pathtest`.`PathTime`,
#             `pathtest`.`PathTest`,
#             `pathtest`.`PathResult`,
#             `pathtest`.`PathUnits`,
#             `pathtest`.`PathNormalRange`,
#             `pathtest`.`PathTestNotes`,
#             `pathtest`.`PathDataSource`,
#             `pathtest`.`PathQuality`,
#             `pathtest`.`EnteredBy`,
#             `pathtest`.`EnteredTime`,
#             `pathtest`.`UpdatedBy`,
#             `pathtest`.`UpdatedTime`,
#             `pathtest`.`PathMethod`
#         FROM `caisis`.`pathtest` LIMIT 10;
# """
# dosqlexecute(mycnx)

build_mysql_caisis()

# cnxdict = read_db_config('caisisprod')
# engine = create_engine('mysql+mysqldb://{0}:{1}@MYSQL-DB-PRD/caisis'.format(cnxdict['mysqluser'], cnxdict['mysqlpwd']))
# cnxdict = connect_to_caisisprod(cnxdict)
# create_temp_population_table(cnxdict)
# create_temp_pathology_table(cnxdict)
# create_temp_pathtest_table(cnxdict)
# create_temp_uwcyto_table(cnxdict)