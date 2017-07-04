from Connection import *
import pyodbc

import pandas as pd
from MySQLdbUtils import *
import pandas.io.sql as sql


def connect_to_caisisprod(cnxdict):
	con = ''
	constring = """
		DRIVER={0};
		SERVER={1};
		DATABASE={2};
		TDS_Version=8.0;
		unicode_results=True;
		CHARSET=UTF8;
		TRUSTED_CONNECTION=yes
	""".format(
				cnxdict['driver'],
				cnxdict['server'],
				cnxdict['database']
	)

	try:
		print ('-- About to Connect')
		con = pyodbc.connect(constring)
		print ('-- Connected to {}'.format(cnxdict['database']))
	except Exception as ErrVal:
		print ('-- Connection Failed')
		print (ErrVal)
	# con.autocommit = False
	return con


# def create_temporary_caisis_table_(cmd, cnxdict):
# 	cnxdict.autocommit = True
# 	crs = cnxdict.cursor()
# 	try:
# 		crs.execute(cmd)
# 		print('Success creating temporary table')
# 	except:
# 		print('Error creating temporary table')


def create_temporary_caisis_table(cmd,cnxdict):
	crs = cnxdict.cursor()
	try:
		crs.execute(cmd)
		print(cmd)
	except:
		print('Error\n'+cmd)
	crs.close()


def test1_config_connect():
	print(pyodbc.drivers())
	cnxdict = read_db_config('caisisprod')
	cnxdict = connect_to_caisisprod(cnxdict)
	cnxdict.close()


def test2_get_table():
	try:
		tempsql = """
			SELECT TOP (10) a.[PatientId]
				, a.[PtMRN]
				, [PtLastName]
				, [PtFirstName]
				, [PtMiddleName]
				, [PtBirthDate]
				, [PtBirthDateText]
				, [PtDeathDate]
				, [PtDeathDateText]
				, [PtDeathType]
				, [PtDeathCause]
				, [CategoryId]
				, [Category]
			FROM [dbo].[vDatasetPatients] a
				LEFT JOIN [dbo].[vDatasetCategories] c on a.[PatientId] = c.[PatientId]
			WHERE c.[Category] LIKE '%AML%';
		"""
		cnxdict = read_db_config('caisisprod')
		cnxdict = connect_to_caisisprod(cnxdict)
		df = pd.read_sql(tempsql,cnxdict)
		print(df)
	except:
		print('-- Failed:'+'test2_get_table()')


def test3_temporary_table():
	try:
		cnxdict = read_db_config('caisisprod')
		con = connect_to_caisisprod(cnxdict)
		con.autocommit = True
		crs = con.cursor()
		cmd = """
			SELECT TOP (10000) 999999 as [OrderId]
				, [PtMRN]
				, [vDatasetPatients].[PatientId]
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
				WHERE [Category] LIKE '%AML%';
		"""
		try:
			crs.execute(cmd)
			print('-- Success creating temporary table')
		except pyodbc.ProgrammingError:
			print('-- Error creating temporary table')
		print("---")
		for row in crs.fetchall():
			for index, field in enumerate(row):
				title = ("Field #%u" % (index))
				sys.stdout.write("%s = %s | " % (title, field))
		crs.close()
		del crs
		crs = con.cursor()
	except:
		print('-- Failed:' + 'test3_temporary_table()')


def test4_twoqueries():
	cnxdict = read_db_config('caisisprod')
	con = connect_to_caisisprod(cnxdict)
	con.autocommit = False
	crs = con.cursor()

	cmd = """
		SELECT TOP (10000) 999999 as [OrderId]
			, [PtMRN]
			, [vDatasetPatients].[PatientId]
			, [PtMRN] AS [PtMRN_]
			, UPPER([PtLastName])   AS [PtLastName]
			, UPPER([PtFirstName])  AS [PtFirstName]
			, UPPER([PtMiddleName]) AS [PtMiddleName]
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
			ORDER BY PtMRN;
	"""
	try:
		crs.execute(cmd)
		print('Success querying first table')
	except pyodbc.ProgrammingError:
		print('Error querying first table')
	print("---")

	cmd = """
		SELECT TOP (10) * FROM #POPULATION;
	"""
	try:
		crs.execute(cmd)
		print('Success querying second table')
	except pyodbc.ProgrammingError:
		print('Error querying second table')
	print("---")
	for row in crs.fetchall():
		print(row)
	con.commit()
	con.close()


def test5_get_table():
	tempsql = """
        SELECT TOP (10000) [vDatasetPatients].[PtMRN]
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
	cnxdict = read_db_config('caisisprod')
	cnxdict = connect_to_caisisprod(cnxdict)
	df = pd.read_sql(tempsql,cnxdict)
	print(df)


def test6_temporary_table():
	try:
		cnxdict = read_db_config('caisisprod')
		con = connect_to_caisisprod(cnxdict)
		con.autocommit = True
		crs = con.cursor()
		cmd = """
			SELECT TOP (10000) [vDatasetPatients].[PtMRN]
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
		try:
			crs.execute(cmd)
			print('Success creating temporary table')
		except pyodbc.ProgrammingError:
			print('Error creating temporary table')
		print("---")
		crs.close()
		del crs
	except:
		print('Failed:' + 'test3_temporary_table()')


def test7_temporary_table():
	cnxdict = read_db_config('caisisprod')
	con = connect_to_caisisprod(cnxdict)
	cmd = """
		SELECT TOP (10000) [vDatasetPatients].[PtMRN]
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
		  INTO #POPTEMP2
		  FROM [dbo].[vDatasetPatients]
		  LEFT JOIN [dbo].[vDatasetCategories]
			ON [vDatasetPatients].[PatientId] = [vDatasetCategories].[PatientId]
		  WHERE [vDatasetCategories].[Category] LIKE '%AML%'
		  ORDER BY [vDatasetPatients].[PtMRN];
	"""
	create_temporary_caisis_table(cmd, con)

# test1_config_connect()
# test2_get_table()
# test3_temporary_table()
# test4_twoqueries()
# test5_get_table()
# test6_temporary_table()
# test7_temporary_table()