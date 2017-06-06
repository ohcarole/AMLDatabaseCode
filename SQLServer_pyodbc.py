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
		print ('About to Connect')
		con = pyodbc.connect(constring)
		print ('Connected to {}'.format(cnxdict['database']))
	except Exception as ErrVal:
		print ('Connection Failed')
		print (ErrVal)
	con.autocommit = False
	return con


def test1_config_connect():
	print(pyodbc.drivers())
	cnxdict = read_db_config('caisisprod')
	cnxdict = connect_to_caisisprod(cnxdict)
	cnxdict.close()


def test2_get_table():
	tempsql = """
		SELECT TOP (10) a.[PatientId]
			, [PtMRN]
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
	

def test3_temporary_table():
	cnxdict = read_db_config('caisisprod')
	con = connect_to_caisisprod(cnxdict)
	con.autocommit = True
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
		SELECT TOP (10) * FROM #POPULATION;
	"""
	try:
		crs.execute(cmd)
		print('Success creating temporary table')
	except pyodbc.ProgrammingError:
		print('Error creating temporary table')
	print("---")
	for row in crs.fetchall():
		for index, field in enumerate(row):
			title = ("Field #%u" % (index))
			sys.stdout.write("%s = %s | " % (title, field))    
	crs.close()
	del crs
	crs = con.cursor()
	

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


# test1_config_connect()
# test2_get_table()
# test3_temporary_table()
# test4_twoqueries()