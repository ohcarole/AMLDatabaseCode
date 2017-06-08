from GeneralUtils import *
import MySQLdbUtils
import sys

"""
    Sample to share on stack overflow
"""

myconfig = {'host': 'MyData', 'password': 'secret_password', 'user': 'private_user', 'database': 'test'}

cnx = MySQLConnection(**myconfig) # connect_to_database(cnxdict['myconfig'])
crs = cnx.cursor()

print ('Doing First Query')
try:
    crs.execute('SELECT PtId FROM `patientdata` GROUP BY PtID ORDER BY PtId DESC LIMIT 3;')
    for r, row in enumerate(crs.fetchall()):
        for c, col in enumerate(row):
            print (col)
    print
except:
    print 'First Select Fails'

print ('Doing Procedure Call')
try:
    crs.callproc('CreatePatientAge') # creates the table patientage
    # DO IN NEED TO DO SOMETHING HERE TO CLEAN UP THE CURSOR OR CONNECTION OBJECTS?? #
    print
except:
    print 'Procedure Call Fails'

print ('Doing Second Select')
try:
    crs.execute('SELECT * FROM `patientage`;')
    cnx.commit()
    print
except:
    print 'Second Select Fails'
    sys.exc_clear()