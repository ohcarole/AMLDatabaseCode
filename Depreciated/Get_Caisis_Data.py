from __future__ import print_function
import pyodbc
from Utilities.MessageBox import *
# from SendNote import mail
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import Encoders
import sys, os
reload(sys)
sys.setdefaultencoding('utf8')

account_dict = {'user': 'cmshaw@fhcrc.org', 'pwd': '<a secret>'}
mail_user = account_dict['user']
mail_pwd  = account_dict['pwd']

def mail(to, subject='', text='', attach=None, filepath=None, filedescription=None):
   """
   This email sender sends email from the logged in authenticated person
   :param to: target email address; required
   :param subject: subject of email
   :param text:  text for email body
   :param attach: complete path to file to attach to email
   :param filepath: complete path to file placed as a link in the email
   :param filedescription: text to use with link to file, defaults to filepath if no text given
   :return: retval: message about success or failure
   """

   if filedescription is None:
      filedescription = filepath

   retval = None
   if retval is None:
      try:
         msg = MIMEMultipart()
         msg['From'] = mail_user
         # msg['To'] = to
         msg['To'] = ", ".join(to)
         msg['Subject'] = subject
         if filepath is None:
            text = MIMEText(text)
         else:
            mappedpath = filepath
            text = MIMEText(text + u'<br><br><a href="{0}">{1}</a>'.format(filepath,filedescription) + '<br><br>Or the mapped directory:  '
                            u'<a href="{0}">{1}</a>'.format(mappedpath,mappedpath), 'html')
         msg.attach(text)
      except:
         retval = 'Failed to build message'

   if retval is None and attach is not None: # add attachment
      try:
         part = MIMEBase('application', 'octet-stream')
         part.set_payload(open(attach, 'rb').read())
         Encoders.encode_base64(part)
         part.add_header('Content-Disposition',
                 'attachment; filename="%s"' % os.path.basename(attach))
         msg.attach(part)
      except:
         retval = 'Failed to add attachment'

      # Don't need to login when I am already authenticated
   if retval is None:
      try:
         mailServer = smtplib.SMTP("smtp.fhcrc.org", 25)  # successfully sent email using fhcrc account
         mailServer.ehlo()
         mailServer.sendmail(mail_user, to, msg.as_string())
         # Should be mailServer.quit(), but that crashes...
         mailServer.close()
      except:
         retval = 'Failed to send message'

   if retval is None:
      retval = 'Mail sent'

   print (retval)
   return retval


def connect_to_caisisprod():

    cnxdict = {'database':     'WorkDBProd'
        ,'trusted_connection': 'yes'
        ,'driver':             '{SQL Server}'
        ,'server':             'CONGO-H\H'
        ,'filepath':            r'c:\temp\output.xlsx'
    }

    cnxdict['connection_string'] = """
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
        cnxdict['sql_connection'] = pyodbc.connect(cnxdict['connection_string'])
        print ('-- Connected to {}'.format(cnxdict['database']))
    except Exception as ErrVal:
        print ('-- Connection Failed')
        print (ErrVal)
    # con.autocommit = False
    return cnxdict


def get_patient_list():
    cnxdict = connect_to_caisisprod()
    filepath = cnxdict['filepath']
    writer = pd.ExcelWriter(filepath, datetime_format='mm/dd/yyyy')

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
    df = pd.read_sql(sqlstmt, cnxdict['sql_connection']) #
    try:
        df.to_excel(writer, sheet_name='PatientList', index=False)
    except:
        filepath = 'File could not be created'
    writer.save()
    writer.close()
    return filepath


def report_caisis_download():
    """

    :return:
    """
    MsgResp = tkMessageBox.showinfo(title="Email Download Log"
                                    , message="Send Download Log to Hutch Data Commonwealth (HDC)?"
                                    , type="yesno")
    window.wm_withdraw()

    filepath = get_patient_list()
    print (filepath)

    if MsgResp == 'yes':
        addresslist = ['cmshaw@fhcrc.org','sgglick@fredhutch.org']
    else:
        addresslist = ['cmshaw@fhcrc.org']
    mail(addresslist,
         'Patients Downloaded Today',
         "Patients downloaded today are available on the shared drive at the following location:",
         filepath=filepath)


report_caisis_download()