#!/usr/bin/python
import smtplib,ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import Encoders
import os

# account_dict = {'user': 'carolemarieshawjunk@gmail.com', 'pwd': '<a secret>'}
# account_dict = {'user': 'cmshaw@fredhutch.org', 'pwd': '<a secret>'}
# account_dict = {'user': 'cmshaw@fhcrc.org', 'pwd': '<a secret>'}
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
            mappedpath = filepath.replace('\\\\cs.fhcrc.org\crtprojects\CRI\CaisisDataRequests\AML Downloads\\', 'G:\\')
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



def mail_depreciated(to, subject, text, attach):
   msg = MIMEMultipart()

   msg['From'] = mail_user
   # msg['To'] = to
   msg['To'] = ", ".join(to)
   msg['Subject'] = subject

   msg.attach(MIMEText(text))

   part = MIMEBase('application', 'octet-stream')
   # Attach image, in test case an emoticon, to the email message
   part.set_payload(open(attach, 'rb').read())
   Encoders.encode_base64(part)
   part.add_header('Content-Disposition',
           'attachment; filename="%s"' % os.path.basename(attach))
   msg.attach(part)

   mailServer = smtplib.SMTP("smtp.fhcrc.org", 25) # successfully sent email using fhcrc account
   # mailServer = smtplib.SMTP("smtp.gmail.com", 587) # successfully sent email using gmail account
   # mailServer = smtplib.SMTP("smtp.fhcrc.org", 587) # failed using fhcrc email
   # mailServer = smtplib.SMTP("140.107.42.11", 587)  # failed using fhcrc email
   # mailServer = smtplib.SMTP("140.107.42.11", 80)   # failed using fhcrc email
   # mailServer = smtplib.SMTP("smtp.fhcrc.org", 80)  # failed using fhcrc email
   mailServer.ehlo()
   # mailServer.starttls()
   # mailServer.ehlo()
   # Don't need to login when I am already authenticated
   # mailServer.login(mail_user, mail_pwd)
   mailServer.sendmail(mail_user, to, msg.as_string())
   # Should be mailServer.quit(), but that crashes...
   mailServer.close()

# mail(['cvanderv@fredhutch.org','cmshaw@fhcrc.org'],
#      "Hello from python!",
#      "This is a email sent with python",
#      "french emoticon.jpg")