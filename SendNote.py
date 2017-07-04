import smtplib,ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders




def send_note(pattxt=''):
    from_address = 'carolemarieshawjunk@gmail.com'
    to_address = 'carolemarieshawjunk@gmail.com'
    post_dictionary = MIMEMultipart('alternatief')
    post_dictionary['Subject'] = "Carole has downloaded Caisis data for the AML patients"
    post_dictionary['From'] = from_address
    post_dictionary['To'] = to_address
    body_text = """
    Data has been downloaded for all AML patients into the following tables:
        albumin
        blast
        encounter
        medicaltherapy
        medicaltherapyadmin
        molecular
        neutrophil
        pathology
        pathtest
        patient
        platelet_count
        procedure
        review_of_system
        sccacyto
        status
        unclassified_cell
        wbc
    """
    fp = open('c:\junk.txt', 'rb')
    body_text = body_text + fp.read()
    fp.close()
    body_text = body_text + pattxt
    print(body_text)
    post_dictionary.attach(MIMEText(body_text,'plain'))
    # post_dictionary.attach(pattxt)
    # post_dictionary.attach(detail)

    mail = smtplib.SMTP('smtp.gmail.com',587)
    # mail = smtplib.SMTP('140.107.42.11',80)
    # mail = smtplib.SMTP('140.107.42.11')
    # mail = smtplib.SMTP('140.107.42.11',587)
    # mail = smtplib.SMTP('smtp.fhcrc.com',587)
    smtplib.SMTP()
    mail.ehlo()
    mail.starttls()
    # mail.login("cmshaw@fredhutch.org",'Court!21')
    mail.login(from_address,'CMSJ-2015')
    # mail.sendmail("cmshaw@fredhutch.org",to_address,post_dictionary.as_string())
    mail.sendmail(from_address,to_address,post_dictionary.as_string())
    mail.close()


def send_mail(send_from='',send_to='',subject='',text='',files='',server='',port='',username='',password='',isTls=True):

    body_text = """
        Data has been downloaded for all AML patients into the following tables:
            albumin
            blast
            encounter
            medicaltherapy
            medicaltherapyadmin
            molecular
            neutrophil
            pathology
            pathtest
            patient
            platelet_count
            procedure
            review_of_system
            sccacyto
            status
            unclassified_cell
            wbc
        """

    msg = MIMEMultipart()
    msg['From'] = 'carolemarieshawjunk@gmail.com'
    msg['To'] = 'carolemarieshawjunk@gmail.com'
    msg['Date'] = formatdate(localtime = True)
    msg['Subject'] = subject
    msg.attach(MIMEText(body_text))

    part = MIMEBase('application', "octet-stream")
    part.set_payload(open("c:\users\cshaw\desktop\Output.xlsx", "rb").read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename="c:\users\cshaw\desktop\Output.xlsx"')
    msg.attach(part)

    #context = ssl.SSLContext(ssl.PROTOCOL_SSLv3)
    #SSL connection only working on Python 3+
    smtp = smtplib.SMTP(server, port)
    if isTls:
        smtp.starttls()
    smtp.login('carolemarieshawjunk@gmail.com','CMSJ-2015')
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.quit()


# send_mail()
# send_note()