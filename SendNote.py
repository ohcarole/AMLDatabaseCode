import smtplib

from email.mime.multipart import MIMEMultipart

from email.mime.text import MIMEText

def send_note():
    from_address = 'carolemarieshawjunk@gmail.com'
    to_address = 'cmshaw@fhcrc.org'
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
    body_text = MIMEText(body_text, 'plain')
    post_dictionary.attach(body_text)
    mail = smtplib.SMTP('smtp.gmail.com',587)
    mail.ehlo()
    mail.starttls()
    mail.login(from_address,'CMSJ-2015')
    mail.sendmail(from_address,to_address,post_dictionary.as_string())
    mail.close()