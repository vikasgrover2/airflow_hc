import smtplib
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart

def email_job(module_name:str, client_name:str, recipients:str):
    p_module= module_name
    p_client_name = client_name
    p_recipients =recipients.split(',')
    
    msg = MIMEMultipart()
    msg['From'] = 'etlsupp@heliocampus.com'
    msg['Reply-to'] = 'etlsupp@heliocampus.com'
    msghdr= " <p> "+p_module+" ETL completed successfully ! </p>"
    msg['Subject'] = str(p_client_name+ " - "+p_module+" Airflow ETL completed successfully")        
    emaillist = [elem.strip().split(',') for elem in p_recipients]
    msg.preamble = 'Multipart massage.\n'
    complete_msg=MIMEText(msghdr,'html')
    msg.attach(complete_msg)
    server = smtplib.SMTP("smtp-relay.gmail.com:587")
    server.ehlo()
    server.starttls()
    server.sendmail(msg['From'], emaillist , msg.as_string())

if __name__=='__main__':
    email_job('TEST', 'Suffolk','vikas.grover@heliocampus.com' )