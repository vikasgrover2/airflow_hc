from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from query_renderer_cls import CustomSQLOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP
import smtplib
import os
import subprocess
import pendulum

AF_Home = os.environ['AIRFLOW_HOME']
add_attr_c_path = AF_Home+ "/sql/ADD_ATTR_C"
etl_cfg_path = AF_Home+ "/sql/etl.cfg"
hercules_home = "hercules_workday"
perseus_home = "perseus_workday"

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

def svn_job(option:str, client_name:str, recipients:str):
    svn_username = Variable.get("svn_user")
    svn_password = Variable.get("svn_password")
    if option =='-u':
        print("svn update "+AF_Home+"/sql --non-interactive -q --username "+svn_username+" --password password_hidden")
        subprocess.run(["svn","update",AF_Home+"/sql","--non-interactive","-q","--username",svn_username,"--password",svn_password])
        print("svn repo updated")

    elif option =='-r':
        result =[]
        svn_status= subprocess.Popen(("svn status -q "+AF_Home+"/sql --username "+svn_username+" --password "+svn_password), stdout=subprocess.PIPE, text = True)
        var_status = svn_status.stdout.read()
        var_status = list((str(var_status).split("\n")))
        result.extend(var_status)

        print(result)

        msg = MIMEMultipart()
        msghdr=  "<b>Uncommitted changes are found in these scripts : </b>"

        msg['From'] = 'etlsupp@heliocampus.com'
        msg['Reply-to'] = 'etlsupp@heliocampus.com'
        triggermail = False

        for change in result:
            if "M" in change:
                varFiles = change[str(change).find("D:"):]
                print("Uncommitted changes found in this file " +varFiles)
                triggermail = True
                msghdr = msghdr+"<p>"+varFiles+"</p>"

        if triggermail:    
            recipients = recipients
            msg['Subject'] = str(client_name) +": ATTENTION: Uncommit changes found on server"
            emaillist = [elem.strip().split(',') for elem in recipients]
            msg.preamble = 'Multipart massage.\n'
            complete_msg=MIMEText(msghdr,'html')
            msg.attach(complete_msg)
            server = smtplib.SMTP("smtp-relay.gmail.com:587")
            server.ehlo()
            server.starttls()
            server.sendmail(msg['From'], emaillist , msg.as_string())

default_args = {
    'start_date':datetime(2024, 1, 1, tzinfo=pendulum.timezone("US/Eastern")),
    'email':['etlsupp@heliocampus.com'],
    'email_on_failure':False
    }

svn_dag= DAG(
    dag_id=f"svn_update",
    schedule="0 18 * * *",
    max_active_runs=1,
    template_searchpath='/opt/airflow',
    catchup=False,
    default_args=default_args
)

svn_start = EmptyOperator(task_id="svn_job_start", dag=svn_dag)

step1 = EmptyOperator(task_id="step1", dag=svn_dag)
step1_svn_update = PythonOperator(task_id='svn_update',python_callable= svn_job, op_kwargs={"option":"-u","client_name":"Suffolk","recipients":"etlsupp@heliocampus.com"}, dag=svn_dag)
end_step1 = EmptyOperator(task_id="end_step1", dag=svn_dag)
svn_start>>step1>>[step1_svn_update]>>end_step1

svn_stop = EmptyOperator(task_id="svn_job_stop", dag=svn_dag)
end_step1 >> svn_stop