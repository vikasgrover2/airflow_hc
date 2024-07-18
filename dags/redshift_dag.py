from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from airflow import DAG
import counts_job as cj
import pendulum

from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.operators.python import BranchPythonOperator
from query_renderer_cls import CustomSQLOperator
import os

AF_Home = os.environ['AIRFLOW_HOME']
add_attr_c_path = AF_Home+ "/sql/ADD_ATTR_C"
etl_cfg_path = AF_Home+ "/sql/ADD_ATTR_C/etl.cfg"
env = Variable.get("env")
counts_schema = "hercules"

if env=='QAT':
    hercules_home = "hercules_workday_qat"
    perseus_home = "perseus_workday_qat"
elif env=='Prod':
    hercules_home = "hercules_workday"
    perseus_home = "perseus_workday"
elif env=='Dev':
    hercules_home = "hercules_workday"
    perseus_home = "perseus_workday"
elif env in ('Test','Local'):
    hercules_home = "hercules_workday"
    perseus_home = "perseus_workday"

def insert_counts(module_name:str, schemaname:str, conn_id:str)-> None:
    cj.insert_counts(conn_id, schemaname, module_name)
    print("Inserted counts")

def get_counts(module_name:str, schemaname:str, conn_id:str)-> None:
    rows = cj.get_counts(conn_id, schemaname, module_name)
    print("Received counts")
    return rows

def email_job(module_name:str, client_name:str, recipients:str,conn_id:str, schemaname:str):
    module= module_name
    client = client_name
    recipients_ls =recipients.split(',')
    counter=0

    tabcount="<table border=\"0\" cellpadding=\"4\"><tr bgcolor=\"yellow\" align=\"left\"> <th>#</th><th>Table Name</th><th>Counts</th></tr><tbody>"
    cur= get_counts(module, schemaname,conn_id )

    for rec2 in cur:
        counter+=1
        tabcount+="<tr><td>" + str(counter) + "</td><td>" + str(rec2[0]) + "</td><td>" + str(rec2[1]) + "</td></tr>"        
    msg = MIMEMultipart()
    msg['From'] = 'etlsupp@heliocampus.com'
    msg['Reply-to'] = 'etlsupp@heliocampus.com'
    msghdr= " <p> "+module+" ETL completed successfully ! </p>"
    msg['Subject'] = str(client+ " - "+module+" Airflow ETL completed successfully")        
    emaillist = [elem.strip().split(',') for elem in recipients_ls]
    msg.preamble = 'Multipart massage.\n'
    complete_msg=MIMEText(msghdr+tabcount,'html')
    print(complete_msg)
    msg.attach(complete_msg)
    server = smtplib.SMTP("smtp-relay.gmail.com:587")
    server.ehlo()
    server.starttls()
    server.sendmail(msg['From'], emaillist , msg.as_string())

default_args = {
    'start_date':datetime(2024, 1, 1, tzinfo=pendulum.timezone("US/Eastern")),
    'email':['etlsupp@heliocampus.com'],
    'email_on_failure':True
    }

def verify_job_on_env():
    env = Variable.get("env")

    if env == 'prod':
        return 'verify_replication_sensor'
    else:
        return 'skip_verify_ods'

etl_dag= DAG(
    dag_id=f"etl_workday",
    schedule="0 9 * * *",
    max_active_runs=1,
    template_searchpath='/opt/airflow',
    catchup=False,
    default_args=default_args
)

def _success_criteria(record):
    print("Process successfully reached the desired status")
    return record


def _failure_criteria(record):
    print("Process failed to proceed")
    return True if not record else False

verify_branch_op = BranchPythonOperator(
        task_id='branch_verify_task',
        python_callable=verify_job_on_env
    )
skip_verify_ods = EmptyOperator(task_id="skip_verify_ods", dag=etl_dag)

verify_replication_sensor = SqlSensor(
    task_id='verify_replication_sensor',
    sql="sql/VERIFY_JOB.sql",
    conn_id=Variable.get("conn_etl"),
    success=_success_criteria,
    failure=_failure_criteria,
    params={"flag_status_column":"load_type","schema_name":"workday_ods","log_table_name":"cdc_log",
                "flag_time_column":"load_timestamp","sysdate_offset":"0","flag_column_name":"local_table_name",
                "module_name":"ALL TABLES"}, 
    poke_interval=60,
    timeout=60*60*2,
    dag=etl_dag
)

verify_branch_op >> [verify_replication_sensor, skip_verify_ods]

etl_start = EmptyOperator(task_id="etl_start", dag=etl_dag, trigger_rule='one_success')

step1 = EmptyOperator(task_id="step1", dag=etl_dag)

step1_dim_address =         CustomSQLOperator(task_id ='step1_dim_address',conn_id =Variable.get("conn_etl")  ,sql_file ='sql/HERCULES_WORKDAY.DIM_ADDRESS.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
step1_dim_address_cascade = CustomSQLOperator(task_id ='step1_dim_address_cascade',conn_id =Variable.get("conn_etl")  ,sql_file ='sql/HERCULES_WORKDAY.DIM_ADDRESS_CASCADE.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
step1_dim_career =          CustomSQLOperator(task_id ='step1_dim_career',conn_id =Variable.get("conn_etl")  ,sql_file ='sql/HERCULES_WORKDAY.DIM_CAREER.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
step1_dim_car_prgrm_plan =  CustomSQLOperator(task_id ='step1_dim_car_prgrm_plan',conn_id =Variable.get("conn_etl")           ,sql_file ='sql/HERCULES_WORKDAY.DIM_CAR_PRGRM_PLAN.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
step1_dim_class =           CustomSQLOperator(task_id ='step1_dim_class',conn_id =Variable.get("conn_etl")   ,sql_file ='sql/HERCULES_WORKDAY.DIM_CLASS.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)  
end_step1 = EmptyOperator(task_id="end_step1", dag=etl_dag)

verify_replication_sensor >> etl_start
skip_verify_ods >> etl_start

etl_start>>step1

step1>>[step1_dim_address, step1_dim_address_cascade, step1_dim_career, step1_dim_car_prgrm_plan,step1_dim_class] >>end_step1

step2 = EmptyOperator(task_id="step2", dag=etl_dag)
step2_dim_course =      CustomSQLOperator(task_id ='step2_dim_course',conn_id =Variable.get("conn_etl")  ,sql_file ='sql/HERCULES_WORKDAY.DIM_COURSE.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
step2_dim_degree =      CustomSQLOperator(task_id ='step2_dim_degree',conn_id =Variable.get("conn_etl")  ,sql_file ='sql/HERCULES_WORKDAY.DIM_DEGREE.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
step2_dim_finaid_type = CustomSQLOperator(task_id ='step2_dim_finaid_type',conn_id =Variable.get("conn_etl")  ,sql_file ='sql/HERCULES_WORKDAY.DIM_FINAID_TYPE.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
#step2_dim_isir =        CustomSQLOperator(task_id ='step2_dim_isir',conn_id =Variable.get("conn_etl")  ,sql_file ='sql/HERCULES_WORKDAY.DIM_ISIR.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
step2_dim_person =      CustomSQLOperator(task_id ='step2_dim_person',conn_id =Variable.get("conn_etl")  ,sql_file ='sql/HERCULES_WORKDAY.DIM_PERSON.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
end_step2 = EmptyOperator(task_id="end_step2", dag=etl_dag)
end_step1 >> step2
step2>>[step2_dim_course,step2_dim_degree,step2_dim_finaid_type,step2_dim_person]>>end_step2

step3 = EmptyOperator(task_id="step3", dag=etl_dag)
step3_dim_person_phone_email = CustomSQLOperator(task_id ='step3_dim_person_phone_email',conn_id =Variable.get("conn_etl")   ,sql_file ='sql/HERCULES_WORKDAY.DIM_PERSON_PHONE_EMAIL.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
step3_dim_plan =               CustomSQLOperator(task_id ='step3_dim_plan',conn_id =Variable.get("conn_etl")  ,sql_file ='sql/HERCULES_WORKDAY.DIM_PLAN.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
step3_dim_program =             CustomSQLOperator(task_id ='step3_dim_program',conn_id =Variable.get("conn_etl")  ,sql_file ='sql/HERCULES_WORKDAY.DIM_PROGRAM.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
step3_dim_term =                CustomSQLOperator(task_id ='step3_dim_term',conn_id =Variable.get("conn_etl")    ,sql_file ='sql/HERCULES_WORKDAY.DIM_TERM.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
end_step3 = EmptyOperator(task_id="end_step3", dag=etl_dag)
end_step2 >> step3
step3>>[step3_dim_person_phone_email,step3_dim_plan,step3_dim_program,step3_dim_term]>>end_step3

step4a = EmptyOperator(task_id="step4a", dag=etl_dag)
step4a_fact_enrollment = CustomSQLOperator(task_id ='step4a_fact_enrollment',conn_id =Variable.get("conn_etl") ,sql_file ='sql/HERCULES_WORKDAY.FACT_ENROLLMENT.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
step4a_ff_student_term = CustomSQLOperator(task_id ='step4a_ff_student_term',conn_id =Variable.get("conn_etl")         ,sql_file ='sql/HERCULES_WORKDAY.FF_STUDENT_TERM.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
end_step4a = EmptyOperator(task_id="end_step4a", dag=etl_dag)
end_step3 >> step4a
step4a>>[step4a_ff_student_term,step4a_fact_enrollment]>>end_step4a

step4b = EmptyOperator(task_id="step4b", dag=etl_dag)
step4b_fact_aos =        CustomSQLOperator(task_id ='step4b_fact_aos',conn_id =Variable.get("conn_etl")        ,sql_file ='sql/HERCULES_WORKDAY.FACT_AOS.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
step4b_ff_applications = CustomSQLOperator(task_id ='step4b_ff_applications',conn_id =Variable.get("conn_etl")                ,sql_file ='sql/HERCULES_WORKDAY.FF_APPLICATIONS.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
end_step4b = EmptyOperator(task_id="end_step4b", dag=etl_dag)
end_step4a >> step4b
step4b>>[step4b_fact_aos,step4b_ff_applications]>>end_step4b

step5a = EmptyOperator(task_id="step5a", dag=etl_dag)
step5a_fact_degree =     CustomSQLOperator(task_id ='step5a_fact_degree',conn_id =Variable.get("conn_etl")     ,sql_file ='sql/HERCULES_WORKDAY.FACT_DEGREE.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
step5a_ff_applications_prog      = CustomSQLOperator(task_id ='step5a_ff_applications_prog',conn_id =Variable.get("conn_etl")    ,sql_file ='sql/HERCULES_WORKDAY.FF_APPLICATIONS_PROG.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
step5a_fact_finaid =     CustomSQLOperator(task_id ='step5a_fact_finaid',conn_id =Variable.get("conn_etl")     ,sql_file ='sql/HERCULES_WORKDAY.FACT_FINAID.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
end_step5a = EmptyOperator(task_id="end_step5a", dag=etl_dag)
end_step4b >> step5a
step5a>>[step5a_ff_applications_prog,step5a_fact_degree,step5a_fact_finaid]>>end_step5a

step5b = EmptyOperator(task_id="step5b", dag=etl_dag)
step5b_ff_retention_grad_prog    = CustomSQLOperator(task_id ='step5b_ff_retention_grad_prog',conn_id =Variable.get("conn_etl")  ,sql_file ='sql/HERCULES_WORKDAY.FF_RETENTION_GRAD_PROG.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
step5b_ff_student_term_trends_re = CustomSQLOperator(task_id ='step5b_ff_student_term_trends_re',conn_id =Variable.get("conn_etl") ,sql_file ='sql/HERCULES_WORKDAY.FF_STUDENT_TERM_TRENDS_RE.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
end_step5b = EmptyOperator(task_id="end_step5b", dag=etl_dag)
step5a >> step5b
step5b>>[step5b_ff_retention_grad_prog,step5b_ff_student_term_trends_re]>>end_step5b

step5e = PythonOperator(task_id='counts_hercules',python_callable= insert_counts,op_kwargs={"module_name":"Workday Hercules","schemaname": counts_schema,"conn_id":Variable.get("conn_etl")}, dag=etl_dag)
step5f = PythonOperator(task_id='send_email_hercules',python_callable= email_job,op_kwargs={"module_name":"Workday Hercules","client_name":"Suffolk","recipients":"etlsupp@heliocampus.com","conn_id":Variable.get("conn_etl"), "schemaname": counts_schema}, dag=etl_dag)
end_step5b>>step5e>> step5f

step6 = EmptyOperator(task_id="step6", dag=etl_dag)
step6_cs_admission     = CustomSQLOperator(task_id ='step6_cs_admission',conn_id =Variable.get("conn_bidev")    ,sql_file ='sql/PERSEUS_WORKDAY.CS_ADMISSION.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
step6_cs_degrees       = CustomSQLOperator(task_id ='step6_cs_degrees',conn_id =Variable.get("conn_bidev")  ,sql_file ='sql/PERSEUS_WORKDAY.CS_DEGREES.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
step6_cs_enrollments   = CustomSQLOperator(task_id ='step6_cs_enrollments',conn_id =Variable.get("conn_bidev")         ,sql_file ='sql/PERSEUS_WORKDAY.CS_ENROLLMENTS.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
step6_cs_financial_aid = CustomSQLOperator(task_id ='step6_cs_financial_aid',conn_id =Variable.get("conn_bidev") ,sql_file ='sql/PERSEUS_WORKDAY.CS_FINANCIAL_AID.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
end_step6 = EmptyOperator(task_id="end_step6", dag=etl_dag)
step5f >> step6
step6>>[step6_cs_admission,step6_cs_degrees,step6_cs_enrollments,step6_cs_financial_aid]>>end_step6

step7 = EmptyOperator(task_id="step7", dag=etl_dag)
step7_cs_financial_aid_term = CustomSQLOperator(task_id ='step7_cs_financial_aid_term',conn_id =Variable.get("conn_bidev") ,sql_file ='sql/PERSEUS_WORKDAY.CS_FINANCIAL_AID_TERM.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
step7_cs_persistence        = CustomSQLOperator(task_id ='step7_cs_persistence',conn_id =Variable.get("conn_bidev")       ,sql_file ='sql/PERSEUS_WORKDAY.CS_PERSISTENCE.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
step7_cs_retention          = CustomSQLOperator(task_id ='step7_cs_retention',conn_id =Variable.get("conn_bidev")         ,sql_file ='sql/PERSEUS_WORKDAY.CS_RETENTION.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
step7_cs_retention_rel      = CustomSQLOperator(task_id ='step7_cs_retention_rel',conn_id =Variable.get("conn_bidev")     ,sql_file ='sql/PERSEUS_WORKDAY.CS_RETENTION_REL.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
end_step7 = EmptyOperator(task_id="end_step7", dag=etl_dag)
end_step6 >> step7
step7>>[step7_cs_financial_aid_term,step7_cs_persistence,step7_cs_retention,step7_cs_retention_rel]>>end_step7

step8 = EmptyOperator(task_id="step8", dag=etl_dag)
step8_colleague_slate_admissions_extract = CustomSQLOperator(task_id ='step8_colleague_slate_admissions_extract',conn_id =Variable.get("conn_bidev") ,sql_file ='sql/PERSEUS_WORKDAY.COLLEAGUE_SLATE_ADMISSIONS_EXTRACT.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
step8_student_term_extract  = CustomSQLOperator(task_id ='step8_student_term_extract',conn_id =Variable.get("conn_bidev")  ,sql_file ='sql/PERSEUS_WORKDAY.STUDENT_TERM_EXTRACT.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
end_step8 = EmptyOperator(task_id="end_step8", dag=etl_dag)
end_step7 >> step8
step8>>[step8_colleague_slate_admissions_extract,step8_student_term_extract]>>end_step8

step9 = EmptyOperator(task_id="step9", dag=etl_dag)
step9_completions_extract                = CustomSQLOperator(task_id ='step9_completions_extract',conn_id =Variable.get("conn_bidev")        ,sql_file ='sql/PERSEUS_WORKDAY.COMPLETIONS_EXTRACT.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
step9_financial_aid_extract = CustomSQLOperator(task_id ='step9_financial_aid_extract',conn_id =Variable.get("conn_bidev") ,sql_file ='sql/PERSEUS_WORKDAY.FINANCIAL_AID_EXTRACT.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
end_step9 = EmptyOperator(task_id="end_step9", dag=etl_dag)
end_step8 >> step9
step9>>[step9_completions_extract,step9_financial_aid_extract]>>end_step9

step10 = EmptyOperator(task_id="step10", dag=etl_dag)
step10_retention_extract     = CustomSQLOperator(task_id ='step10_retention_extract',conn_id =Variable.get("conn_bidev")     ,sql_file ='sql/PERSEUS_WORKDAY.RETENTION_EXTRACT.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
step10_course_registration_extract        = CustomSQLOperator(task_id ='step10_course_registration_extract',conn_id =Variable.get("conn_bidev")          ,sql_file ='sql/PERSEUS_WORKDAY.COURSE_REGISTRATION_EXTRACT.sql',split_statements = True, parameters= {"add_attr_c_path":add_attr_c_path, "etl_cfg_path":etl_cfg_path, "hercules_home":hercules_home,"perseus_home":perseus_home},dag=etl_dag)
end_step10 = EmptyOperator(task_id="end_step10", dag=etl_dag)
end_step9 >> step10
step10>>[step10_retention_extract,step10_course_registration_extract]>>end_step10

step10e = PythonOperator(task_id='counts_perseus',python_callable= insert_counts,op_kwargs={"module_name":"Workday Perseus","schemaname": counts_schema,"conn_id":Variable.get("conn_etl")}, dag=etl_dag)
step10f = PythonOperator(task_id='send_email_perseus',python_callable= email_job,op_kwargs={"module_name":"Workday Perseus","client_name":"Suffolk","recipients":"etlsupp@heliocampus.com","conn_id":Variable.get("conn_etl"), "schemaname": counts_schema}, dag=etl_dag)

end_step10>>step10e>> step10f

etl_stop = EmptyOperator(task_id="etl_stop", dag=etl_dag)
step10f >> etl_stop