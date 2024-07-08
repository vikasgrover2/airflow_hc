from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from airflow import DAG
import pendulum

from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python import PythonOperator

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

default_args = {
    'start_date':datetime(2024, 1, 1, tzinfo=pendulum.timezone("US/Eastern")),
    'email':['etlsupp@heliocampus.com'],
    'email_on_failure':True
    }

etl_dag= DAG(
    dag_id=f"etl_workday",
    schedule="0 6 * * *",
    max_active_runs=1,
    template_searchpath='/opt/airflow',
    catchup=False,
    default_args=default_args
)

etl_start = EmptyOperator(task_id="etl_start", dag=etl_dag)
step1 = EmptyOperator(task_id="step1", dag=etl_dag)

step1_dim_address =         SQLExecuteQueryOperator(task_id ='step1_dim_address',conn_id =Variable.get("conn_etl")  ,sql ='sql/HERCULES_WORKDAY.DIM_ADDRESS.sql',split_statements = True,dag=etl_dag)
step1_dim_address_cascade = SQLExecuteQueryOperator(task_id ='step1_dim_address_cascade',conn_id =Variable.get("conn_etl")  ,sql ='sql/HERCULES_WORKDAY.DIM_ADDRESS_CASCADE.sql',split_statements = True,dag=etl_dag)
step1_dim_career =          SQLExecuteQueryOperator(task_id ='step1_dim_career',conn_id =Variable.get("conn_etl")  ,sql ='sql/HERCULES_WORKDAY.DIM_CAREER.sql',split_statements = True,dag=etl_dag)
step1_dim_car_prgrm_plan =  SQLExecuteQueryOperator(task_id ='step1_dim_car_prgrm_plan',conn_id =Variable.get("conn_etl")           ,sql ='sql/HERCULES_WORKDAY.DIM_CAR_PRGRM_PLAN.sql',split_statements = True,dag=etl_dag)
step1_dim_class =           SQLExecuteQueryOperator(task_id ='step1_dim_class',conn_id =Variable.get("conn_etl")   ,sql ='sql/HERCULES_WORKDAY.DIM_CLASS.sql',split_statements = True,dag=etl_dag)  
end_step1 = EmptyOperator(task_id="end_step1", dag=etl_dag)
etl_start >> step1
step1>>[step1_dim_address, step1_dim_address_cascade, step1_dim_career, step1_dim_car_prgrm_plan,step1_dim_class] >>end_step1

step2 = EmptyOperator(task_id="step2", dag=etl_dag)
step2_dim_course =      SQLExecuteQueryOperator(task_id ='step2_dim_course',conn_id =Variable.get("conn_etl")  ,sql ='sql/HERCULES_WORKDAY.DIM_COURSE.sql',split_statements = True,dag=etl_dag)
step2_dim_degree =      SQLExecuteQueryOperator(task_id ='step2_dim_degree',conn_id =Variable.get("conn_etl")  ,sql ='sql/HERCULES_WORKDAY.DIM_DEGREE.sql',split_statements = True,dag=etl_dag)
step2_dim_finaid_type = SQLExecuteQueryOperator(task_id ='step2_dim_finaid_type',conn_id =Variable.get("conn_etl")  ,sql ='sql/HERCULES_WORKDAY.DIM_FINAID_TYPE.sql',split_statements = True,dag=etl_dag)
#step2_dim_isir =        SQLExecuteQueryOperator(task_id ='step2_dim_isir',conn_id =Variable.get("conn_etl")  ,sql ='sql/HERCULES_WORKDAY.DIM_ISIR.sql',split_statements = True,dag=etl_dag)
step2_dim_person =      SQLExecuteQueryOperator(task_id ='step2_dim_person',conn_id =Variable.get("conn_etl")  ,sql ='sql/HERCULES_WORKDAY.DIM_PERSON.sql',split_statements = True,dag=etl_dag)
end_step2 = EmptyOperator(task_id="end_step2", dag=etl_dag)
end_step1 >> step2
step2>>[step2_dim_course,step2_dim_degree,step2_dim_finaid_type,step2_dim_person]>>end_step2

step3 = EmptyOperator(task_id="step3", dag=etl_dag)
step3_dim_person_phone_email = SQLExecuteQueryOperator(task_id ='step3_dim_person_phone_email',conn_id =Variable.get("conn_etl")   ,sql ='sql/HERCULES_WORKDAY.DIM_PERSON_PHONE_EMAIL.sql',split_statements = True,dag=etl_dag)
step3_dim_plan =               SQLExecuteQueryOperator(task_id ='step3_dim_plan',conn_id =Variable.get("conn_etl")  ,sql ='sql/HERCULES_WORKDAY.DIM_PLAN.sql',split_statements = True,dag=etl_dag)
step3_dim_program =             SQLExecuteQueryOperator(task_id ='step3_dim_program',conn_id =Variable.get("conn_etl")  ,sql ='sql/HERCULES_WORKDAY.DIM_PROGRAM.sql',split_statements = True,dag=etl_dag)
step3_dim_term =                SQLExecuteQueryOperator(task_id ='step3_dim_term',conn_id =Variable.get("conn_etl")    ,sql ='sql/HERCULES_WORKDAY.DIM_TERM.sql',split_statements = True,dag=etl_dag)
end_step3 = EmptyOperator(task_id="end_step3", dag=etl_dag)
end_step2 >> step3
step3>>[step3_dim_person_phone_email,step3_dim_plan,step3_dim_program,step3_dim_term]>>end_step3

step4a = EmptyOperator(task_id="step4a", dag=etl_dag)
step4a_fact_enrollment = SQLExecuteQueryOperator(task_id ='step4a_fact_enrollment',conn_id =Variable.get("conn_etl") ,sql ='sql/HERCULES_WORKDAY.FACT_ENROLLMENT.sql',split_statements = True,dag=etl_dag)
step4a_ff_student_term = SQLExecuteQueryOperator(task_id ='step4a_ff_student_term',conn_id =Variable.get("conn_etl")         ,sql ='sql/HERCULES_WORKDAY.FF_STUDENT_TERM.sql',split_statements = True,dag=etl_dag)
end_step4a = EmptyOperator(task_id="end_step4a", dag=etl_dag)
end_step3 >> step4a
step4a>>[step4a_ff_student_term,step4a_fact_enrollment]>>end_step4a

step4b = EmptyOperator(task_id="step4b", dag=etl_dag)
step4b_fact_aos =        SQLExecuteQueryOperator(task_id ='step4b_fact_aos',conn_id =Variable.get("conn_etl")        ,sql ='sql/HERCULES_WORKDAY.FACT_AOS.sql',split_statements = True,dag=etl_dag)
step4b_ff_applications = SQLExecuteQueryOperator(task_id ='step4b_ff_applications',conn_id =Variable.get("conn_etl")                ,sql ='sql/HERCULES_WORKDAY.FF_APPLICATIONS.sql',split_statements = True,dag=etl_dag)
end_step4b = EmptyOperator(task_id="end_step4b", dag=etl_dag)
end_step4a >> step4b
step4b>>[step4b_fact_aos,step4b_ff_applications]>>end_step4b

step5a = EmptyOperator(task_id="step5a", dag=etl_dag)
step5a_fact_degree =     SQLExecuteQueryOperator(task_id ='step5a_fact_degree',conn_id =Variable.get("conn_etl")     ,sql ='sql/HERCULES_WORKDAY.FACT_DEGREE.sql',split_statements = True,dag=etl_dag)
step5a_ff_applications_prog      = SQLExecuteQueryOperator(task_id ='step5a_ff_applications_prog',conn_id =Variable.get("conn_etl")    ,sql ='sql/HERCULES_WORKDAY.FF_APPLICATIONS_PROG.sql',split_statements = True,dag=etl_dag)
step5a_fact_finaid =     SQLExecuteQueryOperator(task_id ='step5a_fact_finaid',conn_id =Variable.get("conn_etl")     ,sql ='sql/HERCULES_WORKDAY.FACT_FINAID.sql',split_statements = True,dag=etl_dag)
end_step5a = EmptyOperator(task_id="end_step5a", dag=etl_dag)
end_step4b >> step5a
step5a>>[step5a_ff_applications_prog,step5a_fact_degree,step5a_fact_finaid]>>end_step5a

step5b = EmptyOperator(task_id="step5b", dag=etl_dag)
step5b_ff_retention_grad_prog    = SQLExecuteQueryOperator(task_id ='step5b_ff_retention_grad_prog',conn_id =Variable.get("conn_etl")  ,sql ='sql/HERCULES_WORKDAY.FF_RETENTION_GRAD_PROG.sql',split_statements = True,dag=etl_dag)
step5b_ff_student_term_trends_re = SQLExecuteQueryOperator(task_id ='step5b_ff_student_term_trends_re',conn_id =Variable.get("conn_etl") ,sql ='sql/HERCULES_WORKDAY.FF_STUDENT_TERM_TRENDS_RE.sql',split_statements = True,dag=etl_dag)
end_step5b = EmptyOperator(task_id="end_step5b", dag=etl_dag)
step5a >> step5b
step5b>>[step5b_ff_retention_grad_prog,step5b_ff_student_term_trends_re]>>end_step5b

step5f = PythonOperator(task_id='send_email_hercules',python_callable= email_job,op_kwargs={"module_name":"Workday Hercules","client_name":"Suffolk","recipients":"etlsupp@heliocampus.com"}, dag=etl_dag)
end_step5b>> step5f

step6 = EmptyOperator(task_id="step6", dag=etl_dag)
step6_cs_admission     = SQLExecuteQueryOperator(task_id ='step6_cs_admission',conn_id =Variable.get("conn_bidev")    ,sql ='sql/PERSEUS_WORKDAY.CS_ADMISSION.sql',split_statements = True,dag=etl_dag)
step6_cs_degrees       = SQLExecuteQueryOperator(task_id ='step6_cs_degrees',conn_id =Variable.get("conn_bidev")  ,sql ='sql/PERSEUS_WORKDAY.CS_DEGREES.sql',split_statements = True,dag=etl_dag)
step6_cs_enrollments   = SQLExecuteQueryOperator(task_id ='step6_cs_enrollments',conn_id =Variable.get("conn_bidev")         ,sql ='sql/PERSEUS_WORKDAY.CS_ENROLLMENTS.sql',split_statements = True,dag=etl_dag)
step6_cs_financial_aid = SQLExecuteQueryOperator(task_id ='step6_cs_financial_aid',conn_id =Variable.get("conn_bidev") ,sql ='sql/PERSEUS_WORKDAY.CS_FINANCIAL_AID.sql',split_statements = True,dag=etl_dag)
end_step6 = EmptyOperator(task_id="end_step6", dag=etl_dag)
step5f >> step6
step6>>[step6_cs_admission,step6_cs_degrees,step6_cs_enrollments,step6_cs_financial_aid]>>end_step6

step7 = EmptyOperator(task_id="step7", dag=etl_dag)
step7_cs_financial_aid_term = SQLExecuteQueryOperator(task_id ='step7_cs_financial_aid_term',conn_id =Variable.get("conn_bidev") ,sql ='sql/PERSEUS_WORKDAY.CS_FINANCIAL_AID_TERM.sql',split_statements = True,dag=etl_dag)
step7_cs_persistence        = SQLExecuteQueryOperator(task_id ='step7_cs_persistence',conn_id =Variable.get("conn_bidev")       ,sql ='sql/PERSEUS_WORKDAY.CS_PERSISTENCE.sql',split_statements = True,dag=etl_dag)
step7_cs_retention          = SQLExecuteQueryOperator(task_id ='step7_cs_retention',conn_id =Variable.get("conn_bidev")         ,sql ='sql/PERSEUS_WORKDAY.CS_RETENTION.sql',split_statements = True,dag=etl_dag)
step7_cs_retention_rel      = SQLExecuteQueryOperator(task_id ='step7_cs_retention_rel',conn_id =Variable.get("conn_bidev")     ,sql ='sql/PERSEUS_WORKDAY.CS_RETENTION_REL.sql',split_statements = True,dag=etl_dag)
end_step7 = EmptyOperator(task_id="end_step7", dag=etl_dag)
end_step6 >> step7
step7>>[step7_cs_financial_aid_term,step7_cs_persistence,step7_cs_retention,step7_cs_retention_rel]>>end_step7

step8 = EmptyOperator(task_id="step8", dag=etl_dag)
step8_colleague_slate_admissions_extract = SQLExecuteQueryOperator(task_id ='step8_colleague_slate_admissions_extract',conn_id =Variable.get("conn_bidev") ,sql ='sql/PERSEUS_WORKDAY.COLLEAGUE_SLATE_ADMISSIONS_EXTRACT.sql',split_statements = True,dag=etl_dag)
step8_student_term_extract  = SQLExecuteQueryOperator(task_id ='step8_student_term_extract',conn_id =Variable.get("conn_bidev")  ,sql ='sql/PERSEUS_WORKDAY.STUDENT_TERM_EXTRACT.sql',split_statements = True,dag=etl_dag)
end_step8 = EmptyOperator(task_id="end_step8", dag=etl_dag)
end_step7 >> step8
step8>>[step8_colleague_slate_admissions_extract,step8_student_term_extract]>>end_step8

step9 = EmptyOperator(task_id="step9", dag=etl_dag)
step9_completions_extract                = SQLExecuteQueryOperator(task_id ='step9_completions_extract',conn_id =Variable.get("conn_bidev")        ,sql ='sql/PERSEUS_WORKDAY.COMPLETIONS_EXTRACT.sql',split_statements = True,dag=etl_dag)
step9_financial_aid_extract = SQLExecuteQueryOperator(task_id ='step9_financial_aid_extract',conn_id =Variable.get("conn_bidev") ,sql ='sql/PERSEUS_WORKDAY.FINANCIAL_AID_EXTRACT.sql',split_statements = True,dag=etl_dag)
end_step9 = EmptyOperator(task_id="end_step9", dag=etl_dag)
end_step8 >> step9
step9>>[step9_completions_extract,step9_financial_aid_extract]>>end_step9

step10 = EmptyOperator(task_id="step10", dag=etl_dag)
step10_retention_extract     = SQLExecuteQueryOperator(task_id ='step10_retention_extract',conn_id =Variable.get("conn_bidev")     ,sql ='sql/PERSEUS_WORKDAY.RETENTION_EXTRACT.sql',split_statements = True,dag=etl_dag)
step10_course_registration_extract        = SQLExecuteQueryOperator(task_id ='step10_course_registration_extract',conn_id =Variable.get("conn_bidev")          ,sql ='sql/PERSEUS_WORKDAY.COURSE_REGISTRATION_EXTRACT.sql',split_statements = True,dag=etl_dag)
end_step10 = EmptyOperator(task_id="end_step10", dag=etl_dag)
end_step9 >> step10
step10>>[step10_retention_extract,step10_course_registration_extract]>>end_step10

step10f = PythonOperator(task_id='send_email_perseus',python_callable= email_job, op_kwargs={"module_name":"Workday Perseus","client_name":"Suffolk","recipients":"etlsupp@heliocampus.com"}, dag=etl_dag)

end_step10>> step10f

etl_stop = EmptyOperator(task_id="etl_stop", dag=etl_dag)
step10f >> etl_stop