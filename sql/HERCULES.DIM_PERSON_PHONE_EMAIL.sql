
TRUNCATE TABLE hercules_workday.dim_person_phone_email;

INSERT INTO hercules_workday.dim_person_phone_email
(
     src_sys_cd
    ,src_sys_exp_dt
    ,eff_dt
    ,exp_dt
    ,phone_email
    ,phone_email_type_cd
    ,preferred_ind
    ,phone_email_type_ld
    ,emplid
    ,country_code_cd
	,country_code_ld
	,phone_device_type_cd
	,phone_device_type_ld
    ,PHONE_NO_EXT
)
select
     src_sys_cd
    ,src_sys_exp_dt
    ,eff_dt
    ,exp_dt
    ,phone_email
    ,phone_email_type_cd
    ,preferred_ind
    ,phone_email_type_ld
    ,emplid
    ,country_code_cd
	,country_code_ld
	,phone_device_type_cd
	,phone_device_type_ld
    ,PHONE_NO_EXT
from (
select  DECODE(student_id,'','*',NULL,'*',student_id)
       ||'~P_'|| coalesce(home_phone_communication_usage_type_id,work_phone_communication_usage_type_id,'*') as src_sys_cd
    ,load_date as src_sys_exp_dt
    ,load_date as eff_dt
    ,null as exp_dt
    ,coalesce(home_complete_phone_number,work_complete_phone_number,'*') as phone_email
    ,coalesce(home_phone_communication_usage_type_wid,work_phone_communication_usage_type_wid,'*') as phone_email_type_cd
    ,null as preferred_ind
    ,coalesce(home_phone_communication_usage_type_id,work_phone_communication_usage_type_id,'*') as phone_email_type_ld
    ,student_id as emplid
    ,coalesce(home_country_code_wid,work_country_code_wid,'*') as country_code_cd
    ,coalesce(home_country_code_id,work_country_code_id,'*') as country_code_ld
    ,coalesce(home_device_type_wid,work_device_type_wid,'*') as phone_device_type_cd
    ,coalesce(home_device_type_id,work_device_type_id,'*') as phone_device_type_ld
    ,coalesce(home_phone_extension,work_phone_extension,'*') as PHONE_NO_EXT
	,row_number() over (partition by student_id,nvl(home_device_type_id,work_device_type_id)) as rn
  from workday_ods.phone_information_data
  union
   select  DECODE(student_id,'','*',NULL,'*',student_id)
       ||'~E_'|| coalesce(home_communication_usage_type_id,work_communication_usage_type_id,'*') as src_sys_cd
    ,load_date as src_sys_exp_dt
    ,load_date as eff_dt
    ,null as exp_dt
    ,coalesce(home_email_address,work_email_address,'*') as phone_email
    ,coalesce(home_communication_usage_type_wid,work_communication_usage_type_wid,'*') as phone_email_type_cd
    ,null as preferred_ind
    ,coalesce(home_communication_usage_type_id,work_communication_usage_type_id,'*') as phone_email_type_ld
    ,student_id as emplid
    ,'*' as country_code_cd
    ,'*' as country_code_ld
    ,'*' as phone_device_type_cd
    ,'Email' as phone_device_type_ld
    ,'*' as PHONE_NO_EXT
	,row_number() over (partition by student_id,nvl(home_communication_usage_type_id,work_communication_usage_type_id)) as rn
  from workday_ods.email_information_data
 )s 
 where 1=1
 and s.rn=1
;

commit;

analyze hercules_workday.dim_person_phone_email; 