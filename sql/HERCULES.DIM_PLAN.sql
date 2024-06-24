
--drop table hercules_workday.dim_plan;
truncate table hercules_workday.dim_plan;

insert into hercules_workday.dim_plan 
(
  src_sys_cd
, plan_type_cd
, plan_type_ld
, acad_plan_cd
, acad_plan_ld
)
select  distinct
  pos.program_of_study_type_wid as src_sys_cd
--, pos.effective_date as src_sys_eff_dt
--, pos.load_timestamp as eff_dt
--, pos.expir_timestamp as exp_dt
, SUBSTRING(pos.program_of_study_type_id, LENGTH('PROGRAM_OF_STUDY_TYPE_') + 1) AS plan_type_cd
, SUBSTRING(pos.program_of_study_type_id, LENGTH('PROGRAM_OF_STUDY_TYPE_') + 1) AS plan_type_ld
, pos.program_of_study_id as acad_plan_cd
, pos.program_of_study_code as acad_plan_ld
--, row_number()over(partition by pos.id order by pos.load_timestamp desc) as acad_plan_rn
from workday_ods.program_of_study pos
where 1=1
;

commit;

analyze hercules_workday.dim_plan ;

