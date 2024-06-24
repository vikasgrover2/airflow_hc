/*
Object type		    : Dimension
   Description		: 
   Grain			: orion_ods.acad_programs_nf.id
   
   Target table name: DIM_PROGRAM
   
   Source table list: acad_programs_nf
   
   ETL load type	: Full load everyday
   
   Date created 	: 01/29/2020
   Date updated		: 
   
   Update notes		: 
*/

TRUNCATE TABLE hercules_workday.dim_program;

insert into hercules_workday.dim_program
(
	src_syc_cd,
	src_sys_eff_dt,
	src_sys_exp_dt,
	eff_dt,
	exp_dt,
	acad_prog_cd,
	acad_prog_sd,
	acad_prog_ld,
	plan_type,
	first_entry_date,
	last_entry_date,
	acad_career_cd,
	acad_career_ld,
	academic_unit,
	coordinating_academic_unit,
	department_cd,
	department_ld,
	school_cd,
	school_ld,
	Institution,
	acad_prog_cip_sys_cd,
	acad_prog_cip_cd,
	acad_prog_cip_cd_2020,
	degree_cd,
	degree_ld
)
select 
	wpos.program_of_study_wid as src_syc_cd,
	null as src_sys_eff_dt,
	null  as src_sys_exp_dt,
	wpos.effective_date as eff_dt,
	null  as exp_dt,
	wpos.program_of_study_id as acad_prog_cd,
	wpos.program_of_study_code as acad_prog_sd,
	wpos.program_name as acad_prog_ld,
	SUBSTRING(wpos.Program_of_Study_Type_ID, LENGTH('PROGRAM_OF_STUDY_TYPE_') + 1) as plan_type,
	wpos.first_entry_date,
	wpos.last_entry_date,
	wpos.academic_level_wid as acad_career_cd,
	wpos.academic_level_id as acad_career_ld,
	wpos.academic_unit_id as academic_unit,
	wpos.coordinating_academic_unit_id as coordinating_academic_unit,
	au.academic_unit_code as department_cd,
	au.academic_unit_name as department_ld,
	schl.academic_unit_code as school_cd,
	schl.academic_unit_name as school_ld,
	'Suffolk University' as Institution,
	wpos.cip_code_wid as acad_prog_cip_sys_cd,
	wpos.cip_code_id as acad_prog_cip_cd,
	wpos.cip_code_2020_id as acad_prog_cip_cd_2020,
	wpos.default_educational_credential_wid as degree_cd,
	wpos.default_educational_credential_id as degree_ld
from workday_full_test.program_of_study wpos

left join workday_full_test.academicunits au 
on au.reference_academic_unit_wid = wpos.academic_unit_wid 
and au.academic_organization_unit_subtype_id != 'Schools and Centers'

left join workday_full_test.academicunits schl 
   on au.superior_academic_unit_wid = schl.reference_academic_unit_wid 
  and au.academic_organization_unit_subtype_id = 'Schools and Centers'

where 1=1
AND SUBSTRING(wpos.Program_of_Study_Type_ID, LENGTH('PROGRAM_OF_STUDY_TYPE_') + 1)  = 'Program'
;

commit;

analyze hercules_workday.dim_program;