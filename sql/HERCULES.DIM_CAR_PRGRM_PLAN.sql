truncate table hercules_workday.dim_car_prgm_plan;

insert into hercules_workday.dim_car_prgm_plan
(
     src_sys_cd
   , src_sys_eff_dt
   , src_sys_exp_dt
   , eff_dt
   , exp_dt
   , acad_career_cd
   , acad_career_sd
   , acad_career_ld
   , acad_career_collapsed_cd
   , acad_career_collapsed_ld
   , acad_plan_cd
   , acad_plan_sd
   , acad_plan_ld
   , acad_plan_prefix
   , acad_plan_type_cd
   , acad_plan_type_sd
   , acad_plan_type_ld
   , acad_prog_deg_type_cd   
   , acad_prog_deg_type_sd     
   , acad_prog_deg_type_ld
   , acad_prog_cd    
   , acad_prog_sd
   , acad_prog_ld
   , acad_prog_cip_cd
   , acad_prog_grade_scheme
   , default_acad_prog_for_plan
   , degree_cd
   , degree_sd
   , degree_ld
   , degree_education_lvl
   , degree_formal_ld
   , diploma_descr
   , institution_cd
   , bhe_degree_level_cd
   , bhe_degree_level_ld
   , bhe_degree_level_sd
   , specialization_cd
   , specialization_ld
   , study_field_cd
   , study_field_ld
   , suffix_plan_type
   , trnscr_descr
   , ssr_last_adm_term
   , first_term_valid
   , valid_plan_ind
   , valid_prog_ind
   , valid_prog_plan_grouping
   , department_cd
   , department_ld
   , grouping_cd
   , grouping_ld
   , institution_ld
   , program_cd
   , program_ld
   , school_cd
   , school_ld
   , education_level_ld
   , bhe_api_program_change
   , bhe_api_hegis_ld
   , bhe_api_degree_code
   , bhe_api_fice_cd
   , bhe_api_concentration
   , bhe_api_degree_code_ld
   , bhe_api_cip1990_cd
   , bhe_api_hegis_cd
   , bhe_api_cip2000_cd
   , bhe_api_program_type
   , bhe_api_hegis_program_nm
)
Select 
    wpos.program_of_study_wid  as src_sys_cd
   ,wpos.load_date as src_sys_eff_dt
   ,NULL  as src_sys_exp_dt
   ,wpos.effective_date as eff_dt
   ,NULL  as exp_dt
   ,wpos.academic_level_wid as acad_career_cd
   ,wpos.academic_level_id as acad_career_sd
   ,wpos.academic_level_id  as acad_career_ld
   ,wpos.academic_level_id  as acad_career_collapsed_cd
   ,wpos.academic_level_id as acad_career_collapsed_ld
   ,wpos.program_of_study_id  as acad_plan_cd
   ,wpos.program_of_study_code as acad_plan_sd
   ,wpos.program_name  as acad_plan_ld
   ,NULL  as acad_plan_prefix
   ,wpos.program_of_study_type_wid  as acad_plan_type_cd
   ,SUBSTRING(wpos.Program_of_Study_Type_ID, LENGTH('PROGRAM_OF_STUDY_TYPE_') + 1) as acad_plan_type_sd
   ,SUBSTRING(wpos.Program_of_Study_Type_ID, LENGTH('PROGRAM_OF_STUDY_TYPE_') + 1) as acad_plan_type_ld
   ,wpos.default_educational_credential_wid  as acad_prog_deg_type_cd   
   ,wpos.default_educational_credential_id  as acad_prog_deg_type_sd     
   ,wpos.default_educational_credential_id as acad_prog_deg_type_ld
   ,wpos.program_of_study_id  as acad_prog_cd    
   ,wpos.program_of_study_code  as acad_prog_sd
   ,wpos.program_name  as acad_prog_ld
   ,wpos.cip_code_id as acad_prog_cip_cd
   ,NULL as acad_prog_grade_scheme
   ,NULL  as default_acad_prog_for_plan
   ,wpos.default_educational_credential_wid as degree_cd
   ,wpos.default_educational_credential_id  as degree_sd
   ,NULL  as degree_ld
   ,NULL  as degree_education_lvl
   ,NULL  as degree_formal_ld
   ,NULL  as diploma_descr
   ,'Suffolk University'  as institution_cd
   ,NULL  as bhe_degree_level_cd
   ,NULL  as bhe_degree_level_ld
   ,NULL  as bhe_degree_level_sd
   ,NULL  as specialization_cd
   ,NULL  as specialization_ld
   ,NULL  as study_field_cd
   ,NULL  as study_field_ld
   ,NULL  as suffix_plan_type
   ,NULL  as trnscr_descr
   ,NULL  as ssr_last_adm_term
   ,NULL  as first_term_valid
   ,NULL  as valid_plan_ind
   ,NULL  as valid_prog_ind
   ,NULL  as valid_prog_plan_grouping
   ,wpos.academic_unit_wid as department_cd
   ,wpos.academic_unit_id as department_ld
   ,NULL as grouping_cd
   ,NULL as grouping_ld
   ,NULL as institution_ld
   ,wpos.program_of_study_code  as program_cd
   ,wpos.program_name as program_ld
   ,nvl(dept_au.superior_academic_unit_wid,au.superior_academic_unit_wid,wpos.academic_unit_wid) as school_cd
   ,nvl(dept_au.superior_academic_unit_id,au.superior_academic_unit_id,wpos.academic_unit_id) as school_ld
   ,NULL as education_level_ld
   ,NULL as bhe_api_program_change
   ,NULL as bhe_api_hegis_ld
   ,NULL as bhe_api_degree_code
   ,NULL as bhe_api_fice_cd
   ,NULL as bhe_api_concentration
   ,NULL as bhe_api_degree_code_ld
   ,NULL as bhe_api_cip1990_cd
   ,NULL as bhe_api_hegis_cd
   ,NULL as bhe_api_cip2000_cd
   ,NULL as bhe_api_program_type
   ,NULL as bhe_api_hegis_program_nm
 from workday_ods.program_of_study wpos
 
 left join workday_ods.academicunits au
 on  au.academic_unit_id 	     = wpos.academic_unit_id 
 and au.academic_organization_unit_subtype_id 	   = 'Academic Unit'
 left join workday_ods.academicunits dept_au
 on   au.superior_academic_unit_id  = dept_au.academic_unit_id
 and dept_au.academic_organization_unit_subtype_id 	= 'Department' 
 
WHERE 1=1;

commit;


analyze hercules_workday.dim_car_prgm_plan;