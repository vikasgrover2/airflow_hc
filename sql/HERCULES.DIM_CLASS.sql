/*
   Object type      : Dimension (SCD Type 2)
   Description      : Records class schedule information.
   Grain            : Term, Class

   Target table name: DIM_CLASS

   Source table list: workday_ods.coursesections (cs)
   					  workday_ods.courses (crs)
   					  workday_ods.academicunits (au)
   					  

   ETL load type    : Full load everyday

   Date created     : 11/29/23
   Date updated     : 4/09/24 

   Update notes     : 
*/

truncate table hercules_workday.dim_class ;

insert into hercules_workday.dim_class
( src_sys_cd                     
,session_code                  
,src_sys_eff_dt                
,src_sys_exp_dt                
,eff_dt                        
,exp_dt                        
,class_nbr                     
,strm                          
,crse_id                       
,crse_offer_nbr                
,class_type_cd                 
,enrl_stat_ld                  
,crse_career                   
,enrl_stat_cd                  
,class_type_ld                 
,crse_career_ld                
,class_instruction_mode_ld     
,class_stat_ld                 
,catalog_nbr                   
,class_stat_cd                 
,class_section                 
,class_instruction_mode_cd     
,crse_acad_group               
,crse_subject                  
,associated_class              
,class_title_ld                
,crse_career_collapsed_ld      
,crse_career_collapsed_cd      
,class_start_day_key           
,class_end_day_key             
,class_cancel_day_key          
,class_campus_cd               
,class_campus_ld               
,class_asia_region             
,sunday_ind                    
,monday_ind                    
,tuesday_ind                   
,wednesday_ind                 
,thursday_ind                  
,friday_ind                    
,saturday_ind                  
,standard_meeting_times_ld     
,meeting_time_start            
,meeting_time_end              
,meeting_duration              
,course_manager_cd             
,course_manager_ld             
,combined_section_cd           
,combined_section_ld           
,acad_org                      
,institution_cd                
,institution_ld                
,school_cd                     
,grouping_cd                   
,grouping_ld                   
,program_cd                    
,program_ld                    
,department_cd                 
,department_ld                 
,school_ld                     
,session_cd                    
,enrl_cap                      
,active_flag                   
,location_cd                   
,main_component                
,room_cap_request              
,pri_facility_id               
,primary_class_nbr             
,primary_faculty_emplid        
,meeting_pattern_src_sys_cd    
,enrl_tot                      
,class_level_1                 
,class_level_2                 
,component_desc                
,session_desc                  
,subject_desc                  
,consent                       
,consent_desc                  
,cip_code                      
,hegis_code                    
,cip_descr                     
,hegis_descr                   
,subj_cip_code                 
,subj_cip_descr                
,subj_hegis_code               
,subj_hegis_descr              
,acad_org_ld                   
,subj_cip_code_id              
,subj_cip_code_id_desc         
,cip_ld                        
,room_capacity                 
,combined_sect_ld              
,combined_sect_sd              
,comb_sect_enrl_cap            
,comb_sect_enrl_tot            
,comb_sect_wait_cap            
,comb_sect_wait_tot            
,comb_sect_room_cap_req        
,perm_combination              
,combination_type              
,skip_mtgpat_edit              
,sctn_combined_id              
,class_units_minimum           
,class_units_maximum           
,class_grading_basis_cd        
,class_grading_basis_ld        
,crse_acad_group_ld            
,crse_offer_acad_career        
,crse_offer_acad_career_ld     
,crse_offer_subject_cd         
,crse_offer_subject_ld         
,acad_org_sd                   
,crse_acad_group_sd            
,grouping_sd                   
,latest_crse_acad_group        
,latest_crse_acad_group_ld     
,latest_acad_org               
,latest_acad_org_ld            
,latest_hegis_code             
,latest_hegis_descr            
,acad_org_description          
,instructor_edit               
,component_primary_cd          
,component_primary_ld          
,rqmnt_designtn                
,combined_section_fte          
,combined_tot_enrl             
,combined_room_cap_request     
,combined_enrl_cap             
,combined_status               
,career_combined               
,dev_ed_engl_ind               
,dev_ed_math_ind               
,dev_ed_ind                    
,moa_capacity                                   
)
select 
   concat(cs.course_section_id,concat('~',cs.academic_period_course_section_wid)) as  src_sys_cd
    ,null as session_code
    ,null as src_sys_eff_dt
    ,null as src_sys_exp_dt
    ,null as eff_dt
    ,null as exp_dt
    ,cs.course_section_id as class_nbr
    ,replace(cs.academic_period_id,'_',' ') as strm
    ,cs.course_id as crse_id
    ,null as crse_offer_nbr
    ,null as class_type_cd
    ,section_status_offering_status_id as enrl_stat_ld
    ,null  as crse_career
    ,left(section_status_offering_status_id,1) as enrl_stat_cd
    ,null as class_type_ld
    ,null as crse_career_ld  ---repeating this and hoping we can make it look nicer
    ,cs.delivery_mode_id as class_instruction_mode_ld
    ,cs.section_status_offering_status_id as class_stat_ld
    ,cs.course_listing_number as catalog_nbr
    ,cs.section_status_offering_status_id as class_stat_cd
    ,cs.course_listing_section_number as class_section
    ,cs.delivery_mode_wid as class_instruction_mode_cd
    ,null as crse_acad_group
    ,cs.course_subject_id as crse_subject
    ,null as associated_class
    ,cs.title as class_title_ld
    ,null as crse_career_collapsed_ld
    ,null as crse_career_collapsed_cd
    ,cs.start_date as class_start_day_key
    ,cs.end_date as class_end_day_key
    ,null as class_cancel_day_key
    ,null as class_campus_cd
    ,null as class_campus_ld
    ,null as class_asia_region
	,null as sunday_ind
    ,null as monday_ind
    ,null as tuesday_ind
    ,null as wednesday_ind
    ,null as thursday_ind
    ,null as friday_ind
    ,null as saturday_ind
    ,null as standard_meeting_times_ld
    ,null as meeting_time_start
    ,null as meeting_time_end
    ,null as meeting_duration
    ,null as course_manager_cd
    ,null as course_manager_ld
	,null as combined_section_cd
    ,null as combined_section_ld
    ,cs.academic_unit_id as acad_org
   ,null as institution_cd
    ,null as institution_ld 
    ,nvl(dept_au.superior_academic_unit_wid,au.superior_academic_unit_wid,cs.academic_unit_wid) as school_cd
    ,null as grouping_cd
    ,null as grouping_ld
    ,null as program_cd
    ,null as program_ld
    ,nvl(au.reference_academic_unit_wid,cs.academic_unit_wid) as department_cd
    ,nvl(au.academic_unit_name,cs.academic_unit_id) as department_ld
    ,nvl(dept_au.superior_academic_unit_id,au.superior_academic_unit_id,cs.academic_unit_id) as school_ld
    ,null as session_cd
    ,cs.section_capacity as enrl_cap
    ,decode(cs.section_status_offering_status_id,'Open','Y','Waitlist','Y') as active_flag
    ,null as location_cd
    ,cs.instructional_format_id as main_component
    ,null as room_cap_request
    ,null as pri_facility_id
    ,null as primary_class_nbr
    ,null as primary_faculty_emplid
    ,null as meeting_pattern_src_sys_cd
    ,null as enrl_tot  --- will need to calculate
    ,null as class_level_1
    ,null as class_level_2
   ,null as component_desc
    ,null as session_desc
    ,sub.subject_name as subject_desc
    ,null as consent
    ,null as consent_desc
    ,null as cip_code
    ,null as hegis_code
    ,null as cip_descr
    ,null as hegis_descr
    ,null as subj_cip_code
    ,null as subj_cip_descr
    ,null as subj_hegis_code
    ,null as subj_hegis_descr
    ,au.academic_unit_name as acad_org_ld
    ,null as subj_cip_code_id
    ,null as subj_cip_code_id_desc
    ,null as cip_ld
    ,null as room_capacity
    ,null as combined_sect_ld
    ,null as combined_sect_sd
    ,null as comb_sect_enrl_cap
    ,null as comb_sect_enrl_tot
    ,null as comb_sect_wait_cap
    ,null as comb_sect_wait_tot
    ,null as comb_sect_room_cap_req
    ,null as perm_combination
    ,null as combination_type
    ,null as skip_mtgpat_edit
    ,null as sctn_combined_id
    ,cs.minimum_units as class_units_minimum      
    ,cs.maximum_units as class_units_maximum   
    ,null as class_grading_basis_cd
    ,null as class_grading_basis_ld
    ,null as crse_acad_group_ld 
    ,null as crse_offer_acad_career
    ,null as crse_offer_acad_career_ld
    ,null as crse_offer_subject_cd
    ,null as crse_offer_subject_ld
    ,null as acad_org_sd
    ,null as crse_acad_group_sd
    ,null as GROUPING_SD 
    ,null as latest_crse_acad_group
    ,null as latest_crse_acad_group_ld
    ,null as latest_acad_org
    ,null as latest_acad_org_ld
    ,null as latest_hegis_code
    ,null as latest_hegis_descr
    ,null as acad_org_description
    ,null as instructor_edit     
    ,null as component_primary_cd
    ,null as component_primary_ld
    ,null as rqmnt_designtn
    ,null as combined_section_fte                                 
    ,null as combined_tot_enrl                                 
    ,null as combined_room_cap_request                                 
    ,null as combined_enrl_cap                                 
    ,null as combined_status
    ,null as career_combined
    ,null as dev_ed_engl_ind
    ,null as dev_ed_math_ind
    ,null as dev_ed_ind
	,null as MOA_Capacity	

from workday_ods.coursesections cs
--left join workday_ods.courses crs
--     on cs.course_id = crs.course_id
left join workday_ods.coursesubjects sub
	on cs.course_subject_id = sub.course_subject_id
 left join workday_ods.academicunits au
 on  cs.academic_unit_wid = au.reference_academic_unit_wid
 and au.academic_organization_unit_subtype_id 	   = 'Academic Unit'
 left join workday_ods.academicunits dept_au
 on   au.superior_academic_unit_id  = dept_au.academic_unit_id
 and dept_au.academic_organization_unit_subtype_id 	= 'Department' 
 ;
 
commit;

analyze hercules_workday.dim_class;