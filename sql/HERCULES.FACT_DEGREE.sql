/*
   Object type      : Fact
   Description      : Records student degree information.
   Grain            : Emplid, Academic career, Completion Term, Degree, Major

   Target table name: FACT_DEGREE

   Source table list:    acad_credentials_nf (source table with degree awarded information)
                         fact_aos ({hercules_home})           -- Used to retrieve student academic program plan registration information.

   ETL load type    : Full load everyday

   Date created     : 04/07/2020
   Date updated     : 
*/

truncate hercules_workday.fact_degree;


insert into hercules_workday.fact_degree(
     src_sys_cd
    ,term
    ,car_prg_plan_maj
    ,car_prg_plan_min
    ,emplid
    ,acad_career
    ,acad_career_collapsed_cd
    ,stdnt_car_nbr
    ,institution_cd
    ,stdnt_campus_cd
    ,stdnt_campus_ld
    ,prog_status_cd
    ,prog_status_ld
    ,degr_chkout_stat_cd
    ,degr_chkout_stat_sd
    ,acad_prog
    ,completion_term
    ,plan_sequence_maj
    ,plan_sequence_min
    ,application_date
    ,honors_cd
    ,honors_ld
    ,degr_confer_dt_key
    ,degr_status_dt_key
    ,tuition_residency_cd
    ,tuition_residency_ld
    ,career_tot_cumulative
    ,career_tot_test_credit
    ,career_tot_trnsfr
    ,career_unt_passd_gpa
    ,career_unt_passd_nogpa
    ,degree_gpa
    ,ipeds_career_ind
    ,ipeds_taken_credits
    ,ipeds_total_classes
    ,ipeds_attend_type_cd
    ,ipeds_attend_type_ld
    ,bhe_residency_cd
    ,bhe_residency_ld
    ,full_part_time_cd
    ,full_part_time_ld
    ,univ_attend_type_cd
    ,univ_attend_type_ld
    ,acad_plan_maj
    ,src_sys_eff_dt
    ,admit_term
    ,admit_term_seq
    ,acad_plan_type_cd
    ,degree_cd
    ,degree_ld
    ,degree_type_cd
    ,degree_type_ld
    ,stdnt_degr
    ,acad_degr_status_cd
    ,acad_degr_status_ld
    ,degree_year
    ,degree_ld_formal
    ,diploma_print_fl
    ,trnscr_print_fl
    ,department_cd
    ,department_ld
	,acad_plan_seq_no
)
select
		   nvl(aos.emplid,'*')
	||'~'||nvl(aos.acad_prog,'*')
	||'~'||nvl(aos.strm, '*')                                          
	||'~'||nvl(aos.acad_plan, '*')                                                            	   as src_sys_cd
	,aos.strm																					   as term
	,aos.acad_plan                                                                             	   as car_prg_plan_maj
	,null                                                                             			   as car_prg_plan_min
	,aos.emplid                                                                    				   as emplid
	,aos.acad_career                                       										   as acad_career
	,aos.acad_career                                                                               as acad_career_collapsed_cd
	,null::int                                                                                     as stdnt_car_nbr
	,null                                                                       				   as institution_cd           
	,aos.stdnt_campus_cd                                                                           as stdnt_campus_cd
	,aos.stdnt_campus_ld                                                                           as stdnt_campus_ld
	,aos.prog_status_cd                                                                            as prog_status_cd
	,aos.prog_status_ld                                                                            as prog_status_ld
	,aos.degr_chkout_stat_cd 	   																   as degr_chkout_stat_cd
	,aos.degr_chkout_stat_sd   																	   as degr_chkout_stat_sd
	,aos.acad_prog                                        										   as acad_prog
	,aos.strm                                                           						   as completion_term            
	,aos.plan_sequence_maj::int                                                                    as plan_sequence_maj
	,null                                                                                     	   as plan_sequence_min
	,null::date                                   			 									   as application_date
	,null                                                             							   as honors_cd
	,null                                                                            			   as honors_ld
	,to_char(to_date(aos.exp_grad_term,'YYYYMMDD'),'J')                    						   as degr_confer_dt_key
	,null                                               										   as degr_status_dt_key
	,null                                                                     as tuition_residency_cd
	,null                                                                     as tuition_residency_ld
	,ffst.career_tot_cumulative                                                                    as career_tot_cumulative
	,null                                                                                          as career_tot_test_credit
	,ffst.career_tot_trnsfr                                                                        as career_tot_trnsfr
	,null                                                                                          as career_unt_passd_gpa
	,null                                                                                          as career_unt_passd_nogpa
	,null                                                    									   as degree_gpa
	,null                                                                                          as ipeds_career_ind
	,null                                                                                          as ipeds_taken_credits
	,null                                                                                          as ipeds_total_classes
	,null                                                                                          as ipeds_attend_type_cd
	,null                                                                                          as ipeds_attend_type_ld
	,null                                                                                          as bhe_residency_cd
	,null                                                                                          as bhe_residency_ld
	,ffst.full_part_time_cd                                                                        as full_part_time_cd
	,ffst.full_part_time_ld                                                                        as full_part_time_ld
	,ffst.univ_attend_type_cd                                                                      as univ_attend_type_cd
	,ffst.univ_attend_type_ld                                                                      as univ_attend_type_ld
	,aos.acad_plan_maj                                                        				   	   as acad_plan_maj
	,aos.src_sys_eff_dt::date                                                                      as src_sys_eff_dt
	,aos.admit_term                                                                                as admit_term
	,null::int                                                                                     as admit_term_seq
	,aos.acad_plan_maj_type_cd                                                                     as acad_plan_type_cd
	,null                                                                                		   as degree_cd
	,null                                                           							   as degree_ld
	,null                                                                                   	   as degree_type_cd
	,null                                                                     					   as degree_type_ld
	,null          																			       as stdnt_degr
	,null                                                                           			   as acad_degr_status_cd 
	,null                                                                                          as acad_degr_status_ld
	,t.degree_yr::integer                                                                  		   as degree_year
	,null                                                                                          as degree_ld_formal
	,null                                                                         				   as diploma_print_fl
	,null                                                                    					   as trnscr_print_fl
	,aos.department_cd                                                                             as department_cd
	,aos.department_ld                                                                             as department_ld
	,aos.plan_rn																			   	   as acad_plan_seq_no	
	
from hercules_workday.fact_aos aos

left join hercules_workday.dim_term t 
on aos.strm = t.strm

left join hercules_workday.ff_student_term ffst
 on aos.emplid           					= ffst.emplid
and aos.acad_career          				= ffst.acad_career
and aos.strm    						 	= ffst.strm

WHERE true 
--and ac.expir_timestamp is null                                                --no expir_timestamp available for ac
and aos.prog_status_ld  in ('PENDING_COMPLETION','COMPLETE')

;

commit;
analyze hercules_workday.fact_degree;