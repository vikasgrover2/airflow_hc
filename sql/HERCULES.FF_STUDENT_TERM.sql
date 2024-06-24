drop table if exists car_nbr ;

CREATE temp table car_nbr as
SELECT st2.student_id
    ,st2.academic_level_id as acad_career
    ,row_number() over ( partition by st2.student_id order by st2.car_start_dt) as stdnt_car_nbr
FROM (
    SELECT
         strm.rpt_entry_student_id   as student_id
  		,strm.academic_level_id 
        ,min(term.academic_period_start_date::date) as car_start_dt
    FROM workday_ods.raas_student_period_details strm 
    join workday_ods.raas_student_period stdnt_period
    on strm.subtable_index  = stdnt_period.subtable_index 
    join workday_ods.maintained_academic_period term
    ON  strm.academic_period_id = term.id 
  --  and term.expir_timestamp is null
    
    GROUP BY strm.rpt_entry_student_id, strm.academic_level_id
) st2 ;

drop table if exists instr_mode;

create temp table instr_mode as
SELECT
    enrl.student_id 
    ,enrl.academic_period_id 
    ,case
        when max(decode(enrl.delivery_mode_id,'Online',1, 'Hybrid', 1, 0)) + max(decode(enrl.delivery_mode_id,'Online',0, 'Hybrid',0, 1)) = 2
            then 'HY'
        when  max(decode(enrl.delivery_mode_id,'Online', 0, 1)) = 1
            then 'F2F'
        when max(decode(enrl.delivery_mode_id,'Online', 1, 0))  = 1
            then 'ONL'
    end as stdnt_instr_mode_cd
    ,case
       when max(decode(enrl.delivery_mode_id,'Online',1, 'Hybrid', 1, 0)) + max(decode(enrl.delivery_mode_id,'Online',0, 'Hybrid',0, 1)) = 2
            then 'Hybrid'
       when  max(decode(enrl.delivery_mode_id,'Online', 0, 1)) = 1
             then 'F2F'
        when max(decode(enrl.delivery_mode_id,'Online', 1, 0))  = 1
            then 'Online'
    end as stdnt_instr_mode_sd
FROM  workday_ods.raas_student_section_enrollment enrl
GROUP BY enrl.student_id 
    ,enrl.academic_period_id
;

Truncate table hercules_workday.ff_student_term;

Insert into hercules_workday.ff_student_term
(
	 src_sys_cd                      
	,src_sys_exp_dt                  
	,eff_dt                          
	,exp_dt                          
	,src_sys_eff_dt                  
	,emplid                          
	,uid                            
	,strm                            
	,acad_career                     
	,ipeds_billing_credits           
	,ipeds_taken_credits             
	,ipeds_credits_toward_progress   
	,career_billing_credits          
	,career_taken_credits            
	,career_credits_toward_progress  
	,ipeds_total_classes             
	,career_total_classes            
	,career_total_classes_wo_trfr    
	,career_gpa_sem                  
	,career_gpa_cum                  
	,career_gpa_sem_bot              
	,career_gpa_cum_bot              
	,career_unt_term_tot             
	,career_unt_term_tot_ofcl        
	,career_grade_points             
	,career_tot_cumulative           
	,career_tot_grade_points         
	,career_unt_audit                
	,career_unt_trnsfr               
	,career_unt_test_credit          
	,career_tot_audit                
	,career_tot_trnsfr               
	,career_tot_other                
	,career_tot_test_credit          
	,acad_level_bot_cd               
	,acad_level_bot_ld               
	,eligible_to_enroll_ind          
	,em_include_in_reports_ind       
	,full_part_time_cd               
	,full_part_time_sd               
	,full_part_time_ld               
	,full_time_ind                   
	,ipeds_attend_type_cd            
	,ipeds_attend_type_ld            
	,ipeds_attend_type_sd            
	,ipeds_include_in_reports_ind    
	,officially_registered_ind       
	,reg_activity_ind                
	,registered_ind                  
	,univ_attend_type_cd             
	,univ_attend_type_sd             
	,univ_attend_type_ld             
	,independent_study_ind           
	,mixed_ind                       
	,online_ind                      
	,f2f_ind                         
	,hybrid_ind                      
	,missing_ind                     
	,other_ind                       
	,stdnt_instr_mode_cd             
	,student_current_type            
	,student_type                    
	,student_cur_type                
	,institution                     
	,stdnt_car_nbr                   
	,career_unt_passd_gpa            
	,career_unt_passd_nogpa          
	,ipeds_career_ind                
	,full_time_bhe_ind               
	,collapsed_strm_stdnt_term       
	,primary_acad_prog               
	,primary_stdnt_car_nbr           
	,ir_stdnt_campus_cd              
	,bhe_residency_ld                
	,stdnt_instr_mode_ld             
	,stdnt_instr_mode_sd             
	,ir_collapsed_acad_career        
	,plan_attend_type_cd             
	,ir_new_to_inst                  
	,prog_attend_type_cd             
	,form_of_study                   
	,withdraw_reason_cd              
	,withdraw_reason_ld              
	,unt_taken_prgrss                
	,unt_passd_prgrss                
	,withdraw_cd                     
	,withdraw_ld                     
	,withdraw_dt                     
	,last_dt_atnd                    
	,acad_level_eot_cd               
	,acad_level_eot_ld               
	,acad_career_sub                 
	,primary_advr_emplid             
	,primary_advr_cd                 
	,primary_advr_ld                 
	,expir_strm                      
	,min_reg_dt                      
	,max_reg_dt                      
	,min_drop_dt                     
	,max_drop_dt                     
	,career_tot_overall              
	,stdnt_type_cd                   
	,stdnt_type_ld                   
	,tot_payment_amt                 
	,corporate_waiver_amt            
	,tuition_charged                 
	,fees_charged                    
	,tuition_payment                 
	,fee_payment                     
	,tuition_remission               
	,official_credit_registered_ind  
	,active_registered_ind           
	,active_credit_registered_ind    
	,stdnt_status_cd                 
	,stdnt_status_ld                 
	,au_ind                          
	,cuml_tot_payment_amt            
	,cuml_corporate_waiver_amt       
	,cuml_tuition_charged            
	,cuml_fees_charged               
	,cuml_tuition_payment            
	,cuml_fee_payment                
	,cuml_tuition_remission          
	,stdnt_rate_cd                   
	,stdnt_rate_ld                   
	,acad_stdg_eot_cd                
	,acad_stdg_eot_ld                
	,term_start_date                 
	,term_end_date                   
	,student_hiatus_cd               
	,student_hiatus_ld               
	,student_hiatus_reason_cd        
	,student_hiatus_reason_ld        
	,student_hiatus_end_dt           
	,honors_ind                      
)
Select  
   strm.rpt_entry_student_id ||'~'|| strm.academic_level_id ||'~'|| strm.academic_period_id   as src_sys_cd
  ,NULL as src_sys_exp_dt
  ,NULL as eff_dt
  ,NULL as exp_dt 
  ,NULL as src_sys_eff_dt
  ,strm.rpt_entry_student_id   as emplid
  ,strm.rpt_entry_universal_id  as uid
  ,replace(strm.academic_period_id,'_',' ')   as strm
  ,strm.academic_level_id   as acad_career
  ,NULL  as ipeds_billing_credits
  ,NULL  as ipeds_taken_credits
  ,NULL  as ipeds_credits_toward_progress
  ,NULL  as career_billing_credits
  ,institutional_cumulative_attempted_credits  as career_taken_credits
  ,cumulative_credits_gpa as career_credits_toward_progress
  ,NULL as ipeds_total_classes
  ,NULL as career_total_classes
  ,NULL as career_total_classes_wo_trfr
  ,strm.institutional_gpa             as career_gpa_sem
  ,strm.institutional_cumulative_gpa  as career_gpa_cum
  ,lag(strm.institutional_gpa) over (
            partition by
                strm.rpt_entry_student_id
                ,strm.academic_level_id
            order by
                strm.academic_period_id
        )    AS CAREER_GPA_SEM_bot
   ,lag(strm.institutional_cumulative_gpa) over (
            partition by
                strm.rpt_entry_student_id
                ,strm.academic_level_id
            order by
                strm.academic_period_id
        )    AS CAREER_GPA_CUM_bot
      
  ,strm.registered_credits  as career_unt_term_tot
  ,registered_credits::float      as career_unt_term_tot_ofcl
  ,NULL  as career_grade_points
  ,cumulative_credits_gpa       as career_tot_cumulative
  ,NULL as career_tot_grade_points
  ,NULL as career_unt_audit
  ,NULL as career_unt_trnsfr
  ,NULL as career_unt_test_credit
  ,NULL as career_tot_audit
  ,strm.transfer_credit_credits_earned as career_tot_trnsfr
  ,NULL as career_tot_other
  ,NULL as career_tot_test_credit
  ,strm.rpt_entry_primary_level_class_id_descriptor as acad_level_bot_cd 
  ,strm.rpt_entry_primary_level_class_id_descriptor as acad_level_bot_ld
  ,NULL as eligible_to_enroll_ind
  ,NULL as em_include_in_reports_ind
  ,strm.student_time_wid as full_part_time_cd
  ,strm.student_time_code_descriptor as full_part_time_sd
  ,strm.student_time_code_descriptor as full_part_time_ld
  ,CASE
              WHEN strm.student_time_code_descriptor = 'Full-time' THEN 'Y'
              ELSE 'N'
         END  as full_time_ind
  ,NULL as ipeds_attend_type_cd
  ,NULL as ipeds_attend_type_ld
  ,NULL as ipeds_attend_type_sd
  ,NULL as ipeds_include_in_reports_ind
  ,case when registered_credits > 0 then 'Y' else 'N' end as officially_registered_ind
  ,NULL as reg_activity_ind
  ,NULL   as registered_ind
  ,case when strm.rpt_entry_admit_term_id_descriptor = strm.academic_period_id then 'NEW' else 'RET' end                             as univ_attend_type_cd
  ,case when strm.rpt_entry_admit_term_id_descriptor = strm.academic_period_id then 'New'  else 'Continuing' end                     as univ_attend_type_sd
  ,case when strm.rpt_entry_admit_term_id_descriptor = strm.academic_period_id then 'New Student'   else 'Continuing Student' end    as univ_attend_type_ld	
  ,NULL as independent_study_ind
  ,NULL as mixed_ind      
  ,CASE when im.stdnt_instr_mode_sd = 'Online' then 'Y' else '*' end 	as online_ind
  ,CASE when im.stdnt_instr_mode_sd = 'F2F' then 'Y' else '*' end 		as f2f_ind
  ,CASE when im.stdnt_instr_mode_sd = 'Hybrid' then 'Y' else '*' end 	as hybrid_ind
  ,NULL as missing_ind
  ,NULL as other_ind
  ,NULL as stdnt_instr_mode_cd
  ,NULL as student_current_type
  ,NULL as student_type
  ,NULL as student_cur_type
  ,NULL as institution
  ,cn.stdnt_car_nbr   as stdnt_car_nbr
  ,NULL as career_unt_passd_gpa
  ,NULL as career_unt_passd_nogpa
  ,NULL as ipeds_career_ind
  ,NULL as full_time_bhe_ind
  ,NULL as collapsed_strm_stdnt_term
  ,stdnt_period.rpt_entry_primary_degree_desc_descriptor as primary_acad_prog
  ,NULL as primary_stdnt_car_nbr
  ,NULL as ir_stdnt_campus_cd
  ,NULL as bhe_residency_ld
  ,im.stdnt_instr_mode_sd    as stdnt_instr_mode_ld
  ,im.stdnt_instr_mode_sd    as stdnt_instr_mode_sd
  ,NULL as ir_collapsed_acad_career
  ,NULL as plan_attend_type_cd
  ,NULL as ir_new_to_inst
  ,NULL as prog_attend_type_cd
  ,NULL as form_of_study
  ,NULL as withdraw_reason_cd
  ,NULL as withdraw_reason_ld
  ,NULL as unt_taken_prgrss
  ,NULL as unt_passd_prgrss
  ,null as withdraw_cd
  ,null as withdraw_ld
  ,null as withdraw_dt
  ,NULL as last_dt_atnd
  ,LEAD(strm.rpt_entry_primary_level_class_id_descriptor , 1) 
		OVER (PARTITION BY  strm.rpt_entry_student_id , strm.academic_period_id
		ORDER BY term.academic_period_start_date::date)   as acad_level_eot_cd
  ,LEAD(strm.rpt_entry_primary_level_class_id_descriptor , 1) 
		OVER (PARTITION BY  strm.rpt_entry_student_id , strm.academic_period_id
		ORDER BY term.academic_period_start_date::date)    as acad_level_eot_ld
  ,NULL as acad_career_sub
  ,NULL as primary_advr_emplid
  ,NULL as primary_advr_cd
  ,NULL as primary_advr_ld
  ,NULL as expir_strm
  ,NULL as min_reg_dt
  ,NULL as max_reg_dt
  ,NULL as min_drop_dt
  ,NULL as max_drop_dt
  ,NULL as career_tot_overall
  ,NULL as stdnt_type_cd
  ,NULL as stdnt_type_ld
  ,NULL as tot_payment_amt
  ,NULL as corporate_waiver_amt
  ,NULL as tuition_charged
  ,NULL as fees_charged
  ,NULL as tuition_payment
  ,NULL as fee_payment
  ,NULL as tuition_remission
  ,case when registered_credits > 0 then 'Y' else 'N' end as official_credit_registered_ind
  ,NULL as active_registered_ind
  ,NULL as active_credit_registered_ind
  ,stdnt_period.rpt_entry_ar_status_wid as stdnt_status_cd
  ,stdnt_period.rpt_entry_ar_status_id as stdnt_status_ld
  ,NULL as AU_ind
  ,NULL as cuml_tot_payment_amt
  ,NULL as cuml_corporate_waiver_amt
  ,NULL as cuml_tuition_charged
  ,NULL as cuml_fees_charged
  ,NULL as cuml_tuition_payment
  ,NULL as cuml_fee_payment
  ,NULL as cuml_tuition_remission
  ,NULL as stdnt_rate_cd
  ,NULL as stdnt_rate_ld
  ,NULL as acad_stdg_eot_cd
  ,NULL as acad_stdg_eot_ld
  ,term.academic_period_start_date::date  as term_start_date
  ,term.academic_period_end_date::date    as term_end_date
  ,stdnt_period.student_leave_of_absence_student_program_of_study_status_reason_id as student_hiatus_cd
  ,stdnt_period.student_leave_of_absence_reason_descriptor as student_hiatus_ld
  ,NULL as student_hiatus_reason_cd
  ,stdnt_period.student_leave_of_absence_student_program_of_study_status_tenanted_reason_id as student_hiatus_reason_ld
  ,stdnt_period.student_leave_of_absence_return_date as student_hiatus_end_dt
  ,NULL as honors_ind
from workday_ods.raas_student_period_details strm

join workday_ods.raas_student_period stdnt_period
  on strm.subtable_index  = stdnt_period.subtable_index 

JOIN workday_ods.maintained_academic_period term   
        --ON term.expir_timestamp is null
      on strm.academic_period_id = term.id

LEFT JOIN instr_mode im 
ON stdnt_period.student_rpt_entry_student_id     = im.student_id
        AND strm.academic_period_id = im.academic_period_id
LEFT JOIN car_nbr cn    ON strm.rpt_entry_student_id     = cn.student_id
         AND strm.academic_level_id  = cn.acad_career

WHERE 1=1
; 

commit;

analyze hercules_workday.ff_student_term ;