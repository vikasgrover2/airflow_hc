
drop table if exists dim_address_term;

create temp table dim_address_term 
as select * from
    (select *,
      row_number() over (partition by emplid,address_type_ld) as rn 
      from hercules_workday.dim_address 
    )s 
where s.rn = 1;

drop table if exists dim_person_phone_email ;

create temp table dim_person_phone_email 
as select * from (
select *, row_number() over(partition by emplid,phone_email_type_ld ) as rn 
from hercules_workday.dim_person_phone_email
)s 
where s.rn = 1
;

Truncate table perseus_workday.student_term_extract;
            

Insert into perseus_workday.student_term_extract
(src_sys_cd                                  
,residency_cd                               
,tuition_residency_sd                       
,tuition_residency_ld                       
,bhe_residency_cd                           
,em_residency_sd                            
,residency_ld                               
,em_residency_ld                            
,residency_sd                               
,em_residency_cd                            
,tuition_residency_cd                       
,ipeds_billing_credits                      
,ipeds_taken_credits                        
,ipeds_credits_toward_progress              
,career_billing_credits                     
,career_taken_credits                       
,career_credits_toward_progress             
,ipeds_total_classes                        
,career_total_classes                       
,career_gpa_sem                             
,career_gpa_cum                             
,career_unt_audit                           
,career_unt_trnsfr                          
,career_unt_test_credit                     
,career_unt_term_tot                        
,career_grade_points                        
,career_tot_audit                           
,career_tot_trnsfr                          
,career_tot_other                           
,career_tot_test_credit                     
,career_tot_cumulative                      
,career_tot_grade_points                    
,acad_level_bot_cd                          
,acad_level_bot_ld                          
,eligible_to_enroll_ind                     
,em_include_in_reports_ind                  
,full_part_time_cd_calc                     
,full_part_time_ld                          
,full_part_time_cd                          
,full_part_time_sd                          
,full_time_ind_calc                         
,ipeds_attend_type_cd                       
,ipeds_attend_type_ld                       
,ipeds_attend_type_sd                       
,ipeds_include_in_reports_ind               
,officially_registered_ind                  
,reg_activity_ind                           
,registered_ind                             
,univ_attend_type_cd                        
,univ_attend_type_ld                        
,univ_attend_type_sd                        
,independent_study_ind                      
,mixed_ind                                  
,online_ind                                 
,f2f_ind                                    
,hybrid_ind                                 
,missing_ind                                
,other_ind                                  
,src_sys_exp_dt                             
,eff_dt                                     
,exp_dt                                     
,stdnt_instr_mode_cd                        
,src_sys_eff_dt                             
,emplid              
,uid                       
,strm                                       
,acad_career                                
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
,unt_taken_prgrss                           
,unt_passd_prgrss                           
,acad_level_eot_cd                          
,acad_level_eot_ld                          
,withdraw_date                              
,withdraw_cd                                
,withdraw_ld                                
,withdraw_reason_cd                         
,withdraw_reason_ld                         
,last_date_attended                         
,acad_stndng_actn_cd                        
,acad_stndng_actn_ld                        
,career_unt_taken_nogpa                     
,career_unt_taken_gpa                       
,student_term_fte                           
,student_term_exclude_flag                  
,pri_degree_level                           
,audit_only                                 
,address_term_src_sys_cd                    
,home_address1                              
,home_address2                              
,home_address_type                          
,home_city                                  
,home_country_ld                            
,home_geog_county_ld                        
,home_postal                                
,home_state_cd                              
,home_state_ld                              
,home_postal_loc_y                          
,home_postal_loc_x                          
,dorm_address_term_src_sys_cd               
,dorm_address1                              
,dorm_address2                              
,dorm_address_type                          
,dorm_city                                  
,dorm_country_ld                            
,dorm_geog_county_ld                        
,dorm_postal                                
,dorm_state_cd                              
,dorm_state_ld                              
,dorm_postal_loc_y                          
,dorm_postal_loc_x                          
,birthdate_day                              
,first_name                                 
,gender_cd                                  
,ipeds_race_ethn_citz_ld                    
,last_name                                  
,married_status_ld                          
,bhe_birth_year                             
,stdnt_home_phone                           
,stdnt_cell_phone                           
,stdnt_home_email                           
,stdnt_business_email                       
,stdnt_campus_email                         
,advisor_emplid                             
,advisor_name_first_last                    
,advisor_business_email                     
,advisor_campus_email                       
,advisor_home_email                         
,pri_degree_cd                              
,pri_degree_ld                              
,pri_acad_prog_ld                           
,pri_acad_prog_cd                           
,pri_school_ld                              
,pri_school_cd                              
,pri_specialization_ld                      
,pri_acad_plan_ld                           
,pri_acad_plan_cd                           
,pri_education_level_ld                     
,pri_institution_cd                         
,pri_institution_ld                         
,pri_grouping_ld                            
,pri_grouping_cd                            
,pri_department_ld                          
,pri_study_field_cd                         
,pri_study_field_ld                         
,pri_bhe_api_hegis_ld                       
,pri_acad_plan_type_ld                      
,sec_degree_cd                              
,sec_degree_ld                              
,sec_acad_prog_ld                           
,sec_acad_prog_cd                           
,sec_school_ld                              
,sec_school_cd                              
,sec_specialization_ld                      
,sec_acad_plan_ld                           
,sec_acad_plan_cd                           
,sec_education_level_ld                     
,sec_institution_cd                         
,sec_institution_ld                         
,sec_grouping_ld                            
,sec_grouping_cd                            
,sec_department_ld                          
,sec_study_field_cd                         
,sec_study_field_ld                         
,sec_bhe_api_hegis_ld                       
,sec_acad_plan_type_ld                      
,online_pct                                 
,unt_inprog_gpa                             
,unt_inprog_nogpa                           
,trf_taken_gpa                              
,trf_taken_nogpa                            
,trf_grade_points                           
,trf_passed_gpa                             
,trf_passed_nogpa                           
,tc_units_adjust                            
,unt_other                                  
,tot_taken_gpa                              
,tot_taken_nogpa                            
,tot_passd_gpa                              
,tot_passd_nogpa                            
,tot_inprog_nogpa                           
,tot_taken_prgrss                           
,tot_passd_prgrss                           
,stack_begin_term_code                      
,cohort_code                                
,cohort_degree_level                        
,pers_primary_ethnic_group                  
,pers_primary_grp_ldesc                     
,age                                        
,visa_permit_type                           
,pri_citizenship_country                    
,pri_citizenship_country_code               
,pri_citizenship_country_code_iso           
,citizenship_country_iso                    
,usa_citizenship_status                     
,usa_citizenship_status_code                
,va_benefit_code                            
,visa_permit_type_code                      
,part_time_calc                             
,admissions_residency                       
,admissions_residency_code                  
,federal_fa_residency                       
,federal_fa_residency_code                  
,residency_date                             
,state_fa_residency                         
,state_fa_residency_code                    
,distance_percent                           
,distance_type                              
,enrolled_load_code                         
,enrolled_load_desc                         
,pri_acad_plan_degree                       
,readmit_stack_flag                         
,degree_seeking_flag                        
,dorm_residency                             
,grad_degree                                
,honors_flag                                
,nondegree_flag                             
,expected_graduation_term_code              
,latest_degree_checkout_status_code         
,latest_degree_checkout_status_desc         
,locl_country                               
,locl_country_ldesc                         
,locl_postal                                
,locl_state                                 
,locl_state_desc                            
,mail_country                               
,mail_country_ldesc                         
,mail_src_sys_cd                            
,mail_postal                                
,mail_state                                 
,mail_state_desc                            
,perm_city                                  
,perm_country                               
,perm_country_lon_description               
,perm_state                                 
,perm_state_desc                            
,perm_zip_code                              
,dorm_zip_code                              
,freeze_strm                                
,stdnt_campus_sd                            
,stdnt_campus_ld                            
,stdnt_acad_career_cd                       
,stdnt_acad_car_collapsed_cd                
,stdnt_acad_car_collapsed_ld                
,stdnt_acad_career_ld                       
,term_other_units                           
,term_in_progress_units_not_for_gpa         
,term_transfer_earned_units_for_gpa         
,cum_in_progress_units_not_for_gpa          
,latest_degree_checkout_status              
,primary_plan_application_number            
,middle_name                                
,student_name                            
,military_status                            
,military_status_code                       
,custom_ipeds_ethnicity_ld                  
,custom_ipeds_ethnicity_cd                  
,ipeds_ethnicity_ld                         
,ipeds_ethnicity_cd                         
,birth_date                                 
,term_in_progress_units_for_gpa             
,term_transfer_units                        
,total_combined_term_units                  
,cum_combined_grade_points                  
,cum_combined_graded_units_for_gpa          
,cum_combined_earned_units_not_for_gpa      
,cum_combined_earned_units_for_gpa          
,cum_in_progress_units_for_gpa              
,soc_flag                                   
,urm_flag                                   
,plan_cip_id                                
,campus_latitude                            
,campus_longitude                           
,exp_grad_term                              
,degr_chkout_stat_cd                        
,degr_chkout_stat_sd                        
,exp_grad_term_year_ld                      
,exp_grad_year                              
,pri_acad_prog                              
,pri_prog_status_cd                         
,pri_prog_status_ld                         
,pri_prog_action_cd                         
,pri_prog_action_ld                         
,pri_prog_reason_cd                         
,pri_prog_reason_ld                         
,acad_career_collapsed_cd                   
,acad_career_collapsed_ld                   
,acad_career_ld                             
,perm_address1                              
,perm_address2                              
,perm_address_type                          
,perm_country_ld                            
,perm_geog_county_ld                        
,perm_postal                                
,perm_state_cd                              
,perm_state_ld                              
,stdnt_campus_cd                            
,trn_pi_totalcredit                         
,trn_ls_school_type                         
,trn_ls_school_type_ld                      
,trn_transfer_inst_name                     
,trn_school_type_lpi                        
,hs_final_gpa                               
,hs_term_year                               
,hs_highest_ed_lvl                          
,hs_final_gpa_excl_last_strm                
,hs_last_year                               
,hs_start_year                              
,hs_years_attended                          
,hs_class_pop                               
,hs_rank                                    
,hs_unt_comp_total                          
,hs_name                                    
,hs_short_name                              
,hs_proprietorship                          
,hs_address1                                
,hs_address2                                
,hs_city                                    
,hs_county                                  
,hs_state                                   
,hs_postal                                  
,hs_country                                 
,hs_lat                                     
,hs_long                                    
,test_super_act_score                       
,test_super_sat_score                       
,test_first_act_score                       
,test_last_act_score                        
,test_max_act_score                         
,test_avg_act_score                         
,test_num_act_attempts                      
,test_first_sat_score                       
,test_last_sat_score                        
,test_max_sat_score                         
,test_avg_sat_score                         
,test_num_sat_attempts                      
,stdnt_cntct_latitude                       
,stdnt_cntct_longitude                      
,mail_address1                              
,mail_address2                              
,mail_address_type                          
,mail_city                                  
,mail_country_ld                            
,mail_geog_county_ld                        
,mail_state_cd                              
,mail_state_ld                              
,mail_postal_loc_y                          
,mail_postal_loc_x                          
,resh_address1                              
,resh_address_type                          
,resh_city                                  
,resh_postal                                
,resh_country_ld                            
,resh_geog_county_ld                        
,resh_state_cd                              
,resh_state_ld                              
,test_first_gre_score                       
,test_last_gre_score                        
,test_max_gre_score                         
,test_avg_gre_score                         
,test_num_gre_attempts                      
,test_first_gmat_score                      
,test_last_gmat_score                       
,test_max_gmat_score                        
,test_avg_gmat_score                        
,test_num_gmat_attempts                     
,total_balances                             
,total_future_balances                      
,total_current_balances                     
,total_pd_balances                          
,total_pd_lt30_balances                     
,total_pd_gt30_balances                     
,total_pd_gt60_balances                     
,total_pd_gt90_balances                     
,total_pd_gt120_balances                    
,pri_plan_effdt                             
,pri_plan_action_dt                         
,pri_education_level_cd                     
,admit_term                                 
,age_as_of_today                            
,reenrollment_type                          
,persistence_flg                            
,degr_confer_dt                             
,degr_completion_term                       
,degr_acad_plan_maj                         
,degr_application_dt                        
,degr_honors_cd                             
,degr_honors_ld                             
,degr_status_dt                             
,degr_gpa                                   
,tot_fa_type                                
,tot_scholarship_disbursed_amount           
,tot_vabenefits_disbursed_amount            
,tot_loan_disbursed_amount                  
,tot_grant_disbursed_amount                 
,tot_workstudy_disbursed_amount             
,tot_waiver_disbursed_amount                
,tot_fellowship_disbursed_amount            
,tot_other_disbursed_amount                 
,tot_disbursed_amount                       
,fa_type                                    
,scholarship_disbursed_amount               
,vabenefits_disbursed_amount                
,loan_disbursed_amount                      
,grant_disbursed_amount                     
,workstudy_disbursed_amount                 
,waiver_disbursed_amount                    
,fellowship_disbursed_amount                
,other_disbursed_amount                     
,disbursed_amount                           
,aid_year                                   
,federal_grant_disbursed_amount             
,federal_loan_disbursed_amount              
,federal_workstudy_disbursed_amount         
,institutional_grant_disbursed_amount       
,institutional_other_disbursed_amount       
,institutional_scholarship_disbursed_amount 
,other_scholarship_disbursed_amount         
,private_grant_disbursed_amount             
,private_loan_disbursed_amount              
,private_scholarship_disbursed_amount       
,state_grant_disbursed_amount               
,state_scholarship_disbursed_amount         
,federal_grant_offer_amount                 
,federal_loan_offer_amount                  
,federal_workstudy_offer_amount             
,institutional_grant_offer_amount           
,institutional_other_offer_amount           
,institutional_scholarship_offer_amount     
,other_scholarship_offer_amount             
,private_grant_offer_amount                 
,private_loan_offer_amount                  
,private_scholarship_offer_amount           
,state_grant_offer_amount                   
,state_scholarship_offer_amount             
,offer_amount                               
,federal_grant_accept_amount                
,federal_loan_accept_amount                 
,federal_workstudy_accept_amount            
,institutional_grant_accept_amount          
,institutional_other_accept_amount          
,institutional_scholarship_accept_amount    
,other_scholarship_accept_amount            
,private_grant_accept_amount                
,private_loan_accept_amount                 
,private_scholarship_accept_amount          
,state_grant_accept_amount                  
,state_scholarship_accept_amount            
,accept_amount                              
,current_fa_app_date                        
,first_fa_app_date                          
,pell_eligibility                           
,adj_efc                                    
,efc_status                                 
,titleiv_elig                               
,ssa_citizenshp_ind                         
,num_family_members                         
,number_in_college                          
,veteran                                    
,graduate_student                           
,married                                    
,marital_stat                               
,orphan                                     
,dependents                                 
,agi                                        
,school_choice_1                            
,housing_code_1                             
,school_choice_2                            
,housing_code_2                             
,school_choice_3                            
,housing_code_3                             
,school_choice_4                            
,housing_code_4                             
,school_choice_5                            
,housing_code_5                             
,school_choice_6                            
,housing_code_6                             
,first_bach_degree                          
,interested_in_ws                           
,interested_in_sl                           
,interested_in_plus                         
,degree_certif                              
,current_grade_lvl                          
,depndncy_stat                              
,citizenship_status                         
,children                                   
,sfa_active_duty                            
,parent_marital_stat                        
,parent_number_in_family                    
,parent_num_in_college                      
,parent_agi                                 
,father_grade_lvl                           
,mother_grade_lvl                           
,parent_sfa_grant_aid                       
,fed_efc                                    
,efc_status2                                
,fed_need_base_aid                          
,fed_year_coa                               
,inst_year_coa                              
,pell_year_coa                              
,fed_need                                   
,inst_need                                  
,fed_unmet_need                             
,inst_unmet_need                            
,fed_stdnt_contrb                           
,isir_calc_sc                               
,isir_calc_efc                              
,time_first_enrl_add_day_key                
,tot_courses_taken                          
,tot_courses_withdrawn                      
,tot_unt_billing                            
,tot_unt_earned                             
,tot_grade_points                           
,tot_grade_points_per_unit                  
,tot_term_end_dt                            
,tot_term_begin_dt                          
,tot_first_course_id                        
,tot_first_crse_offer_nbr                   
,tot_first_session_cd                       
,tot_first_class_section                    
,tot_first_class_grade_point                
,tot_fin_tot_payment_amt                    
,tot_fin_tuition                            
,tot_fin_tuition_payment                    
,tot_fin_fees                               
,tot_fin_fee_payment                        
,tot_fin_total_charges                      
,tot_fin_credit_hours                       
,tot_fin_corporate_waiver_amt               
,tot_time_enrl_add_day_key                  
,tot_time_min_enrl_drop_day_key             
,tot_time_enrl_drop_day_key                 
,fin_tot_payment_amt                        
,fin_tuition                                
,fin_tuition_payment                        
,fin_fees                                   
,fin_fee_payment                            
,fin_total_charges                          
,fin_credit_hours                           
,fin_corporate_waiver_amt                   
,time_enrl_add_day_key                      
,time_enrl_drop_day_key                     
,courses_taken                              
,unt_billing                                
,unt_earned                                 
,grade_points                               
,grade_points_per_unit                      
,term_end_dt                                
,term_begin_dt                              
,first_course_id                            
,first_crse_offer_nbr                       
,first_session_cd                           
,first_class_section                        
,first_class_grade_point                    
,time_min_enrl_drop_day_key                 
,courses_withdrawn                          
,ret_cohort_strm                            
,ret_full_part_time_ld                      
,retention_year_1_flag                      
,retention_year_1_status                    
,retention_year_2_flag                      
,retention_year_2_status                    
,retention_year_3_flag                      
,retention_year_3_status                    
,retention_year_4_flag                      
,retention_year_4_status                    
,retention_year_5_flag                      
,retention_year_5_status                    
,retention_year_6_flag                      
,retention_year_6_status                    
,retention_year_7_flag                      
,retention_year_7_status                    
,retention_year_8_flag                      
,retention_year_8_status                    
,retention_year_9_flag                      
,retention_year_9_status                    
,retention_year_10_flag                     
,retention_year_10_status                   
,term_end_day                               
,term_begin_day                             
,accepted_dt                                
,stdnt_admit_type_cd                        
,stdnt_admit_type_ld                        
,app_admit_term                             
,app_dt                                     
,retention_flag                             
,retention_status                           
,term_year_ld                               
,fiscal_yr                                  
,collapsed_strm                             
,term_type_ld                               
,calendar_yr                                
,academic_yr                                
,collapsed_term_year_ld                     
,next_strm                                  
,major_start_dt                             
,career_start_dt                            
,citizenship_country_status_cd              
,citizenship_country_status_ld              
,pri_acad_plan_dept_cd                      
,pri_acad_plan_dept_ld                      
,urm_status                                 
,act_band                                   
,sat_band                                   
,honors_acad_prog_ind                       
,military_status_cd                         
,military_status_ld                         
,cip2000_cd                                 
,cip2000_ld                                 
,acad_stndng_actn_cd_bot                    
,acad_stndng_actn_ld_bot                    
,leave_of_absence_ind                       
,approved_acad_load_cd                      
,approved_acad_load_sd                      
,approved_acad_load_ld                      
,pri_bhe_api_hegis_cd                       
,sec_bhe_api_hegis_cd                       
,stdnt_admit_type_group                     
,stdnt_dev_ed_engl_ind                      
,stdnt_dev_ed_math_ind                      
,stdnt_dev_ed_ind                           
,hs_inst_name_and_location                  
,orig_address_term_src_sys_cd               
,orig_home_address1                         
,orig_home_address2                         
,orig_home_address_type                     
,orig_home_city                             
,orig_home_country_ld                       
,orig_home_geog_county_ld                   
,orig_home_postal                           
,orig_home_state_cd                         
,orig_home_state_ld                         
,orig_home_postal_loc_y                     
,orig_home_postal_loc_x                     
,stdnt_status_cd                            
,stdnt_status_ld                            
,stu_current_type_cd                        
,stu_current_type_ld                        
,student_hiatus_cd                          
,student_hiatus_ld                          
,student_hiatus_reason_cd                   
,student_hiatus_reason_ld                   
,stem_category                              
,appl_current_location                      
,stdnt_first_gen_flag                       
,appl_rating                                
,student_hiatus_end_dt                      
,rel_strm                                   
,next_rel_strm                              
,hs_proximity                               
,orig_home_proximity                        
,pri_reporting_acad_program                 
,pri_reporting_acad_dept_cd                 
,pri_reporting_acad_dept_ld                 
,pri_reporting_acad_school                  
,erp_geo_break                              
,samia_scholars_flag                        
,all_pass_fail                              
,fa_pell_disb_flag                          
,athlete_flag                               
,sport_codes                                
,honors_ind                                 
,honors_ever_ind                            
,acad_plan_maj1_2                           
,maj1_2_degree_cd                           
,maj1_2_degree_ld                           
,maj1_2_acad_prog_cd                        
,maj1_2_acad_prog_ld                        
,maj1_2_acad_plan_ld                        
,maj1_2_grouping_ld                         
,maj1_2_grouping_cd                         
,maj1_2_department_cd                       
,maj1_2_department_ld                       
,maj1_2_acad_plan_type_ld                   
,maj1_2_acad_plan_degree                    
,maj1_2_secondary_school_ld                 
,maj1_2_secondary_school_cd                 
,acad_plan_min1_2                           
,min1_2_degree_cd                           
,min1_2_degree_ld                           
,min1_2_acad_prog_cd                        
,min1_2_acad_prog_ld                        
,min1_2_acad_plan_ld                        
,min1_2_grouping_ld                         
,min1_2_grouping_cd                         
,min1_2_department_cd                       
,min1_2_department_ld                       
,min1_2_acad_plan_type_ld                   
,min1_2_acad_plan_degree                    
,min1_2_secondary_school_ld                 
,min1_2_secondary_school_cd                 
,acad_plan_min1_3                           
,min1_3_degree_cd                           
,min1_3_degree_ld                           
,min1_3_acad_prog_cd                        
,min1_3_acad_prog_ld                        
,min1_3_acad_plan_ld                        
,min1_3_grouping_ld                         
,min1_3_grouping_cd                         
,min1_3_department_cd                       
,min1_3_department_ld                       
,min1_3_acad_plan_type_ld                   
,min1_3_acad_plan_degree                    
,min1_3_secondary_school_ld                 
,min1_3_secondary_school_cd                 
,tesol_flag                                 
,ever_stdnt_first_gen_flag                  
,institutional_grant_disbursed_amount_term  
,rolling_admit_su_adv_flag                  
,rolling_max_su_adv_flag                    
,rolling_su_adv_plan_flag                   
,su_adv_ind
,stdnt_type_cd
,stdnt_type_ld
,stdnt_type_collapsed_cd
,stdnt_type_collapsed_ld             
)
select
    ffst.src_sys_cd
	,NULL as residency_cd
	,NULL as tuition_residency_sd
	,NULL as tuition_residency_ld
	,NULL as bhe_residency_cd
	,NULL as em_residency_sd
	,NULL as residency_ld
	,NULL as em_residency_ld
	,NULL as residency_sd
	,NULL as em_residency_cd
	,NULL as tuition_residency_cd
    ,ffst.ipeds_billing_credits
    ,ffst.ipeds_taken_credits
    ,ffst.ipeds_credits_toward_progress
    ,ffst.career_billing_credits
    ,ffst.career_taken_credits
    ,ffst.career_credits_toward_progress
    ,ffst.ipeds_total_classes
    ,ffst.career_total_classes
    ,ffst.career_gpa_sem
    ,ffst.career_gpa_cum
    ,ffst.career_unt_audit
    ,ffst.career_unt_trnsfr
    ,ffst.career_unt_test_credit
    ,ffst.career_unt_term_tot
    ,ffst.career_grade_points
    ,ffst.career_tot_audit
    ,ffst.career_tot_trnsfr
    ,ffst.career_tot_other
    ,ffst.career_tot_test_credit
    ,ffst.career_tot_cumulative
    ,ffst.career_tot_grade_points
    ,ffst.acad_level_bot_cd
    ,ffst.acad_level_bot_ld
    ,ffst.eligible_to_enroll_ind
    ,ffst.em_include_in_reports_ind
    ,NULL as full_part_time_cd_calc
    ,ffst.full_part_time_ld
    ,ffst.full_part_time_cd
    ,ffst.full_part_time_sd
    ,NULL as full_time_ind_calc
    ,ffst.ipeds_attend_type_cd
    ,ffst.ipeds_attend_type_ld
    ,ffst.ipeds_attend_type_sd
    ,ffst.ipeds_include_in_reports_ind
    ,ffst.officially_registered_ind
    ,ffst.reg_activity_ind
    ,ffst.registered_ind
    ,ffst.univ_attend_type_cd
    ,ffst.univ_attend_type_ld
    ,ffst.univ_attend_type_sd
    ,ffst.independent_study_ind
    ,ffst.mixed_ind
    ,ffst.online_ind
    ,ffst.f2f_ind
    ,ffst.hybrid_ind
    ,ffst.missing_ind
    ,ffst.other_ind
    ,ffst.src_sys_exp_dt
    ,ffst.eff_dt
    ,ffst.exp_dt
    ,ffst.stdnt_instr_mode_cd
    ,ffst.src_sys_eff_dt
    ,ffst.emplid
	,ffst.uid
    ,ffst.strm
    ,ffst.acad_career
    ,ffst.institution
    ,aos1.stdnt_car_nbr
    ,ffst.career_unt_passd_gpa
    ,ffst.career_unt_passd_nogpa
    ,ffst.ipeds_career_ind
    ,ffst.full_time_bhe_ind
    ,ffst.collapsed_strm_stdnt_term
    ,replace(COALESCE
      (CASE
          WHEN dcpp_maj1.acad_prog_cd IS NULL OR dcpp_maj1.acad_prog_cd = '*' THEN dim_program.acad_prog_cd
          ELSE dcpp_maj1.acad_prog_cd
        END,'*'),'_',' ') AS primary_acad_prog
    ,aos1.stdnt_car_nbr as primary_stdnt_car_nbr
    ,ffst.ir_stdnt_campus_cd
    ,ffst.bhe_residency_ld
    ,ffst.stdnt_instr_mode_ld
    ,ffst.stdnt_instr_mode_sd
    ,ffst.ir_collapsed_acad_career
    ,ffst.plan_attend_type_cd
    ,ffst.ir_new_to_inst
    ,ffst.prog_attend_type_cd
    ,ffst.form_of_study
    ,ffst.unt_taken_prgrss
    ,ffst.unt_passd_prgrss
    ,ffst.acad_level_eot_cd
    ,ffst.acad_level_eot_ld
    ,NULL as withdraw_date -- ,ffst.withdraw_date
    ,ffst.withdraw_cd
    ,ffst.withdraw_ld
    ,ffst.withdraw_reason_cd
    ,ffst.withdraw_reason_ld
    ,NULL as last_date_attended --,ffst.last_date_attended
    ,NULL as acad_stndng_actn_cd --,ffst.acad_stndng_actn_cd
    ,NULL as acad_stndng_actn_ld
    ,NULL as career_unt_taken_nogpa
    ,NULL as career_unt_taken_gpa
    ,NULL as student_term_fte
    ,NULL as student_term_exclude_flag
    ,NULL as pri_degree_level
    ,NULL as audit_only
	,dim_address_term.address_cd as address_term_src_sys_cd	  
	,NULL as home_address1	              
	,NULL as home_address2
	,dim_address_term.address_type_ld     as home_address_type
	,dim_address_term.county              as home_city	
	,dim_address_term.country_ld          as home_country_ld	
	,dim_address_term.geog_fips_county_cd as home_geog_county_ld	
	,dim_address_term.postal              as home_postal	
	,dim_address_term.state_cd            as home_state_cd	
	,dim_address_term.state_ld            as home_state_ld	
    ,home_zip.postal_loc_y         		  as home_postal_loc_y
    ,home_zip.postal_loc_x        		  as home_postal_loc_x
    ,NULL as dorm_address_term_src_sys_cd
    ,NULL as dorm_address1
    ,NULL as dorm_address2
    ,NULL as dorm_address_type
    ,NULL as dorm_city
    ,NULL as dorm_country_ld
    ,NULL as dorm_geog_county_ld
    ,NULL as dorm_postal
    ,NULL as dorm_state_cd
    ,NULL as dorm_state_ld
    ,NULL as dorm_postal_loc_y
    ,NULL as dorm_postal_loc_x
    ,person_student.birthdate_day
    ,person_student.first_name
    ,person_student.gender_cd
    ,NULL as ipeds_race_ethn_citz_ld -- ,person_student.ipeds_race_ethn_citz_ld
    ,person_student.last_name
    ,person_student.married_status_ld
    ,person_student.bhe_birth_year
    ,phone_home.phone_email as stdnt_home_phone
    ,work_phone.phone_email as stdnt_cell_phone
    ,home_email.phone_email as stdnt_home_email
    ,busn_email.phone_email as stdnt_business_email 
    ,NULL as stdnt_campus_email
    ,NULL as advisor_emplid
    ,NULL as advisor_name_first_last
    ,NULL as advisor_business_email
    ,NULL as advisor_campus_email
    ,NULL as advisor_home_email
    ,dcpp_maj1.degree_cd AS pri_degree_cd
    ,dcpp_maj1.degree_ld AS pri_degree_ld
    ,ffst.primary_acad_prog AS pri_acad_prog_ld
    ,ffst.primary_acad_prog AS pri_acad_prog_cd
    ,replace(dcpp_maj1.school_ld,'_',' ') AS pri_school_ld
    ,dcpp_maj1.school_cd AS pri_school_cd
    ,dcpp_maj1.specialization_ld AS pri_specialization_ld
    ,dcpp_maj1.acad_plan_ld AS pri_acad_plan_ld
    ,dcpp_maj1.acad_plan_cd AS pri_acad_plan_cd
    ,dcpp_maj1.acad_prog_deg_type_ld  AS pri_education_level_ld
    ,dcpp_maj1.institution_cd AS pri_institution_cd
    ,dcpp_maj1.institution_ld AS pri_institution_ld
    ,dcpp_maj1.grouping_ld AS pri_grouping_ld
    ,dcpp_maj1.grouping_cd AS pri_grouping_cd
    ,replace(dcpp_maj1.department_ld,'_',' ') AS pri_department_ld
    ,dcpp_maj1.study_field_cd AS pri_study_field_cd
    ,dcpp_maj1.study_field_ld AS pri_study_field_ld
    ,dcpp_maj1.bhe_api_hegis_ld AS pri_bhe_api_hegis_ld
    ,dcpp_maj1.acad_plan_type_ld AS pri_acad_plan_type_ld
    --these fields are for the second major but we use a min alias
    ,dcpp_min1.degree_cd AS sec_degree_cd
    ,dcpp_min1.degree_ld AS sec_degree_ld
    ,dcpp_min1.acad_prog_ld AS sec_acad_prog_ld
    ,dcpp_min1.acad_prog_cd AS sec_acad_prog_cd
    ,dcpp_min1.school_ld AS sec_school_ld
    ,dcpp_min1.school_cd AS sec_school_cd
    ,dcpp_min1.specialization_ld AS sec_specialization_ld
    ,dcpp_min1.acad_plan_ld AS sec_acad_plan_ld
    ,dcpp_min1.acad_plan_cd AS sec_acad_plan_cd
    ,dcpp_min1.acad_prog_deg_type_ld  AS sec_education_level_ld
    ,dcpp_min1.institution_cd AS sec_institution_cd
    ,dcpp_min1.institution_ld AS sec_institution_ld
    ,dcpp_min1.grouping_ld AS sec_grouping_ld
    ,dcpp_min1.grouping_cd AS sec_grouping_cd
    ,dcpp_min1.department_ld AS sec_department_ld
    ,dcpp_min1.study_field_cd AS sec_study_field_cd
    ,dcpp_min1.study_field_ld AS sec_study_field_ld
    ,dcpp_min1.bhe_api_hegis_ld AS sec_bhe_api_hegis_ld
    ,dcpp_min1.acad_plan_type_ld AS sec_acad_plan_type_ld
    ,NULL as online_pct
    ,NULL as unt_inprog_gpa
    ,NULL as unt_inprog_nogpa
    ,NULL as trf_taken_gpa
    ,NULL as trf_taken_nogpa
    ,NULL as trf_grade_points
    ,NULL as trf_passed_gpa
    ,NULL as trf_passed_nogpa
    ,NULL as tc_units_adjust
    ,NULL as unt_other
    ,NULL as tot_taken_gpa
    ,NULL as tot_taken_nogpa
    ,NULL as tot_passd_gpa
    ,NULL as tot_passd_nogpa
    ,NULL as tot_inprog_nogpa
    ,NULL as tot_taken_prgrss
    ,NULL as tot_passd_prgrss
    ,NULL as stack_begin_term_code
    ,NULL as cohort_code
    ,NULL as cohort_degree_level
    ,NULL as pers_primary_ethnic_group
    ,NULL as pers_primary_grp_ldesc
    ,NULL as age
    ,person_student.visa_permit_type
	,person_student.natl_id_country_ld as pri_citizenship_country	    	--    ,person_student.pri_citizenship_country
	,person_student.natl_id_country_cd as pri_citizenship_country_code      --    ,person_student.pri_citizenship_country_code
	,person_student.natl_id_country_sd as pri_citizenship_country_code_iso  --    ,person_student.pri_citizenship_country_code_iso
	,person_student.natl_id_country_cd as citizenship_country_iso		    --    ,person_student.citizenship_country_iso
    ,person_student.usa_citizenship_status
    ,person_student.usa_citizenship_status_code
    ,NULL as va_benefit_code 
    ,person_student.visa_permit_type_code
    ,NULL as part_time_calc
    ,NULL as admissions_residency
    ,NULL as admissions_residency_code
    ,NULL as federal_fa_residency
    ,NULL as federal_fa_residency_code
    ,NULL as residency_date
    ,NULL as state_fa_residency
    ,NULL as state_fa_residency_code
    ,NULL as distance_percent
	,NULL as distance_type	--    ,ffst.distance_type
	,NULL as enrolled_load_code	--    ,ffst.enrolled_load_code
	,NULL as enrolled_load_desc	--    ,ffst.enrolled_load_desc
    ,dcpp_maj1.degree_cd  as pri_acad_plan_degree
    ,NULL as readmit_stack_flag
    ,CASE
      WHEN aos1.enrlmtr_deg_seeking_ind = 'Y' THEN 1
      ELSE 0
    END as degree_seeking_flag
    ,NULL as dorm_residency
    ,CASE
      WHEN aos1.enrlmtr_deg_seeking_ind = 'Y' AND dcpp_maj1.acad_career_cd = 'GRAD' THEN 1
      ELSE 0
    END as grad_degree
    ,NULL as honors_flag
    ,NULL as nondegree_flag
    ,aos1.exp_grad_term AS expected_graduation_term_code
    ,aos1.degr_chkout_stat_cd AS latest_degree_checkout_status_code
    ,aos1.degr_chkout_stat_sd AS latest_degree_checkout_status_desc
    ,NULL as locl_country
    ,NULL as locl_country_ldesc
    ,NULL as locl_postal
    ,NULL as locl_state
    ,NULL as locl_state_desc
	,NULL as mail_country	--    ,dim_address_term.mail_country
	,NULL as mail_country_ldesc	--    ,dim_address_term.mail_country_ldesc
	,NULL as mail_src_sys_cd	--    ,dim_address_term.mail_src_sys_cd
	,NULL as mail_postal	--    ,dim_address_term.mail_postal
	,NULL as mail_state	--    ,dim_address_term.mail_state
	,NULL as mail_state_desc	--    ,dim_address_term.mail_state_desc
	,dim_address_term.county              as perm_city	--    ,dim_address_term.perm_city
	,dim_address_term.country_cd         as perm_country	--    ,dim_address_term.perm_country
	,dim_address_term.country_ld as perm_country_lon_description	--    ,dim_address_term.perm_country_lon_description
	,dim_address_term.state_cd           as perm_state	--    ,dim_address_term.perm_state
	,dim_address_term.state_ld             as perm_state_desc	--    ,dim_address_term.perm_state_desc           
	,dim_address_term.postal  as perm_zip_code	--    ,dim_address_term.perm_zip_code
    ,NULL as dorm_zip_code
    ,NULL as freeze_strm
    ,aos1.stdnt_campus_sd
    ,aos1.stdnt_campus_ld
    ,dcpp_maj1.acad_career_cd AS stdnt_acad_career_cd
    ,dcpp_maj1.acad_career_collapsed_cd AS stdnt_acad_car_collapsed_cd
    ,dcpp_maj1.acad_career_collapsed_ld AS stdnt_acad_car_collapsed_ld
    ,dcpp_maj1.acad_career_ld AS stdnt_acad_career_ld
    ,NULL as term_other_units
    ,NULL as term_in_progress_units_not_for_gpa
    ,NULL as term_transfer_earned_units_for_gpa
    ,NULL as cum_in_progress_units_not_for_gpa
    ,NULL as latest_degree_checkout_status
    ,NULL as primary_plan_application_number
    ,person_student.middle_name
    ,person_student.name_first_last
	,NULL as military_status	--    ,person_student.military_status
	,NULL as military_status_code	--    ,person_student.military_status_code
    ,NULL as custom_ipeds_ethnicity_ld
    ,NULL as custom_ipeds_ethnicity_cd
    ,NULL as ipeds_ethnicity_ld
    ,NULL as ipeds_ethnicity_cd
    ,NULL as birth_date
    ,NULL as term_in_progress_units_for_gpa
    ,NULL as term_transfer_units
    ,NULL as total_combined_term_units
    ,NULL as cum_combined_grade_points
    ,NULL as cum_combined_graded_units_for_gpa
    ,NULL as cum_combined_earned_units_not_for_gpa
    ,NULL as cum_combined_earned_units_for_gpa
    ,NULL as cum_in_progress_units_for_gpa
    ,NULL as soc_flag
    ,NULL as urm_flag
    ,NULL as plan_cip_id
    ,NULL as campus_latitude
    ,NULL as campus_longitude
    ,aos1.exp_grad_term AS exp_grad_term
    ,aos1.degr_chkout_stat_cd
    ,aos1.degr_chkout_stat_sd
    ,null as exp_grad_term_year_ld
    ,NULL as exp_grad_year
    ,replace(COALESCE
      (CASE
          WHEN dcpp_maj1.acad_prog_cd IS NULL OR dcpp_maj1.acad_prog_cd = '*' THEN dim_program.acad_prog_cd
          ELSE dcpp_maj1.acad_prog_cd
        END,'*'),'_',' ') as pri_acad_prog
    ,aos1.prog_status_cd as pri_prog_status_cd
    , aos1.prog_status_ld as pri_prog_status_ld
    ,NULL as pri_prog_action_cd
    ,NULL as pri_prog_action_ld
    ,NULL as pri_prog_reason_cd
    ,NULL as pri_prog_reason_ld
	,dim_career.collapsed_acad_career_cd as acad_career_collapsed_cd	--    ,dim_career.acad_career_collapsed_cd
	,dim_career.collapsed_acad_career_ld as acad_career_collapsed_ld	--    ,dim_career.acad_career_collapsed_ld
	,dim_career.acad_career_ld as acad_career_ld
	,NULL as perm_address1	--    ,dim_address_term.perm_address1
	,NULL as perm_address2	--    ,dim_address_term.perm_address2
	,dim_address_term.address_type_ld as perm_address_type	--    ,dim_address_term.perm_address_type
	,dim_address_term.country_ld as perm_country_ld	--    ,dim_address_term.perm_country_ld
	,dim_address_term.geog_fips_county_cd as perm_geog_county_ld	--    ,dim_address_term.perm_geog_county_ld
	,dim_address_term.postal as perm_postal	--    ,dim_address_term.perm_postal
	,dim_address_term.state_cd as perm_state_cd	--    ,dim_address_term.perm_state_cd
	,dim_address_term.state_ld as perm_state_ld	--    ,dim_address_term.perm_state_ld
    ,NULL as stdnt_campus_cd
    ,NULL as trn_pi_totalcredit
    ,NULL as trn_ls_school_type
    ,NULL as trn_ls_school_type_ld
    ,NULL as trn_transfer_inst_name
    ,NULL as trn_school_type_lpi
    ,NULL as hs_final_gpa
    ,NULL as hs_term_year
    ,NULL as hs_highest_ed_lvl
    ,NULL as hs_final_gpa_excl_last_strm
    ,NULL as hs_last_year
    ,NULL as hs_start_year
    ,NULL as hs_years_attended
    ,NULL as hs_class_pop
    ,NULL as hs_rank
    ,NULL as hs_unt_comp_total
    ,NULL as hs_name
    ,NULL as hs_short_name
    ,NULL as hs_proprietorship
    ,NULL as hs_address1
    ,NULL as hs_address2
    ,NULL as hs_city
    ,NULL as hs_county
    ,NULL as hs_state
    ,NULL as hs_postal
    ,NULL as hs_country
    ,NULL as hs_lat
    ,NULL as hs_long
    ,NULL as test_super_act_score
    ,NULL as test_super_sat_score
    ,NULL as test_first_act_score
    ,NULL as test_last_act_score
    ,NULL as test_max_act_score
    ,NULL as test_avg_act_score
    ,NULL as test_num_act_attempts
    ,NULL as test_first_sat_score
    ,NULL as test_last_sat_score
    ,NULL as test_max_sat_score
    ,NULL as test_avg_sat_score
    ,NULL as test_num_sat_attempts
    ,NULL as stdnt_cntct_latitude
    ,NULL as stdnt_cntct_longitude
	,NULL as mail_address1	--    ,dim_address_term.mail_address1
	,NULL as mail_address2	--    ,dim_address_term.mail_address2
	,NULL as mail_address_type	--    ,dim_address_term.mail_address_type
	,NULL as mail_city	--    ,dim_address_term.mail_city
	,NULL as mail_country_ld	--    ,dim_address_term.mail_country_ld
	,NULL as mail_geog_county_ld	--    ,dim_address_term.mail_geog_county_ld
	,NULL as mail_state_cd	--    ,dim_address_term.mail_state_cd
	,NULL as mail_state_ld	--    ,dim_address_term.mail_state_ld
    ,NULL as mail_postal_loc_y
    ,NULL as mail_postal_loc_x
    ,NULL as resh_address1
    ,NULL as resh_address_type
    ,NULL as resh_city
    ,NULL as resh_postal
    ,NULL as resh_country_ld
    ,NULL as resh_geog_county_ld
    ,NULL as resh_state_cd
    ,NULL as resh_state_ld
    ,NULL as test_first_gre_score
    ,NULL as test_last_gre_score
    ,NULL as test_max_gre_score
    ,NULL as test_avg_gre_score
    ,NULL as test_num_gre_attempts
    ,NULL as test_first_gmat_score
    ,NULL as test_last_gmat_score
    ,NULL as test_max_gmat_score
    ,NULL as test_avg_gmat_score
    ,NULL as test_num_gmat_attempts
    ,NULL as total_balances
    ,NULL as total_future_balances
    ,NULL as total_current_balances
    ,NULL as total_pd_balances
    ,NULL as total_pd_lt30_balances
    ,NULL as total_pd_gt30_balances
    ,NULL as total_pd_gt60_balances
    ,NULL as total_pd_gt90_balances
    ,NULL as total_pd_gt120_balances
    ,aos1.eff_dt    as pri_plan_effdt
    ,aos1.action_dt as pri_plan_action_dt
    ,dcpp_maj1.degree_education_lvl as pri_education_level_cd
    ,aos1.admit_term
    ,NULL as age_as_of_today
    ,NULL as reenrollment_type
    ,NULL as persistence_flg
    ,NULL as degr_confer_dt
    ,NULL as degr_completion_term
    ,NULL as degr_acad_plan_maj
    ,NULL as degr_application_dt
    ,NULL as degr_honors_cd
    ,NULL as degr_honors_ld
    ,NULL as degr_status_dt
    ,NULL as degr_gpa
    ,fin_aid.tot_fa_type
    ,fin_aid.tot_scholarship_disbursed_amount
    ,fin_aid.tot_vabenefits_disbursed_amount
    ,fin_aid.tot_loan_disbursed_amount
    ,fin_aid.tot_grant_disbursed_amount
    ,fin_aid.tot_workstudy_disbursed_amount
    ,fin_aid.tot_waiver_disbursed_amount
    ,fin_aid.tot_fellowship_disbursed_amount
    ,fin_aid.tot_other_disbursed_amount
    ,fin_aid.tot_disbursed_amount
    ,fin_aid.fa_type
    ,fin_aid.scholarship_disbursed_amount
    ,fin_aid.vabenefits_disbursed_amount
    ,fin_aid.loan_disbursed_amount
    ,fin_aid.grant_disbursed_amount
    ,fin_aid.workstudy_disbursed_amount
    ,fin_aid.waiver_disbursed_amount
    ,fin_aid.fellowship_disbursed_amount
    ,fin_aid.other_disbursed_amount
    ,fin_aid.disbursed_amount
    ,fin_aid.aid_year
    ,fin_aid.federal_grant_disbursed_amount
    ,fin_aid.federal_loan_disbursed_amount
    ,fin_aid.federal_workstudy_disbursed_amount
    ,fin_aid.institutional_grant_disbursed_amount
    ,fin_aid.institutional_other_disbursed_amount
    ,fin_aid.institutional_scholarship_disbursed_amount
    ,fin_aid.other_scholarship_disbursed_amount
    ,fin_aid.private_grant_disbursed_amount
    ,fin_aid.private_loan_disbursed_amount
    ,fin_aid.private_scholarship_disbursed_amount
    ,fin_aid.state_grant_disbursed_amount
    ,fin_aid.state_scholarship_disbursed_amount
    ,fin_aid.federal_grant_offer_amount
    ,fin_aid.federal_loan_offer_amount
    ,fin_aid.federal_workstudy_offer_amount
    ,fin_aid.institutional_grant_offer_amount
    ,fin_aid.institutional_other_offer_amount
    ,fin_aid.institutional_scholarship_offer_amount
    ,fin_aid.other_scholarship_offer_amount
    ,fin_aid.private_grant_offer_amount
    ,fin_aid.private_loan_offer_amount
    ,fin_aid.private_scholarship_offer_amount
    ,fin_aid.state_grant_offer_amount
    ,fin_aid.state_scholarship_offer_amount
    ,fin_aid.offer_amount
    ,fin_aid.federal_grant_accept_amount
    ,fin_aid.federal_loan_accept_amount
    ,fin_aid.federal_workstudy_accept_amount
    ,fin_aid.institutional_grant_accept_amount
    ,fin_aid.institutional_other_accept_amount
    ,fin_aid.institutional_scholarship_accept_amount
    ,fin_aid.other_scholarship_accept_amount
    ,fin_aid.private_grant_accept_amount
    ,fin_aid.private_loan_accept_amount
    ,fin_aid.private_scholarship_accept_amount
    ,fin_aid.state_grant_accept_amount
    ,fin_aid.state_scholarship_accept_amount
    ,fin_aid.accept_amount
    ,dim_isir.current_fa_app_date
    ,dim_isir.first_fa_app_date
    ,dim_isir.pell_eligibility
    ,dim_isir.adj_efc
    ,dim_isir.efc_status
    ,dim_isir.titleiv_elig
    ,dim_isir.ssa_citizenshp_ind
    ,dim_isir.num_family_members
    ,dim_isir.number_in_college
    ,dim_isir.veteran
    ,dim_isir.graduate_student
    ,dim_isir.married
    ,dim_isir.marital_stat
    ,dim_isir.orphan
    ,dim_isir.dependents
    ,dim_isir.agi
    ,dim_isir.school_choice_1
    ,dim_isir.housing_code_1
    ,dim_isir.school_choice_2
    ,dim_isir.housing_code_2
    ,dim_isir.school_choice_3
    ,dim_isir.housing_code_3
    ,dim_isir.school_choice_4
    ,dim_isir.housing_code_4
    ,dim_isir.school_choice_5
    ,dim_isir.housing_code_5
    ,dim_isir.school_choice_6
    ,dim_isir.housing_code_6
    ,dim_isir.first_bach_degree
    ,dim_isir.interested_in_ws
    ,dim_isir.interested_in_sl
    ,dim_isir.interested_in_plus
    ,dim_isir.degree_certif
    ,dim_isir.current_grade_lvl
    ,dim_isir.depndncy_stat
    ,dim_isir.citizenship_status
    ,dim_isir.children
    ,dim_isir.sfa_active_duty
    ,dim_isir.parent_marital_stat
    ,dim_isir.parent_number_in_family
    ,dim_isir.parent_num_in_college
    ,dim_isir.parent_agi
    ,dim_isir.father_grade_lvl
    ,dim_isir.mother_grade_lvl
    ,dim_isir.parent_sfa_grant_aid
    ,dim_isir.fed_efc
    ,dim_isir.efc_status2
    ,dim_isir.fed_need_base_aid
    ,dim_isir.fed_year_coa
    ,dim_isir.inst_year_coa
    ,dim_isir.pell_year_coa
    ,dim_isir.fed_need
    ,dim_isir.inst_need
    ,dim_isir.fed_unmet_need
    ,dim_isir.inst_unmet_need
    ,dim_isir.fed_stdnt_contrb
    ,dim_isir.isir_calc_sc
    ,dim_isir.isir_calc_efc
    ,enrl.time_first_enrl_add_day_key
    ,enrl.tot_courses_taken
    ,enrl.tot_courses_withdrawn
    ,enrl.tot_unt_billing
    ,enrl.tot_unt_earned
    ,enrl.tot_grade_points
    ,enrl.tot_grade_points_per_unit
    ,enrl.tot_term_end_dt
    ,enrl.tot_term_begin_dt
    ,null as tot_first_course_id
    ,null as tot_first_crse_offer_nbr
    ,null as tot_first_session_cd
    ,null as tot_first_class_section
    ,enrl.tot_first_class_grade_point
    ,enrl.tot_fin_tot_payment_amt
    ,enrl.tot_fin_tuition
    ,enrl.tot_fin_tuition_payment
    ,enrl.tot_fin_fees
    ,enrl.tot_fin_fee_payment
    ,enrl.tot_fin_total_charges
    ,enrl.tot_fin_credit_hours
    ,enrl.tot_fin_corporate_waiver_amt
    ,enrl.tot_time_enrl_add_day_key
    ,enrl.tot_time_min_enrl_drop_day_key
    ,enrl.tot_time_enrl_drop_day_key
    ,enrl.fin_tot_payment_amt
    ,enrl.fin_tuition
    ,enrl.fin_tuition_payment
    ,enrl.fin_fees
    ,enrl.fin_fee_payment
    ,enrl.fin_total_charges
    ,enrl.fin_credit_hours
    ,enrl.fin_corporate_waiver_amt
    ,enrl.time_enrl_add_day_key
    ,enrl.time_enrl_drop_day_key
    ,enrl.courses_taken
    ,enrl.unt_billing
    ,enrl.unt_earned
    ,enrl.grade_points
    ,enrl.grade_points_per_unit
    ,enrl.term_end_dt
    ,enrl.term_begin_dt
    ,null as first_course_id
    ,null as tot_first_crse_offer_nbr
    ,null as first_session_cd
    ,null as first_class_section
    ,null as first_class_grade_point
    ,enrl.time_min_enrl_drop_day_key
    ,enrl.courses_withdrawn
    ,NULL as ret_cohort_strm
    ,NULL as ret_full_part_time_ld
    ,NULL as retention_year_1_flag
    ,NULL as retention_year_1_status
    ,NULL as retention_year_2_flag
    ,NULL as retention_year_2_status
    ,NULL as retention_year_3_flag
    ,NULL as retention_year_3_status
    ,NULL as retention_year_4_flag
    ,NULL as retention_year_4_status
    ,NULL as retention_year_5_flag
    ,NULL as retention_year_5_status
    ,NULL as retention_year_6_flag
    ,NULL as retention_year_6_status
    ,NULL as retention_year_7_flag
    ,NULL as retention_year_7_status
    ,NULL as retention_year_8_flag
    ,NULL as retention_year_8_status
    ,NULL as retention_year_9_flag
    ,NULL as retention_year_9_status
    ,NULL as retention_year_10_flag
    ,NULL as retention_year_10_status
    ,CASE
      WHEN dim_term.term_end_day_key = 0 THEN NULL
      ELSE TO_DATE (dim_term.term_end_day_key,'J')
    END AS term_end_day
    ,CASE
      WHEN dim_term.term_begin_day_key = 0 THEN NULL
      ELSE TO_DATE (dim_term.term_begin_day_key,'J')
    END AS term_begin_day
    ,NULL as accepted_dt
    ,NULL as stdnt_admit_type_cd
    ,NULL as stdnt_admit_type_ld
    ,NULL as app_admit_term
    ,NULL as app_dt
    ,NULL as retention_flag
    ,NULL as retention_status
    ,dim_term.term_year_ld
    ,dim_term.fiscal_yr
    ,dim_term.collapsed_strm
    ,dim_term.term_type_ld
    ,dim_term.calendar_yr
    ,dim_term.academic_yr
    ,dim_term.collapsed_term_year_ld
    ,NVL (LEAD (ffst.strm) OVER (PARTITION BY ffst.emplid,ffst.acad_career ORDER BY ffst.strm),'9999') AS next_strm
    ,NULL as major_start_dt
    ,NULL as career_start_dt
    ,replace(person_student.citizenship_country_status_cd,'_',' ') as citizenship_country_status_cd
    ,replace(person_student.citizenship_country_ld,'_',' ') as citizenship_country_status_ld
    ,dcpp_maj1.department_cd as pri_acad_plan_dept_cd
    ,dcpp_maj1.department_ld as pri_acad_plan_dept_ld
    ,NULL as urm_status
    ,NULL as act_band
    ,NULL as sat_band
    ,NULL as honors_acad_prog_ind
    ,NULL as military_status_cd
    ,NULL as military_status_ld
    ,NULL as cip2000_cd
    ,NULL as cip2000_ld
    ,NULL as acad_stndng_actn_cd_bot
    ,NULL as acad_stndng_actn_ld_bot
    ,NULL as leave_of_absence_ind
    ,NULL as approved_acad_load_cd
    ,NULL as approved_acad_load_sd
    ,NULL as approved_acad_load_ld
    ,NULL as pri_bhe_api_hegis_cd
    ,NULL as sec_bhe_api_hegis_cd
    ,NULL as stdnt_admit_type_group
    ,NULL::integer as stdnt_dev_ed_engl_ind
    ,NULL::integer as stdnt_dev_ed_math_ind
    ,NULL::integer as stdnt_dev_ed_ind
    ,NULL as hs_inst_name_and_location
    ,NULL as orig_address_term_src_sys_cd
    ,NULL as orig_home_address1
    ,NULL as orig_home_address2
    ,NULL as orig_home_address_type
    ,NULL as orig_home_city
    ,NULL as orig_home_country_ld
    ,NULL as orig_home_geog_county_ld
    ,NULL as orig_home_postal
    ,NULL as orig_home_state_cd
    ,NULL as orig_home_state_ld
    ,NULL as orig_home_postal_loc_y
    ,NULL as orig_home_postal_loc_x
	,ffst.stdnt_status_cd
	,ffst.stdnt_status_ld
	,NULL as stu_current_type_cd	--    ,ffst.stu_current_type_cd
	,NULL as stu_current_type_ld	--    ,ffst.stu_current_type_ld
	,ffst.student_hiatus_cd       
	,ffst.student_hiatus_ld       
	,ffst.student_hiatus_reason_cd
	,ffst.student_hiatus_reason_ld
    ,NULL as stem_category
    ,NULL as appl_current_location
    ,NULL as stdnt_first_gen_flag
    ,NULL as appl_rating
    ,NULL as student_hiatus_end_dt	--   ,ffst.student_hiatus_end_dt
    ,dim_term.rel_strm as rel_strm
    ,lead(dim_term.rel_strm) over (partition by ffst.acad_career, ffst.emplid order by dim_term.rel_strm)  as next_rel_strm
    ,NULL as hs_proximity
    ,NULL as orig_home_proximity
    ,NULL as pri_reporting_acad_program
    ,NULL as pri_reporting_acad_dept_cd
    ,NULL as pri_reporting_acad_dept_ld
    ,NULL as pri_reporting_acad_school
    ,NULL as erp_geo_break
    ,NULL as samia_scholars_flag
    ,NULL as all_pass_fail
    ,NULL as fa_pell_disb_flag
    ,NULL as athlete_flag
    ,NULL as sport_codes
    ,NULL as honors_ind
	,NULL as honors_ever_ind
	,aos2.acad_plan as acad_plan_maj1_2
	,dim_program.degree_cd AS maj1_2_degree_cd
	,dim_program.degree_ld AS maj1_2_degree_ld
	,aos1.acad_prog as maj1_2_acad_prog_cd
	,dim_program.acad_prog_ld as maj1_2_acad_prog_ld
	,dcpp_maj2.acad_plan_ld as maj1_2_acad_plan_ld
	,dcpp_maj1.grouping_ld as maj1_2_grouping_ld	--	,dim_program.grouping_ld as maj1_2_grouping_ld
	,dcpp_maj1.grouping_cd as maj1_2_grouping_cd	--	,dim_program.grouping_cd as maj1_2_grouping_cd
	,dim_program.department_cd AS maj1_2_department_cd
	,dim_program.department_ld AS maj1_2_department_ld
	,dcpp_maj2.plan_type_ld as maj1_2_acad_plan_type_ld
	,dim_program.degree_cd AS  maj1_2_acad_plan_degree
	,aos1.stdnt_school_ld as maj1_2_secondary_school_ld
	,aos1.stdnt_school_cd as maj1_2_secondary_school_cd
	,aos2_min.acad_plan as acad_plan_min1_2
	,dim_program.degree_cd as min1_2_degree_cd
	,dim_program.degree_ld as min1_2_degree_ld
	,aos2_min.acad_prog as min1_2_acad_prog_cd
	,dim_program.acad_prog_ld as min1_2_acad_prog_ld
	,dcpp_min2.acad_plan_ld as min1_2_acad_plan_ld
	,dcpp_min1.grouping_ld as min1_2_grouping_ld	--	,dim_program.grouping_ld  as min1_2_grouping_ld
	,dcpp_min1.grouping_cd as min1_2_grouping_cd	--	,dim_program.grouping_cd   as min1_2_grouping_cd
	,dim_program.department_cd as min1_2_department_cd
	,dim_program.department_ld as min1_2_department_ld
	,dcpp_min2.plan_type_ld as min1_2_acad_plan_type_ld
	,dim_program.degree_cd as min1_2_acad_plan_degree
	,aos2_min.stdnt_school_ld as min1_2_secondary_school_ld
	,aos2_min.stdnt_school_cd as min1_2_secondary_school_cd
	,aos3_min.acad_plan as acad_plan_min1_3
	--concentrations
	,NULL as min1_3_degree_cd
	,NULL as min1_3_degree_ld
	,NULL as min1_3_acad_prog_cd
	,NULL as min1_3_acad_prog_ld
	,dcpp_min3.acad_plan_ld  as min1_3_acad_plan_ld
	,NULL as min1_3_grouping_ld	--	,dim_program.grouping_ld   as min1_3_grouping_ld
	,NULL as min1_3_grouping_cd	--	,dim_program.grouping_cd   as min1_3_grouping_cd
	,dim_program.department_cd as min1_3_department_cd
	,dim_program.department_ld as min1_3_department_ld
	,dcpp_min3.plan_type_ld  as min1_3_acad_plan_type_ld
	,dim_program.degree_cd as min1_3_acad_plan_degree
	,aos3_min.stdnt_school_ld  as min1_3_secondary_school_ld
	,aos3_min.stdnt_school_cd as min1_3_secondary_school_cd
	,NULL as tesol_flag
	,NULL as ever_stdnt_first_gen_flag
	,fin_aid_term.institutional_grant_disbursed_amount as institutional_grant_disbursed_amount_term
	,NULL as rolling_admit_su_adv_flag
	,NULL as rolling_max_su_adv_flag
	,NULL as rolling_su_adv_plan_flag
	,NULL as su_adv_ind
	,ffst.stdnt_type_cd
	,ffst.stdnt_type_ld
	,ffst.stdnt_type_cd as stdnt_type_collapsed_ld
    ,ffst.stdnt_type_ld as stdnt_type_collapsed_cd

 from       hercules_workday.ff_student_term ffst

left join   hercules_workday.dim_career on dim_career.acad_career_cd = ffst.acad_career			 
left join   hercules_workday.dim_term dim_term on ffst.strm = dim_term.strm
left join   (select strm, cal_begin_day, row_number() over (order by cal_begin_day) rel_strm from workday_ods.hercules_dim_term where term_type_ld in ('FALL','SPRING')
and ((calender_yr < 2016 and right(strm,3) in ('/WS','/FA','/SM','/SP'))
or (calender_yr = 2016 and right(strm,3) in ('/WS','FAL','/SM','/SP'))
or (calender_yr > 2016 and right(strm,3) in ('/WS','FAL','SUM','SPR'))
) ) dim_term_major 
on          ffst.strm = dim_term_major.strm

left join  dim_address_term dim_address_term 
            
            on ffst.emplid = dim_address_term.emplid
            AND     dim_address_term.active_flag = 'Y'
            AND     dim_address_term.address_type_ld = 'HOME'
                                                 
                                                 
left join  dim_address_term dim_work_address_term       
            on ffst.emplid = dim_work_address_term.emplid
            AND     dim_work_address_term.active_flag = 'Y'
            AND     dim_work_address_term.address_type_ld = 'WORK'
          													  
left join   hercules_workday.fact_aos fact_aos on ffst.emplid          = fact_aos.emplid
                                  and ffst.acad_career        = fact_aos.acad_career
                                  and ffst.strm               = fact_aos.strm
                                  --and fact_aos.eff_dt            <= ffst.term_end_date
                                  --and fact_aos.src_sys_exp_dt    >= ffst.term_end_date
								  and fact_aos.plan_type_cd         in ('PROG')
								  and fact_aos.plan_rn			= 1
								  --131792
								  
left join   hercules_workday.fact_aos aos1 on ffst.emplid             = aos1.emplid
                                  and ffst.acad_career        = aos1.acad_career
                                  and ffst.strm               = aos1.strm
                                  --and aos1.eff_dt            <= ffst.term_end_date
                                  --and aos1.src_sys_exp_dt    >= ffst.term_end_date
								  and aos1.plan_type_cd         not in ('UGCN','MIN', 'GRCN', 'AS','LAW')
								  and aos1.plan_rn			= 1
							--	  and aos1.is_primary = true 
								  
left join   hercules_workday.fact_aos aos1_min on ffst.emplid             = aos1_min.emplid
                                  and ffst.acad_career        = aos1_min.acad_career
                                  and ffst.strm               = aos1_min.strm
                                  --and aos1_min.eff_dt            <= ffst.term_end_date
                                  --and aos1_min.src_sys_exp_dt    >= ffst.term_end_date
								  and aos1_min.plan_type_cd         in ('MAJ', 'PROG')
								  and aos1_min.plan_type_rn			= 2

left join   hercules_workday.fact_aos aos2 on ffst.emplid             = aos2.emplid
                                  and ffst.acad_career        = aos2.acad_career
                                  and ffst.strm               = aos2.strm
                                  --and aos2.eff_dt            <= ffst.term_end_date
                                  --and aos2.src_sys_exp_dt    >= ffst.term_end_date
								  and aos2.plan_type_cd         in ('MAJ','AMAJ')
								  and aos2.plan_type_rn			= 2
								  
left join   hercules_workday.fact_aos aos2_min on ffst.emplid             = aos2_min.emplid
                                  and ffst.acad_career        = aos2_min.acad_career
                                  and ffst.strm               = aos2_min.strm
                                  --and aos2_min.eff_dt            <= ffst.term_end_date
                                  --and aos2_min.src_sys_exp_dt    >= ffst.term_end_date
								  and aos2_min.plan_type_cd         in ('MIN')
								  and aos2_min.plan_type_rn			= 2
								  
left join   hercules_workday.fact_aos aos3_min on ffst.emplid             = aos3_min.emplid
                                  and ffst.acad_career        = aos3_min.acad_career
                                  and ffst.strm               = aos3_min.strm
                                  --and aos2_min.eff_dt            <= ffst.term_end_date
                                  --and aos2_min.src_sys_exp_dt    >= ffst.term_end_date
								  and aos3_min.plan_type_cd         in ('UGCN', 'GRCN', 'LAW')
								  and aos3_min.plan_type_rn			= 1
								

													  
left join   hercules_workday.dim_program dim_program on  fact_aos.acad_prog = dim_program.acad_prog_cd

left join   hercules_workday.dim_car_prgm_plan dcpp_maj1 on     --aos1.acad_prog = dcpp_maj1.acad_prog_cd
                                                      aos1.acad_plan = dcpp_maj1.acad_plan_cd
                                                -- and     dcpp_maj1.acad_plan_type_cd = 'MAJ'

left join   hercules_workday.dim_car_prgm_plan dcpp_min1 on    -- aos1_min.acad_prog = dcpp_min1.acad_prog_cd
                                                      aos1_min.acad_plan = dcpp_min1.acad_plan_cd
                                              --   and     dcpp_min1.acad_plan_type_cd = 'MIN'
												 
left join   hercules_workday.dim_plan dcpp_maj2 on 		         aos2.acad_plan = dcpp_maj2.acad_plan_cd
                                                 --and     dcpp_maj2.plan_type_cd = 'Major'
												 
left join   hercules_workday.dim_plan dcpp_min2 on 		         aos2_min.acad_plan = dcpp_min2.acad_plan_cd
                                                 --and     dcpp_min2.plan_type_cd = 'Minor'

left join   hercules_workday.dim_plan dcpp_min3 on 		         aos3_min.acad_plan = dcpp_min3.acad_plan_cd
                                                 --and     dcpp_min3.plan_type_cd = 'Minor'
												 
left join   hercules_workday.dim_person  person_student 
			on ffst.emplid = person_student.emplid
left join   dim_person_phone_email phone_home 
			on ffst.emplid = phone_home.emplid
			and phone_home.phone_email_type_ld = 'HOME'
left join   dim_person_phone_email work_phone 
			on ffst.emplid = work_phone.emplid
			and work_phone.phone_email_type_ld = 'WORK'
left join   dim_person_phone_email home_email 
			on ffst.emplid = home_email.emplid
			and home_email.phone_email_type_ld = 'HOME'
			and home_email.phone_device_type_ld= 'Email'
left join   dim_person_phone_email busn_email 
			on ffst.emplid = busn_email.emplid
			and busn_email.phone_email_type_ld = 'WORK'
			and busn_email.phone_device_type_ld= 'Email'

LEFT JOIN perseus_workday.cs_financial_aid fin_aid 
ON ffst.emplid      = fin_aid.emplid
AND ffst.acad_career = fin_aid.acad_career
AND dim_term.aid_yr  = fin_aid.aid_year
LEFT JOIN perseus_workday.cs_financial_aid_term fin_aid_term 
ON ffst.emplid         = fin_aid_term.emplid
AND ffst.acad_career    = fin_aid_term.acad_career
AND ffst.strm           = fin_aid_term.strm
LEFT JOIN perseus_workday.cs_enrollments enrl       
ON ffst.emplid         = enrl.emplid
AND ffst.acad_career    = enrl.acad_career
AND ffst.strm    		= enrl.strm
LEFT JOIN hercules_workday.dim_isir dim_isir 
ON     ffst.emplid = dim_isir.emplid
AND dim_term.aid_yr = dim_isir.aid_year
LEFT JOIN edwutil.masterlookup STDNT_TYPE_COLLAPSED   
ON  STDNT_TYPE_COLLAPSED.target_column_name = 'STDNT_TYPE_COLLAPSED'           
and STDNT_TYPE_COLLAPSED.sys_value 		    = ffst.stdnt_type_cd

left join edwutil.dim_oth_raw_zip_data_t home_zip 
on left(dim_address_term.postal_zip5,5) = home_zip.postal

WHERE 1=1

;


commit; 

Analyze perseus_workday.student_term_extract; 
