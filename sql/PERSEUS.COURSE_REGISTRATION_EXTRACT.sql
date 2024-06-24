/*
   Object type      : Extract
   Description      : Final Extract for Course Registrations datat
   Grain            : Emplid, Academic career, strm, course_id, course_section_id (class_nbr)

   Target table name: COURSE_REGISTRATIONS_EXTRACT

   Source table list: workday_ods.hercules_Fact_Enrollment
                      {perseus_home}.student_term_extract

   ETL load type    : Full load everyday

   Date created     : 03/02/20
   Date updated     :

*/


Truncate table perseus_workday.course_registrations_extract;

Insert into perseus_workday.course_registrations_extract
(acad_stndng_actn_cd                            
,acad_stndng_actn_ld                           
,career_unt_passd_gpa                          
,career_unt_passd_nogpa                        
,career_unt_taken_nogpa                        
,career_unt_taken_gpa                          
,term_degr_chkout_stat_cd                      
,term_degr_chkout_stat_sd                      
,exp_grad_term                                 
,exp_grad_term_year_ld                         
,term_end_day                                  
,term_begin_day                                
,stdnt_enrl_status                             
,enrl_status_reason                            
,plan_acad_org_degree_cd                       
,plan_acad_org_degree_ld                       
,plan_acad_org_program_ld                      
,plan_acad_org_program_cd                      
,plan_acad_org_school_ld                       
,plan_acad_org_school_cd                       
,plan_acad_org_spec_ld                         
,plan_acad_plan_ld                             
,plan_acad_plan_code                           
,plan_acad_prog                                
,plan_prog_status_cd                           
,plan_degree_type                              
,plan_grouping                                 
,plan_grouping_cd                              
,plan_bridge_department                        
,plan_field_of_study_cd                        
,plan_field_of_study_ld                        
,plan_acad_prog_ld                             
,plan_acad_prog_cd                             
,plan_type                                     
,minor_plan_type                               
,class_pri_facility_id                         
,class_meeting_time_start                      
,class_meeting_time_end                        
,class_meeting_duration                        
,class_sunday_ind                              
,class_monday_ind                              
,class_tuesday_ind                             
,class_wednesday_ind                           
,class_thursday_ind                            
,class_friday_ind                              
,class_saturday_ind                            
,class_standard_meeting_times_ld               
,class_room_cap_request                        
,class_acad_org                                
,class_acad_org_ld                             
,class_program_ld                              
,class_grouping_ld                             
,class_bridge_career                           
,class_enroll_cap                              
,class_start_day_key                           
,class_end_day_key                             
,class_cancel_day_key                          
,class_grade                                   
,class_repeat_code                             
,class_location_cd                             
,catalog_nbr                                   
,class_campus_ld                               
,class_instruction_mode_ld                     
,class_course_completion_flag                  
,class_course_success_flag                     
,class_course_compl_base                       
,class_course_compl_success                    
,class_crse_car_collapsed_ld                   
,crse_career_ld                                
,crse_id                                       
,class_number                                  
,class_section                                 
,class_nbr_section_id                          
,class_term_class_nbr_section_id               
,class_term_section_id                         
,class_term_section_session_id                 
,crse_subject                                  
,crse_subject_ld                               
,class_component                               
,class_course                                  
,class_session_cd                              
,class_session_ld                              
,class_tuition_residency_code                  
,class_tuition_residency                       
,sess_begin_day_key                            
,sess_end_day_key                              
,fin_tot_payment_amt                           
,fin_tuition                                   
,fin_tuition_payment                           
,fin_tui_paid_in_full_ind                      
,fin_tui_self_pay_ind                          
,fin_fees                                      
,fin_fee_payment                               
,fin_total_charges                             
,stdnt_new_returning_desc                      
,credit_hours                                  
,stdnt_acad_career_cd                          
,stdnt_acad_car_collapsed_cd                   
,stdnt_acad_car_collapsed_ld                   
,stdnt_acad_career_ld                          
,stdnt_acad_level_bot_cd                       
,stdnt_acad_level_bot_ld                       
,stdnt_cntct_address1                          
,stdnt_cntct_address2                          
,stdnt_cntct_address_type                      
,stdnt_birthdate_day                           
,stdnt_career_gpa_cum                          
,stdnt_car_tot_cumulative                      
,stdnt_car_tot_test_credit                     
,stdnt_car_tot_trnsfr                          
,stdnt_car_unt_audit                           
,stdnt_car_unt_test_credit                     
,stdnt_car_unt_trnsfr                          
,stdnt_cntct_country_ld                        
,stdnt_cntct_county                            
,stdnt_cntct_city                              
,stdnt_cntct_zipcode                           
,stdnt_cntct_state_cd                          
,stdnt_cntct_state_ld                          
,stdnt_campus_cd                               
,stdnt_campus_ld                               
,stdnt_term_status_cd                          
,stdnt_instr_mode_ld                           
,stdnt_education_level_ld                      
,stdnt_eligible_to_enroll_ind                  
,stdnt_emplid      
,uid                            
,stdnt_em_include_in_rep_ind                   
,stdnt_first_name                              
,stdnt_full_part_time_ld                       
,stdnt_gender_ld                               
,stdnt_ipeds_include_in_rep_ind                
,stdnt_ipeds_ethnicity_ld                      
,stdnt_last_name                               
,stdnt_married_status_ld                       
,stdnt_bhe_birth_year                          
,stdnt_cntct_postal                            
,stdnt_tuition_residency                       
,stdnt_residency_ld                            
,stdnt_minor_program_desc                      
,stdnt_minor_spec_desc                         
,stdnt_minor_school_code                       
,stdnt_minor_school                            
,stdnt_minor_program_code                      
,stdnt_minor_plan_code                         
,stdnt_minor_plan_desc                         
,trn_pi_totalcredit                            
,trn_ls_school_type                            
,trn_ls_school_type_ld                         
,trn_transfer_inst_name                        
,trn_school_type_lpi                           
,time_term_year_ld                             
,time_fiscal_yr                                
,time_enrl_add_day_key                         
,time_enrl_drop_day_key                        
,time_strm                                     
,time_collapsed_strm                           
,time_term_type                                
,time_term_type_collapsed                      
,time_calendar_yr                              
,time_academic_yr                              
,time_collapsed_term_year_ld                   
,trn_career_tot_transfer                       
,corporate_waiver_amt                          
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
,campus_latitude                               
,campus_longitude                              
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
,isir_current_fa_app_date                      
,isir_first_fa_app_date                        
,isir_pell_eligibility                         
,isir_adj_efc                                  
,isir_efc_status                               
,isir_titleiv_elig                             
,isir_ssa_citizenshp_ind                       
,isir_num_family_members                       
,isir_number_in_college                        
,isir_veteran                                  
,isir_graduate_student                         
,isir_married                                  
,isir_marital_stat                             
,isir_orphan                                   
,isir_dependents                               
,isir_agi                                      
,isir_school_choice_1                          
,isir_housing_code_1                           
,isir_school_choice_2                          
,isir_housing_code_2                           
,isir_school_choice_3                          
,isir_housing_code_3                           
,isir_school_choice_4                          
,isir_housing_code_4                           
,isir_school_choice_5                          
,isir_housing_code_5                           
,isir_school_choice_6                          
,isir_housing_code_6                           
,isir_first_bach_degree                        
,isir_interested_in_ws                         
,isir_interested_in_sl                         
,isir_interested_in_plus                       
,isir_degree_certif                            
,isir_current_grade_lvl                        
,isir_depndncy_stat                            
,isir_citizenship_status                       
,isir_children                                 
,isir_sfa_active_duty                          
,isir_parent_marital_stat                      
,isir_parent_number_in_family                  
,isir_parent_num_in_college                    
,isir_parent_agi                               
,isir_father_grade_lvl                         
,isir_mother_grade_lvl                         
,isir_parent_sfa_grant_aid                     
,isir_fed_efc                                  
,isir_efc_status2                              
,isir_fed_need_base_aid                        
,isir_fed_year_coa                             
,isir_inst_year_coa                            
,isir_pell_year_coa                            
,isir_fed_need                                 
,isir_inst_need                                
,isir_fed_unmet_need                           
,isir_inst_unmet_need                          
,isir_fed_stdnt_contrb                         
,isir_calc_sc                                  
,isir_calc_efc                                 
,reenrollment_type                             
,persistence_flg                               
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
,retention_flag                                
,retention_status                              
,app_accepted_dt                               
,stdnt_admit_type_cd                           
,stdnt_admit_type_ld                           
,app_admit_term                                
,application_dt                                
,degr_completion_term                          
,degr_acad_plan_maj                            
,degr_application_dt                           
,degr_honors_cd                                
,degr_honors_ld                                
,degr_chkout_stat_cd                           
,degr_chkout_stat_sd                           
,degr_confer_dt                                
,degr_status_dt                                
,degr_gpa                                      
,enrollment_address1                           
,enrollment_address2                           
,enrollment_address_type                       
,enrollment_address_city                       
,enrollment_address_country_ld                 
,enrollment_address_county                     
,enrollment_address_postal                     
,enrollment_address_state_cd                   
,enrollment_address_state_ld                   
,enrollment_address_zipcode                    
,enrollment_address_latitude                   
,enrollment_address_longitude                  
,dorm_enrollment_address1                      
,dorm_enrollment_address2                      
,dorm_enrollment_address_type                  
,dorm_enrollment_address_city                  
,dorm_enrollment_address_country_ld            
,dorm_enrollment_address_county                
,dorm_enrollment_address_postal                
,dorm_enrollment_address_state_cd              
,dorm_enrollment_address_state_ld              
,dorm_enrollment_address_zipcode               
,credits_unt_taken_prgrss                      
,credits_unt_passd_prgrss                      
,plan_sub_plan1                                
,plan_sub_plan2                                
,plan_sub_plan3                                
,plan_sub_plan1_cd                             
,plan_sub_plan2_cd                             
,plan_sub_plan3_cd                             
,plan_sub_plan1_type                           
,plan_sub_plan2_type                           
,plan_sub_plan3_type                           
,plan_minor_sub_plan1                          
,plan_minor_sub_plan2                          
,plan_minor_sub_plan3                          
,plan_minor_sub_plan1_cd                       
,plan_minor_sub_plan2_cd                       
,plan_minor_sub_plan3_cd                       
,plan_minor_sub_plan1_type                     
,plan_minor_sub_plan2_type                     
,plan_minor_sub_plan3_type                     
,ret_cohort_strm                               
,stdnt_acad_level_eot_cd                       
,stdnt_acad_level_eot_ld                       
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
,stdnt_home_phone                              
,stdnt_cell_phone                              
,advisor_id                                    
,advisor_name                                  
,stdnt_home_email                              
,stdnt_business_email                          
,stdnt_campus_email                            
,advisor_business_email                        
,advisor_campus_email                          
,advisor_home_email                            
,stdnt_grp_first_gen                           
,pri_class_src_sys_cd                          
,class_primary_faculty_emplid                  
,class_primary_faculty_first_name              
,class_primary_faculty_last_name               
,meeting_pattern_src_sys_cd                    
,level_1                                       
,level_2                                       
,tot_enrl                                      
,primary_class_pri_facility_id                 
,primary_class_pri_class_src_sys_cd            
,primary_class_primary_faculty_emplid          
,primary_class_pri_faculty_first_name          
,primary_class_pri_faculty_last_name           
,primary_class_meeting_pattern_src_sys_cd      
,primary_class_level_1                         
,primary_class_level_2                         
,combined_section_fte                          
,combined_tot_enrl                             
,combined_room_cap_request                     
,combined_enrl_cap                             
,student_term_fte                              
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
,unt_taken_prgrss                              
,unt_passd_prgrss                              
,student_term_exclude_flag                     
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
,pri_acad_plan_dept_cd                         
,pri_acad_plan_dept_ld                         
,urm_status                                    
,bldg_cd                                       
,bldg_desc                                     
,class_stat_ld                                 
,class_type_cd                                 
,form_of_study                                 
,perseus_load_time                             
,withdraw_cd                                   
,combined_sect_ld                              
,combined_sect_sd                              
,comb_sect_enrl_cap                            
,comb_sect_enrl_tot                            
,comb_sect_room_cap_req                        
,act_band                                      
,sat_band                                      
,class_title_ld                                
,cip2000_cd                                    
,cip2000_ld                                    
,admit_term_desc                               
,honors_acad_prog_ind                          
,military_status_cd                            
,military_status_ld                            
,acad_stndng_actn_cd_bot                       
,acad_stndng_actn_ld_bot                       
,leave_of_absence_ind                          
,plan_institution_cd                           
,plan_institution_ld                           
,latest_hegis_code                             
,latest_hegis_descr                            
,pri_bhe_api_hegis_cd                          
,pri_bhe_api_hegis_ld                          
,sec_bhe_api_hegis_cd                          
,sec_bhe_api_hegis_ld                          
,stdnt_admit_type_group                        
,dev_ed_engl_ind                               
,dev_ed_math_ind                               
,dev_ed_ind                                    
,stdnt_dev_ed_engl_ind                         
,stdnt_dev_ed_math_ind                         
,stdnt_dev_ed_ind                              
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
,trn_transfer_inst_name_and_location           
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
,class_school_cd                               
,class_school_ld                               
,stu_current_type_cd                           
,stu_current_type_ld                           
,student_hiatus_cd                             
,student_hiatus_ld                             
,student_hiatus_reason_cd                      
,student_hiatus_reason_ld                      
,stem_category                                 
,appl_current_location                         
,student_hiatus_end_dt                         
,erp_geo_break                                 
,pri_reporting_acad_program                    
,pri_reporting_acad_dept_cd                    
,pri_reporting_acad_dept_ld                    
,pri_reporting_acad_school                     
,fa_pell_disb_flag                             
,athlete_flag                                  
,sport_codes                                   
,freeze_strm                                   
,res_bldg                                      
,res_room                                      
,res_room_type                                 
,honors_ind                                    
,res_room_status                               
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
,ft_pt_ind                                     
,pri_citizenship_country                       
,pri_citizenship_country_code                  
,pri_citizenship_country_code_iso              
,ever_stdnt_first_gen_flag                     
,tesol_flag                                    
,institutional_grant_disbursed_amount_term     
,rolling_admit_su_adv_flag                     
,rolling_max_su_adv_flag                       
,rolling_su_adv_plan_flag                      
,su_adv_ind                                    
,faculty_rank                                  
)

SELECT
    student_term_extract.acad_stndng_actn_cd
    ,student_term_extract.acad_stndng_actn_ld
    ,student_term_extract.career_unt_passd_gpa
    ,student_term_extract.career_unt_passd_nogpa
    ,student_term_extract.career_unt_taken_nogpa
    ,student_term_extract.career_unt_taken_gpa
    ,student_term_extract.degr_chkout_stat_cd                      AS term_degr_chkout_stat_cd
    ,student_term_extract.degr_chkout_stat_sd                      AS term_degr_chkout_stat_sd
    ,student_term_extract.exp_grad_term
    ,student_term_extract.exp_grad_term_year_ld
    ,student_term_extract.term_end_day
    ,student_term_extract.term_begin_day
    ,fact_enrollment.stdnt_enrl_status
    ,fact_enrollment.enrl_status_reason
    ,student_term_extract.pri_degree_cd                                              AS plan_acad_org_degree_cd
    ,student_term_extract.pri_degree_ld                                              AS plan_acad_org_degree_ld
    ,student_term_extract.pri_acad_prog_ld                                           AS plan_acad_org_program_ld
    ,student_term_extract.pri_acad_prog_cd                                           AS plan_acad_org_program_cd
    ,student_term_extract.pri_school_ld                                              AS plan_acad_org_school_ld
    ,student_term_extract.pri_school_cd                                              AS plan_acad_org_school_cd
    ,student_term_extract.pri_specialization_ld                                      AS plan_acad_org_spec_ld
    ,student_term_extract.pri_acad_plan_ld                                           AS plan_acad_plan_ld
    ,student_term_extract.pri_acad_plan_cd                                           AS plan_acad_plan_code
    ,student_term_extract.pri_acad_prog                                              AS plan_acad_prog
    ,student_term_extract.pri_prog_status_cd                                         AS plan_prog_status_cd
    ,student_term_extract.pri_education_level_ld                                     AS plan_degree_type
    ,student_term_extract.pri_grouping_ld                                            AS plan_grouping
    ,student_term_extract.pri_grouping_cd                                            AS plan_grouping_cd
    ,student_term_extract.pri_department_ld                                          AS plan_bridge_department
    ,student_term_extract.pri_study_field_cd                                         AS plan_field_of_study_cd
    ,student_term_extract.pri_study_field_ld                                         AS plan_field_of_study_ld
    ,student_term_extract.pri_acad_prog_ld                                           AS plan_acad_prog_ld
    ,student_term_extract.pri_acad_prog_cd                                           AS plan_acad_prog_cd
    ,student_term_extract.pri_acad_plan_type_ld                                      AS plan_type
    ,student_term_extract.sec_acad_plan_type_ld                                      AS minor_plan_type
    ,dim_class.pri_facility_id                                                       as class_pri_facility_id
    ,RIGHT(dim_class.meeting_time_start, 8)                                          as class_meeting_time_start
    ,RIGHT(dim_class.meeting_time_end  , 8)                                          as class_meeting_time_end
    ,dim_class.meeting_duration                                                      as class_meeting_duration
    ,dim_class.sunday_ind                                                            as class_sunday_ind
    ,dim_class.monday_ind                                                            as class_monday_ind
    ,dim_class.tuesday_ind                                                           as class_tuesday_ind
    ,dim_class.wednesday_ind                                                         as class_wednesday_ind
    ,dim_class.thursday_ind                                                          as class_thursday_ind
    ,dim_class.friday_ind                                                            as class_friday_ind
    ,dim_class.saturday_ind                                                          as class_saturday_ind
    ,dim_class.standard_meeting_times_ld                                             as class_standard_meeting_times_ld
    ,dim_class.room_cap_request                                                      as class_room_cap_request
    ,dim_class.department_cd                                                         as class_acad_org
    ,dim_class.department_ld                                                         as class_acad_org_ld
    ,dim_class.program_ld                                                            as class_program_ld
    ,dim_class.grouping_ld                                                           as class_grouping_ld
    ,dim_class.crse_career_ld                                                        as class_bridge_career
    ,dim_class.enrl_cap                                                              as class_enroll_cap
    ,CASE
      WHEN dim_class.class_start_day_key = 0 THEN NULL
      ELSE TO_DATE (dim_class.class_start_day_key,'J')
    END                                                                              AS class_start_day_key
    ,CASE
      WHEN dim_class.class_end_day_key = 0 THEN NULL
      ELSE TO_DATE (dim_class.class_end_day_key,'J')
    END                                                                              AS class_end_day_key
    ,CASE
      WHEN dim_class.class_cancel_day_key = 0 THEN NULL
      ELSE TO_DATE (dim_class.class_cancel_day_key,'J')
    END                                                                              AS class_cancel_day_key
    ,fact_enrollment.crse_grade_off                                                  as class_grade
    ,null                                                                            as class_repeat_code
    ,dim_class.location_cd                                                           as class_location_cd
    ,dim_class.catalog_nbr
    ,dim_class.class_campus_ld
    ,dim_class.class_instruction_mode_ld
    ,fact_enrollment.crse_completion_flag                                            as class_course_completion_flag
    ,fact_enrollment.crse_success_flag                                               as class_course_success_flag
    ,CASE
      WHEN fact_enrollment.crse_completion_flag = 0 OR fact_enrollment.crse_completion_flag = 1 THEN 1
      ELSE 0
    END                                                                              AS class_course_compl_base
    ,CASE
      WHEN fact_enrollment.crse_completion_flag = 1 THEN 1
      ELSE 0
    END                                                                              AS class_course_compl_success
    ,dim_class.crse_career_collapsed_ld                                              as class_crse_car_collapsed_ld
    ,dim_class.crse_career_ld
    ,dim_class.crse_id
    ,fact_enrollment.class_nbr                                                       AS class_number
    ,dim_class.class_section
    ,dim_class.class_nbr || '~' || dim_class.class_section                           AS class_nbr_section_id
    ,dim_class.strm || '~' || dim_class.class_nbr || '~' || dim_class.class_section  AS class_term_class_nbr_section_id
    ,dim_class.strm || '~' || dim_class.class_section                                AS class_term_section_id
    ,dim_class.strm || '~' || dim_class.class_section || '~' || dim_class.session_cd AS class_term_section_session_id
    ,dim_class.crse_subject                                                          AS crse_subject
    ,dim_class.subject_desc                                                          AS crse_subject_ld
    ,dim_class.main_component                                                        AS class_component
    ,fact_enrollment.course_id                                                       AS class_course
    ,dim_class.session_cd                                                            AS class_session_cd
    ,dim_class.session_desc                                                          AS class_session_ld
    ,null                                                                            AS class_tuition_residency_code
    ,null                                                                            AS class_tuition_residency
    ,null                                                                            as sess_begin_day_key
    ,null                                                                            as sess_end_day_key
    ,fact_enrollment.tot_payment_amt                                                 AS fin_tot_payment_amt
    ,fact_enrollment.tuition                                                         AS fin_tuition
    ,fact_enrollment.tuition_payment                                                 AS fin_tuition_payment
    ,null                                                                            as fin_tui_paid_in_full_ind
    ,null                                                                            as fin_tui_self_pay_ind
    ,fact_enrollment.fees                                                            AS fin_fees
    ,fact_enrollment.fee_payment                                                     AS fin_fee_payment
    ,fact_enrollment.fees + fact_enrollment.tuition                                  AS fin_total_charges
    ,student_term_extract.univ_attend_type_sd                                        as stdnt_new_returning_desc
    ,fact_enrollment.unt_billing                                                     AS credit_hours
    ,fact_enrollment.acad_career                                                     AS stdnt_acad_career_cd
    ,student_term_extract.acad_career_collapsed_cd                                   AS stdnt_acad_car_collapsed_cd
    ,student_term_extract.acad_career_collapsed_ld                                   AS stdnt_acad_car_collapsed_ld
    ,student_term_extract.acad_career_ld                                             AS stdnt_acad_career_ld
    ,student_term_extract.acad_level_bot_cd                                          AS stdnt_acad_level_bot_cd
    ,student_term_extract.acad_level_bot_ld                                          AS stdnt_acad_level_bot_ld
    ,student_term_extract.perm_address1                                              AS stdnt_cntct_address1
    ,student_term_extract.perm_address2                                              AS stdnt_cntct_address2
    ,student_term_extract.perm_address_type                                          AS stdnt_cntct_address_type
    ,student_term_extract.birthdate_day                                              AS stdnt_birthdate_day
    ,student_term_extract.career_gpa_cum                                             AS stdnt_career_gpa_cum
    ,student_term_extract.career_tot_cumulative                                      AS stdnt_car_tot_cumulative
    ,student_term_extract.career_tot_test_credit                                     AS stdnt_car_tot_test_credit
    ,student_term_extract.career_tot_trnsfr                                          AS stdnt_car_tot_trnsfr
    ,student_term_extract.career_unt_audit                                           AS stdnt_car_unt_audit
    ,student_term_extract.career_unt_test_credit                                     AS stdnt_car_unt_test_credit
    ,student_term_extract.career_unt_trnsfr                                          AS stdnt_car_unt_trnsfr
    ,student_term_extract.perm_country_ld                                            AS stdnt_cntct_country_ld
    ,student_term_extract.perm_geog_county_ld                                        AS stdnt_cntct_county
    ,student_term_extract.perm_city                                                  AS stdnt_cntct_city
    ,"substring" (student_term_extract.perm_postal,1,5)                              AS stdnt_cntct_zipcode
    ,student_term_extract.perm_state_cd                                              AS stdnt_cntct_state_cd
    ,student_term_extract.perm_state_ld                                              AS stdnt_cntct_state_ld
    ,student_term_extract.stdnt_campus_cd
    ,student_term_extract.stdnt_campus_ld
    ,student_term_extract.stdnt_status_cd           as stdnt_term_status_cd
    ,student_term_extract.stdnt_instr_mode_ld
    ,student_term_extract.pri_education_level_ld    AS stdnt_education_level_ld
    ,student_term_extract.eligible_to_enroll_ind    AS stdnt_eligible_to_enroll_ind
    ,fact_enrollment.emplid_stdnt                   AS stdnt_emplid
	,fact_enrollment.uid as uid
    ,student_term_extract.em_include_in_reports_ind AS stdnt_em_include_in_rep_ind
    ,student_term_extract.first_name                AS stdnt_first_name
    ,student_term_extract.full_part_time_ld         AS stdnt_full_part_time_ld
    ,student_term_extract.gender_cd                 AS stdnt_gender_ld     ------this needs to be fixed in student_term_extract
    ,student_term_extract.ipeds_include_in_reports_ind        AS stdnt_ipeds_include_in_rep_ind
    ,student_term_extract.ipeds_race_ethn_citz_ld             AS stdnt_ipeds_ethnicity_ld
    ,student_term_extract.last_name                           AS stdnt_last_name
    ,student_term_extract.married_status_ld                   AS stdnt_married_status_ld
    ,student_term_extract.bhe_birth_year                      AS stdnt_bhe_birth_year
    ,student_term_extract.perm_postal                         AS stdnt_cntct_postal
    ,student_term_extract.tuition_residency_ld                AS stdnt_tuition_residency
    ,student_term_extract.residency_ld                        AS stdnt_residency_ld
    ,student_term_extract.sec_acad_prog_ld                    AS stdnt_minor_program_desc
    ,student_term_extract.sec_specialization_ld               AS stdnt_minor_spec_desc
    ,student_term_extract.sec_school_cd                       AS stdnt_minor_school_code
    ,student_term_extract.sec_school_ld                       AS stdnt_minor_school
    ,student_term_extract.sec_acad_prog_cd                    AS stdnt_minor_program_code
    ,student_term_extract.sec_acad_plan_cd                    AS stdnt_minor_plan_code
    ,student_term_extract.sec_acad_plan_ld                    AS stdnt_minor_plan_desc
    ,student_term_extract.trn_pi_totalcredit
    ,student_term_extract.trn_ls_school_type
    ,student_term_extract.trn_ls_school_type_ld
    ,student_term_extract.trn_transfer_inst_name
    ,student_term_extract.trn_school_type_lpi
    ,student_term_extract.term_year_ld                        AS time_term_year_ld
    ,student_term_extract.fiscal_yr                           AS time_fiscal_yr
    ,CASE
      WHEN fact_enrollment.enrl_add_day_key = 0 THEN NULL
      ELSE TO_DATE (fact_enrollment.enrl_add_day_key,'J')
    END                                                       AS time_enrl_add_day_key
    ,CASE
      WHEN fact_enrollment.enrl_drop_day_key = 0 THEN NULL
      ELSE TO_DATE (fact_enrollment.enrl_drop_day_key,'J')
    END                                                       AS time_enrl_drop_day_key
    ,fact_enrollment.strm                                 AS time_strm
    ,fact_enrollment.strm                                     AS time_collapsed_strm
    ,student_term_extract.term_type_ld                        AS time_term_type
    ,student_term_extract.term_type_ld                        AS time_term_type_collapsed
    ,null as                        time_calendar_yr   ---needs to be fixed student_term_extract.calendar_yr  
    ,student_term_extract.academic_yr                         AS time_academic_yr
    ,student_term_extract.collapsed_term_year_ld              AS time_collapsed_term_year_ld
    ,student_term_extract.career_tot_trnsfr                   AS trn_career_tot_transfer
    ,null as corporate_waiver_amt
    ,student_term_extract.hs_final_gpa
    ,student_term_extract.hs_term_year
    ,student_term_extract.hs_highest_ed_lvl
    ,student_term_extract.hs_final_gpa_excl_last_strm
    ,student_term_extract.hs_last_year
    ,student_term_extract.hs_start_year
    ,student_term_extract.hs_years_attended
    ,student_term_extract.hs_class_pop
    ,student_term_extract.hs_rank
    ,student_term_extract.hs_unt_comp_total
    ,student_term_extract.hs_name
    ,student_term_extract.hs_short_name
    ,student_term_extract.hs_proprietorship
    ,student_term_extract.hs_address1
    ,student_term_extract.hs_address2
    ,student_term_extract.hs_city
    ,student_term_extract.hs_county
    ,student_term_extract.hs_state
    ,student_term_extract.hs_postal
    ,student_term_extract.hs_country
    ,student_term_extract.hs_lat
    ,student_term_extract.hs_long
    ,student_term_extract.test_super_act_score
    ,student_term_extract.test_super_sat_score
    ,student_term_extract.test_first_act_score
    ,student_term_extract.test_last_act_score
    ,student_term_extract.test_max_act_score
    ,student_term_extract.test_avg_act_score
    ,student_term_extract.test_num_act_attempts
    ,student_term_extract.test_first_sat_score
    ,student_term_extract.test_last_sat_score
    ,student_term_extract.test_max_sat_score
    ,student_term_extract.test_avg_sat_score
    ,student_term_extract.test_num_sat_attempts
    ,student_term_extract.stdnt_cntct_latitude
    ,student_term_extract.stdnt_cntct_longitude
    ,student_term_extract.campus_latitude
    ,student_term_extract.campus_longitude
    ,student_term_extract.fa_type
    ,student_term_extract.scholarship_disbursed_amount
    ,student_term_extract.vabenefits_disbursed_amount
    ,student_term_extract.loan_disbursed_amount
    ,student_term_extract.grant_disbursed_amount
    ,student_term_extract.workstudy_disbursed_amount
    ,student_term_extract.waiver_disbursed_amount
    ,student_term_extract.fellowship_disbursed_amount
    ,student_term_extract.other_disbursed_amount
    ,student_term_extract.disbursed_amount
    ,student_term_extract.aid_year
    ,student_term_extract.federal_grant_disbursed_amount
    ,student_term_extract.federal_loan_disbursed_amount
    ,student_term_extract.federal_workstudy_disbursed_amount
    ,student_term_extract.institutional_grant_disbursed_amount
    ,student_term_extract.institutional_other_disbursed_amount
    ,student_term_extract.institutional_scholarship_disbursed_amount
    ,student_term_extract.other_scholarship_disbursed_amount
    ,student_term_extract.private_grant_disbursed_amount
    ,student_term_extract.private_loan_disbursed_amount
    ,student_term_extract.private_scholarship_disbursed_amount
    ,student_term_extract.state_grant_disbursed_amount
    ,student_term_extract.state_scholarship_disbursed_amount
    ,student_term_extract.federal_grant_offer_amount
    ,student_term_extract.federal_loan_offer_amount
    ,student_term_extract.federal_workstudy_offer_amount
    ,student_term_extract.institutional_grant_offer_amount
    ,student_term_extract.institutional_other_offer_amount
    ,student_term_extract.institutional_scholarship_offer_amount
    ,student_term_extract.other_scholarship_offer_amount
    ,student_term_extract.private_grant_offer_amount
    ,student_term_extract.private_loan_offer_amount
    ,student_term_extract.private_scholarship_offer_amount
    ,student_term_extract.state_grant_offer_amount
    ,student_term_extract.state_scholarship_offer_amount
    ,student_term_extract.offer_amount
    ,student_term_extract.federal_grant_accept_amount
    ,student_term_extract.federal_loan_accept_amount
    ,student_term_extract.federal_workstudy_accept_amount
    ,student_term_extract.institutional_grant_accept_amount
    ,student_term_extract.institutional_other_accept_amount
    ,student_term_extract.institutional_scholarship_accept_amount
    ,student_term_extract.other_scholarship_accept_amount
    ,student_term_extract.private_grant_accept_amount
    ,student_term_extract.private_loan_accept_amount
    ,student_term_extract.private_scholarship_accept_amount
    ,student_term_extract.state_grant_accept_amount
    ,student_term_extract.state_scholarship_accept_amount
    ,student_term_extract.accept_amount
    ,student_term_extract.current_fa_app_date                     AS isir_current_fa_app_date
    ,student_term_extract.first_fa_app_date                       AS isir_first_fa_app_date
    ,student_term_extract.pell_eligibility                        AS isir_pell_eligibility
    ,student_term_extract.adj_efc                                 AS isir_adj_efc
    ,student_term_extract.efc_status                              AS isir_efc_status
    ,student_term_extract.titleiv_elig                            AS isir_titleiv_elig
    ,student_term_extract.ssa_citizenshp_ind                      AS isir_ssa_citizenshp_ind
    ,student_term_extract.num_family_members                      AS isir_num_family_members
    ,student_term_extract.number_in_college                       AS isir_number_in_college
    ,student_term_extract.veteran                                 AS isir_veteran
    ,student_term_extract.graduate_student                        AS isir_graduate_student
    ,student_term_extract.married                                 AS isir_married
    ,student_term_extract.marital_stat                            AS isir_marital_stat
    ,student_term_extract.orphan                                  AS isir_orphan
    ,student_term_extract.dependents                              AS isir_dependents
    ,student_term_extract.agi                                     AS isir_agi
    ,student_term_extract.school_choice_1                         AS isir_school_choice_1
    ,student_term_extract.housing_code_1                          AS isir_housing_code_1
    ,student_term_extract.school_choice_2                         AS isir_school_choice_2
    ,student_term_extract.housing_code_2                          AS isir_housing_code_2
    ,student_term_extract.school_choice_3                         AS isir_school_choice_3
    ,student_term_extract.housing_code_3                          AS isir_housing_code_3
    ,student_term_extract.school_choice_4                         AS isir_school_choice_4
    ,student_term_extract.housing_code_4                          AS isir_housing_code_4
    ,student_term_extract.school_choice_5                         AS isir_school_choice_5
    ,student_term_extract.housing_code_5                          AS isir_housing_code_5
    ,student_term_extract.school_choice_6                         AS isir_school_choice_6
    ,student_term_extract.housing_code_6                          AS isir_housing_code_6
    ,student_term_extract.first_bach_degree                       AS isir_first_bach_degree
    ,student_term_extract.interested_in_ws                        AS isir_interested_in_ws
    ,student_term_extract.interested_in_sl                        AS isir_interested_in_sl
    ,student_term_extract.interested_in_plus                      AS isir_interested_in_plus
    ,student_term_extract.degree_certif                           AS isir_degree_certif
    ,student_term_extract.current_grade_lvl                       AS isir_current_grade_lvl
    ,student_term_extract.depndncy_stat                           AS isir_depndncy_stat
    ,student_term_extract.citizenship_status                      AS isir_citizenship_status
    ,student_term_extract.children                                AS isir_children
    ,student_term_extract.sfa_active_duty                         AS isir_sfa_active_duty
    ,student_term_extract.parent_marital_stat                     AS isir_parent_marital_stat
    ,student_term_extract.parent_number_in_family                 AS isir_parent_number_in_family
    ,student_term_extract.parent_num_in_college                   AS isir_parent_num_in_college
    ,student_term_extract.parent_agi                              AS isir_parent_agi
    ,student_term_extract.father_grade_lvl                        AS isir_father_grade_lvl
    ,student_term_extract.mother_grade_lvl                        AS isir_mother_grade_lvl
    ,student_term_extract.parent_sfa_grant_aid                    AS isir_parent_sfa_grant_aid
    ,student_term_extract.fed_efc                                 AS isir_fed_efc
    ,student_term_extract.efc_status2                             AS isir_efc_status2
    ,student_term_extract.fed_need_base_aid                       AS isir_fed_need_base_aid
    ,student_term_extract.fed_year_coa                            AS isir_fed_year_coa
    ,student_term_extract.inst_year_coa                           AS isir_inst_year_coa
    ,student_term_extract.pell_year_coa                           AS isir_pell_year_coa
    ,student_term_extract.fed_need                                AS isir_fed_need
    ,student_term_extract.inst_need                               AS isir_inst_need
    ,student_term_extract.fed_unmet_need                          AS isir_fed_unmet_need
    ,student_term_extract.inst_unmet_need                         AS isir_inst_unmet_need
    ,student_term_extract.fed_stdnt_contrb                        AS isir_fed_stdnt_contrb
    ,student_term_extract.isir_calc_sc
    ,student_term_extract.isir_calc_efc
    ,student_term_extract.reenrollment_type
    ,student_term_extract.persistence_flg
    ,student_term_extract.tot_fa_type
    ,student_term_extract.tot_scholarship_disbursed_amount
    ,student_term_extract.tot_vabenefits_disbursed_amount
    ,student_term_extract.tot_loan_disbursed_amount
    ,student_term_extract.tot_grant_disbursed_amount
    ,student_term_extract.tot_workstudy_disbursed_amount
    ,student_term_extract.tot_waiver_disbursed_amount
    ,student_term_extract.tot_fellowship_disbursed_amount
    ,student_term_extract.tot_other_disbursed_amount
    ,student_term_extract.tot_disbursed_amount
    ,student_term_extract.retention_flag
    ,student_term_extract.retention_status
    ,student_term_extract.accepted_dt app_accepted_dt
    ,student_term_extract.stdnt_admit_type_cd
    ,student_term_extract.stdnt_admit_type_ld
    ,student_term_extract.app_admit_term
    ,student_term_extract.app_dt application_dt
    ,student_term_extract.degr_completion_term
    ,student_term_extract.degr_acad_plan_maj
    ,student_term_extract.degr_application_dt
    ,student_term_extract.degr_honors_cd
    ,student_term_extract.degr_honors_ld
    ,student_term_extract.degr_chkout_stat_cd
    ,student_term_extract.degr_chkout_stat_sd
    ,student_term_extract.degr_confer_dt
    ,student_term_extract.degr_status_dt
    ,student_term_extract.degr_gpa
    ,COALESCE (student_term_extract.mail_address1,'*')                 AS enrollment_address1
    ,COALESCE (student_term_extract.mail_address2,'*')                 AS enrollment_address2
    ,COALESCE (student_term_extract.mail_address_type,'*')             AS enrollment_address_type
    ,COALESCE (student_term_extract.mail_city,'*')                     AS enrollment_address_city
    ,COALESCE (student_term_extract.mail_country_ld,'*')               AS enrollment_address_country_ld
    ,COALESCE (student_term_extract.mail_geog_county_ld,'*')           AS enrollment_address_county
    ,COALESCE (student_term_extract.mail_postal,'*')                   AS enrollment_address_postal
    ,COALESCE (student_term_extract.mail_state_cd,'*')                 AS enrollment_address_state_cd
    ,COALESCE (student_term_extract.mail_state_ld,'*')                 AS enrollment_address_state_ld
    ,COALESCE ("substring" (student_term_extract.mail_postal,1,5),'*') AS enrollment_address_zipcode
    ,student_term_extract.mail_postal_loc_y                            AS enrollment_address_latitude
    ,student_term_extract.mail_postal_loc_x                            AS enrollment_address_longitude
    ,COALESCE (student_term_extract.resh_address1,'*')                 AS dorm_enrollment_address1
    ,null                                                              as dorm_enrollment_address2
    ,COALESCE (student_term_extract.resh_address_type,'*')             AS dorm_enrollment_address_type
    ,COALESCE (student_term_extract.resh_city,'*')                     AS dorm_enrollment_address_city
    ,COALESCE (student_term_extract.resh_country_ld,'*')               AS dorm_enrollment_address_country_ld
    ,COALESCE (student_term_extract.resh_geog_county_ld,'*')           AS dorm_enrollment_address_county
    ,COALESCE (student_term_extract.resh_postal,'*')                   AS dorm_enrollment_address_postal
    ,COALESCE (student_term_extract.resh_state_cd,'*')                 AS dorm_enrollment_address_state_cd
    ,COALESCE (student_term_extract.resh_state_ld,'*')                 AS dorm_enrollment_address_state_ld
    ,COALESCE ("substring" (student_term_extract.dorm_postal,1,5),'*') AS dorm_enrollment_address_zipcode
    ,student_term_extract.unt_taken_prgrss                             AS credits_unt_taken_prgrss
    ,student_term_extract.unt_passd_prgrss                             AS credits_unt_passd_prgrss
    ,null                                                              as plan_sub_plan1
    ,null                                                              as plan_sub_plan2
    ,null                                                              as plan_sub_plan3
    ,null                                                              as plan_sub_plan1_cd
    ,null                                                              as plan_sub_plan2_cd
    ,null                                                              as plan_sub_plan3_cd
    ,null                                                              as plan_sub_plan1_type
    ,null                                                              as plan_sub_plan2_type
    ,null                                                              as plan_sub_plan3_type
    ,null                                                              as plan_minor_sub_plan1
    ,null                                                              as plan_minor_sub_plan2
    ,null                                                              as plan_minor_sub_plan3
    ,null                                                              as plan_minor_sub_plan1_cd
    ,null                                                              as plan_minor_sub_plan2_cd
    ,null                                                              as plan_minor_sub_plan3_cd
    ,null                                                              as plan_minor_sub_plan1_type
    ,null                                                              as plan_minor_sub_plan2_type
    ,null                                                              as plan_minor_sub_plan3_type
    ,student_term_extract.ret_cohort_strm
    ,student_term_extract.acad_level_eot_cd                            AS stdnt_acad_level_eot_cd
    ,student_term_extract.acad_level_eot_ld                            AS stdnt_acad_level_eot_ld
    ,student_term_extract.test_first_gre_score
    ,student_term_extract.test_last_gre_score
    ,student_term_extract.test_max_gre_score
    ,student_term_extract.test_avg_gre_score
    ,student_term_extract.test_num_gre_attempts
    ,student_term_extract.test_first_gmat_score
    ,student_term_extract.test_last_gmat_score
    ,student_term_extract.test_max_gmat_score
    ,student_term_extract.test_avg_gmat_score
    ,student_term_extract.test_num_gmat_attempts
    ,student_term_extract.total_balances
    ,student_term_extract.total_future_balances
    ,student_term_extract.total_current_balances
    ,student_term_extract.total_pd_balances
    ,student_term_extract.total_pd_lt30_balances
    ,student_term_extract.total_pd_gt30_balances
    ,student_term_extract.total_pd_gt60_balances
    ,student_term_extract.total_pd_gt90_balances
    ,student_term_extract.total_pd_gt120_balances
    ,student_term_extract.stdnt_home_phone
    ,student_term_extract.stdnt_cell_phone
    ,student_term_extract.advisor_emplid                                                             AS advisor_id
    ,student_term_extract.advisor_name_first_last                                                    AS advisor_name
    ,student_term_extract.stdnt_home_email                                                           AS stdnt_home_email
    ,student_term_extract.stdnt_business_email                                                       AS stdnt_business_email
    ,student_term_extract.stdnt_campus_email                                                         AS stdnt_campus_email
    ,student_term_extract.advisor_business_email                                                     AS advisor_business_email
    ,student_term_extract.advisor_campus_email                                                       AS advisor_campus_email
    ,student_term_extract.advisor_home_email                                                         AS advisor_home_email
    ,student_term_extract.stdnt_first_gen_flag                                                       as stdnt_grp_first_gen
    ,NULL                                                                as pri_class_src_sys_cd  --- does not exist
    ,null as class_primary_faculty_emplid --dim_class.primary_faculty_emplid                                                                as class_primary_faculty_emplid
    ,null                                                               as class_primary_faculty_first_name
    ,null                                                           as class_primary_faculty_last_name
    ,NULL                                                            as meeting_pattern_src_sys_cd  --dim_class.meeting_pattern_src_sys_cd
    ,dim_class.class_level_1                                                                         as level_1
    ,dim_class.class_level_2                                                                         as level_2
    ,dim_class.enrl_tot                                                                              as tot_enrl
    ,dim_class.pri_facility_id                                                                       as primary_class_pri_facility_id
    ,dim_class.src_sys_cd                                                                            as primary_class_pri_class_src_sys_cd
    ,null                                              as primary_class_primary_faculty_emplid
    ,null                                                  as primary_class_pri_faculty_first_name
    ,null                                                  as primary_class_pri_faculty_last_name
    ,null                                                            as primary_class_meeting_pattern_src_sys_cd
    ,dim_class.class_level_1                                                                         as primary_class_level_1
    ,dim_class.class_level_1                                                                         as primary_class_level_2
--    ,dim_class.enrl_tot                                                                              as primary_class_tot_enrl
--    ,RIGHT(dim_class.primary_meeting_time_start , 8)                                                 as primary_class_meeting_time_start
--    ,RIGHT(dim_class.primary_meeting_time_end   , 8)                                                 as primary_class_meeting_time_end
--    ,dim_class.primary_meeting_duration                                                              as primary_class_meeting_duration
--    ,dim_class.primary_sunday_ind                                                                    as primary_class_sunday_ind
--    ,dim_class.primary_monday_ind                                                                    as primary_class_monday_ind
--    ,dim_class.primary_tuesday_ind                                                                   as primary_class_tuesday_ind
--    ,dim_class.primary_thursday_ind                                                                  as primary_class_wednesday_ind
--    ,dim_class.primary_friday_ind                                                                    as primary_class_thursday_ind
--    ,dim_class.primary_saturday_ind                                                                  as primary_class_friday_ind
--    ,dim_class.primary_sunday_ind                                                                    as primary_class_saturday_ind
--    ,dim_class.standard_meeting_times_ld                                                             as primary_class_standard_meeting_times_ld
--    ,dim_class.room_cap_request                                                                      as primary_class_room_cap_request
--    ,dim_class.primary_grouping_ld                                                                   as primary_class_bridge_department
--    ,dim_class.primary_program_ld                                                                    as primary_class_bridge_program
--    ,dim_class.primary_school_cd                                                                     as primary_class_bridge_school
--    ,dim_class.primary_crse_career_ld                                                                as primary_class_bridge_career
--    ,dim_class.primary_enrl_cap                                                                      as primary_class_enroll_cap
--    ,dim_class.primary_class_start_day_key                                                           as primary_class_start_day_key
--    ,dim_class.primary_class_end_day_key                                                             as primary_class_end_day_key
--    ,dim_class.primary_class_cancel_day_key                                                          as primary_class_cancel_day_key
--    ,dim_class.primary_catalog_nbr                                                                   as primary_class_catalog_nbr
--    ,dim_class.primary_class_campus_ld                                                               as primary_class_campus_ld
--    ,dim_class.primary_class_instruction_mode_ld                                                     as primary_class_instruction_mode_ld
--    ,dim_class.primary_class_stat_ld                                                                 as primary_class_stat_ld
--    ,dim_class.primary_crse_career_collapsed_ld                                                      as primary_class_crse_career_collapsed_ld
--    ,dim_class.primary_crse_career_ld                                                                as primary_class_crse_career_ld
--    ,dim_class.primary_crse_id                                                                       as primary_class_crse_id
--    ,dim_class.primary_class_nbr                                                                     as primary_class_nbr
--    ,dim_class.primary_class_section                                                                 as primary_class_section
--    ,dim_class.primary_class_nbr || '~' || dim_class.primary_class_section                           AS primary_class_nbr_section_id
--    ,dim_class.strm || '~' || dim_class.primary_class_nbr || '~' || dim_class.primary_class_section  AS primary_class_term_class_nbr_section_id
--    ,dim_class.strm || '~' || dim_class.primary_class_section                                        AS primary_class_term_section_id
--    ,dim_class.strm || '~' || dim_class.primary_class_section || '~' || dim_class.primary_session_cd AS primary_class_term_section_session_id
--    ,dim_class.primary_crse_subject                                                                  as primary_class_crse_subject
--    ,dim_class.component_primary_cd                                                                  as primary_class_component
--    ,dim_class.primary_crse_id                                                                       AS primary_class_course
    ,null                                                                                            as combined_section_fte
    ,null                                                                                            as combined_tot_enrl
    ,null                                                                                            as combined_room_cap_request
    ,null                                                                                            as combined_enrl_cap
    ,ROUND(
    CASE WHEN 
			case when left(student_term_extract.pri_degree_cd,2)= 'JD' 
				and student_term_extract.stdnt_status_cd in ('P','R','T') 
					and enrl_fte.unt_bill_fte >= 13 then 'FT'
			when student_term_extract.stu_current_type_cd in ('LLMF','LLMP','PHDE') 
				AND student_term_extract.stdnt_status_cd in ('P','R','T') 
					and enrl_fte.unt_bill_fte >= 9 then 'FT'
			when student_term_extract.stdnt_status_cd in ('P','R','T') 
				and enrl_fte.unt_bill_fte >= 12 then 'FT'
		else 'PT'
		end in ('FT') THEN 1
         ELSE CASE
                   WHEN student_term_extract.pri_school_cd in ('SSOM', 'CAS') THEN NVL(sum(case when fact_enrollment.stdnt_enrl_status in ('A','N') and student_term_extract.stdnt_status_cd in ('P','R','T') then fact_enrollment.unt_billing else 0 end) over(
						partition by fact_enrollment.emplid_stdnt, dim_term.strm) ,0) / 12.0000
                   WHEN student_term_extract.pri_school_cd in ('LAW') AND student_term_extract.pri_degree_cd in ('LLM') THEN NVL(sum(case when fact_enrollment.stdnt_enrl_status in ('A','N') and student_term_extract.stdnt_status_cd in ('P','R','T') then fact_enrollment.unt_billing else 0 end) over(
						partition by fact_enrollment.emplid_stdnt, dim_term.strm),0) / 9.0000
                   WHEN student_term_extract.pri_school_cd in ('LAW') AND student_term_extract.pri_degree_cd not in ('LLM') THEN NVL(sum(case when fact_enrollment.stdnt_enrl_status in ('A','N') and student_term_extract.stdnt_status_cd in ('P','R','T') then fact_enrollment.unt_billing else 0 end) over(
						partition by fact_enrollment.emplid_stdnt, dim_term.strm),0) / 13.0000
                   ELSE 0
              END
    END, 5) as student_term_fte
    ,student_term_extract.career_gpa_sem
    ,student_term_extract.career_gpa_cum
    ,student_term_extract.career_unt_audit
    ,student_term_extract.career_unt_trnsfr
    ,student_term_extract.career_unt_test_credit
    ,student_term_extract.career_unt_term_tot
    ,student_term_extract.career_grade_points
    ,student_term_extract.career_tot_audit
    ,student_term_extract.career_tot_trnsfr
    ,student_term_extract.career_tot_other
    ,student_term_extract.career_tot_test_credit
    ,student_term_extract.career_tot_cumulative
    ,student_term_extract.career_tot_grade_points
    ,student_term_extract.unt_taken_prgrss
    ,student_term_extract.unt_passd_prgrss
    ,student_term_extract.student_term_exclude_flag
    ,student_term_extract.online_pct
    ,student_term_extract.unt_inprog_gpa
    ,student_term_extract.unt_inprog_nogpa
    ,student_term_extract.trf_taken_gpa
    ,student_term_extract.trf_taken_nogpa
    ,student_term_extract.trf_grade_points
    ,student_term_extract.trf_passed_gpa
    ,student_term_extract.trf_passed_nogpa
    ,student_term_extract.tc_units_adjust
    ,student_term_extract.unt_other
    ,student_term_extract.tot_taken_gpa
    ,student_term_extract.tot_taken_nogpa
    ,student_term_extract.tot_passd_gpa
    ,student_term_extract.tot_passd_nogpa
    ,student_term_extract.tot_inprog_nogpa
    ,student_term_extract.tot_taken_prgrss
    ,student_term_extract.tot_passd_prgrss
--    ,dim_class.latest_crse_acad_group
--    ,dim_class.latest_school_cd
--    ,dim_class.latest_grouping_cd
--    ,dim_class.latest_acad_org
--    ,dim_class.latest_course_manager_cd
--    ,dim_class.latest_school_ld
--    ,dim_class.latest_grouping_ld
--    ,dim_class.latest_acad_org_ld
--    ,dim_class.latest_crse_acad_group_ld
--    ,dim_class.latest_course_manager_ld
    ,student_term_extract.pri_acad_plan_dept_cd
    ,student_term_extract.pri_acad_plan_dept_ld
    ,student_term_extract.urm_status                AS urm_status
    ,null                            as bldg_cd
    ,null                                           as bldg_desc
    ,dim_class.class_stat_ld                        AS class_stat_ld
    ,dim_class.class_type_cd                        AS class_type_cd
    ,student_term_extract.form_of_study             AS form_of_study
    ,null                                           as perseus_load_time
    ,student_term_extract.withdraw_cd               AS withdraw_cd
    ,dim_class.combined_sect_ld                     AS combined_sect_ld
    ,dim_class.combined_sect_sd                     AS combined_sect_sd
    ,dim_class.comb_sect_enrl_cap::int              AS comb_sect_enrl_cap
    ,dim_class.comb_sect_enrl_tot                   AS comb_sect_enrl_tot
    ,dim_class.comb_sect_room_cap_req               AS comb_sect_room_cap_req
    ,student_term_extract.act_band                  AS act_band
    ,student_term_extract.sat_band                  AS sat_band
    ,dim_class.class_title_ld                       AS class_title_ld
    ,student_term_extract.cip2000_cd                AS cip2000_cd
    ,student_term_extract.cip2000_ld                AS cip2000_ld
    ,null                                           as admit_term_desc
    ,student_term_extract.honors_acad_prog_ind
    ,student_term_extract.military_status_cd
    ,student_term_extract.military_status_ld
    ,student_term_extract.acad_stndng_actn_cd_bot
    ,student_term_extract.acad_stndng_actn_ld_bot
    ,student_term_extract.leave_of_absence_ind      AS leave_of_absence_ind
    ,student_term_extract.pri_institution_cd        AS plan_institution_cd
    ,student_term_extract.pri_institution_ld        AS plan_institution_ld
    ,dim_class.latest_hegis_code
    ,dim_class.latest_hegis_descr
    ,student_term_extract.pri_bhe_api_hegis_cd
    ,student_term_extract.pri_bhe_api_hegis_ld
    ,student_term_extract.sec_bhe_api_hegis_cd
    ,student_term_extract.sec_bhe_api_hegis_ld
    ,student_term_extract.stdnt_admit_type_group             AS stdnt_admit_type_group
    ,null                                                    as dev_ed_engl_ind
    ,null                                                    as dev_ed_math_ind
    ,null                                                    as dev_ed_ind
    ,NVL (student_term_extract.stdnt_dev_ed_engl_ind::int,0) AS stdnt_dev_ed_engl_ind
    ,NVL (student_term_extract.stdnt_dev_ed_math_ind::int,0) AS stdnt_dev_ed_math_ind
    ,NVL (student_term_extract.stdnt_dev_ed_ind::int,0)      AS stdnt_dev_ed_ind
    ,student_term_extract.retention_year_1_flag
    ,student_term_extract.retention_year_1_status
    ,student_term_extract.retention_year_2_flag
    ,student_term_extract.retention_year_2_status
    ,student_term_extract.retention_year_3_flag
    ,student_term_extract.retention_year_3_status
    ,student_term_extract.retention_year_4_flag
    ,student_term_extract.retention_year_4_status
    ,student_term_extract.retention_year_5_flag
    ,student_term_extract.retention_year_5_status
    ,student_term_extract.retention_year_6_flag
    ,student_term_extract.retention_year_6_status
    ,student_term_extract.retention_year_7_flag
    ,student_term_extract.retention_year_7_status
    ,student_term_extract.retention_year_8_flag
    ,student_term_extract.retention_year_8_status
    ,student_term_extract.retention_year_9_flag
    ,student_term_extract.retention_year_9_status
    ,student_term_extract.retention_year_10_flag
    ,student_term_extract.retention_year_10_status
    ,null as trn_transfer_inst_name_and_location
    ,null as hs_inst_name_and_location
    ,student_term_extract.orig_address_term_src_sys_cd
    ,student_term_extract.orig_home_address1
    ,student_term_extract.orig_home_address2
    ,student_term_extract.orig_home_address_type
    ,student_term_extract.orig_home_city
    ,student_term_extract.orig_home_country_ld
    ,student_term_extract.orig_home_geog_county_ld
    ,student_term_extract.orig_home_postal
    ,student_term_extract.orig_home_state_cd
    ,student_term_extract.orig_home_state_ld
    ,student_term_extract.orig_home_postal_loc_y
    ,student_term_extract.orig_home_postal_loc_x
    ,dim_class.school_cd                              as class_school_cd
    ,dim_class.school_ld                              as class_school_ld
--    ,dim_class.primary_school_cd                      as primary_class_school_cd
--    ,dim_class.primary_school_ld                      as primary_class_school_ld
--    ,dim_class.primary_session_cd                     as primary_class_session_cd
--    ,dim_class.primary_session_desc                   as primary_class_session_ld
--    ,dim_class.pri_bldg_cd                            as primary_class_bldg_cd
--    ,dim_class.room_cd                                as room_cd
--    ,dim_class.pri_room_cd                            as primary_class_room_cd
--    ,dim_class.meeting_pattern                        as meeting_pattern
--    ,dim_class.pri_meeting_pattern                    as primary_class_meeting_pattern
 --   ,dim_class.contact_hours                          as contact_hours
  --  ,dim_class.primary_contact_hours                  as primary_contact_hours
    ,student_term_extract.stu_current_type_cd         as stu_current_type_cd
    ,student_term_extract.stu_current_type_ld         as stu_current_type_ld
    ,student_term_extract.student_hiatus_cd           as student_hiatus_cd
    ,student_term_extract.student_hiatus_ld           as student_hiatus_ld
    ,student_term_extract.student_hiatus_reason_cd    as student_hiatus_reason_cd
    ,student_term_extract.student_hiatus_reason_ld    as student_hiatus_reason_ld
    ,student_term_extract.stem_category as stem_category
    ,student_term_extract.appl_current_location
    ,student_term_extract.student_hiatus_end_dt
    ,student_term_extract.erp_geo_break
    ,student_term_extract.pri_reporting_acad_program
    ,student_term_extract.pri_reporting_acad_dept_cd
    ,student_term_extract.pri_reporting_acad_dept_ld
    ,student_term_extract.pri_reporting_acad_school
    ,nvl(student_term_extract.fa_pell_disb_flag,'N')    as fa_pell_disb_flag
    ,case
        when left(dim_term.academic_yr,4)::int < 2015
                then null
        when student_term_extract.athlete_flag is not null
                then student_term_extract.athlete_flag
        else 'N'
    end as athlete_flag
    ,student_term_extract.sport_codes as sport_codes     
   ,fact_enrollment.strm                                     AS freeze_strm	
   ,ra.rmas_bldg                                   as res_bldg
   ,ra.rmas_room                                   as res_room
   ,ra.rmas_room_type                              as res_room_type
   ,nvl(student_term_extract.honors_ind,'N')     as honors_ind
   ,ra.rmas_current_status res_room_status
   
   ,student_term_extract.acad_plan_maj1_2
   ,student_term_extract.maj1_2_degree_cd
   ,student_term_extract.maj1_2_degree_ld
   ,student_term_extract.maj1_2_acad_prog_cd
   ,student_term_extract.maj1_2_acad_prog_ld
   ,student_term_extract.maj1_2_acad_plan_ld
   ,student_term_extract.maj1_2_grouping_ld
   ,student_term_extract.maj1_2_grouping_cd
   ,student_term_extract.maj1_2_department_cd
   ,student_term_extract.maj1_2_department_ld
   ,student_term_extract.maj1_2_acad_plan_type_ld
   ,student_term_extract.maj1_2_acad_plan_degree
   ,student_term_extract.maj1_2_secondary_school_ld
   ,student_term_extract.maj1_2_secondary_school_cd

   ,student_term_extract.acad_plan_min1_2
   ,student_term_extract.min1_2_degree_cd
   ,student_term_extract.min1_2_degree_ld
   ,student_term_extract.min1_2_acad_prog_cd
   ,student_term_extract.min1_2_acad_prog_ld
   ,student_term_extract.min1_2_acad_plan_ld
   ,student_term_extract.min1_2_grouping_ld
   ,student_term_extract.min1_2_grouping_cd
   ,student_term_extract.min1_2_department_cd
   ,student_term_extract.min1_2_department_ld
   ,student_term_extract.min1_2_acad_plan_type_ld
   ,student_term_extract.min1_2_acad_plan_degree
   ,student_term_extract.min1_2_secondary_school_ld
   ,student_term_extract.min1_2_secondary_school_cd 
   ,case when left(student_term_extract.pri_degree_cd,2)= 'JD' 
		and student_term_extract.stdnt_status_cd in ('P','R','T') 
			and enrl_fte.unt_bill_fte >= 13 then 'FT'
	when student_term_extract.stu_current_type_cd in ('LLMF','LLMP','PHDE') 
		AND student_term_extract.stdnt_status_cd in ('P','R','T') 
			and enrl_fte.unt_bill_fte >= 9 then 'FT'
	when student_term_extract.stdnt_status_cd in ('P','R','T') 
		and enrl_fte.unt_bill_fte >= 12 then 'FT'
else 'PT'
end as FT_PT_IND
    ,student_term_extract.pri_citizenship_country 			as pri_citizenship_country
    ,student_term_extract.pri_citizenship_country_code 		as pri_citizenship_country_code
    ,student_term_extract.pri_citizenship_country_code_iso 	as pri_citizenship_country_code_iso
    , student_term_extract.ever_stdnt_first_gen_flag
	, student_term_extract.tesol_flag
	, student_term_extract.institutional_grant_disbursed_amount_term
	, student_term_extract.rolling_admit_su_adv_flag
    , student_term_extract.rolling_max_su_adv_flag
    , student_term_extract.rolling_su_adv_plan_flag
    , student_term_extract.su_adv_ind
	, dim_empl_job.academic_rank 							as faculty_rank
FROM        hercules_workday.fact_enrollment fact_enrollment
left join (
	Select fact_enrollment.emplid_stdnt,
			fact_enrollment.acad_career,
			fact_enrollment.strm, 
			sum(fact_enrollment.unt_billing) unt_bill_fte
	from hercules_workday.fact_enrollment fact_enrollment
	where stdnt_enrl_status = 'E'
	group by fact_enrollment.emplid_stdnt,
			fact_enrollment.acad_career,
			fact_enrollment.strm
) enrl_fte
on (
fact_enrollment.emplid_stdnt = enrl_fte.emplid_stdnt
    AND     fact_enrollment.acad_career  = enrl_fte.acad_career
    AND     fact_enrollment.strm         = enrl_fte.strm
)
LEFT JOIN   perseus_workday.student_term_extract student_term_extract
    ON      fact_enrollment.emplid_stdnt = student_term_extract.emplid
    AND     fact_enrollment.acad_career  = student_term_extract.acad_career
    AND     fact_enrollment.strm         = student_term_extract.strm
LEFT JOIN   hercules_workday.dim_term dim_term
    ON      fact_enrollment.strm         = dim_term.strm
LEFT JOIN   hercules_workday.dim_class dim_class
    ON  fact_enrollment.class_nbr    = dim_class.class_nbr
	
left join   (
    select
        ras.att_person_id
        ,ras.rmas_term
        ,dt.strm
        ,dt.collapsed_strm
        ,ras.rmas_bldg
        ,ras.rmas_room
        ,ras.rmas_room_type
		,ras.rmas_current_status
        ,row_number() OVER(
            PARTITION BY
                ras.att_person_id
                ,dt.collapsed_strm
            order by
                ras.id
        ) as row_num
    from orion_ods.room_assignment ras
    left join workday_ods.hercules_dim_term dt
    on ras.rmas_term = dt.strm
    where true
	and ras.rmas_bldg <> 'TEMP'
    and ras.expir_timestamp is null
    and nvl(ras.rmas_bldg,ras.rmas_room,ras.rmas_room_type) is not null
) ra
    on      ra.collapsed_strm               = dim_term.collapsed_strm 
    and     ra.att_person_id                = fact_enrollment.emplid_stdnt 
    and     ra.row_num                      = 1
LEFT JOIN
(
		Select *, nvl( lead(src_sys_eff_dt,1) over( partition by id order by src_sys_eff_dt), '2100-01-01' ) as next_src_sys_eff_dt 
		  from 
		  (
		  Select * from (
						  Select row_number() over(partition by id,effective_date order by  academic_unit,  case when academic_rank = '' then 'zzz' else academic_rank end  ) job_rn, 
							id,lpad(sis_id,7,'0') as emplid,first_name,last_name,race_ethnicity,gender,business_title,job_number
							,business_unit_cd,employee_type_sd,employee_type_cd,employee_type_ld,paygroup,full_part_time_cd
							,full_part_time_ld,std_hrs_frequency,position_nbr,job_cd,job_ld,eeo6code,dept_cd,dept_ld,fte,location_cd
							,term_date,college_cd,effective_date as src_sys_eff_dt,academic_unit,academic_rank,track_type,tenure_status,tenure_award_date
							,nvl(lead(effective_date,1) over(partition by id order by effective_date),'2099-12-31') as src_sys_exp_dt
							from cln_util.apm_worker_data 
							)
		) where job_rn = 1
) dim_empl_job
             ON           	  dim_class.primary_faculty_emplid  			=  dim_empl_job.emplid
                          AND to_date(dim_term.term_begin_day_key, 'J') 	>=  dim_empl_job.src_sys_eff_dt
						  and to_date(dim_term.term_begin_day_key, 'J')  	 <  dim_empl_job.next_src_sys_eff_dt	

;


commit;

analyze perseus_workday.course_registrations_extract; 