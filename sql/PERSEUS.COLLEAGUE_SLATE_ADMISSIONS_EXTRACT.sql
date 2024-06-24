truncate table perseus_workday.colleague_slate_admissions_extract;

insert into perseus_workday.colleague_slate_admissions_extract
(
acad_career
,acad_level_bot_ld
,acad_stndng_actn_cd
,acad_stndng_actn_cd_bot
,acad_stndng_actn_ld
,acad_stndng_actn_ld_bot
,academic_yr
,accept_amount
,act_band
,adm_appl_complete
,adm_appl_nbr_reverse_seq
,admission_address_city
,admission_address_country_ld
,admission_address_county
,admission_address_latitude
,admission_address_longitude
,admission_address_postal
,admission_address_state_cd
,admission_address_state_ld
,admission_address_type
,admission_address_zipcode
,admission_address1
,admission_address2
,admtted_ea
,advisor_business_email
,advisor_campus_email
,advisor_home_email
,advisor_id
,admit_honors_flag
,admit2matrdays
,advisor_name
,aid_year
,app_acad_plan
,app_acad_plan_cd
,app_acad_plan_maj
,app_acad_prog
,app_adm_appl_ctr
,app_adm_appl_nbr
,app_admit_term_type
,app_admit_type_cd
,app_admit_type_desc
,app_alum_any
,app_alum_father
,app_alum_mother
,app_alum_other_relative
,app_alum_sibling
,app_appl_fee_dt
,app_appl_fee_status_cd
,app_appl_fee_type_ld
,app_degree_ind
,app_fin_aid_interest
,app_notification_plan_cd
,app_notification_plan_ld
,app_prior_appl_ind
,app_prog_action
,app_prog_action_desc
,app_prog_action_dt
,app_prog_active_date
,app_prog_active_ind
,app_prog_admit_term
,app_prog_admit_term_collapsed
,app_prog_admitted_date
,app_prog_admitted_ind
,app_prog_applied_ind
,app_prog_ca_ind
,app_prog_cancelled_date
,app_prog_cancelled_ind
,app_prog_discontinued_date
,app_prog_discontinued_ind
,app_prog_inquiry_ind
,app_prog_pre_matric_date
,app_prog_pre_matric_ind
,app_prog_prospect_ind
,app_prog_reason
,app_prog_reason_desc
,app_prog_req_term
,app_prog_status
,app_prog_status_desc
,app_prog_true_app_ind
,app_prog_waitlisted_date
,app_prog_waitlisted_ind
,app_satr_m_score
,app_satr_r_score
,app_stage
,app_stage_accepted_ind
,app_stage_admitted_ind
,app_stage_admitted_rev_ind
,app_stage_applied_ind
,app_stage_cancel_last_dt
,app_stage_cancel_paid_ind
,app_stage_cancelled_ind
,app_stage_comp_app
,app_stage_deny_app
,app_stage_deposit_paid_ind
,app_stage_deposit_paid_rev_ind
,app_stage_discontinued_ind
,app_stage_enrolled_ind
,app_stage_enrolled_rev_ind
,app_stage_graduated_ind
,app_stage_inquiry_ind
,app_stage_net_paid_ind
,app_stage_paid_last_dt
,app_stage_pre_accpeted_ind
,app_stage_prospect_ind
,app_stage_true_app
,app_stage_waitlisted_ind
,app_track_course_success_flag
,app_track_course_success_ind
,app_track_enrolled_ind
,app_track_first_enrl_date
,app2admitdays
,app2enrolldays
,appl_current_location
,appl_prog_nbr
,appl_rating
,application_key
,applied_acad_career_cd
,applied_acad_career_collapsed_cd
,applied_acad_career_ld
,applied_acad_plan_cd
,applied_acad_plan_dept_cd
,applied_acad_plan_dept_ld
,applied_acad_plan_ld
,applied_acad_plan_type_cd
,applied_acad_plan_type_ld
,applied_acad_prog_cd
,applied_acad_prog_ld
,applied_bhe_api_hegis_ld
,applied_degree_code
,applied_ea
,applied_education_level_cd
,applied_education_level_ld
,applied_grouping_cd
,applied_grouping_ld
,applied_honors_flag
,applied_school_cd
,applied_school_ld
,applied_specialization_ld
,ath_recruit
,calendar_yr
,campus_latitude
,campus_longitude
,career_taken_credits
,career_unt_passd_gpa
,career_unt_passd_nogpa
,career_unt_taken_gpa
,career_unt_taken_nogpa
,career_unt_term_tot
,cell_phone
,cip2000_cd
,cip2000_ld
,citizenship_country
,citizenship_country_code
,citizenship_country_code_iso
,citizenship_country_iso
,citizenship_country_status_cd
,citizenship_country_status_ld
,courses_taken
,courses_withdrawn
,degr_acad_plan_maj
,degr_application_dt
,degr_chkout_stat_cd
,degr_chkout_stat_sd
,degr_completion_term
,degr_confer_dt
,degr_gpa
,degr_honors_cd
,degr_honors_ld
,degr_status_dt
,degree_cd
,degree_code
,degree_ld
,degree_type_cd
,degree_type_ld
,dem_ethnicity
,dem_gender
,dem_marital_status
,disbursed_amount
,effdt
,effseq
,enroll2graddays
,enrollscoore_flag_canc
,enrollscore_enrl_score
,enrollscore_entry_stat
,enrollscore_flag_con
,enrollscore_flag_enr
,eps_market
,erp_geo_break
,exp_grad_term
,exp_grad_term_year_ld
,fa_type
,federal_grant_accept_amount
,federal_grant_disbursed_amount
,federal_grant_offer_amount
,federal_loan_accept_amount
,federal_loan_disbursed_amount
,federal_loan_offer_amount
,federal_workstudy_accept_amount
,federal_workstudy_disbursed_amount
,federal_workstudy_offer_amount
,fellowship_disbursed_amount
,fin_corporate_waiver_amt
,fin_credit_hours
,fin_fee_payment
,fin_fees
,fin_tot_payment_amt
,fin_total_charges
,fin_tuition
,fin_tuition_payment
,first_class_grade_point
,first_class_section
,first_course_id
,first_crse_offer_nbr
,first_session_cd
,freeze_strm
,gender
,geo_dma
,geo_state
,gmat_wvd_flag
,grade_points
,grade_points_per_unit
,grant_disbursed_amount
,gre_wvd_flag
,home_phone
,honors_acad_prog_ind
,honors_offer
,honors_offer_acc
,housing_interest_cd
,housing_interest_ld
,housing_interest_sd
,hs_address1
,hs_address2
,hs_city
,hs_class_pop
,hs_country
,hs_county
,hs_final_gpa
,hs_inst_name_and_location
,hs_last_year
,hs_lat
,hs_lat_from_zip
,hs_lat_long_source_flag
,hs_lat_manual
,hs_long
,hs_long_from_zip
,hs_long_manual
,hs_name
,hs_postal
,hs_proprietorship
,hs_rank
,hs_reported_class_place
,hs_reported_class_size
,hs_short_name
,hs_start_year
,hs_state
,hs_term_year
,hs_unt_comp_total
,hs_years_attended
,institutional_grant_accept_amount
,institutional_grant_disbursed_amount
,institutional_grant_offer_amount
,institutional_other_accept_amount
,institutional_other_disbursed_amount
,institutional_other_offer_amount
,institutional_scholarship_accept_amount
,institutional_scholarship_disbursed_amount
,institutional_scholarship_offer_amount
,isir_adj_efc
,isir_agi
,isir_calc_efc
,isir_calc_sc
,isir_children
,isir_citizenship_status
,isir_current_fa_app_date
,isir_current_grade_lvl
,isir_degree_certif
,isir_dependents
,isir_depndncy_stat
,isir_efc_status
,isir_efc_status2
,isir_father_grade_lvl
,isir_fed_efc
,isir_fed_need
,isir_fed_need_base_aid
,isir_fed_stdnt_contrb
,isir_fed_unmet_need
,isir_fed_year_coa
,isir_first_bach_degree
,isir_first_fa_app_date
,isir_graduate_student
,isir_housing_code_1
,isir_housing_code_2
,isir_housing_code_3
,isir_housing_code_4
,isir_housing_code_5
,isir_housing_code_6
,isir_inst_need
,isir_inst_unmet_need
,isir_inst_year_coa
,isir_interested_in_plus
,isir_interested_in_sl
,isir_interested_in_ws
,isir_marital_stat
,isir_married
,isir_mother_grade_lvl
,isir_num_family_members
,isir_number_in_college
,isir_orphan
,isir_parent_agi
,isir_parent_marital_stat
,isir_parent_num_in_college
,isir_parent_number_in_family
,isir_parent_sfa_grant_aid
,isir_pell_eligibility
,isir_pell_year_coa
,isir_school_choice_1
,isir_school_choice_2
,isir_school_choice_3
,isir_school_choice_4
,isir_school_choice_5
,isir_school_choice_6
,isir_sfa_active_duty
,isir_ssa_citizenshp_ind
,isir_titleiv_elig
,isir_veteran
,last_applied_stage
,loan_disbursed_amount
,matr2enrolldays
,military_status_cd
,military_status_ld
,next_effdt
,offer_amount
,other_disbursed_amount
,other_scholarship_accept_amount
,other_scholarship_disbursed_amount
,other_scholarship_offer_amount
,paid_flag
,perseus_load_date
,persistence_flg
,pi_address1
,pi_address2
,pi_adm_typ_address1
,pi_adm_typ_address2
,pi_adm_typ_city
,pi_adm_typ_country
,pi_adm_typ_county
,pi_adm_typ_name
,pi_adm_typ_postal
,pi_adm_typ_short_name
,pi_adm_typ_state
,pi_city
,pi_class_pop
,pi_converted_gpa
,pi_country
,pi_county
,pi_degr_confer_dt
,pi_degree_cd
,pi_degree_gpa
,pi_degree_ld
,pi_degree_type_cd
,pi_degree_type_ld
,pi_final_gpa
,pi_inst_name_and_location
,pi_institution_ld
,pi_last_year
,pi_lat_from_zip
,pi_long_from_zip
,pi_name
,pi_postal
,pi_proprietorship
,pi_rank
,pi_short_name
,pi_start_year
,pi_state
,pi_term_year
,pi_unt_comp_total
,pi_years_attended
,plan_acad_career_cd
,plan_acad_career_desc
,plan_acad_org_program_cd
,plan_acad_org_program_ld
,plan_acad_org_school_cd
,plan_acad_org_school_ld
,plan_acad_org_spec_ld
,plan_field_of_study_cd
,plan_field_of_study_ld
,plan_grouping
,plan_grouping_cd
,plan_institution_cd
,plan_institution_ld
,plan_sub_plan1
,plan_sub_plan1_cd
,plan_sub_plan1_type
,plan_sub_plan2
,plan_sub_plan2_cd
,plan_sub_plan2_type
,plan_sub_plan3
,plan_sub_plan3_cd
,plan_sub_plan3_type
,plan_type
,pref_address1
,pref_address2
,pref_city
,pref_country_ld
,pref_county
,pref_postal
,pref_state_cd
,pref_state_ld
,pref_zipcode
,pri_acad_plan_dept_cd
,pri_acad_plan_dept_ld
,pri_bhe_api_hegis_cd
,pri_bhe_api_hegis_ld
,private_grant_accept_amount
,private_grant_disbursed_amount
,private_grant_offer_amount
,private_loan_accept_amount
,private_loan_disbursed_amount
,private_loan_offer_amount
,private_scholarship_accept_amount
,private_scholarship_disbursed_amount
,private_scholarship_offer_amount
,prog_stage_cd
,prog_stage_dt
,prog_status_cd
,prog_status_ld
,reenrollment_type
,res_bldg
,res_room
,res_room_type
,ret_cohort_strm
,ret_full_part_time_ld
,retention_year_1_flag
,retention_year_1_status
,retention_year_10_flag
,retention_year_10_status
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
,sat_band
,scholarship_disbursed_amount
,sec_bhe_api_hegis_cd
,sec_bhe_api_hegis_ld
,source_category
,source_memo
,source_summary
,state_grant_accept_amount
,state_grant_disbursed_amount
,state_grant_offer_amount
,state_scholarship_accept_amount
,state_scholarship_disbursed_amount
,state_scholarship_offer_amount
,stdnt_acad_car_collapsed_cd
,stdnt_acad_career_cd
,stdnt_acad_career_ld
,stdnt_acad_level_bot_cd
,stdnt_acad_level_eot_cd
,stdnt_acad_level_eot_ld
,stdnt_acad_plan_cd
,stdnt_acad_plan_dept_cd
,stdnt_acad_plan_dept_ld
,stdnt_acad_plan_ld
,stdnt_acad_plan_type_ld
,stdnt_acad_prog_cd
,stdnt_acad_prog_ld
,stdnt_admit_type_group
,stdnt_admit_type_group_cd
,stdnt_admit_type_group_ld
,stdnt_approved_acad_load_cd
,stdnt_approved_acad_load_ld
,stdnt_approved_acad_load_sd
,stdnt_bhe_api_hegis_ld
,stdnt_birthdate_day
,stdnt_business_email
,stdnt_campus_email
,stdnt_cntct_address_type
,stdnt_cntct_address1
,stdnt_cntct_address2
,stdnt_cntct_city
,stdnt_cntct_country_ld
,stdnt_cntct_county
,stdnt_cntct_email
,stdnt_cntct_home_email
,stdnt_cntct_latitude
,stdnt_cntct_longitude
,stdnt_cntct_phone
,stdnt_cntct_postal
,stdnt_cntct_state_cd
,stdnt_cntct_state_ld
,stdnt_cntct_zipcode
,stdnt_degree_code
,stdnt_dev_ed_engl_ind
,stdnt_dev_ed_ind
,stdnt_dev_ed_math_ind
,stdnt_education_level_cd
,stdnt_education_level_ld
,stdnt_emplid
,stdnt_enrolled_load_cd
,stdnt_enrolled_load_ld
,stdnt_first_gen_flag
,stdnt_first_name
,stdnt_full_part_time_ld
,stdnt_grouping_cd
,stdnt_grouping_ld
,stdnt_last_name
,stdnt_new_returning_desc
,stdnt_residency_ld
,stdnt_school_cd
,stdnt_school_ld
,stdnt_specialization_ld
,stdnt_student_campus
,stdnt_tuition_residency
,stdnt_tuition_residency_cd
,term_begin_day
,term_begin_dt
,term_degr_chkout_stat_cd
,term_degr_chkout_stat_sd
,term_end_day
,test_avg_act_score
,term_end_dt
,test_avg_gmat_score
,test_avg_gre_score
,test_avg_grea_score
,test_avg_greq_score
,test_avg_grev_score
,test_avg_ielts_score
,test_avg_lsat_score
,test_avg_sat_score
,test_avg_satr_score
,test_avg_toefl_internet_score
,test_avg_toefl_paper_score
,test_first_act_score
,test_first_gmat_score
,test_first_gre_score
,test_first_grea_score
,test_first_greq_score
,test_first_grev_score
,test_first_ielts_score
,test_first_lsat_score
,test_first_sat_score
,test_first_satr_score
,test_first_toefl_internet_score
,test_first_toefl_paper_score
,test_last_act_score
,test_last_gmat_score
,test_last_gre_score
,test_last_grea_score
,test_last_greq_score
,test_last_grev_score
,test_last_ielts_score
,test_last_lsat_score
,test_last_sat_score
,test_last_satr_score
,test_last_toefl_internet_score
,test_last_toefl_paper_score
,test_max_act_score
,test_max_gmat_score
,test_max_gre_score
,test_max_grea_score
,test_max_greq_score
,test_max_grev_score
,test_max_ielts_score
,test_max_lsat_score
,test_max_sat_score
,test_max_satm_score
,test_max_satr_m_score
,test_max_satr_r_score
,test_max_satr_score
,test_max_satv_score
,test_max_satw_score
,test_max_toefl_internet_score
,test_max_toefl_paper_score
,test_num_act_attempts
,test_num_gmat_attempts
,test_num_gre_attempts
,test_num_ielts_attempts
,test_num_lsat_attempts
,test_num_sat_attempts
,test_num_toefl_internet_attempts
,test_num_toefl_paper_attempts
,test_super_act_score
,test_super_gre_score
,test_super_sat_score
,test_super_satr_score
,time_academic_yr_range
,time_application_date
,time_application_month
,time_application_start_date
,time_application_start_month
,time_application_start_year
,time_application_year
,time_collapsed_strm
,time_collapsed_term_year_ld
,time_collapsed_year_term_ld
,time_enrl_add_day_key
,time_enrl_drop_day_key
,time_first_enrl_add_day_key
,time_fiscal_yr
,time_min_enrl_drop_day_key
,time_std_strm
,time_std_term_year_ld
,time_std_year_term_ld
,time_strm
,time_term_fiscal_year
,time_term_type_cd
,time_term_type_ld
,time_term_year_desc
,time_term_year_ld
,time_weeks_of_instruction
,toefl_wvd_flag
,tot_courses_taken
,tot_courses_withdrawn
,tot_disbursed_amount
,tot_fa_type
,tot_fellowship_disbursed_amount
,tot_fin_corporate_waiver_amt
,tot_fin_credit_hours
,tot_fin_fee_payment
,tot_fin_fees
,tot_fin_tot_payment_amt
,tot_fin_total_charges
,tot_fin_tuition
,tot_fin_tuition_payment
,tot_first_class_grade_point
,tot_first_class_section
,tot_first_course_id
,tot_first_crse_offer_nbr
,tot_first_session_cd
,tot_grade_points
,tot_grade_points_per_unit
,tot_grant_disbursed_amount
,tot_loan_disbursed_amount
,tot_other_disbursed_amount
,tot_scholarship_disbursed_amount
,tot_term_begin_dt
,tot_term_end_dt
,tot_time_enrl_add_day_key
,tot_time_enrl_drop_day_key
,tot_time_min_enrl_drop_day_key
,tot_unt_billing
,tot_unt_earned
,tot_vabenefits_disbursed_amount
,tot_waiver_disbursed_amount
,tot_workstudy_disbursed_amount
,tr_creds_when_acc
,transact_pri_acad_career_cd
,transact_pri_acad_career_ld
,transact_pri_acad_plan_cd
,transact_pri_acad_plan_ld
,transact_pri_acad_plan_type_ld
,transact_pri_acad_prog_cd
,transact_pri_acad_prog_ld
,transact_pri_bhe_api_hegis_ld
,transact_pri_grouping_cd
,transact_pri_grouping_ld
,transact_pri_plan_dept_cd
,transact_pri_plan_dept_ld
,transact_pri_school_cd
,transact_pri_school_ld
,transact_pri_specialization_ld
,transfer_address1
,transfer_address2
,transfer_city
,transfer_class_pop
,transfer_converted_gpa
,transfer_country
,transfer_county
,transfer_final_gpa
,transfer_last_year
,transfer_name
,transfer_postal
,transfer_proprietorship
,transfer_rank
,transfer_short_name
,transfer_start_year
,transfer_state
,transfer_term_year
,transfer_unt_comp_total
,transfer_years_attended
,trn_county
,trn_final_gpa
,trn_lat_from_zip
,trn_long_from_zip
,trn_transfer_inst_name
,trn_transfer_inst_name_and_location
,trn_unt_comp_total
,unt_billing
,unt_earned
,urm_status
,vabenefits_disbursed_amount
,waiver_disbursed_amount
,workstudy_disbursed_amount 
)
select
acad_career,
acad_level_bot_ld,
acad_stndng_actn_cd,
acad_stndng_actn_cd_bot,
acad_stndng_actn_ld,
acad_stndng_actn_ld_bot,
academic_yr,
accept_amount :: varchar,
act_band,
adm_appl_complete,
adm_appl_nbr_reverse_seq,
admission_address_city,
admission_address_country_ld,
admission_address_county,
admission_address_latitude,
admission_address_longitude,
admission_address_postal,
admission_address_state_cd,
admission_address_state_ld,
admission_address_type,
admission_address_zipcode,
admission_address1,
admission_address2,
admtted_ea,
advisor_business_email,
advisor_campus_email,
advisor_home_email,
advisor_id,
admit_honors_flag,
admit2matrdays ,
advisor_name,
aid_year,
app_acad_plan,
app_acad_plan_cd,
app_acad_plan_maj,
app_acad_prog,
app_adm_appl_ctr,
app_adm_appl_nbr,

app_admit_term_type,
app_admit_type_cd,
app_admit_type_desc,
app_alum_any,
app_alum_father,
app_alum_mother,
app_alum_other_relative,
app_alum_sibling,
app_appl_fee_dt,
app_appl_fee_status_cd,
app_appl_fee_type_ld,
app_degree_ind,
app_fin_aid_interest,
app_notification_plan_cd,
app_notification_plan_ld,
app_prior_appl_ind,
app_prog_action,
app_prog_action_desc,
app_prog_action_dt,
app_prog_active_date,
app_prog_active_ind,
app_prog_admit_term,

app_prog_admit_term_collapsed,
app_prog_admitted_date,
app_prog_admitted_ind,
app_prog_applied_ind,
app_prog_ca_ind,
app_prog_cancelled_date,
app_prog_cancelled_ind,
app_prog_discontinued_date,
app_prog_discontinued_ind,
app_prog_inquiry_ind,
app_prog_pre_matric_date,
app_prog_pre_matric_ind,
app_prog_prospect_ind,
app_prog_reason,
app_prog_reason_desc,
app_prog_req_term,
app_prog_status,
app_prog_status_desc,
app_prog_true_app_ind,
app_prog_waitlisted_date,
app_prog_waitlisted_ind,
app_satr_m_score,
app_satr_r_score,
app_stage,
app_stage_accepted_ind,
app_stage_admitted_ind,
app_stage_admitted_rev_ind,
app_stage_applied_ind,
app_stage_cancel_last_dt,
app_stage_cancel_paid_ind,
app_stage_cancelled_ind,
app_stage_comp_app,
app_stage_deny_app,
app_stage_deposit_paid_ind,
app_stage_deposit_paid_rev_ind,
app_stage_discontinued_ind,
app_stage_enrolled_ind,
app_stage_enrolled_rev_ind,
app_stage_graduated_ind,
app_stage_inquiry_ind,
app_stage_net_paid_ind,
app_stage_paid_last_dt,
app_stage_pre_accpeted_ind,
app_stage_prospect_ind,
app_stage_true_app,
app_stage_waitlisted_ind,
app_track_course_success_flag,
app_track_course_success_ind,
app_track_enrolled_ind,
app_track_first_enrl_date,
app2admitdays,
app2enrolldays,
appl_current_location,
appl_prog_nbr :: varchar,
appl_rating,
application_key,
applied_acad_career_cd,
applied_acad_career_collapsed_cd,
applied_acad_career_ld,
applied_acad_plan_cd,
applied_acad_plan_dept_cd,
applied_acad_plan_dept_ld,
applied_acad_plan_ld,
applied_acad_plan_type_cd,
applied_acad_plan_type_ld,
applied_acad_prog_cd,
applied_acad_prog_ld,
applied_bhe_api_hegis_ld,
applied_degree_code,
applied_ea,
applied_education_level_cd,
applied_education_level_ld,
applied_grouping_cd,
applied_grouping_ld,
applied_honors_flag,
applied_school_cd,
applied_school_ld,
applied_specialization_ld,
ath_recruit,
calendar_yr,
campus_latitude :: varchar,
campus_longitude :: varchar,
career_taken_credits :: varchar,
career_unt_passd_gpa :: varchar,
career_unt_passd_nogpa :: varchar,
career_unt_taken_gpa :: varchar,
career_unt_taken_nogpa :: varchar,
career_unt_term_tot :: varchar,
cell_phone,
cip2000_cd,
cip2000_ld,
citizenship_country,
citizenship_country_code,
citizenship_country_code_iso,
citizenship_country_iso,
citizenship_country_status_cd,
citizenship_country_status_ld,
courses_taken,
courses_withdrawn,
degr_acad_plan_maj,
degr_application_dt,
degr_chkout_stat_cd,
degr_chkout_stat_sd,
degr_completion_term,
degr_confer_dt,
degr_gpa :: varchar,
degr_honors_cd,
degr_honors_ld,
degr_status_dt,
degree_cd,
null as degree_code,
degree_ld,
degree_type_cd,
degree_type_ld,
dem_ethnicity,
dem_gender,
dem_marital_status,
disbursed_amount :: varchar,
effdt,
effseq,
enroll2graddays,
enrollscoore_flag_canc,
enrollscore_enrl_score,
enrollscore_entry_stat,
enrollscore_flag_con,
enrollscore_flag_enr,
NULL as eps_market,
erp_geo_break,
exp_grad_term,
exp_grad_term_year_ld,
fa_type,
federal_grant_accept_amount :: varchar,
federal_grant_disbursed_amount :: varchar,
federal_grant_offer_amount :: varchar,
federal_loan_accept_amount :: varchar,
federal_loan_disbursed_amount :: varchar,
federal_loan_offer_amount :: varchar,
federal_workstudy_accept_amount :: varchar,
federal_workstudy_disbursed_amount :: varchar,
federal_workstudy_offer_amount :: varchar,
fellowship_disbursed_amount :: varchar,
fin_corporate_waiver_amt,
fin_credit_hours,
fin_fee_payment,
fin_fees,
fin_tot_payment_amt,
fin_total_charges,
fin_tuition,
fin_tuition_payment,
first_class_grade_point,
first_class_section,
first_course_id,
first_crse_offer_nbr :: varchar,
first_session_cd,
freeze_strm,
null as gender,
geo_dma,
geo_state,
gmat_wvd_flag,
grade_points,
grade_points_per_unit,
grant_disbursed_amount :: varchar,
gre_wvd_flag,
home_phone,
honors_acad_prog_ind,
honors_offer,
honors_offer_acc,
housing_interest_cd,
housing_interest_ld,
housing_interest_sd,
hs_address1,
hs_address2,
hs_city,
hs_class_pop,
hs_country,
hs_county,
hs_final_gpa :: varchar,
hs_inst_name_and_location,
hs_last_year,
hs_lat,
hs_lat_from_zip :: varchar,
hs_lat_long_source_flag,
hs_lat_manual,
hs_long,
hs_long_from_zip :: varchar,
hs_long_manual,
hs_name,
hs_postal,
hs_proprietorship,
hs_rank,
hs_reported_class_place,
hs_reported_class_size,
hs_short_name,
hs_start_year,
hs_state,
hs_term_year,
hs_unt_comp_total :: varchar,
hs_years_attended :: int,
institutional_grant_accept_amount :: varchar,
institutional_grant_disbursed_amount :: varchar,
institutional_grant_offer_amount :: varchar,
institutional_other_accept_amount :: varchar,
institutional_other_disbursed_amount :: varchar,
institutional_other_offer_amount :: varchar,
institutional_scholarship_accept_amount :: varchar,
institutional_scholarship_disbursed_amount :: varchar,
institutional_scholarship_offer_amount :: varchar,
isir_adj_efc,
isir_agi :: varchar,
isir_calc_efc,
isir_calc_sc,
isir_children,
isir_citizenship_status,
isir_current_fa_app_date,
isir_current_grade_lvl,
isir_degree_certif,
isir_dependents,
isir_depndncy_stat,
isir_efc_status,
isir_efc_status2,
isir_father_grade_lvl,
isir_fed_efc,
isir_fed_need :: varchar,
isir_fed_need_base_aid :: varchar,
isir_fed_stdnt_contrb,
isir_fed_unmet_need :: varchar,
isir_fed_year_coa :: varchar,
isir_first_bach_degree,
isir_first_fa_app_date,
isir_graduate_student,
isir_housing_code_1,
isir_housing_code_2,
isir_housing_code_3,
isir_housing_code_4,
isir_housing_code_5,
isir_housing_code_6,
isir_inst_need :: varchar,
isir_inst_unmet_need :: varchar,
isir_inst_year_coa :: varchar,
isir_interested_in_plus,
isir_interested_in_sl,
isir_interested_in_ws,
isir_marital_stat,
isir_married,
isir_mother_grade_lvl,
isir_num_family_members,
isir_number_in_college,
isir_orphan,
isir_parent_agi :: varchar,
isir_parent_marital_stat,
isir_parent_num_in_college,
isir_parent_number_in_family,
isir_parent_sfa_grant_aid :: varchar,
isir_pell_eligibility,
isir_pell_year_coa :: varchar,
isir_school_choice_1,
isir_school_choice_2,
isir_school_choice_3,
isir_school_choice_4,
isir_school_choice_5,
isir_school_choice_6,
isir_sfa_active_duty,
isir_ssa_citizenshp_ind,
isir_titleiv_elig,
isir_veteran,
last_applied_stage,
loan_disbursed_amount :: varchar,
matr2enrolldays :: varchar,
military_status_cd,
military_status_ld,
next_effdt,
offer_amount :: varchar,
other_disbursed_amount :: varchar,
other_scholarship_accept_amount :: varchar,
other_scholarship_disbursed_amount :: varchar,
other_scholarship_offer_amount :: varchar,
paid_flag,
perseus_load_date,
persistence_flg :: varchar,
pi_address1,
pi_address2,
pi_adm_typ_address1,
pi_adm_typ_address2,
pi_adm_typ_city,
pi_adm_typ_country,
pi_adm_typ_county,
pi_adm_typ_final_gpa
pi_adm_typ_name,
pi_adm_typ_postal,
pi_adm_typ_short_name,
pi_adm_typ_state,
pi_city,
pi_class_pop,
pi_converted_gpa :: real,
pi_country,
pi_county,
pi_degr_confer_dt,
pi_degree_cd,
pi_degree_gpa,
pi_degree_ld,
pi_degree_type_cd,
pi_degree_type_ld,
pi_final_gpa :: real,
pi_inst_name_and_location,
pi_institution_ld,
pi_last_year,
pi_lat_from_zip :: varchar,
pi_long_from_zip :: varchar,
pi_name,
pi_postal,
pi_proprietorship,
pi_rank,
pi_short_name,
pi_start_year,
pi_state,
pi_term_year,
pi_unt_comp_total :: varchar,
pi_years_attended :: varchar,
plan_acad_career_cd,
plan_acad_career_desc,
plan_acad_org_program_cd,
plan_acad_org_program_ld,
plan_acad_org_school_cd,
plan_acad_org_school_ld,
plan_acad_org_spec_ld,
plan_field_of_study_cd,
plan_field_of_study_ld,
plan_grouping,
plan_grouping_cd,
plan_institution_cd,
plan_institution_ld,
plan_sub_plan1,
plan_sub_plan1_cd,
plan_sub_plan1_type,
plan_sub_plan2,
plan_sub_plan2_cd,
plan_sub_plan2_type,
plan_sub_plan3,
plan_sub_plan3_cd,
plan_sub_plan3_type,
plan_type,
pref_address1,
pref_address2,
pref_city,
pref_country_ld,
pref_county,
pref_postal,
pref_state_cd,
pref_state_ld,
pref_zipcode,
pri_acad_plan_dept_cd,
pri_acad_plan_dept_ld,
pri_bhe_api_hegis_cd,
pri_bhe_api_hegis_ld,
private_grant_accept_amount :: varchar,
private_grant_disbursed_amount :: varchar,
private_grant_offer_amount :: varchar,
private_loan_accept_amount :: varchar,
private_loan_disbursed_amount :: varchar,
private_loan_offer_amount :: varchar,
private_scholarship_accept_amount :: varchar,
private_scholarship_disbursed_amount :: varchar,
private_scholarship_offer_amount :: varchar,
prog_stage_cd,
prog_stage_dt,
prog_status_cd,
prog_status_ld,
reenrollment_type,
res_bldg,
res_room,
res_room_type,
ret_cohort_strm,
ret_full_part_time_ld,
retention_year_1_flag :: varchar,
retention_year_1_status,
retention_year_10_flag :: varchar,
retention_year_10_status,
retention_year_2_flag :: varchar,
retention_year_2_status,
retention_year_3_flag :: varchar,
retention_year_3_status,
retention_year_4_flag :: varchar,
retention_year_4_status,
retention_year_5_flag :: varchar,
retention_year_5_status,
retention_year_6_flag :: varchar,
retention_year_6_status,
retention_year_7_flag :: varchar,
retention_year_7_status,
retention_year_8_flag :: varchar,
retention_year_8_status,
retention_year_9_flag :: varchar,
retention_year_9_status,
sat_band,
scholarship_disbursed_amount :: varchar,
sec_bhe_api_hegis_cd,
sec_bhe_api_hegis_ld,
source_category,
source_memo,
source_summary,
state_grant_accept_amount :: varchar,
state_grant_disbursed_amount :: varchar,
state_grant_offer_amount :: varchar,
state_scholarship_accept_amount :: varchar,
state_scholarship_disbursed_amount :: varchar,
state_scholarship_offer_amount :: varchar,
stdnt_acad_car_collapsed_cd  :: varchar,
stdnt_acad_career_cd,
stdnt_acad_career_ld,
stdnt_acad_level_bot_cd,
stdnt_acad_level_eot_cd,
stdnt_acad_level_eot_ld,
stdnt_acad_plan_cd,
stdnt_acad_plan_dept_cd,
stdnt_acad_plan_dept_ld,
stdnt_acad_plan_ld,
stdnt_acad_plan_type_ld,
stdnt_acad_prog_cd,
stdnt_acad_prog_ld,
stdnt_admit_type_group,
stdnt_admit_type_group_cd,
stdnt_admit_type_group_ld,
stdnt_approved_acad_load_cd,
stdnt_approved_acad_load_ld,
stdnt_approved_acad_load_sd,
stdnt_bhe_api_hegis_ld,
stdnt_birthdate_day,
stdnt_business_email,
stdnt_campus_email,
stdnt_cntct_address_type,
stdnt_cntct_address1,
stdnt_cntct_address2,
stdnt_cntct_city,
stdnt_cntct_country_ld,
stdnt_cntct_county,
stdnt_cntct_email,
stdnt_cntct_home_email,
stdnt_cntct_latitude  :: varchar,
stdnt_cntct_longitude  :: varchar,
stdnt_cntct_phone,
stdnt_cntct_postal,
stdnt_cntct_state_cd,
stdnt_cntct_state_ld,
stdnt_cntct_zipcode,
stdnt_degree_code,
stdnt_dev_ed_engl_ind  :: varchar,
stdnt_dev_ed_ind  :: varchar,
stdnt_dev_ed_math_ind  :: varchar,
stdnt_education_level_cd,
stdnt_education_level_ld,
stdnt_emplid,
stdnt_enrolled_load_cd,
stdnt_enrolled_load_ld,
stdnt_first_gen_flag,
stdnt_first_name,
stdnt_full_part_time_ld,
stdnt_grouping_cd,
stdnt_grouping_ld,
stdnt_last_name,
stdnt_new_returning_desc,
stdnt_residency_ld,
stdnt_school_cd,
stdnt_school_ld,
stdnt_specialization_ld,
stdnt_student_campus,
stdnt_tuition_residency,
stdnt_tuition_residency_cd,
term_begin_day,
term_begin_dt,
term_degr_chkout_stat_cd,
term_degr_chkout_stat_sd,
term_end_day,
test_avg_act_score,
term_end_dt,
test_avg_gmat_score,
test_avg_gre_score,
test_avg_grea_score,
test_avg_greq_score,
test_avg_grev_score,
test_avg_ielts_score,
test_avg_lsat_score,
test_avg_sat_score,
test_avg_satr_score,
test_avg_toefl_internet_score,
test_avg_toefl_paper_score,
test_first_act_score,
test_first_gmat_score,
test_first_gre_score,
test_first_grea_score,
test_first_greq_score,
test_first_grev_score,
test_first_ielts_score,
test_first_lsat_score,
test_first_sat_score,
test_first_satr_score,
test_first_toefl_internet_score,
test_first_toefl_paper_score,
test_last_act_score,
test_last_gmat_score,
test_last_gre_score,
test_last_grea_score,
test_last_greq_score,
test_last_grev_score,
test_last_ielts_score,
test_last_lsat_score,
test_last_sat_score,
test_last_satr_score,
test_last_toefl_internet_score,
test_last_toefl_paper_score,
test_max_act_score,
test_max_gmat_score,
test_max_gre_score,
test_max_grea_score,
test_max_greq_score,
test_max_grev_score,
test_max_ielts_score,
test_max_lsat_score,
test_max_sat_score,
test_max_satm_score,
test_max_satr_m_score,
test_max_satr_r_score,
test_max_satr_score,
test_max_satv_score,
test_max_satw_score,
test_max_toefl_internet_score,
test_max_toefl_paper_score,
test_num_act_attempts,
test_num_gmat_attempts,
test_num_gre_attempts,
test_num_ielts_attempts,
test_num_lsat_attempts,
test_num_sat_attempts,
test_num_toefl_internet_attempts,
test_num_toefl_paper_attempts,
test_super_act_score,
test_super_gre_score,
test_super_sat_score,
test_super_satr_score,
time_academic_yr_range,
time_application_date,
time_application_month,
time_application_start_date,
time_application_start_month,
time_application_start_year,
time_application_year,
time_collapsed_strm,
time_collapsed_term_year_ld,
time_collapsed_year_term_ld,
time_enrl_add_day_key,
time_enrl_drop_day_key,
time_first_enrl_add_day_key,
time_fiscal_yr,
time_min_enrl_drop_day_key,
time_std_strm,
time_std_term_year_ld,
time_std_year_term_ld,
time_strm,
time_term_fiscal_year,
time_term_type_cd,
time_term_type_ld,
time_term_year_desc,
time_term_year_ld,
time_weeks_of_instruction,
toefl_wvd_flag,
tot_courses_taken,
tot_courses_withdrawn,
tot_disbursed_amount :: varchar, 
tot_fa_type,
tot_fellowship_disbursed_amount :: varchar, 
tot_fin_corporate_waiver_amt,
tot_fin_credit_hours,
tot_fin_fee_payment,
tot_fin_fees,
tot_fin_tot_payment_amt,
tot_fin_total_charges,
tot_fin_tuition,
tot_fin_tuition_payment,
tot_first_class_grade_point,
tot_first_class_section,
tot_first_course_id,
tot_first_crse_offer_nbr :: varchar, 
tot_first_session_cd,
tot_grade_points,
tot_grade_points_per_unit,
tot_grant_disbursed_amount :: varchar,
tot_loan_disbursed_amount :: varchar,
tot_other_disbursed_amount :: varchar,
tot_scholarship_disbursed_amount :: varchar,
tot_term_begin_dt,
tot_term_end_dt,
tot_time_enrl_add_day_key,
tot_time_enrl_drop_day_key,
tot_time_min_enrl_drop_day_key,
tot_unt_billing,
tot_unt_earned,
tot_vabenefits_disbursed_amount :: varchar, 
tot_waiver_disbursed_amount :: varchar,
tot_workstudy_disbursed_amount :: varchar,
tr_creds_when_acc,
transact_pri_acad_career_cd,
transact_pri_acad_career_ld,
transact_pri_acad_plan_cd,
transact_pri_acad_plan_ld,
transact_pri_acad_plan_type_ld,
transact_pri_acad_prog_cd,
transact_pri_acad_prog_ld,
transact_pri_bhe_api_hegis_ld,
transact_pri_grouping_cd,
transact_pri_grouping_ld,
transact_pri_plan_dept_cd,
transact_pri_plan_dept_ld,
transact_pri_school_cd,
transact_pri_school_ld,
transact_pri_specialization_ld,
transfer_address1,
transfer_address2,
transfer_city,
transfer_class_pop,
transfer_converted_gpa :: real,
transfer_country,
transfer_county,
transfer_final_gpa :: real,
transfer_last_year,
transfer_name,
transfer_postal,
transfer_proprietorship,
transfer_rank,
transfer_short_name,
transfer_start_year,
transfer_state,
transfer_term_year :: varchar,
transfer_unt_comp_total :: varchar,
transfer_years_attended :: varchar,
trn_county,
trn_final_gpa :: real,
trn_lat_from_zip :: varchar,
trn_long_from_zip :: varchar,
trn_transfer_inst_name,
trn_transfer_inst_name_and_location,
trn_unt_comp_total :: varchar,
unt_billing,
unt_earned,
urm_status,
vabenefits_disbursed_amount :: varchar,
waiver_disbursed_amount :: varchar,
workstudy_disbursed_amount :: varchar
from perseus.admissions_extract ae 
union all 
select
acad_career,
acad_level_bot_ld,
acad_stndng_actn_cd,
acad_stndng_actn_cd_bot,
acad_stndng_actn_ld,
acad_stndng_actn_ld_bot,
academic_yr,
accept_amount,
act_band,
adm_appl_complete,
adm_appl_nbr_reverse_seq,
admission_address_city,
admission_address_country_ld,
admission_address_county,
admission_address_latitude,
admission_address_longitude,
admission_address_postal,
admission_address_state_cd,
admission_address_state_ld,
admission_address_type,
admission_address_zipcode,
admission_address1,
admission_address2,
admtted_ea,
advisor_business_email,
advisor_campus_email,
advisor_home_email,
advisor_id,
NULL as admit_honors_flag,
NULL :: bigint as admit2matrdays,
advisor_name,
aid_year,
app_acad_plan,
app_acad_plan_cd,
app_acad_plan_maj,
app_acad_prog,
app_adm_appl_ctr,
app_adm_appl_nbr,

app_admit_term_type,
app_admit_type_cd,
app_admit_type_desc,
app_alum_any,
app_alum_father,
app_alum_mother,
app_alum_other_relative,
app_alum_sibling,
app_appl_fee_dt :: timestamp,
app_appl_fee_status_cd,
app_appl_fee_type_ld,
app_degree_ind,
app_fin_aid_interest,
app_notification_plan_cd,
app_notification_plan_ld,
app_prior_appl_ind,
app_prog_action,
app_prog_action_desc,
app_prog_action_dt :: timestamp,
app_prog_active_date :: timestamp,
app_prog_active_ind,
app_prog_admit_term,

app_prog_admit_term_collapsed,
app_prog_admitted_date :: timestamp, 
app_prog_admitted_ind,
app_prog_applied_ind,
app_prog_ca_ind,
app_prog_cancelled_date :: timestamp, 
app_prog_cancelled_ind,
app_prog_discontinued_date :: timestamp,
app_prog_discontinued_ind,
app_prog_inquiry_ind,
app_prog_pre_matric_date :: timestamp, 
app_prog_pre_matric_ind,
app_prog_prospect_ind,
app_prog_reason,
app_prog_reason_desc,
app_prog_req_term,
app_prog_status,
app_prog_status_desc,
app_prog_true_app_ind,
app_prog_waitlisted_date :: timestamp,
app_prog_waitlisted_ind,
0 as app_satr_m_score,
0 as app_satr_r_score,
app_stage,
app_stage_accepted_ind,
app_stage_admitted_ind,
app_stage_admitted_rev_ind,
app_stage_applied_ind,
NULL as app_stage_cancel_last_dt,
NULL :: int as app_stage_cancel_paid_ind,
app_stage_cancelled_ind,
app_stage_comp_app,
app_stage_deny_app,
app_stage_deposit_paid_ind,
app_stage_deposit_paid_rev_ind,
app_stage_discontinued_ind,
app_stage_enrolled_ind,
app_stage_enrolled_rev_ind,
app_stage_graduated_ind,
app_stage_inquiry_ind,
NULL :: int as app_stage_net_paid_ind,
NULL :: date as app_stage_paid_last_dt,
app_stage_pre_accpeted_ind,
app_stage_prospect_ind,
app_stage_true_app,
app_stage_waitlisted_ind,
app_track_course_success_flag,
app_track_course_success_ind,
app_track_enrolled_ind,
app_track_first_enrl_date,
NULL :: bigint as app2admitdays,
NULL :: bigint as app2enrolldays,
appl_current_location,
appl_prog_nbr,
appl_rating,
application_key,
applied_acad_career_cd,
applied_acad_career_collapsed_cd,
NULL as applied_acad_career_ld,
applied_acad_plan_cd,
applied_acad_plan_dept_cd,
applied_acad_plan_dept_ld,
applied_acad_plan_ld,
NULL as applied_acad_plan_type_cd,
applied_acad_plan_type_ld,
applied_acad_prog_cd,
applied_acad_prog_ld,
applied_bhe_api_hegis_ld,
null as applied_degree_code,
applied_ea,
applied_education_level_cd,
applied_education_level_ld,
applied_grouping_cd,
applied_grouping_ld,
NULL as applied_honors_flag,
applied_school_cd,
applied_school_ld,
applied_specialization_ld,
ath_recruit,
calendar_yr,
campus_latitude,
campus_longitude,
career_taken_credits,
career_unt_passd_gpa,
career_unt_passd_nogpa,
career_unt_taken_gpa,
career_unt_taken_nogpa,
career_unt_term_tot,
cell_phone,
NULL as cip2000_cd,
NULL as cip2000_ld,
citizenship_country,
citizenship_country_code,
citizenship_country_code_iso,
citizenship_country_iso,
citizenship_country_status_cd,
citizenship_country_status_ld,
courses_taken,
courses_withdrawn,
degr_acad_plan_maj,
degr_application_dt :: date, 
degr_chkout_stat_cd,
degr_chkout_stat_sd,
degr_completion_term,
degr_confer_dt,
degr_gpa,
degr_honors_cd,
degr_honors_ld,
degr_status_dt :: date,
degree_cd,
degree_code,
degree_ld,
degree_type_cd,
degree_type_ld,
dem_ethnicity,
dem_gender,
dem_marital_status,
disbursed_amount,
effdt :: timestamp,
effseq,
NULL :: bigint as enroll2graddays,
enrollscoore_flag_canc,
enrollscore_enrl_score,
enrollscore_entry_stat,
enrollscore_flag_con,
enrollscore_flag_enr,
eps_market,
erp_geo_break,
exp_grad_term :: varchar,
exp_grad_term_year_ld,
fa_type,
federal_grant_accept_amount,
federal_grant_disbursed_amount,
federal_grant_offer_amount,
federal_loan_accept_amount,
federal_loan_disbursed_amount,
federal_loan_offer_amount,
federal_workstudy_accept_amount,
federal_workstudy_disbursed_amount,
federal_workstudy_offer_amount,
fellowship_disbursed_amount,
fin_corporate_waiver_amt,
fin_credit_hours,
fin_fee_payment,
fin_fees,
fin_tot_payment_amt,
fin_total_charges,
fin_tuition,
fin_tuition_payment,
first_class_grade_point,
first_class_section,
first_course_id,
first_crse_offer_nbr,
first_session_cd,
freeze_strm,
gender,
geo_dma,
geo_state,
gmat_wvd_flag,
grade_points,
grade_points_per_unit,
grant_disbursed_amount,
gre_wvd_flag,
home_phone,
honors_acad_prog_ind,
honors_offer,
honors_offer_acc,
housing_interest_cd,
housing_interest_ld,
housing_interest_sd,
hs_address1,
hs_address2,
hs_city,
hs_class_pop,
hs_country,
hs_county,
hs_final_gpa,
hs_inst_name_and_location,
hs_last_year :: varchar,
hs_lat,
hs_lat_from_zip,
hs_lat_long_source_flag,
hs_lat_manual,
hs_long,
hs_long_from_zip,
hs_long_manual,
hs_name,
hs_postal,
hs_proprietorship,
hs_rank,
hs_reported_class_place,
hs_reported_class_size,
hs_short_name,
hs_start_year  :: varchar,
hs_state,
hs_term_year,
hs_unt_comp_total,
hs_years_attended,
institutional_grant_accept_amount,
institutional_grant_disbursed_amount,
institutional_grant_offer_amount,
institutional_other_accept_amount,
institutional_other_disbursed_amount,
institutional_other_offer_amount,
institutional_scholarship_accept_amount,
institutional_scholarship_disbursed_amount,
institutional_scholarship_offer_amount,
isir_adj_efc,
isir_agi,
isir_calc_efc,
isir_calc_sc,
isir_children,
isir_citizenship_status,
isir_current_fa_app_date :: timestamp,
isir_current_grade_lvl,
isir_degree_certif,
isir_dependents,
isir_depndncy_stat,
isir_efc_status,
isir_efc_status2,
isir_father_grade_lvl,
isir_fed_efc,
isir_fed_need,
isir_fed_need_base_aid,
isir_fed_stdnt_contrb,
isir_fed_unmet_need,
isir_fed_year_coa,
isir_first_bach_degree,
isir_first_fa_app_date :: timestamp,
isir_graduate_student,
isir_housing_code_1,
isir_housing_code_2,
isir_housing_code_3,
isir_housing_code_4,
isir_housing_code_5,
isir_housing_code_6,
isir_inst_need,
isir_inst_unmet_need,
isir_inst_year_coa,
isir_interested_in_plus,
isir_interested_in_sl,
isir_interested_in_ws,
isir_marital_stat,
isir_married,
isir_mother_grade_lvl,
isir_num_family_members,
isir_number_in_college,
isir_orphan,
isir_parent_agi,
isir_parent_marital_stat,
isir_parent_num_in_college,
isir_parent_number_in_family,
isir_parent_sfa_grant_aid,
isir_pell_eligibility,
isir_pell_year_coa,
isir_school_choice_1,
isir_school_choice_2,
isir_school_choice_3,
isir_school_choice_4,
isir_school_choice_5,
isir_school_choice_6,
isir_sfa_active_duty,
isir_ssa_citizenshp_ind,
isir_titleiv_elig,
isir_veteran,
last_applied_stage,
loan_disbursed_amount,
NULL as matr2enrolldays,
NULL as military_status_cd,
NULL as military_status_ld,
NULL :: timestamp as next_effdt,
offer_amount,
other_disbursed_amount,
other_scholarship_accept_amount,
other_scholarship_disbursed_amount,
other_scholarship_offer_amount,
paid_flag,
perseus_load_date,
persistence_flg,
pi_address1,
pi_address2,
pi_adm_typ_address1,
pi_adm_typ_address2,
pi_adm_typ_city,
pi_adm_typ_country,
pi_adm_typ_county,
pi_adm_typ_final_gpa
pi_adm_typ_name,
pi_adm_typ_postal,
pi_adm_typ_short_name,
pi_adm_typ_state,
pi_city,
pi_class_pop,
pi_converted_gpa,
pi_country,
pi_county,
pi_degr_confer_dt :: timestamp,
pi_degree_cd,
pi_degree_gpa,
pi_degree_ld,
pi_degree_type_cd,
pi_degree_type_ld,
pi_final_gpa,
pi_inst_name_and_location,
pi_institution_ld,
pi_last_year :: varchar,
pi_lat_from_zip,
pi_long_from_zip,
pi_name,
pi_postal,
pi_proprietorship,
pi_rank,
pi_short_name,
pi_start_year :: varchar,
pi_state,
pi_term_year,
pi_unt_comp_total,
pi_years_attended :: varchar,
plan_acad_career_cd,
plan_acad_career_desc,
plan_acad_org_program_cd,
plan_acad_org_program_ld,
plan_acad_org_school_cd,
plan_acad_org_school_ld,
plan_acad_org_spec_ld,
plan_field_of_study_cd,
plan_field_of_study_ld,
plan_grouping,
plan_grouping_cd,
plan_institution_cd,
plan_institution_ld,
plan_sub_plan1,
plan_sub_plan1_cd,
plan_sub_plan1_type,
plan_sub_plan2,
plan_sub_plan2_cd,
plan_sub_plan2_type,
plan_sub_plan3,
plan_sub_plan3_cd,
plan_sub_plan3_type,
plan_type,
pref_address1,
pref_address2,
pref_city,
pref_country_ld,
pref_county,
pref_postal,
pref_state_cd,
pref_state_ld,
pref_zipcode,
pri_acad_plan_dept_cd,
pri_acad_plan_dept_ld,
pri_bhe_api_hegis_cd,
pri_bhe_api_hegis_ld,
private_grant_accept_amount,
private_grant_disbursed_amount,
private_grant_offer_amount,
private_loan_accept_amount,
private_loan_disbursed_amount,
private_loan_offer_amount,
private_scholarship_accept_amount,
private_scholarship_disbursed_amount,
private_scholarship_offer_amount,
prog_stage_cd,
prog_stage_dt :: timestamp,
prog_status_cd,
prog_status_ld,
reenrollment_type,
res_bldg,
res_room,
res_room_type,
ret_cohort_strm,
ret_full_part_time_ld,
retention_year_1_flag,
retention_year_1_status,
retention_year_10_flag,
retention_year_10_status,
retention_year_2_flag,
retention_year_2_status,
retention_year_3_flag,
retention_year_3_status,
retention_year_4_flag,
retention_year_4_status,
retention_year_5_flag,
retention_year_5_status,
retention_year_6_flag,
retention_year_6_status,
retention_year_7_flag,
retention_year_7_status,
retention_year_8_flag,
retention_year_8_status,
retention_year_9_flag,
retention_year_9_status,
sat_band,
scholarship_disbursed_amount,
sec_bhe_api_hegis_cd,
sec_bhe_api_hegis_ld,
source_category,
source_memo,
source_summary,
state_grant_accept_amount,
state_grant_disbursed_amount,
state_grant_offer_amount,
state_scholarship_accept_amount,
state_scholarship_disbursed_amount,
state_scholarship_offer_amount,
stdnt_acad_car_collapsed_cd,
stdnt_acad_career_cd,
stdnt_acad_career_ld,
NULL as stdnt_acad_level_bot_cd,
NULL as stdnt_acad_level_eot_cd,
NULL as stdnt_acad_level_eot_ld,
stdnt_acad_plan_cd,
stdnt_acad_plan_dept_cd,
stdnt_acad_plan_dept_ld,
stdnt_acad_plan_ld,
stdnt_acad_plan_type_ld,
stdnt_acad_prog_cd,
stdnt_acad_prog_ld,
stdnt_admit_type_group,
NULL as stdnt_admit_type_group_cd,
NULL as stdnt_admit_type_group_ld,
stdnt_approved_acad_load_cd,
stdnt_approved_acad_load_ld,
NULL as stdnt_approved_acad_load_sd,
stdnt_bhe_api_hegis_ld,
stdnt_birthdate_day,
stdnt_business_email,
stdnt_campus_email,
stdnt_cntct_address_type,
stdnt_cntct_address1,
stdnt_cntct_address2,
stdnt_cntct_city,
stdnt_cntct_country_ld,
stdnt_cntct_county,
stdnt_cntct_email,
NULL as stdnt_cntct_home_email,
stdnt_cntct_latitude,
stdnt_cntct_longitude,
stdnt_cntct_phone,
stdnt_cntct_postal,
stdnt_cntct_state_cd,
stdnt_cntct_state_ld,
stdnt_cntct_zipcode,
stdnt_degree_code,
NULL as stdnt_dev_ed_engl_ind,
NULL as stdnt_dev_ed_ind,
NULL as stdnt_dev_ed_math_ind,
stdnt_education_level_cd,
stdnt_education_level_ld,
stdnt_emplid,
NULL as stdnt_enrolled_load_cd,
NULL as stdnt_enrolled_load_ld,
stdnt_first_gen_flag  :: varchar,
stdnt_first_name,
stdnt_full_part_time_ld,
stdnt_grouping_cd,
stdnt_grouping_ld,
stdnt_last_name,
stdnt_new_returning_desc,
stdnt_residency_ld,
stdnt_school_cd,
stdnt_school_ld,
stdnt_specialization_ld,
stdnt_student_campus,
stdnt_tuition_residency,
NULL as stdnt_tuition_residency_cd,
term_begin_day,
term_begin_dt,
term_degr_chkout_stat_cd,
term_degr_chkout_stat_sd,
term_end_day,
0 as test_avg_act_score,
term_end_dt,
0 as test_avg_gmat_score,
0 as test_avg_gre_score,
0 as test_avg_grea_score,
0 as test_avg_greq_score,
0 as test_avg_grev_score,
0 as test_avg_ielts_score,
0 as test_avg_lsat_score,
0 as test_avg_sat_score,
0 as test_avg_satr_score,
0 as test_avg_toefl_internet_score,
0 as test_avg_toefl_paper_score,
0 as test_first_act_score,
0 as test_first_gmat_score,
0 as test_first_gre_score,
0 as test_first_grea_score,
0 as test_first_greq_score,
0  as test_first_grev_score,
0 as test_first_ielts_score,
0 as test_first_lsat_score,
0 as test_first_sat_score,
0 as test_first_satr_score,
0 as test_first_toefl_internet_score,
0 as test_first_toefl_paper_score,
0 as test_last_act_score,
0 as test_last_gmat_score,
0 as test_last_gre_score,
0  as test_last_grea_score,
0  as test_last_greq_score,
0 as test_last_grev_score,
0 as test_last_ielts_score,
0 as test_last_lsat_score,
0 as test_last_sat_score,
0 as test_last_satr_score,
0 as test_last_toefl_internet_score,
0 as test_last_toefl_paper_score,
0 as test_max_act_score,
0 as test_max_gmat_score,
0 as test_max_gre_score,
0 as test_max_grea_score,
0 as test_max_greq_score,
0 as test_max_grev_score,
0 as test_max_ielts_score,
0 as test_max_lsat_score,
0 as test_max_sat_score,
0 as test_max_satm_score,
0 as test_max_satr_m_score,
0 as test_max_satr_r_score,
0 as test_max_satr_score,
0 as test_max_satv_score,
0 as test_max_satw_score,
0 as test_max_toefl_internet_score,
0 as test_max_toefl_paper_score,
0 as test_num_act_attempts,
0 as test_num_gmat_attempts,
0 as test_num_gre_attempts,
0 as test_num_ielts_attempts,
0 as test_num_lsat_attempts,
0 as test_num_sat_attempts,
0 as test_num_toefl_internet_attempts,
0 as test_num_toefl_paper_attempts,
0 as test_super_act_score,
0 as test_super_gre_score,
0 as test_super_sat_score,
0 as test_super_satr_score,
NULL as time_academic_yr_range,
time_application_date :: timestamp,
time_application_month,
time_application_start_date :: timestamp,
time_application_start_month,
time_application_start_year,
time_application_year,
NULL as time_collapsed_strm,
NULL as time_collapsed_term_year_ld,
NULL as time_collapsed_year_term_ld,
time_enrl_add_day_key,
time_enrl_drop_day_key,
time_first_enrl_add_day_key,
NULL as time_fiscal_yr,
time_min_enrl_drop_day_key,
NULL as time_std_strm,
NULL as time_std_term_year_ld,
NULL as time_std_year_term_ld,
time_strm,
time_term_fiscal_year,
NULL as time_term_type_cd,
NULL as time_term_type_ld,
time_term_year_desc,
NULL as time_term_year_ld,
NULL :: int as time_weeks_of_instruction,
toefl_wvd_flag,
tot_courses_taken,
tot_courses_withdrawn,
tot_disbursed_amount,
tot_fa_type,
tot_fellowship_disbursed_amount,
tot_fin_corporate_waiver_amt,
tot_fin_credit_hours,
tot_fin_fee_payment,
tot_fin_fees,
tot_fin_tot_payment_amt,
tot_fin_total_charges,
tot_fin_tuition,
tot_fin_tuition_payment,
tot_first_class_grade_point,
tot_first_class_section,
tot_first_course_id,
tot_first_crse_offer_nbr,
tot_first_session_cd,
tot_grade_points,
tot_grade_points_per_unit,
tot_grant_disbursed_amount,
tot_loan_disbursed_amount,
tot_other_disbursed_amount,
tot_scholarship_disbursed_amount,
tot_term_begin_dt,
tot_term_end_dt,
tot_time_enrl_add_day_key,
tot_time_enrl_drop_day_key,
tot_time_min_enrl_drop_day_key,
tot_unt_billing,
tot_unt_earned,
tot_vabenefits_disbursed_amount,
tot_waiver_disbursed_amount,
tot_workstudy_disbursed_amount,
tr_creds_when_acc,
transact_pri_acad_career_cd,
transact_pri_acad_career_ld,
transact_pri_acad_plan_cd,
transact_pri_acad_plan_ld,
transact_pri_acad_plan_type_ld,
transact_pri_acad_prog_cd,
transact_pri_acad_prog_ld,
transact_pri_bhe_api_hegis_ld,
transact_pri_grouping_cd,
transact_pri_grouping_ld,
transact_pri_plan_dept_cd,
transact_pri_plan_dept_ld,
transact_pri_school_cd,
transact_pri_school_ld,
transact_pri_specialization_ld,
transfer_address1,
transfer_address2,
transfer_city,
transfer_class_pop,
transfer_converted_gpa,
transfer_country,
transfer_county,
transfer_final_gpa,
transfer_last_year :: varchar,
transfer_name,
transfer_postal,
transfer_proprietorship,
transfer_rank,
transfer_short_name,
transfer_start_year :: varchar,
transfer_state,
transfer_term_year,
transfer_unt_comp_total,
transfer_years_attended :: varchar,
trn_county,
trn_final_gpa,
trn_lat_from_zip,
trn_long_from_zip,
trn_transfer_inst_name,
trn_transfer_inst_name_and_location,
trn_unt_comp_total,
unt_billing,
unt_earned,
urm_status,
vabenefits_disbursed_amount,
waiver_disbursed_amount,
workstudy_disbursed_amount
from perseus_workday.slate_admissions_extract sae;