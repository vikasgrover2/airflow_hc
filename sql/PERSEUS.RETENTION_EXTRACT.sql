/*###################################################
Object type       : PERSEUS EXTRACT TABLE
Description       :
Grain             :
Target table name : RETENTION_EXTRACT
Source table list : CS_ADMISSION (PERSEUS)
                    CS_DEGREES (PERSEUS)
                    CS_ENROLLMENTS (PERSEUS)
                    CS_FINANCIAL_AID (PERSEUS)
                    CS_PERSISTENCE (PERSEUS)
                    CS_SERVICE_INDICATORS (PERSEUS)
                    DIM_ADDRESS
                    DIM_ADDRESS_CASCADE
                    DIM_CAMPUS
                    DIM_CAR_PRGRM_PLAN
                    DIM_ISIR
                    DIM_LOCATION
                    DIM_OTH_RAW_ZIP_DATA_T (EDWUTIL)
                    DIM_PERSON (PERSEUS)
                    DIM_SUB_PLAN
                    DIM_TERM
                    FACT_AOS
                    FACT_AOS_SCD
                    FACT_PI_API
                    FACT_SF_ACCOUNT_AGING
                    FACT_TESTSCORE
                    FACT_TRANSCRIPTS
                    FF_RETENTION_GRAD_PROG (PERSEUS)
                    FF_STUDENT_TRACKER
                    HIGH_SCHOOL_LAT_AND_LONG
                    PERSEUS_LOOKUP (EDWUTIL)
                    STUDENT_TERM_EXTRACT (PERSEUS)
                    TEST_SCORE_CONVERSIONS_TBL
ETL load type     : FULL LOAD EVERYDAY
Date created      : 01-MARCH-2019
#####################################################*/


TRUNCATE TABLE perseus_workday.retention_extract;

INSERT INTO perseus_workday.retention_extract(
     eligible_to_enroll_ind
    ,officially_registered_ind
    ,stu_car_term_strm
    ,rel_strm_term_year_ld
    ,rel_strm_year_term_ld
    ,acad_stndng_actn_cd
    ,acad_stndng_actn_ld
    ,career_unt_passd_gpa
    ,career_unt_passd_nogpa
    ,career_unt_taken_nogpa
    ,career_unt_taken_gpa
    ,term_degr_chkout_stat_cd
    ,term_degr_chkout_stat_sd
    ,rel_exp_grad_term
    ,exp_grad_term_year_ld
    ,exp_grad_year
    ,rel_exp_grad_term_year_ld
    ,rel_exp_grad_year
    ,rel_admit_term
    ,rel_acad_prog
    ,rel_acad_plan_maj
    ,rel_acad_plan_min
    ,rel_prog_status_cd
    ,rel_prog_status_ld
    ,stdnt_emplid
    ,time_strm
    ,time_term_year
    ,term_end_day
    ,term_begin_day
    ,ret_year
    ,ret_term
    ,changed_program_flag
    ,non_enrolled_flag
    ,ret_status
    ,ret_status_collapsed
    ,ret_year_status
    ,ret_year_status_collapsed
    ,lag_1_ret_status
    ,lag_2_ret_status
    ,lead_1_ret_status
    ,lead_2_ret_status
    ,stdnt_student_career
    ,stdnt_student_campus
    ,stdnt_gender
    ,stdnt_ethnicity
    ,plan_major_code
    ,plan_primary_acad_prog
    ,new_or_returning
    ,cohort_flag
    ,stdnt_instr_mode_ld
    ,rel_stdnt_instr_mode_ld
    ,hs_final_gpa
    ,hs_final_gpa_excl_last_strm
    ,hs_last_year
    ,hs_start_year
    ,hs_years_attended
    ,hs_class_pop
    ,hs_rank
    ,hs_unt_comp_total
    ,hs_name
    ,hs_short_name
    ,hs_address1
    ,hs_address2
    ,hs_city
    ,hs_county
    ,hs_state
    ,hs_postal
    ,hs_country
    ,test_first_act_score
    ,test_last_act_score
    ,test_max_act_score
    ,test_avg_act_score
    ,test_num_act_attempts
    ,test_super_act_score
    ,test_super_sat_score
    ,test_first_sat_score
    ,test_last_sat_score
    ,test_max_sat_score
    ,test_avg_sat_score
    ,test_num_sat_attempts
    ,stdnt_cntct_address1
    ,stdnt_cntct_address2
    ,stdnt_cntct_address_type
    ,stdnt_cntct_city
    ,stdnt_cntct_country_ld
    ,stdnt_cntct_county
    ,stdnt_cntct_postal
    ,stdnt_cntct_state_cd
    ,stdnt_cntct_state_ld
    ,stdnt_cntct_zipcode
    ,stdnt_cntct_latitude
    ,stdnt_cntct_longitude
    ,campus_latitude
    ,campus_longitude
    ,hs_lat
    ,hs_long
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
    ,aid_year
    ,fa_type
    ,disbursed_amount
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
    ,rel_aid_year
    ,rel_fa_type
    ,rel_disbursed_amount
    ,rel_federal_grant_disbursed_amount
    ,rel_federal_loan_disbursed_amount
    ,rel_federal_workstudy_disbursed_amount
    ,rel_institutional_grant_disbursed_amount
    ,rel_institutional_other_disbursed_amount
    ,rel_institutional_scholarship_disbursed_amount
    ,rel_other_scholarship_disbursed_amount
    ,rel_private_grant_disbursed_amount
    ,rel_private_loan_disbursed_amount
    ,rel_private_scholarship_disbursed_amount
    ,rel_state_grant_disbursed_amount
    ,rel_state_scholarship_disbursed_amount
    ,req_term
    ,accepted_dt
    ,app_dt
    ,stdnt_admit_type_cd
    ,stdnt_admit_type_ld
    ,admit_term
    ,stdnt_tuition_residency
    ,rel_stdnt_tuition_residency
    ,stdnt_full_part_time_cd
    ,stdnt_full_part_time_ld
    ,rel_stdnt_full_part_time_cd
    ,rel_stdnt_full_part_time_ld
    ,dem_marital_status
    ,stdnt_birthdate_day
    ,tot_courses_taken
    ,tot_courses_withdrawn
    ,tot_unt_billing
    ,tot_unt_earned
    ,cum_tot_unt_earned
    ,cohort_tot_cumulative
    ,rel_tot_audit
    ,cohort_tot_audit
    ,rel_tot_trnsfr
    ,cohort_tot_trnsfr
    ,rel_tot_other
    ,cohort_tot_other
    ,rel_tot_test_credit
    ,cohort_tot_test_credit
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
    ,time_first_enrl_add_day_key
    ,rel_unt_billing
    ,rel_unt_earned
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
    ,cum_courses_taken
    ,cum_courses_dropped
    ,cum_courses_withdrawn
    ,cum_courses_passed
    ,cum_courses_failed
    ,courses_taken
    ,courses_dropped
    ,courses_withdrawn
    ,courses_passed
    ,courses_failed
    ,enrl_career_gpa_sem
    ,enrl_career_gpa_cum
    ,first_enrl_add_day
    ,first_enrl_drop_day
    ,enrl_term_end_dt
    ,enrl_term_begin_dt
    ,next_courses_taken
    ,next_courses_dropped
    ,next_courses_withdrawn
    ,next_courses_passed
    ,next_courses_failed
    ,next_unt_billing
    ,next_unt_earned
    ,next_grade_points
    ,next_grade_points_per_unit
    ,next_first_enrl_add_day
    ,next_first_enrl_drop_day
    ,next_enrl_term_end_dt
    ,next_enrl_term_begin_dt
    ,cohort_courses_taken
    ,cohort_courses_dropped
    ,cohort_courses_withdrawn
    ,cohort_courses_passed
    ,cohort_courses_failed
    ,cohort_unt_billing
    ,cohort_unt_earned
    ,cohort_grade_points
    ,cohort_grade_points_per_unit
    ,cohort_first_enrl_add_day
    ,cohort_first_enrl_drop_day
    ,cohort_enrl_term_end_dt
    ,cohort_enrl_term_begin_dt
    ,plan_acad_org_program_ld
    ,plan_acad_org_program_cd
    ,plan_acad_org_school_ld
    ,plan_acad_org_school_cd
    ,plan_acad_plan_ld
    ,plan_acad_plan_code
    ,acad_plan_type_ld
    ,rel_acad_plan_type_ld
    ,rel_plan_acad_org_program_ld
    ,rel_plan_major
    ,rel_plan_acad_org_program_cd
    ,rel_plan_acad_org_school_ld
    ,rel_plan_acad_org_school_cd
    ,rel_plan_acad_plan_ld
    ,rel_plan_acad_plan_code
    ,rel_plan_grouping_cd
    ,rel_plan_grouping
    ,plan_acad_prog
    ,plan_prog_status_ld
    ,plan_prog_status_cd
    ,plan_degree_type
    ,plan_institution_cd
    ,plan_institution_ld
    ,plan_grouping_cd
    ,plan_grouping
    ,plan_bridge_department
    ,stdnt_acad_career_cd
    ,stdnt_acad_car_collapsed_cd
    ,stdnt_acad_car_collapsed_ld
    ,stdnt_acad_career_ld
    ,stdnt_education_level_ld
    ,completion_term
    ,acad_plan_maj
    ,application_date
    ,honors_cd
    ,honors_ld
    ,prog_status_cd
    ,prog_status_ld
    ,degr_chkout_stat_cd
    ,degr_chkout_stat_sd
    ,degr_confer_dt
    ,degr_status_dt
    ,degree_gpa
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
    ,rel_isir_current_fa_app_date
    ,rel_isir_first_fa_app_date
    ,rel_isir_pell_eligibility
    ,isir_ever_pell_eligibility
    ,rel_isir_adj_efc
    ,rel_isir_efc_status
    ,rel_isir_titleiv_elig
    ,rel_isir_ssa_citizenshp_ind
    ,rel_isir_num_family_members
    ,rel_isir_number_in_college
    ,rel_isir_veteran
    ,rel_isir_graduate_student
    ,rel_isir_married
    ,rel_isir_marital_stat
    ,rel_isir_orphan
    ,rel_isir_dependents
    ,rel_isir_agi
    ,rel_isir_school_choice_1
    ,rel_isir_housing_code_1
    ,rel_isir_school_choice_2
    ,rel_isir_housing_code_2
    ,rel_isir_school_choice_3
    ,rel_isir_housing_code_3
    ,rel_isir_school_choice_4
    ,rel_isir_housing_code_4
    ,rel_isir_school_choice_5
    ,rel_isir_housing_code_5
    ,rel_isir_school_choice_6
    ,rel_isir_housing_code_6
    ,rel_isir_first_bach_degree
    ,rel_isir_interested_in_ws
    ,rel_isir_interested_in_sl
    ,rel_isir_interested_in_plus
    ,rel_isir_degree_certif
    ,rel_isir_current_grade_lvl
    ,rel_isir_depndncy_stat
    ,rel_isir_citizenship_status
    ,rel_isir_children
    ,rel_isir_sfa_active_duty
    ,rel_isir_parent_marital_stat
    ,rel_isir_parent_number_in_family
    ,rel_isir_parent_num_in_college
    ,rel_isir_parent_agi
    ,rel_isir_father_grade_lvl
    ,rel_isir_mother_grade_lvl
    ,rel_isir_parent_sfa_grant_aid
    ,rel_isir_fed_efc
    ,rel_isir_efc_status2
    ,rel_isir_fed_need_base_aid
    ,rel_isir_fed_year_coa
    ,rel_isir_inst_year_coa
    ,rel_isir_pell_year_coa
    ,rel_isir_fed_need
    ,rel_isir_inst_need
    ,rel_isir_fed_unmet_need
    ,rel_isir_inst_unmet_need
    ,rel_isir_fed_stdnt_contrb
    ,rel_isir_calc_sc
    ,rel_isir_calc_efc
    ,withdraw_reason_cd
    ,withdraw_reason_ld
    ,withdraw_cd
    ,withdraw_ld
    ,prog_action_cd
    ,prog_action_ld
    ,prog_reason_cd
    ,prog_reason_ld
    ,rel_strm
    ,persis_rel_strm
    ,continually_enrolled_flg
    ,plan_major
    ,stdnt_acad_level_bot_cd
    ,stdnt_acad_level_bot_ld
    ,stdnt_acad_level_eot_cd
    ,stdnt_acad_level_eot_ld
    ,rel_stdnt_acad_level_bot_cd
    ,rel_stdnt_acad_level_bot_ld
    ,rel_stdnt_acad_level_eot_cd
    ,rel_stdnt_acad_level_eot_ld
    ,initially_undecided_flag
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
    ,non_full_time_enrollment_flag
    ,change_in_tuition_residency_ld
    ,major_change_ever_flag
    ,second_year_ret_flag
    ,third_year_ret_flag
    ,second_year_grad_flag
    ,third_year_grad_flag
    ,four_year_grad_flag
    ,fifth_year_grad_flag
    ,six_year_grad_flag
    ,ret_category
    ,first_degree_seeking_strm
    ,dev_ed_flag
    ,gen_ed_flag
    ,stdnt_first_name
    ,stdnt_last_name
    ,home_phone
    ,cell_phone
    ,advisor_id
    ,advisor_name
    ,stdnt_home_email
    ,stdnt_business_email
    ,stdnt_campus_email
    ,stdnt_grp_first_gen
    ,nsc_transfer_flag
    ,pri_acad_plan_dept_cd
    ,pri_acad_plan_dept_ld
    ,urm_status
    ,academic_yr
    ,calendar_yr
    ,housing_interest_cd
    ,housing_interest_sd
    ,housing_interest_ld
    ,cip2000_cd
    ,cip2000_ld
    ,military_status_cd
    ,military_status_ld
    ,honors_acad_prog_ind
    ,acad_stndng_actn_cd_bot
    ,acad_stndng_actn_ld_bot
    ,leave_of_absence_ind
    ,stdnt_approved_acad_load_cd
    ,stdnt_approved_acad_load_ld
    ,rel_stdnt_approved_acad_load_cd
    ,rel_stdnt_approved_acad_load_ld
    ,stdnt_admit_type_group
    ,stdnt_car_nbr
    ,stdnt_dev_ed_engl_ind
    ,stdnt_dev_ed_math_ind
    ,stdnt_dev_ed_ind
    ,pri_bhe_api_hegis_cd
    ,pri_bhe_api_hegis_ld
    ,sec_bhe_api_hegis_cd
    ,sec_bhe_api_hegis_ld
    ,trn_transfer_inst_name_and_location
    ,hs_inst_name_and_location
    ,rel_trn_transfer_inst_name_and_location
    ,rel_hs_inst_name_and_location
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
  --{ins_custom_extract_fields}
 )
SELECT
    student_term_extract_eff.eligible_to_enroll_ind
    ,student_term_extract_eff.officially_registered_ind
    ,x.strm_plus_0 AS stu_car_term_strm
    ,dim_term_eff.term_year_ld AS rel_strm_term_year_ld
    ,dim_term_eff.year_term_ld AS rel_strm_year_term_ld
    ,student_term_extract_eff.acad_stndng_actn_cd
    ,student_term_extract_eff.acad_stndng_actn_ld
    ,student_term_extract.career_unt_passd_gpa
    ,student_term_extract.career_unt_passd_nogpa
    ,student_term_extract.career_unt_taken_nogpa
    ,student_term_extract.career_unt_taken_gpa
    ,student_term_extract_eff.degr_chkout_stat_cd AS term_degr_chkout_stat_cd
    ,student_term_extract_eff.degr_chkout_stat_sd AS term_degr_chkout_stat_sd
    ,student_term_extract_eff.exp_grad_term AS rel_exp_grad_term
    ,student_term_extract.exp_grad_term_year_ld
    ,student_term_extract.exp_grad_year
    ,student_term_extract_eff.exp_grad_term_year_ld AS rel_exp_grad_term_year_ld
    ,student_term_extract_eff.exp_grad_year AS rel_exp_grad_year
    ,student_term_extract_eff.app_admit_term AS rel_admit_term
    ,student_term_extract_eff.pri_acad_prog_cd AS rel_acad_prog
    ,student_term_extract_eff.pri_acad_plan_cd AS rel_acad_plan_maj
    ,student_term_extract_eff.sec_acad_plan_cd AS rel_acad_plan_min
    ,student_term_extract_eff.pri_prog_status_cd AS rel_prog_status_cd
    ,student_term_extract_eff.pri_prog_status_ld AS rel_prog_status_ld
    ,x.emplid AS stdnt_emplid
    ,x.cohort_strm AS time_strm
    ,dim_term.term_year_ld AS time_term_year
    ,CASE
      WHEN dim_term.term_end_day_key = 0 THEN NULL
      ELSE TO_DATE (dim_term.term_end_day_key,'J')
    END AS term_end_day
    ,CASE
      WHEN dim_term.term_begin_day_key = 0 THEN NULL
      ELSE TO_DATE (dim_term.term_begin_day_key,'J')
    END AS term_begin_day
    ,x.ret_year
    ,x.ret_term
    ,CASE
      WHEN x.academic_year_status = 2 AND (LEAD (x.academic_year_status) OVER (PARTITION BY x.emplid,x.student_career ORDER BY x.strm_plus_0 DESC)) = 1 THEN 'Y'
      ELSE 'N'
    END AS changed_program_flag
    ,CASE
      WHEN x.academic_year_status = 0 AND (LEAD (x.academic_year_status) OVER (PARTITION BY x.emplid,x.student_career ORDER BY x.strm_plus_0 DESC)) > 0 THEN 'Y'
      ELSE 'N'
    END AS non_enrolled_flag
    ,x.ret_status
    ,x.ret_status_collapsed
    ,x.ret_year_status
    ,x.ret_year_status_collapsed
    ,pg_catalog.lead (x.ret_status,1) OVER (PARTITION BY x.emplid,x.student_career ORDER BY x.strm_plus_0 DESC) AS lag_1_ret_status
    ,pg_catalog.lead (x.ret_status,2) OVER (PARTITION BY x.emplid,x.student_career ORDER BY x.strm_plus_0 DESC) AS lag_2_ret_status
    ,pg_catalog.lead (x.ret_status,1) OVER (PARTITION BY x.emplid,x.student_career ORDER BY x.strm_plus_0) AS lead_1_ret_status
    ,pg_catalog.lead (x.ret_status,2) OVER (PARTITION BY x.emplid,x.student_career ORDER BY x.strm_plus_0) AS lead_2_ret_status
    ,x.student_career AS stdnt_student_career
    ,student_term_extract.stdnt_campus_ld AS stdnt_student_campus
    ,dim_person.gender_ld AS stdnt_gender
    ,dim_person.ipeds_race_ethn_citz_ld AS stdnt_ethnicity
    ,x.acad_plan_cd AS plan_major_code
    ,x.primary_acad_prog AS plan_primary_acad_prog
    ,student_term_extract.univ_attend_type_sd AS new_or_returning
    ,x.cohort_flag
    ,student_term_extract.stdnt_instr_mode_ld
    ,student_term_extract_eff.stdnt_instr_mode_ld AS rel_stdnt_instr_mode_ld
    ,student_term_extract.hs_final_gpa
    ,student_term_extract.hs_final_gpa_excl_last_strm
    ,student_term_extract.hs_last_year
    ,student_term_extract.hs_start_year
    ,student_term_extract.hs_years_attended
    ,student_term_extract.hs_class_pop::integer  as hs_class_pop
    ,student_term_extract.hs_rank::integer  as  hs_rank
    ,student_term_extract.hs_unt_comp_total::integer  as hs_unt_comp_total
    ,student_term_extract.hs_name
    ,student_term_extract.hs_short_name
    ,student_term_extract.hs_address1
    ,student_term_extract.hs_address2
    ,student_term_extract.hs_city
    ,student_term_extract.hs_county
    ,student_term_extract.hs_state
    ,student_term_extract.hs_postal
    ,student_term_extract.hs_country
    ,dim_tests.first_act_score AS test_first_act_score
    ,dim_tests.last_act_score AS test_last_act_score
    ,dim_tests.max_act_score AS test_max_act_score
    ,dim_tests.avg_act_score AS test_avg_act_score
    ,dim_tests.num_act_attempts AS test_num_act_attempts
    ,COALESCE (dim_tests.super_act_score::float,sat_to_act_super.to_score::float) AS test_super_act_score
    ,COALESCE (dim_tests.super_sat_score::float,act_to_sat_super.to_score::float) AS test_super_sat_score
    ,dim_tests.first_sat_score AS test_first_sat_score
    ,dim_tests.last_sat_score AS test_last_sat_score
    ,dim_tests.max_sat_score AS test_max_sat_score
    ,dim_tests.avg_sat_score AS test_avg_sat_score
    ,dim_tests.num_sat_attempts AS test_num_sat_attempts
    ,COALESCE (dim_address_cascade_vw.address1,'*') AS stdnt_cntct_address1
    ,COALESCE (dim_address_cascade_vw.address2,'*') AS stdnt_cntct_address2
    ,COALESCE (dim_address_cascade_vw.address_type_cd,'*') AS stdnt_cntct_address_type
    ,COALESCE (dim_address_cascade_vw.city,'*') AS stdnt_cntct_city
    ,COALESCE (dim_address_cascade_vw.country_ld,'*') AS stdnt_cntct_country_ld
    ,COALESCE (dim_address_cascade_vw.geog_county_ld,'*') AS stdnt_cntct_county
    ,COALESCE (dim_address_cascade_vw.postal,'*') AS stdnt_cntct_postal
    ,COALESCE (dim_address_cascade_vw.state_cd,'*') AS stdnt_cntct_state_cd
    ,COALESCE (dim_address_cascade_vw.state_ld,'*') AS stdnt_cntct_state_ld
    ,COALESCE ("SUBSTRING" (dim_address_cascade_vw.postal,1,5),'*') AS stdnt_cntct_zipcode
    ,geo_zip.postal_loc_y AS stdnt_cntct_latitude
    ,geo_zip.postal_loc_x AS stdnt_cntct_longitude
    ,campus_zip.postal_loc_y AS campus_latitude
    ,campus_zip.postal_loc_x AS campus_longitude
    ,student_term_extract.hs_lat
    ,student_term_extract.hs_long
    ,COALESCE (enrollment_address_vw.address1,'*') AS enrollment_address1
    ,COALESCE (enrollment_address_vw.address2,'*') AS enrollment_address2
    ,COALESCE (enrollment_address_vw.address_type_cd,'*') AS enrollment_address_type
    ,COALESCE (enrollment_address_vw.city,'*') AS enrollment_address_city
    ,COALESCE (enrollment_address_vw.country_ld,'*') AS enrollment_address_country_ld
    ,COALESCE (enrollment_address_vw.geog_county_ld,'*') AS enrollment_address_county
    ,COALESCE (enrollment_address_vw.postal,'*') AS enrollment_address_postal
    ,COALESCE (enrollment_address_vw.state_cd,'*') AS enrollment_address_state_cd
    ,COALESCE (enrollment_address_vw.state_ld,'*') AS enrollment_address_state_ld
    ,COALESCE ("SUBSTRING" (enrollment_address_vw.postal,1,5),'*') AS enrollment_address_zipcode
    ,enrollment_zip.postal_loc_y AS enrollment_address_latitude
    ,enrollment_zip.postal_loc_x AS enrollment_address_longitude
    ,COALESCE (dorm_enrollment_address_vw.address1,'*') AS dorm_enrollment_address1
    ,COALESCE (dorm_enrollment_address_vw.address2,'*') AS dorm_enrollment_address2
    ,COALESCE (dorm_enrollment_address_vw.address_type_cd,'*') AS dorm_enrollment_address_type
    ,COALESCE (dorm_enrollment_address_vw.city,'*') AS dorm_enrollment_address_city
    ,COALESCE (dorm_enrollment_address_vw.country_ld,'*') AS dorm_enrollment_address_country_ld
    ,COALESCE (dorm_enrollment_address_vw.geog_county_ld,'*') AS dorm_enrollment_address_county
    ,COALESCE (dorm_enrollment_address_vw.postal,'*') AS dorm_enrollment_address_postal
    ,COALESCE (dorm_enrollment_address_vw.state_cd,'*') AS dorm_enrollment_address_state_cd
    ,COALESCE (dorm_enrollment_address_vw.state_ld,'*') AS dorm_enrollment_address_state_ld
    ,COALESCE ("SUBSTRING" (dorm_enrollment_address_vw.postal,1,5),'*') AS dorm_enrollment_address_zipcode
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
    ,fin_aid.aid_year
    ,fin_aid.fa_type
    ,fin_aid.disbursed_amount
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
    ,rel_fin_aid.aid_year AS rel_aid_year
    ,rel_fin_aid.fa_type AS rel_fa_type
    ,rel_fin_aid.disbursed_amount AS rel_disbursed_amount
    ,rel_fin_aid.federal_grant_disbursed_amount AS rel_federal_grant_disbursed_amount
    ,rel_fin_aid.federal_loan_disbursed_amount AS rel_federal_loan_disbursed_amount
    ,rel_fin_aid.federal_workstudy_disbursed_amount AS rel_federal_workstudy_disbursed_amount
    ,rel_fin_aid.institutional_grant_disbursed_amount AS rel_institutional_grant_disbursed_amount
    ,rel_fin_aid.institutional_other_disbursed_amount AS rel_institutional_other_disbursed_amount
    ,rel_fin_aid.institutional_scholarship_disbursed_amount AS rel_institutional_scholarship_disbursed_amount
    ,rel_fin_aid.other_scholarship_disbursed_amount AS rel_other_scholarship_disbursed_amount
    ,rel_fin_aid.private_grant_disbursed_amount AS rel_private_grant_disbursed_amount
    ,rel_fin_aid.private_loan_disbursed_amount AS rel_private_loan_disbursed_amount
    ,rel_fin_aid.private_scholarship_disbursed_amount AS rel_private_scholarship_disbursed_amount
    ,rel_fin_aid.state_grant_disbursed_amount AS rel_state_grant_disbursed_amount
    ,rel_fin_aid.state_scholarship_disbursed_amount AS rel_state_scholarship_disbursed_amount
    ,adm.req_term
    ,adm.accepted_dt
    ,adm.app_dt
    ,adm.stdnt_admit_type_cd
    ,adm.stdnt_admit_type_ld
    ,adm.admit_term 
    ,COALESCE (student_term_extract.tuition_residency_ld,'*') AS stdnt_tuition_residency
    ,COALESCE (student_term_extract_eff.tuition_residency_ld,'*') AS rel_stdnt_tuition_residency
    ,student_term_extract.full_part_time_cd AS stdnt_full_part_time_cd
    ,student_term_extract.full_part_time_ld AS stdnt_full_part_time_ld
    ,student_term_extract_eff.full_part_time_cd AS rel_stdnt_full_part_time_cd
    ,student_term_extract_eff.full_part_time_ld AS rel_stdnt_full_part_time_ld
    ,dim_person.married_status_ld AS dem_marital_status, 
    to_Date(dim_person.birthdate_day,'J') AS stdnt_birthdate_day
    ,COALESCE (enrl.tot_courses_taken,0) AS tot_courses_taken
    ,COALESCE (enrl.tot_courses_withdrawn,0) AS tot_courses_withdrawn
    ,SUM (student_term_extract_eff.career_billing_credits) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_unt_billing
    ,SUM (student_term_extract_eff.career_credits_toward_progress) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_unt_earned
    ,student_term_extract_eff.career_tot_cumulative AS cum_tot_unt_earned
    ,student_term_extract.career_tot_cumulative AS cohort_tot_cumulative
    ,student_term_extract_eff.career_tot_audit AS rel_tot_audit
    ,student_term_extract.career_tot_audit AS cohort_tot_audit
    ,student_term_extract_eff.career_tot_trnsfr AS rel_tot_trnsfr
    ,student_term_extract.career_tot_trnsfr AS cohort_tot_trnsfr
    ,student_term_extract_eff.career_tot_other AS rel_tot_other
    ,student_term_extract.career_tot_other AS cohort_tot_other
    ,student_term_extract_eff.career_tot_test_credit AS rel_tot_test_credit
    ,student_term_extract.career_tot_test_credit AS cohort_tot_test_credit
    ,COALESCE (enrl.tot_grade_points::float,0) AS tot_grade_points
    ,COALESCE (enrl.tot_grade_points_per_unit::float,0) AS tot_grade_points_per_unit
    ,enrl.tot_term_end_dt
    ,enrl.tot_term_begin_dt
    ,enrl.tot_first_course_id
    ,enrl.tot_first_crse_offer_nbr
    ,enrl.tot_first_session_cd
    ,enrl.tot_first_class_section
    ,COALESCE (enrl.tot_first_class_grade_point::float,0) AS tot_first_class_grade_point
    ,COALESCE (enrl.tot_fin_tot_payment_amt::float,0) AS tot_fin_tot_payment_amt
    ,COALESCE (enrl.tot_fin_tuition::float,0) AS tot_fin_tuition
    ,COALESCE (enrl.tot_fin_tuition_payment::float,0) AS tot_fin_tuition_payment
    ,COALESCE (enrl.tot_fin_fees::float,0) AS tot_fin_fees
    ,COALESCE (enrl.tot_fin_fee_payment::float,0) AS tot_fin_fee_payment
    ,COALESCE (enrl.tot_fin_total_charges::float,0) AS tot_fin_total_charges
    ,COALESCE (SUM (student_term_extract_eff.career_billing_credits::float) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND unbounded following),0) AS tot_fin_credit_hours
    ,COALESCE (enrl.tot_fin_corporate_waiver_amt::float,0) AS tot_fin_corporate_waiver_amt
    ,enrl.tot_time_enrl_add_day_key
    ,enrl.tot_time_min_enrl_drop_day_key
    ,enrl.tot_time_enrl_drop_day_key
    ,COALESCE (enrl.fin_tot_payment_amt::float,0) AS fin_tot_payment_amt
    ,COALESCE (enrl.fin_tuition::float,0) AS fin_tuition
    ,COALESCE (enrl.fin_tuition_payment::float,0) AS fin_tuition_payment
    ,COALESCE (enrl.fin_fees::float,0) AS fin_fees
    ,COALESCE (enrl.fin_fee_payment::float,0) AS fin_fee_payment
    ,COALESCE (enrl.fin_total_charges::float,0) AS fin_total_charges
    ,student_term_extract_eff.career_billing_credits::float AS fin_credit_hours
    ,COALESCE (enrl.fin_corporate_waiver_amt::float,0) AS fin_corporate_waiver_amt
    ,enrl.time_enrl_add_day_key
    ,enrl.time_enrl_drop_day_key
    ,enrl.time_first_enrl_add_day_key
    ,student_term_extract_eff.unt_taken_prgrss AS rel_unt_billing
    ,student_term_extract_eff.unt_passd_prgrss AS rel_unt_earned
    ,student_term_extract.unt_taken_prgrss AS unt_billing
    ,student_term_extract.unt_passd_prgrss AS unt_earned
    ,student_term_extract_eff.career_grade_points AS grade_points
    ,student_term_extract_eff.career_gpa_sem AS grade_points_per_unit
    ,enrl.term_end_dt
    ,enrl.term_begin_dt
    ,enrl.first_course_id
    ,enrl.first_crse_offer_nbr
    ,enrl.first_session_cd
    ,enrl.first_class_section
    ,COALESCE (enrl.first_class_grade_point::integer,0) AS first_class_grade_point
    ,enrl.time_min_enrl_drop_day_key
    ,COALESCE (SUM (enrl.courses_taken) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW),0) AS cum_courses_taken
    ,COALESCE (SUM (enrl.courses_dropped) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW),0) AS cum_courses_dropped
    ,COALESCE (SUM (enrl.courses_withdrawn) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW),0) AS cum_courses_withdrawn
    ,COALESCE (SUM (enrl.courses_passed) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW),0) AS cum_courses_passed
    ,COALESCE (SUM (enrl.courses_failed) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW),0) AS cum_courses_failed
    ,COALESCE (enrl.courses_taken::integer,0) AS courses_taken
    ,COALESCE (enrl.courses_dropped::integer,0) AS courses_dropped
    ,COALESCE (enrl.courses_withdrawn::integer,0) AS courses_withdrawn
    ,COALESCE (enrl.courses_passed::integer,0) AS courses_passed
    ,COALESCE (enrl.courses_failed::integer,0) AS courses_failed
    ,student_term_extract_eff.career_gpa_sem AS enrl_career_gpa_sem
    ,student_term_extract_eff.career_gpa_cum AS enrl_career_gpa_cum
    ,enrl.first_enrl_add_day
    ,enrl.first_enrl_drop_day
    ,enrl.term_end_dt AS enrl_term_end_dt
    ,enrl.term_begin_dt AS enrl_term_begin_dt
    ,COALESCE (next_enrl.courses_taken::integer,0) AS next_courses_taken
    ,COALESCE (next_enrl.courses_dropped::integer,0) AS next_courses_dropped
    ,COALESCE (next_enrl.courses_withdrawn::integer,0) AS next_courses_withdrawn
    ,COALESCE (next_enrl.courses_passed::integer,0) AS next_courses_passed
    ,COALESCE (next_enrl.courses_failed::integer,0) AS next_courses_failed
    ,COALESCE (next_enrl.unt_billing::integer,0) AS next_unt_billing
    ,COALESCE (next_enrl.unt_earned::integer,0) AS next_unt_earned
    ,COALESCE (next_enrl.grade_points::integer,0) AS next_grade_points
    ,COALESCE (next_enrl.grade_points_per_unit::integer,0) AS next_grade_points_per_unit
    ,next_enrl.first_enrl_add_day AS next_first_enrl_add_day
    ,next_enrl.first_enrl_drop_day AS next_first_enrl_drop_day
    ,next_enrl.term_end_dt AS next_enrl_term_end_dt
    ,next_enrl.term_begin_dt AS next_enrl_term_begin_dt
    ,COALESCE (cohort_enrl.courses_taken::integer,0) AS cohort_courses_taken
    ,COALESCE (cohort_enrl.courses_dropped::integer,0) AS cohort_courses_dropped
    ,COALESCE (cohort_enrl.courses_withdrawn::integer,0) AS cohort_courses_withdrawn
    ,COALESCE (cohort_enrl.courses_passed::integer,0) AS cohort_courses_passed
    ,COALESCE (cohort_enrl.courses_failed::integer,0) AS cohort_courses_failed
    ,COALESCE (cohort_enrl.unt_billing::integer,0) AS cohort_unt_billing
    ,COALESCE (cohort_enrl.unt_earned::integer,0) AS cohort_unt_earned
    ,COALESCE (cohort_enrl.grade_points::integer,0) AS cohort_grade_points
    ,COALESCE (cohort_enrl.grade_points_per_unit::integer,0) AS cohort_grade_points_per_unit
    ,cohort_enrl.first_enrl_add_day AS cohort_first_enrl_add_day
    ,cohort_enrl.first_enrl_drop_day AS cohort_first_enrl_drop_day
    ,cohort_enrl.term_end_dt AS cohort_enrl_term_end_dt
    ,cohort_enrl.term_begin_dt AS cohort_enrl_term_begin_dt
    ,cohort_dim_program.acad_prog_ld AS plan_acad_org_program_ld
    ,cohort_dim_program.acad_prog_cd AS plan_acad_org_program_cd
    ,cohort_dim_program.school_ld AS plan_acad_org_school_ld
    ,cohort_dim_program.school_cd AS plan_acad_org_school_cd
    ,cohort_dim_plan_maj.acad_plan_ld AS plan_acad_plan_ld
    ,cohort_dim_plan_maj.acad_plan_cd AS plan_acad_plan_code
    ,cohort_dim_plan_maj.plan_type_ld as acad_plan_type_ld
    ,rel_dim_plan_maj.plan_type_ld AS rel_acad_plan_type_ld
    ,rel_dim_program.acad_prog_ld AS rel_plan_acad_org_program_ld
    ,COALESCE (rel_dim_program.bhe_api_hegis_ld,'*') AS rel_plan_major
    ,rel_dim_program.acad_prog_cd AS rel_plan_acad_org_program_cd
    ,rel_dim_program.school_ld AS rel_plan_acad_org_school_ld
    ,rel_dim_program.school_cd AS rel_plan_acad_org_school_cd
    ,rel_dim_plan_maj.acad_plan_ld AS rel_plan_acad_plan_ld
    ,rel_dim_plan_maj.acad_plan_cd AS rel_plan_acad_plan_code
    ,rel_dim_program.grouping_cd AS rel_plan_grouping_cd
    ,rel_dim_program.grouping_ld AS rel_plan_grouping
    ,student_term_extract.pri_acad_prog AS plan_acad_prog
    ,student_term_extract.pri_prog_status_ld AS plan_prog_status_ld
    ,student_term_extract.pri_prog_status_cd AS plan_prog_status_cd
    ,cohort_dim_degree.education_level_ld AS plan_degree_type
    ,cohort_dim_program.institution_cd AS plan_institution_cd
    ,cohort_dim_program.institution_ld AS plan_institution_ld
    ,cohort_dim_program.grouping_cd AS plan_grouping_cd
    ,cohort_dim_program.grouping_ld AS plan_grouping
    ,cohort_dim_program.department_ld AS plan_bridge_department
    --,{stdnt_acad_career_cd} AS stdnt_acad_career_cd
    --,{stdnt_acad_car_collapsed_cd} AS stdnt_acad_car_collapsed_cd
    --,{stdnt_acad_car_collapsed_ld} AS stdnt_acad_car_collapsed_ld
    --,{stdnt_acad_career_ld} AS stdnt_acad_career_ld
	,cohort_dim_program.acad_career_cd AS stdnt_acad_career_cd
    ,cohort_dim_program.acad_career_collapsed_cd AS stdnt_acad_car_collapsed_cd
    ,cohort_dim_program.acad_career_collapsed_ld AS stdnt_acad_car_collapsed_ld
    ,cohort_dim_program.acad_career_ld AS stdnt_acad_career_ld
    ,cohort_dim_degree.education_level_ld AS stdnt_education_level_ld
    ,deg.completion_term
    ,deg.acad_plan_maj
    ,deg.application_date
    ,deg.honors_cd
    ,deg.honors_ld
    ,deg.prog_status_cd
    ,deg.prog_status_ld
    ,deg.degr_chkout_stat_cd
    ,deg.degr_chkout_stat_sd
    ,deg.degr_confer_dt
    ,deg.degr_status_dt
    ,deg.degree_gpa
    ,dim_isir.current_fa_app_date AS isir_current_fa_app_date
    ,dim_isir.first_fa_app_date AS isir_first_fa_app_date
    ,COALESCE
      (CASE
          WHEN btrim (dim_isir.pell_eligibility) = '' THEN 'N'
          ELSE btrim (dim_isir.pell_eligibility)
        END,'N') AS isir_pell_eligibility
    ,dim_isir.adj_efc AS isir_adj_efc
    ,dim_isir.efc_status AS isir_efc_status
    ,dim_isir.titleiv_elig AS isir_titleiv_elig
    ,dim_isir.ssa_citizenshp_ind AS isir_ssa_citizenshp_ind
    ,dim_isir.num_family_members AS isir_num_family_members
    ,dim_isir.number_in_college AS isir_number_in_college
    ,dim_isir.veteran AS isir_veteran
    ,dim_isir.graduate_student AS isir_graduate_student
    ,dim_isir.married AS isir_married
    ,dim_isir.marital_stat AS isir_marital_stat
    ,dim_isir.orphan AS isir_orphan
    ,dim_isir.dependents AS isir_dependents
    ,dim_isir.agi AS isir_agi
    ,dim_isir.school_choice_1 AS isir_school_choice_1
    ,dim_isir.housing_code_1 AS isir_housing_code_1
    ,dim_isir.school_choice_2 AS isir_school_choice_2
    ,dim_isir.housing_code_2 AS isir_housing_code_2
    ,dim_isir.school_choice_3 AS isir_school_choice_3
    ,dim_isir.housing_code_3 AS isir_housing_code_3
    ,dim_isir.school_choice_4 AS isir_school_choice_4
    ,dim_isir.housing_code_4 AS isir_housing_code_4
    ,dim_isir.school_choice_5 AS isir_school_choice_5
    ,dim_isir.housing_code_5 AS isir_housing_code_5
    ,dim_isir.school_choice_6 AS isir_school_choice_6
    ,dim_isir.housing_code_6 AS isir_housing_code_6
    ,dim_isir.first_bach_degree AS isir_first_bach_degree
    ,dim_isir.interested_in_ws AS isir_interested_in_ws
    ,dim_isir.interested_in_sl AS isir_interested_in_sl
    ,dim_isir.interested_in_plus AS isir_interested_in_plus
    ,dim_isir.degree_certif AS isir_degree_certif
    ,dim_isir.current_grade_lvl AS isir_current_grade_lvl
    ,dim_isir.depndncy_stat AS isir_depndncy_stat
    ,dim_isir.citizenship_status AS isir_citizenship_status
    ,dim_isir.children AS isir_children
    ,dim_isir.sfa_active_duty AS isir_sfa_active_duty
    ,dim_isir.parent_marital_stat AS isir_parent_marital_stat
    ,dim_isir.parent_number_in_family AS isir_parent_number_in_family
    ,dim_isir.parent_num_in_college AS isir_parent_num_in_college
    ,dim_isir.parent_agi AS isir_parent_agi
    ,dim_isir.father_grade_lvl AS isir_father_grade_lvl
    ,dim_isir.mother_grade_lvl AS isir_mother_grade_lvl
    ,dim_isir.parent_sfa_grant_aid AS isir_parent_sfa_grant_aid
    ,dim_isir.fed_efc AS isir_fed_efc
    ,dim_isir.efc_status2 AS isir_efc_status2
    ,dim_isir.fed_need_base_aid AS isir_fed_need_base_aid
    ,dim_isir.fed_year_coa AS isir_fed_year_coa
    ,dim_isir.inst_year_coa AS isir_inst_year_coa
    ,dim_isir.pell_year_coa AS isir_pell_year_coa
    ,dim_isir.fed_need AS isir_fed_need
    ,dim_isir.inst_need AS isir_inst_need
    ,dim_isir.fed_unmet_need AS isir_fed_unmet_need
    ,dim_isir.inst_unmet_need AS isir_inst_unmet_need
    ,dim_isir.fed_stdnt_contrb AS isir_fed_stdnt_contrb
    ,dim_isir.isir_calc_sc
    ,dim_isir.isir_calc_efc
    ,rel_dim_isir.current_fa_app_date AS rel_isir_current_fa_app_date
    ,rel_dim_isir.first_fa_app_date AS rel_isir_first_fa_app_date
    ,COALESCE
      (CASE
          WHEN btrim (rel_dim_isir.pell_eligibility) = '' THEN 'N'
          ELSE btrim (rel_dim_isir.pell_eligibility)
        END,'N') AS rel_isir_pell_eligibility
    ,"MAX" (COALESCE
      (CASE
          WHEN btrim (rel_dim_isir.pell_eligibility) = '' THEN 'N'
          ELSE btrim (rel_dim_isir.pell_eligibility)
        END,'N')) OVER (PARTITION BY x.emplid,x.student_career ROWS BETWEEN unbounded preceding AND unbounded following) AS isir_ever_pell_eligibility
    ,rel_dim_isir.adj_efc AS rel_isir_adj_efc
    ,rel_dim_isir.efc_status AS rel_isir_efc_status
    ,rel_dim_isir.titleiv_elig AS rel_isir_titleiv_elig
    ,rel_dim_isir.ssa_citizenshp_ind AS rel_isir_ssa_citizenshp_ind
    ,rel_dim_isir.num_family_members AS rel_isir_num_family_members
    ,rel_dim_isir.number_in_college AS rel_isir_number_in_college
    ,rel_dim_isir.veteran AS rel_isir_veteran
    ,rel_dim_isir.graduate_student AS rel_isir_graduate_student
    ,rel_dim_isir.married AS rel_isir_married
    ,rel_dim_isir.marital_stat AS rel_isir_marital_stat
    ,rel_dim_isir.orphan AS rel_isir_orphan
    ,rel_dim_isir.dependents AS rel_isir_dependents
    ,rel_dim_isir.agi AS rel_isir_agi
    ,rel_dim_isir.school_choice_1 AS rel_isir_school_choice_1
    ,rel_dim_isir.housing_code_1 AS rel_isir_housing_code_1
    ,rel_dim_isir.school_choice_2 AS rel_isir_school_choice_2
    ,rel_dim_isir.housing_code_2 AS rel_isir_housing_code_2
    ,rel_dim_isir.school_choice_3 AS rel_isir_school_choice_3
    ,rel_dim_isir.housing_code_3 AS rel_isir_housing_code_3
    ,rel_dim_isir.school_choice_4 AS rel_isir_school_choice_4
    ,rel_dim_isir.housing_code_4 AS rel_isir_housing_code_4
    ,rel_dim_isir.school_choice_5 AS rel_isir_school_choice_5
    ,rel_dim_isir.housing_code_5 AS rel_isir_housing_code_5
    ,rel_dim_isir.school_choice_6 AS rel_isir_school_choice_6
    ,rel_dim_isir.housing_code_6 AS rel_isir_housing_code_6
    ,rel_dim_isir.first_bach_degree AS rel_isir_first_bach_degree
    ,rel_dim_isir.interested_in_ws AS rel_isir_interested_in_ws
    ,rel_dim_isir.interested_in_sl AS rel_isir_interested_in_sl
    ,rel_dim_isir.interested_in_plus AS rel_isir_interested_in_plus
    ,rel_dim_isir.degree_certif AS rel_isir_degree_certif
    ,rel_dim_isir.current_grade_lvl AS rel_isir_current_grade_lvl
    ,rel_dim_isir.depndncy_stat AS rel_isir_depndncy_stat
    ,rel_dim_isir.citizenship_status AS rel_isir_citizenship_status
    ,rel_dim_isir.children AS rel_isir_children
    ,rel_dim_isir.sfa_active_duty AS rel_isir_sfa_active_duty
    ,rel_dim_isir.parent_marital_stat AS rel_isir_parent_marital_stat
    ,rel_dim_isir.parent_number_in_family AS rel_isir_parent_number_in_family
    ,rel_dim_isir.parent_num_in_college AS rel_isir_parent_num_in_college
    ,rel_dim_isir.parent_agi AS rel_isir_parent_agi
    ,rel_dim_isir.father_grade_lvl AS rel_isir_father_grade_lvl
    ,rel_dim_isir.mother_grade_lvl AS rel_isir_mother_grade_lvl
    ,rel_dim_isir.parent_sfa_grant_aid AS rel_isir_parent_sfa_grant_aid
    ,rel_dim_isir.fed_efc AS rel_isir_fed_efc
    ,rel_dim_isir.efc_status2 AS rel_isir_efc_status2
    ,rel_dim_isir.fed_need_base_aid AS rel_isir_fed_need_base_aid
    ,rel_dim_isir.fed_year_coa AS rel_isir_fed_year_coa
    ,rel_dim_isir.inst_year_coa AS rel_isir_inst_year_coa
    ,rel_dim_isir.pell_year_coa AS rel_isir_pell_year_coa
    ,rel_dim_isir.fed_need AS rel_isir_fed_need
    ,rel_dim_isir.inst_need AS rel_isir_inst_need
    ,rel_dim_isir.fed_unmet_need AS rel_isir_fed_unmet_need
    ,rel_dim_isir.inst_unmet_need AS rel_isir_inst_unmet_need
    ,rel_dim_isir.fed_stdnt_contrb AS rel_isir_fed_stdnt_contrb
    ,rel_dim_isir.isir_calc_sc AS rel_isir_calc_sc
    ,rel_dim_isir.isir_calc_efc AS rel_isir_calc_efc
    ,student_term_extract_eff.withdraw_reason_cd AS withdraw_reason_cd
    ,student_term_extract_eff.withdraw_reason_ld AS withdraw_reason_ld
    ,student_term_extract_eff.withdraw_cd AS withdraw_cd
    ,student_term_extract_eff.withdraw_ld AS withdraw_ld
    ,student_term_extract_eff.pri_prog_action_cd AS prog_action_cd
    ,student_term_extract_eff.pri_prog_action_ld AS prog_action_ld
    ,student_term_extract_eff.pri_prog_reason_cd AS prog_reason_cd
    ,student_term_extract_eff.pri_prog_reason_ld AS prog_reason_ld
    ,x.strm_plus_0 AS rel_strm
    ,pstrm.strm AS persis_rel_strm
    ,CASE
      WHEN x.status_year_cd = 'STATUS_YR_0' AND (SUM
          (CASE
              WHEN (dim_term.term_type_ld IN ('Fall','Spring')) AND x.academic_year_status > 0 THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW)) = 1 THEN 'Y'
      WHEN x.status_year_cd = 'STATUS_YR_0.5' AND (SUM
          (CASE
              WHEN (dim_term.term_type_ld IN ('Fall','Spring')) AND x.academic_year_status > 0 THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW)) = 2 THEN 'Y'
      WHEN x.status_year_cd = 'STATUS_YR_1' AND (SUM
          (CASE
              WHEN (dim_term.term_type_ld IN ('Fall','Spring')) AND x.academic_year_status > 0 THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW)) = 3 THEN 'Y'
      WHEN x.status_year_cd = 'STATUS_YR_1.5' AND (SUM
          (CASE
              WHEN (dim_term.term_type_ld IN ('Fall','Spring')) AND x.academic_year_status > 0 THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW)) = 4 THEN 'Y'
      WHEN x.status_year_cd = 'STATUS_YR_2' AND (SUM
          (CASE
              WHEN (dim_term.term_type_ld IN ('Fall','Spring')) AND x.academic_year_status > 0 THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW)) = 5 THEN 'Y'
      WHEN x.status_year_cd = 'STATUS_YR_2.5' AND (SUM
          (CASE
              WHEN (dim_term.term_type_ld IN ('Fall','Spring')) AND x.academic_year_status > 0 THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW)) = 6 THEN 'Y'
      WHEN x.status_year_cd = 'STATUS_YR_3' AND (SUM
          (CASE
              WHEN (dim_term.term_type_ld IN ('Fall','Spring')) AND x.academic_year_status > 0 THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW)) = 7 THEN 'Y'
      WHEN x.status_year_cd = 'STATUS_YR_3.5' AND (SUM
          (CASE
              WHEN (dim_term.term_type_ld IN ('Fall','Spring')) AND x.academic_year_status > 0 THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW)) = 8 THEN 'Y'
      WHEN x.status_year_cd = 'STATUS_YR_4' AND (SUM
          (CASE
              WHEN (dim_term.term_type_ld IN ('Fall','Spring')) AND x.academic_year_status > 0 THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW)) = 9 THEN 'Y'
      WHEN x.status_year_cd = 'STATUS_YR_4.5' AND (SUM
          (CASE
              WHEN (dim_term.term_type_ld IN ('Fall','Spring')) AND x.academic_year_status > 0 THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW)) = 10 THEN 'Y'
      WHEN x.status_year_cd = 'STATUS_YR_5' AND (SUM
          (CASE
              WHEN (dim_term.term_type_ld IN ('Fall','Spring')) AND x.academic_year_status > 0 THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW)) = 11 THEN 'Y'
      WHEN x.status_year_cd = 'STATUS_YR_5.5' AND (SUM
          (CASE
              WHEN (dim_term.term_type_ld IN ('Fall','Spring')) AND x.academic_year_status > 0 THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW)) = 12 THEN 'Y'
      WHEN x.status_year_cd = 'STATUS_YR_6' AND (SUM
          (CASE
              WHEN (dim_term.term_type_ld IN ('Fall','Spring')) AND x.academic_year_status > 0 THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW)) = 13 THEN 'Y'
      WHEN x.status_year_cd = 'STATUS_YR_6.5' AND (SUM
          (CASE
              WHEN (dim_term.term_type_ld IN ('Fall','Spring')) AND x.academic_year_status > 0 THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW)) = 14 THEN 'Y'
      WHEN x.status_year_cd = 'STATUS_YR_7' AND (SUM
          (CASE
              WHEN (dim_term.term_type_ld IN ('Fall','Spring')) AND x.academic_year_status > 0 THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW)) = 15 THEN 'Y'
      WHEN x.status_year_cd = 'STATUS_YR_7.5' AND (SUM
          (CASE
              WHEN (dim_term.term_type_ld IN ('Fall','Spring')) AND x.academic_year_status > 0 THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW)) = 16 THEN 'Y'
      WHEN x.status_year_cd = 'STATUS_YR_8' AND (SUM
          (CASE
              WHEN (dim_term.term_type_ld IN ('Fall','Spring')) AND x.academic_year_status > 0 THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW)) = 17 THEN 'Y'
      WHEN x.status_year_cd = 'STATUS_YR_8.5' AND (SUM
          (CASE
              WHEN (dim_term.term_type_ld IN ('Fall','Spring')) AND x.academic_year_status > 0 THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW)) = 18 THEN 'Y'
      WHEN x.status_year_cd = 'STATUS_YR_9' AND (SUM
          (CASE
              WHEN (dim_term.term_type_ld IN ('Fall','Spring')) AND x.academic_year_status > 0 THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW)) = 19 THEN 'Y'
      WHEN x.status_year_cd = 'STATUS_YR_9.5' AND (SUM
          (CASE
              WHEN (dim_term.term_type_ld IN ('Fall','Spring')) AND x.academic_year_status > 0 THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW)) = 20 THEN 'Y'
      WHEN x.status_year_cd = 'STATUS_YR_10' AND (SUM
          (CASE
              WHEN (dim_term.term_type_ld IN ('Fall','Spring')) AND x.academic_year_status > 0 THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career,x.acad_plan_cd,x.cohort_strm ORDER BY x.strm_plus_0 ROWS BETWEEN unbounded preceding AND CURRENT ROW)) = 21 THEN 'Y'
      ELSE 'N'
    END AS continually_enrolled_flg
    ,COALESCE (cohort_dim_program.bhe_api_hegis_ld,'*') AS plan_major
    ,student_term_extract.acad_level_bot_cd AS stdnt_acad_level_bot_cd
    ,student_term_extract.acad_level_bot_ld AS stdnt_acad_level_bot_ld
    ,student_term_extract.acad_level_eot_cd AS stdnt_acad_level_eot_cd
    ,student_term_extract.acad_level_eot_ld AS stdnt_acad_level_eot_ld
    ,student_term_extract_eff.acad_level_bot_cd AS rel_stdnt_acad_level_bot_cd
    ,student_term_extract_eff.acad_level_bot_ld AS rel_stdnt_acad_level_bot_ld
    ,CASE
      WHEN x.ret_year_status_collapsed = 'GRADUATED' THEN 'GRAD'
      ELSE student_term_extract_eff.acad_level_eot_cd
    END AS rel_stdnt_acad_level_eot_cd
    ,CASE
      WHEN x.ret_year_status_collapsed = 'GRADUATED' THEN 'GRADUATED'
      ELSE student_term_extract_eff.acad_level_eot_ld
    END AS rel_stdnt_acad_level_eot_ld
    ,CASE
      WHEN x.acad_plan_cd = undecided_flag.fieldvalue THEN 'Y'
      ELSE 'N'
    END AS initially_undecided_flag
    ,dim_tests.first_gre_score AS test_first_gre_score
    ,dim_tests.last_gre_score AS test_last_gre_score
    ,dim_tests.max_gre_score AS test_max_gre_score
    ,dim_tests.avg_gre_score AS test_avg_gre_score
    ,dim_tests.num_gre_attempts AS test_num_gre_attempts
    ,dim_tests.first_gmat_score AS test_first_gmat_score
    ,dim_tests.last_gmat_score AS test_last_gmat_score
    ,dim_tests.max_gmat_score AS test_max_gmat_score
    ,dim_tests.avg_gmat_score AS test_avg_gmat_score
    ,dim_tests.num_gmat_attempts AS test_num_gmat_attempts
    ,CASE
      WHEN ("MAX"
          (CASE
              WHEN student_term_extract_eff.full_part_time_ld <> 'ENROLLED FULL-TIME' THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career ROWS BETWEEN unbounded preceding AND unbounded following)) = 1 THEN 'Y'
      ELSE 'N'
    END AS non_full_time_enrollment_flag
    ,CASE
      WHEN ("MAX" (student_term_extract_eff.tuition_residency_ld) OVER (PARTITION BY x.emplid,x.student_career ROWS BETWEEN unbounded preceding AND unbounded following)) <> (MIN (student_term_extract_eff.tuition_residency_ld) OVER (PARTITION BY x.emplid,x.student_career ROWS BETWEEN unbounded preceding AND unbounded following)) THEN 'Y'
      ELSE 'N'
    END AS change_in_tuition_residency_ld
    ,CASE
      WHEN ("MAX"
          (CASE
              WHEN x.ret_status = 'ENROLLED ALT PROGRAM' OR x.ret_status = 'GRADUATED ALT PROGRAM' THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career ROWS BETWEEN unbounded preceding AND unbounded following)) = 1 THEN 'Y'
      ELSE 'N'
    END AS major_change_ever_flag
    ,CASE
      WHEN ("MAX"
          (CASE
              WHEN (x.academic_year_status = 1 OR x.academic_year_status = 2) AND UPPER (x.ret_year) = '2ND YEAR' THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career ROWS BETWEEN unbounded preceding AND unbounded following)) = 1 THEN 'Y'
      ELSE 'N'
    END AS second_year_ret_flag
    ,CASE
      WHEN ("MAX"
          (CASE
              WHEN (x.academic_year_status = 1 OR x.academic_year_status = 2) AND UPPER (x.ret_year) = '3RD YEAR' THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career ROWS BETWEEN unbounded preceding AND unbounded following)) = 1 THEN 'Y'
      ELSE 'N'
    END AS third_year_ret_flag
    ,CASE
      WHEN ("MAX"
          (CASE
              WHEN (x.academic_year_status = 3 OR x.academic_year_status = 4) AND UPPER (x.ret_year) = '2ND YEAR' THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career ROWS BETWEEN unbounded preceding AND unbounded following)) = 1 THEN 'Y'
      ELSE 'N'
    END AS second_year_grad_flag
    ,CASE
      WHEN ("MAX"
          (CASE
              WHEN (x.academic_year_status = 3 OR x.academic_year_status = 4) AND UPPER (x.ret_year) = '3RD YEAR' THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career ROWS BETWEEN unbounded preceding AND unbounded following)) = 1 THEN 'Y'
      ELSE 'N'
    END AS third_year_grad_flag
    ,CASE
      WHEN ("MAX"
          (CASE
              WHEN (x.academic_year_status = 3 OR x.academic_year_status = 4) AND UPPER (x.ret_year) = '4TH YEAR' THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career ROWS BETWEEN unbounded preceding AND unbounded following)) = 1 THEN 'Y'
      ELSE 'N'
    END AS four_year_grad_flag
    ,CASE
      WHEN ("MAX"
          (CASE
              WHEN (x.academic_year_status = 3 OR x.academic_year_status = 4) AND UPPER (x.ret_year) = '5TH YEAR' THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career ROWS BETWEEN unbounded preceding AND unbounded following)) = 1 THEN 'Y'
      ELSE 'N'
    END AS fifth_year_grad_flag
    ,CASE
      WHEN ("MAX"
          (CASE
              WHEN (x.academic_year_status = 3 OR x.academic_year_status = 4) AND UPPER (x.ret_year) = '6TH YEAR' THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career ROWS BETWEEN unbounded preceding AND unbounded following)) = 1 THEN 'Y'
      ELSE 'N'
    END AS six_year_grad_flag
    ,CASE
      WHEN ("MAX"
          (CASE
              WHEN (x.academic_year_status = 3 OR x.academic_year_status = 4) AND UPPER (x.ret_year) = '4TH YEAR' THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career ROWS BETWEEN unbounded preceding AND unbounded following)) = 1 THEN '4TH YEAR OR SOONER'
      WHEN ("MAX"
          (CASE
              WHEN (x.academic_year_status = 3 OR x.academic_year_status = 4) AND x.ret_year = '5TH YEAR' THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career ROWS BETWEEN unbounded preceding AND unbounded following)) = 1 THEN '5TH YEAR'
      WHEN ("MAX"
          (CASE
              WHEN (x.academic_year_status = 3 OR x.academic_year_status = 4) AND x.ret_year = '6TH YEAR' THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career ROWS BETWEEN unbounded preceding AND unbounded following)) = 1 THEN '6TH YEAR'
      WHEN ("MAX"
          (CASE
              WHEN x.academic_year_status = 3 OR x.academic_year_status = 4 THEN 1
              ELSE 0
            END) OVER (PARTITION BY x.emplid,x.student_career ROWS BETWEEN unbounded preceding AND unbounded following)) = 1 THEN 'GRADUATED AFTER 6TH YEAR'
      WHEN ("FIRST_VALUE" (x.term_status) OVER (PARTITION BY x.emplid,x.student_career ORDER BY x.strm_plus_0 DESC ROWS BETWEEN unbounded preceding AND unbounded following)) = 1 OR ("FIRST_VALUE" (x.term_status) OVER (PARTITION BY x.emplid,x.student_career ORDER BY x.strm_plus_0 DESC ROWS BETWEEN unbounded preceding AND unbounded following)) = 2 THEN 'STILL ENROLLED'
      WHEN ("FIRST_VALUE" (x.term_status) OVER (PARTITION BY x.emplid,x.student_career ORDER BY x.strm_plus_0 DESC ROWS BETWEEN unbounded preceding AND unbounded following)) = 0 THEN 'NOT ENROLLED '
      ELSE 'N'
    END AS ret_category
    ,x.first_degree_seeking_strm
    ,cohort_enrl.dev_ed_flag
    ,cohort_enrl.gen_ed_flag
    , student_term_extract.first_name AS stdnt_first_name
    , student_term_extract.last_name AS stdnt_last_name
    , student_term_extract.stdnt_home_phone
    , student_term_extract.stdnt_cell_phone
    , student_term_extract.advisor_emplid AS advisor_id
    , student_term_extract.advisor_name_first_last AS advisor_name
    , student_term_extract.stdnt_home_email AS stdnt_home_email
    , student_term_extract.stdnt_business_email AS stdnt_business_email
    , student_term_extract.stdnt_campus_email AS stdnt_campus_email
    , 0    AS stdnt_grp_first_gen
    ,'TBD' AS nsc_transfer_flag
    ,student_term_extract.pri_acad_plan_dept_cd
    ,student_term_extract.pri_acad_plan_dept_ld
    ,student_term_extract.urm_status AS urm_status
    ,dim_term.academic_yr AS academic_yr
    ,dim_term.calendar_yr AS calendar_yr
    ,adm.housing_interest_cd
    ,adm.housing_interest_sd
    ,adm.housing_interest_ld
    ,dim_program.acad_prog_cip_cd AS cip2000_cd
    ,dim_program.acad_prog_cip_cd AS cip2000_ld
    ,dim_person.military_status_cd
    ,dim_person.military_status_ld
    ,student_term_extract.honors_acad_prog_ind
    ,student_term_extract.acad_stndng_actn_cd_bot
    ,student_term_extract.acad_stndng_actn_ld_bot
    ,student_term_extract.leave_of_absence_ind AS leave_of_absence_ind
    ,student_term_extract.approved_acad_load_cd AS stdnt_approved_acad_load_cd
    ,student_term_extract.approved_acad_load_ld AS stdnt_approved_acad_load_ld
    ,student_term_extract_eff.approved_acad_load_cd AS rel_stdnt_approved_acad_load_cd
    ,student_term_extract_eff.approved_acad_load_ld AS rel_stdnt_approved_acad_load_ld
    ,adm.stdnt_admit_type_group AS stdnt_admit_type_group
    ,x.stdnt_car_nbr as stdnt_car_nbr
    ,NVL (student_term_extract.stdnt_dev_ed_engl_ind::integer,0) AS stdnt_dev_ed_engl_ind
    ,NVL (student_term_extract.stdnt_dev_ed_math_ind::integer,0) AS stdnt_dev_ed_math_ind
    ,NVL (student_term_extract.stdnt_dev_ed_ind::integer,0) AS stdnt_dev_ed_ind
    ,student_term_extract.pri_bhe_api_hegis_cd
    ,student_term_extract.pri_bhe_api_hegis_ld
    ,student_term_extract.sec_bhe_api_hegis_cd
    ,student_term_extract.sec_bhe_api_hegis_ld
    ,null as trn_transfer_inst_name_and_location
    ,student_term_extract.hs_inst_name_and_location
    ,null as rel_trn_transfer_inst_name_and_location
    ,student_term_extract_eff.hs_inst_name_and_location AS rel_hs_inst_name_and_location
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
    
     --  {sel_custom_extract_fields}  
FROM hercules_workday.ff_retention_grad_prog x

left join   hercules_workday.dim_program as dim_program
    on      x.primary_acad_prog       = dim_program.acad_prog_cd

LEFT JOIN hercules_workday.dim_person dim_person
   ON x.emplid = dim_person.emplid

LEFT JOIN hercules_workday.dim_term dim_term
   ON dim_term.strm = x.cohort_strm
   
LEFT JOIN perseus_workday.student_term_extract student_term_extract
   ON x.emplid             = student_term_extract.emplid
  AND x.student_career     = student_term_extract.acad_career
  AND x.cohort_strm     = student_term_extract.strm

LEFT JOIN hercules_workday.dim_address enrollment_address_vw
   ON student_term_extract.address_term_src_sys_cd = enrollment_address_vw.src_sys_cd

LEFT JOIN hercules_workday.dim_address dorm_enrollment_address_vw
   ON student_term_extract.dorm_address_term_src_sys_cd = dorm_enrollment_address_vw.src_sys_cd

LEFT JOIN edwutil.dim_oth_raw_zip_data_t enrollment_zip
   ON LPAD ("LEFT" (enrollment_address_vw.postal,5),5,'0') = enrollment_zip.postal

LEFT JOIN hercules_workday.dim_address_cascade dim_address_cascade_vw
   ON x.emplid = dim_address_cascade_vw.emplid
   
LEFT JOIN perseus_workday.cs_financial_aid fin_aid
   ON x.emplid = fin_aid.emplid
    AND dim_term.aid_yr = fin_aid.aid_year

LEFT JOIN hercules_workday.dim_campus dim_campus
    ON student_term_extract.stdnt_campus_cd = dim_campus.campus_cd

LEFT JOIN hercules_workday.dim_location dim_location_campus
   ON DIM_CAMPUS."LOCATION" = dim_location_campus.src_sys_cd

LEFT JOIN edwutil.dim_oth_raw_zip_data_t campus_zip
   ON "LEFT" (dim_location_campus.postal,5) = campus_zip.postal

LEFT JOIN edwutil.dim_oth_raw_zip_data_t geo_zip
   ON LPAD ("LEFT" (dim_address_cascade_vw.postal,5),5,'0') = geo_zip.postal

LEFT JOIN hercules_workday.fact_testscore dim_tests
   ON x.emplid = dim_tests.emplid

LEFT JOIN perseus_workday.cs_enrollments enrl
   ON x.emplid = enrl.emplid
  AND x.strm_plus_0 = enrl.strm
  AND x.student_career = enrl.acad_career
  
  
LEFT JOIN hercules_workday.fact_aos fact_aos_scd_vw
   ON student_term_extract.acad_career  			= fact_aos_scd_vw.acad_career
  ANd student_term_extract.emplid       			= fact_aos_scd_vw.emplid
  AND student_term_extract.strm         			= fact_aos_scd_vw.strm
  and fact_aos_scd_vw.plan_type_cd					= 'PROG'
  and fact_aos_scd_vw.plan_type_rn					= 1
    
left join   hercules_workday.dim_program as cohort_dim_program
    on      x.primary_acad_prog 			= cohort_dim_program.acad_prog_cd

left join   hercules_workday.dim_degree as cohort_dim_degree
    on      cohort_dim_program.degree_cd   	= cohort_dim_degree.degree_cd
    
LEFT JOIN hercules_workday.fact_aos fact_aos_vw_maj1 
ON      student_term_extract.ACAD_CAREER         	= fact_aos_vw_maj1.ACAD_CAREER
    AND student_term_extract.emplid             	= fact_aos_vw_maj1.emplid
    AND student_term_extract.strm                 	= fact_aos_vw_maj1.strm
    AND fact_aos_vw_maj1.plan_type_cd             	= 'MAJ' 
    and fact_aos_vw_maj1.plan_type_rn           	= 1
    
left join hercules_workday.dim_plan as cohort_dim_plan_maj
    on   fact_aos_vw_maj1.acad_plan             = cohort_dim_plan_maj.acad_plan_cd
    and  fact_aos_vw_maj1.plan_type_cd             = cohort_dim_plan_maj.plan_type_cd
    
LEFT JOIN hercules_workday.fact_aos fact_aos_vw_min1 
ON      student_term_extract.ACAD_CAREER         	= fact_aos_vw_min1.ACAD_CAREER
    AND student_term_extract.emplid             	= fact_aos_vw_min1.emplid
    AND student_term_extract.strm                 	= fact_aos_vw_min1.strm
    AND fact_aos_vw_min1.plan_type_cd             	= 'MIN' 
    and fact_aos_vw_min1.plan_type_rn           	= 1
    
left join hercules_workday.dim_plan as cohort_dim_plan_min
    on   fact_aos_vw_min1.acad_plan            	   = cohort_dim_plan_min.acad_plan_cd
    and  fact_aos_vw_min1.plan_type_cd             = cohort_dim_plan_min.plan_type_cd
    
LEFT JOIN hercules_workday.fact_aos fact_aos_vw_cer1 
ON      student_term_extract.ACAD_CAREER         	= fact_aos_vw_cer1.ACAD_CAREER
    AND student_term_extract.emplid             	= fact_aos_vw_cer1.emplid
    AND student_term_extract.strm                 	= fact_aos_vw_cer1.strm
    AND fact_aos_vw_cer1.plan_type_cd             	= 'CERT' 
    and fact_aos_vw_cer1.plan_type_rn           	= 1
    
left join hercules_workday.dim_plan as cohort_dim_plan_cer
    on   fact_aos_vw_cer1.acad_plan             = cohort_dim_plan_cer.acad_plan_cd
    and  fact_aos_vw_cer1.plan_type_cd             = cohort_dim_plan_cer.plan_type_cd
    
LEFT JOIN hercules_workday.fact_aos fact_aos_vw_spec1 
ON      student_term_extract.ACAD_CAREER         	= fact_aos_vw_spec1.ACAD_CAREER
    AND student_term_extract.emplid            	 	= fact_aos_vw_spec1.emplid
    AND student_term_extract.strm                 	= fact_aos_vw_spec1.strm
    AND fact_aos_vw_spec1.plan_type_cd             	= 'SPEC' 
    and fact_aos_vw_spec1.plan_type_rn           	= 1
    
left join hercules_workday.dim_plan as cohort_dim_plan_spec
    on   fact_aos_vw_spec1.acad_plan             = cohort_dim_plan_spec.acad_plan_cd
    and  fact_aos_vw_spec1.plan_type_cd         = cohort_dim_plan_spec.plan_type_cd
    
LEFT JOIN perseus_workday.cs_degrees deg
   ON student_term_extract.emplid = deg.emplid
  AND student_term_extract.acad_career = deg.acad_career
  
LEFT JOIN hercules_workday.dim_isir dim_isir 
    ON student_term_extract.emplid = dim_isir.emplid::TEXT
    AND dim_isir.aid_year = dim_term.aid_yr 

LEFT JOIN perseus_workday.cs_admission adm
   ON x.emplid 				= 	adm.emplid
  AND x.student_career 		= 	adm.acad_career
  AND dim_term.rel_strm   	>= 	adm.eff_strm
  AND dim_term.rel_strm	 	<  	adm.next_eff_strm
  
LEFT JOIN hercules_workday.fact_pi_api ls
   ON student_term_extract.strm            = ls.strm
  AND student_term_extract.acad_career     = ls.acad_career_cd
  AND student_term_extract.emplid         = ls.emplid
  AND ls.best_trn_flg                     = 'Y' 

LEFT JOIN hercules_workday.dim_term cur_strm
   ON x.cohort_strm = cur_strm.strm

LEFT JOIN hercules_workday.dim_term next_strm
   ON cur_strm.rel_strm = (next_strm.rel_strm - 2)
 
LEFT JOIN perseus_workday.cs_enrollments next_enrl
   ON x.emplid = next_enrl.emplid
  AND next_strm.strm = next_enrl.strm
  AND x.student_career = next_enrl.acad_career
  
LEFT JOIN perseus_workday.cs_enrollments cohort_enrl
   ON x.emplid = cohort_enrl.emplid
  AND x.cohort_strm = cohort_enrl.strm
  AND x.student_career = cohort_enrl.acad_career
  
LEFT JOIN hercules_workday.dim_campus campus
   ON x.student_campus = campus.src_sys_cd


LEFT JOIN hercules_workday.dim_term dim_term_eff
   ON dim_term_eff.strm                     = x.strm_plus_0

LEFT JOIN perseus_workday.student_term_extract student_term_extract_eff
   ON student_term_extract.emplid = student_term_extract_eff.emplid
  AND student_term_extract.acad_career = student_term_extract_eff.acad_career
  AND dim_term_eff.rel_strm                 >= student_term_extract_eff.rel_strm
  AND dim_term_eff.rel_strm                 <  student_term_extract_eff.next_rel_strm

LEFT JOIN hercules_workday.fact_aos fact_aos_vw_eff
   ON student_term_extract_eff.acad_career     			= fact_aos_vw_eff.acad_career
  AND student_term_extract_eff.emplid         			= fact_aos_vw_eff.emplid
  AND student_term_extract_eff.strm         			= fact_aos_vw_eff.strm
  and fact_aos_vw_eff.plan_type_cd						= 'PROG'
  and fact_aos_vw_eff.plan_type_rn               		= 1
  
left join hercules_workday.dim_program as rel_dim_program
    on      fact_aos_vw_eff.acad_prog             = rel_dim_program.acad_prog_cd

left join hercules_workday.dim_degree as rel_dim_degree
    on      rel_dim_program.degree_cd         = rel_dim_degree.degree_cd
    
LEFT JOIN hercules_workday.fact_aos rel_fact_aos_vw_maj1 
ON      student_term_extract_eff.ACAD_CAREER    		= rel_fact_aos_vw_maj1.ACAD_CAREER
    AND student_term_extract_eff.emplid         		= rel_fact_aos_vw_maj1.emplid
    AND student_term_extract_eff.strm           		= rel_fact_aos_vw_maj1.strm
    AND rel_fact_aos_vw_maj1.plan_type_cd       		= 'MAJ' 
    and rel_fact_aos_vw_maj1.plan_type_rn  				= 1
    
left join hercules_workday.dim_plan as rel_dim_plan_maj
    on   rel_fact_aos_vw_maj1.acad_plan             = rel_dim_plan_maj.acad_plan_cd
    and  rel_fact_aos_vw_maj1.plan_type_cd             = rel_dim_plan_maj.plan_type_cd
    
LEFT JOIN hercules_workday.fact_aos rel_fact_aos_vw_min1 
ON      student_term_extract_eff.ACAD_CAREER         	= rel_fact_aos_vw_min1.ACAD_CAREER
    AND student_term_extract_eff.emplid            	 	= rel_fact_aos_vw_min1.emplid
    AND student_term_extract_eff.strm                 	= rel_fact_aos_vw_min1.strm
    AND rel_fact_aos_vw_min1.plan_type_cd             	= 'MIN' 
    and rel_fact_aos_vw_min1.plan_type_rn  				= 1
    
left join hercules_workday.dim_plan as rel_dim_plan_min
    on   rel_fact_aos_vw_min1.acad_plan             = rel_dim_plan_min.acad_plan_cd
    and  rel_fact_aos_vw_min1.plan_type_cd             = rel_dim_plan_min.plan_type_cd
    
LEFT JOIN hercules_workday.fact_aos rel_fact_aos_vw_cer1 
ON      student_term_extract_eff.ACAD_CAREER         	= rel_fact_aos_vw_cer1.ACAD_CAREER
    AND student_term_extract_eff.emplid             	= rel_fact_aos_vw_cer1.emplid
    AND student_term_extract_eff.strm                 	= rel_fact_aos_vw_cer1.strm
    AND rel_fact_aos_vw_cer1.plan_type_cd             	= 'CERT' 
    and rel_fact_aos_vw_cer1.plan_type_rn           	= 1
    
left join hercules_workday.dim_plan as rel_dim_plan_cer
    on   rel_fact_aos_vw_cer1.acad_plan             = rel_dim_plan_cer.acad_plan_cd
    and  rel_fact_aos_vw_cer1.plan_type_cd             = rel_dim_plan_cer.plan_type_cd
    
LEFT JOIN hercules_workday.fact_aos rel_fact_aos_vw_spec1 
ON      student_term_extract_eff.ACAD_CAREER         	= rel_fact_aos_vw_spec1.ACAD_CAREER
    AND student_term_extract_eff.emplid             	= rel_fact_aos_vw_spec1.emplid
    AND student_term_extract_eff.strm                 	= rel_fact_aos_vw_spec1.strm
    AND rel_fact_aos_vw_spec1.plan_type_cd             	= 'SPEC' 
    and rel_fact_aos_vw_spec1.plan_type_rn           	= 1
    
left join hercules_workday.dim_plan as rel_dim_plan_spec
    on   rel_fact_aos_vw_spec1.acad_plan             = rel_dim_plan_spec.acad_plan_cd
    and  rel_fact_aos_vw_spec1.plan_type_cd         = rel_dim_plan_spec.plan_type_cd
    
LEFT JOIN perseus_workday.cs_financial_aid rel_fin_aid
   ON x.emplid                 = rel_fin_aid.emplid
  AND dim_term_eff.aid_yr     = rel_fin_aid.aid_year

LEFT JOIN hercules_workday.dim_isir as rel_dim_isir 
    ON student_term_extract_eff.emplid     = rel_dim_isir.emplid::TEXT
    AND rel_dim_isir.aid_year             = dim_term_eff.aid_yr

LEFT JOIN hercules_workday.dim_term cur_pstrm
   ON cur_pstrm.strm = x.strm_plus_0

  
LEFT JOIN hercules_workday.dim_term pstrm
   ON (cur_pstrm.rel_strm - 2)     = pstrm.rel_strm
   
LEFT JOIN edwutil.test_score_conversions_tbl act_to_sat_super
   ON ACT_TO_SAT_SUPER."TYPE" = 'ACT TO SAT'
  AND act_to_sat_super.from_score = dim_tests.super_act_score

LEFT JOIN edwutil.test_score_conversions_tbl sat_to_act_super
   ON SAT_TO_ACT_SUPER."TYPE" = 'SAT TO ACT'
  AND sat_to_act_super.from_score = dim_tests.super_sat_score

LEFT JOIN hercules_workday.ff_student_tracker nsc_st
   ON nsc_st.enrl_strm = x.cohort_strm
  AND nsc_st.emplid = x.emplid

LEFT JOIN edwutil.perseus_lookup acad_career_filter
   ON acad_career_filter.fieldname = 'ACAD_CAREER_FILTER'
  AND acad_career_filter.fieldvalue = x.student_career
  AND acad_career_filter.field_sd = '0'

LEFT JOIN edwutil.perseus_lookup undecided_flag
   ON undecided_flag.fieldname = 'INITIALLY_UNDECIDED_FLAG'
  AND undecided_flag.fieldvalue = x.acad_plan_cd
  
--{join_custom_extract_join}

WHERE 1 = 1
--and true
AND x.acad_plan_cd <> '*'
AND x.status_year_cd IS NOT NULL
AND to_date(dim_term.term_begin_day_key,'J') >= dateadd(year,-10,sysdate) 

;

COMMIT;

ANALYZE perseus_workday.retention_extract;