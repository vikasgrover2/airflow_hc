/*######################################
Object type       : PERSEUS EXTRACT TABLE
Description       :
Grain             :
Target table name : FINANCIAL_AID_EXTRACT
Source table list : CS_ADMISSION (PERSEUS)
                    CS_DEGREES (PERSEUS)
                    CS_ENROLLMENTS (PERSEUS)
                    CS_PERSISTENCE (PERSEUS)
                    CS_RETENTION (PERSEUS)
                    CS_RETENTION_REL (PERSEUS)
                    CS_SERVICE_INDICATORS (PERSEUS)
                    DIM_ADDRESS
                    DIM_ADDRESS_CASCADE
                    DIM_CAR_PRGRM_PLAN
                    DIM_CAREER
                    DIM_FINAID_TYPE
                    DIM_ISIR
                    DIM_OTH_RAW_ZIP_DATA_T (EDWUTIL)
                    DIM_PERSON
                    DIM_SUB_PLAN
                    DIM_TERM
                    FACT_AOS
                    FACT_FINAID
                    FACT_FINAID_BUDGET
                    FACT_PI_API
                    FACT_TESTSCORE
                    STUDENT_TERM_EXTRACT (PERSEUS)
ETL load type     : FULL LOAD EVERYDAY
Date created      : 06-June-2024
########################################*/

TRUNCATE TABLE perseus_workday.financial_aid_extract;

INSERT INTO perseus_workday.financial_aid_extract
    (plan_acad_org_program_ld
    ,plan_acad_org_program_cd
    ,plan_acad_org_school_ld
    ,plan_acad_org_school_cd
    ,plan_acad_org_spec_ld
    ,plan_acad_plan_ld
    ,plan_acad_plan_code
    ,acad_stndng_actn_cd
    ,acad_stndng_actn_ld
    ,career_unt_passd_gpa
    ,career_unt_passd_nogpa
    ,career_unt_taken_nogpa
    ,career_unt_taken_gpa
    ,term_degr_chkout_stat_cd
    ,term_degr_chkout_stat_sd
    ,exp_grad_term
    ,exp_grad_term_year_ld
    ,plan_sub_plan1
    ,plan_sub_plan2
    ,plan_sub_plan3
    ,plan_sub_plan1_cd
    ,plan_sub_plan2_cd
    ,plan_sub_plan3_cd
    ,plan_sub_plan1_type
    ,plan_sub_plan2_type
    ,plan_sub_plan3_type
    ,plan_acad_prog
    ,plan_prog_status_cd
    ,plan_degree_type
    ,plan_institution_cd
    ,plan_institution_ld
    ,plan_grouping
    ,plan_bridge_department
    ,plan_field_of_study_cd
    ,plan_field_of_study_ld
    ,trn_pi_totalcredit
    ,trn_ls_school_type
    ,trn_ls_school_type_ld
    ,emplid
    ,acad_career
    ,acad_career_ld
    ,strm
    ,disbursement_plan
    ,split_code
    ,award_status
    ,award_msg_cd
    ,offer_amount
    ,authorized_amount
    ,highest_offer_amt
    ,highest_accept_amt
    ,ws_ernd_to_dt
    ,currency_cd
    ,net_award_amt
    ,cancel_decline_amt
    ,offer_activity_ind
    ,fin_aid_type
    ,loan_adjust_cd
    ,scheduled_award
    ,loan_tot_orig
    ,loan_tot_disb
    ,lock_award_flag
    ,charge_priority
    ,pkg_status_cd
    ,accept_loan_fee
    ,offer_loan_fee
    ,net_offer_amt
    ,award_period
    ,pkg_plan_id
    ,pkg_seq_nbr
    ,award_posted
    ,fa_prof_judgement
    ,custom_loan_fee
    ,pkg_app_data_used
    ,override_need
    ,override_fl
    ,ln_dest_fee_prcnt
    ,offer_rebate_amt
    ,accept_rebate_amt
    ,sfa_pkg_dep_stat
    ,sfa_aggr_src_used
    ,sfa_ea_type
    ,sfa_ea_source
    ,sfa_ea_program_cd
    ,sfa_ea_loan_cert
    ,sfa_rpkg_plan_id
    ,sfa_ea_indicator
    ,offered_dt
    ,offer_accepted_dt
    ,fin_aid_accepted_dt
    ,authorized_dt
    ,auth_override_dt
    ,cancelled_dt
    ,declined_dt
    ,disbursed_dt
    ,offer_to_accpt_days
    ,accept_to_auth_days
    ,auth_to_disbursed_days
    ,offer_to_disbursed_days
    ,business_unit
    ,adjust_reason_cd
    ,adjust_amount
    ,disb_to_date
    ,auth_to_date
    ,accept_amount
    ,disbursed_amount
    ,pell
    ,grant_schol
    ,fed_sub
    ,fed_unsub
    ,private_alt_loan
    ,loan
    ,workstudy
    ,tuition_remission
    ,military_ta
    ,vet_ben
    ,aid_year
    ,fa_source
    ,fa_type
    ,need_based
    ,award_description
    ,fin_aid_cat
    ,total_grant_schol_tuit_remis_amt
    ,total_loan_amt
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
    ,stdnt_new_returning_desc
    ,stdnt_acad_level_ld
    ,stdnt_career_gpa_cum
    ,stdnt_car_tot_cumulative
    ,stdnt_car_tot_test_credit
    ,stdnt_car_tot_trnsfr
    ,stdnt_car_unt_audit
    ,stdnt_car_unt_test_credit
    ,stdnt_car_unt_trnsfr
    ,stdnt_tuition_residency
    ,stdnt_full_part_time_ld
    ,stdnt_residency_ld
    ,stdnt_campus_cd
    ,stdnt_campus_ld
    ,dem_ethnicity
    ,dem_gender
    ,dem_marital_status
    ,stdnt_birthdate_day
    ,stdnt_emplid
    ,stdnt_first_name
    ,stdnt_last_name
    ,fin_aid_flag
    ,fin_aid_disbursed_flag
    ,fin_aid_offered_flag
    ,fin_aid_accept_flag
    ,geo_dma
    ,geo_state
    ,geo_postal
    ,geo_address1
    ,geo_address2
    ,geo_address_type
    ,geo_country_ld
    ,geo_county
    ,geo_city
    ,geo_zipcode
    ,geo_state_cd
    ,geo_state_ld
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
    ,req_term
    ,accepted_dt
    ,stdnt_admit_type_cd
    ,stdnt_admit_type_ld
    ,admit_term
    ,app_dt
    ,completion_term
    ,completion_term_year_ld
    ,completion_term_time_calendar_yr
    ,completion_term_time_academic_yr
    ,completion_term_time_fiscal_yr
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
    ,time_term_year_ld
    ,time_fiscal_yr
    ,time_strm
    ,time_collapsed_strm
    ,time_term_type
    ,time_term_type_collapsed
    ,time_calendar_yr
    ,time_academic_yr
    ,time_collapsed_term_year_ld
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
    ,reenrollment_type
    ,persistence_flg
    ,retention_flag
    ,retention_status
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
    ,ret_cohort_strm
    ,plan_major
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
    ,home_phone
    ,cell_phone
    ,advisor_id
    ,advisor_name
    ,stdnt_home_email
    ,stdnt_business_email
    ,stdnt_campus_email
    ,advisor_business_email
    ,advisor_campus_email
    ,advisor_home_email
    ,stdnt_grp_first_gen
    ,disbursement_id
    ,item_type
    ,tuition_amount
    ,fees_amount
    ,pri_acad_plan_dept_cd
    ,pri_acad_plan_dept_ld
    ,initial_offer_amount
    ,initial_accept_amount
    ,urm_status
    ,perseus_load_time
    ,award_period_fed_efc
    ,award_period_fed_year_coa
    ,award_period_fed_need
    ,award_period_fed_unmet_need
    ,cip2000_cd
    ,cip2000_ld
    ,military_status_cd
    ,military_status_ld
    ,honors_acad_prog_ind
    ,acad_stndng_actn_cd_bot
    ,acad_stndng_actn_ld_bot
    ,stdnt_admit_type_group
    ,stdnt_dev_ed_engl_ind
    ,stdnt_dev_ed_math_ind
    ,stdnt_dev_ed_ind
    ,pri_bhe_api_hegis_cd
    ,pri_bhe_api_hegis_ld
    ,sec_bhe_api_hegis_cd
    ,sec_bhe_api_hegis_ld
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
    ,disburse_method_cd
    ,disburse_method_sd
    ,disburse_method_ld
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
    ,full_part_time_cd_calc
    ,pri_reporting_acad_program
    ,pri_reporting_acad_dept_cd
    ,pri_reporting_acad_dept_ld
    ,pri_reporting_acad_school
    ,isir_results_id
    ,officially_registered_ind
  )
SELECT   
    student_term_extract.pri_acad_prog_ld                   AS plan_acad_org_program_ld
    ,student_term_extract.pri_acad_prog_cd                   AS plan_acad_org_program_cd
    ,student_term_extract.pri_school_ld                      AS plan_acad_org_school_ld
    ,student_term_extract.pri_school_cd                      AS plan_acad_org_school_cd
    ,student_term_extract.pri_specialization_ld              AS plan_acad_org_spec_ld
    ,student_term_extract.pri_acad_plan_ld                   AS plan_acad_plan_ld
    ,student_term_extract.pri_acad_plan_cd                   AS plan_acad_plan_code
    ,student_term_extract.acad_stndng_actn_cd
    ,student_term_extract.acad_stndng_actn_ld
    ,student_term_extract.career_unt_passd_gpa
    ,student_term_extract.career_unt_passd_nogpa
    ,student_term_extract.career_unt_taken_nogpa
    ,student_term_extract.career_unt_taken_gpa
    ,student_term_extract.degr_chkout_stat_cd                 AS term_degr_chkout_stat_cd
    ,student_term_extract.degr_chkout_stat_sd                 AS term_degr_chkout_stat_sd
    ,student_term_extract.exp_grad_term
    ,student_term_extract.exp_grad_term_year_ld               AS exp_grad_term_year_ld
    ,null                        AS plan_sub_plan1
    ,null                       AS plan_sub_plan2
    ,null                       AS plan_sub_plan3
    ,null                       AS plan_sub_plan1_cd
    ,null                       AS plan_sub_plan2_cd
    ,null                       AS plan_sub_plan3_cd
    ,null                   AS plan_sub_plan1_type
    ,null                   AS plan_sub_plan2_type
    ,null                   AS plan_sub_plan3_type
    ,student_term_extract.pri_acad_prog                       AS plan_acad_prog
    ,student_term_extract.pri_prog_status_cd                  AS plan_prog_status_cd
    ,student_term_extract.pri_education_level_ld AS plan_degree_type
    ,dim_car_prgrm_major_vw.institution_cd                    AS plan_institution_cd
    ,dim_car_prgrm_major_vw.institution_ld                    AS plan_institution_ld
    ,COALESCE (dim_car_prgrm_major_vw.grouping_ld,'*')        AS plan_grouping
    ,COALESCE (dim_car_prgrm_major_vw.department_ld,'*')      AS plan_bridge_department
    ,dim_car_prgrm_major_vw.study_field_cd                    AS plan_field_of_study_cd
    ,dim_car_prgrm_major_vw.study_field_ld                    AS plan_field_of_study_ld
    ,null::numeric                                            AS trn_pi_totalcredit
    ,NULL                                                     AS trn_ls_school_type
    ,null                                                     AS trn_ls_school_type_ld
    ,sfa.emplid
    ,c.acad_career_cd                                         AS acad_career
    ,c.acad_career_ld
    ,sfa.disb_strm                                            AS strm
    ,sfa.disbursement_plan
    ,sfa.split_code
    ,sfa.award_status
    ,sfa.award_msg_cd
    ,sfa.offer_amount
    ,sfa.authorized_amount
    ,sfa.highest_offer_amt
    ,sfa.highest_accept_amt
    ,sfa.ws_ernd_to_dt
    ,sfa.currency_cd
    ,sfa.net_award_amt
    ,sfa.cancel_decline_amt
    ,sfa.offer_activity_ind
    ,case when fa.finaid_cat = 'MISCL' then 'LOAN' else sfa.fin_aid_type end                                          AS fin_aid_type
    ,sfa.loan_adjust_cd
    ,sfa.scheduled_award
    ,sfa.loan_tot_orig
    ,sfa.loan_tot_disb
    ,sfa.lock_award_flag
    ,sfa.charge_priority
    ,sfa.pkg_status_cd
    ,sfa.accept_loan_fee
    ,sfa.offer_loan_fee
    ,sfa.net_offer_amt
    ,sfa.award_period
    ,sfa.pkg_plan_id
    ,sfa.pkg_seq_nbr
    ,sfa.award_posted
    ,sfa.fa_prof_judgement
    ,sfa.custom_loan_fee
    ,sfa.pkg_app_data_used
    ,sfa.override_need
    ,sfa.override_fl
    ,sfa.ln_dest_fee_prcnt
    ,sfa.offer_rebate_amt
    ,sfa.accept_rebate_amt
    ,sfa.sfa_pkg_dep_stat
    ,sfa.sfa_aggr_src_used
    ,sfa.sfa_ea_type
    ,sfa.sfa_ea_source
    ,sfa.sfa_ea_program_cd
    ,sfa.sfa_ea_loan_cert
    ,sfa.sfa_rpkg_plan_id
    ,sfa.sfa_ea_indicator
    ,sfa.offered_dt
    ,sfa.offer_accepted_dt
    ,sfa.accepted_dt AS fin_aid_accepted_dt
    ,sfa.authorized_dt
    ,sfa.auth_override_dt
    ,sfa.cancelled_dt
    ,sfa.declined_dt
    ,sfa.disbursed_dt
    ,sfa.offer_to_accpt_days
    ,sfa.accept_to_auth_days
    ,sfa.auth_to_disbursed_days
    ,sfa.offer_to_disbursed_days
    ,sfa.business_unit
    ,sfa.adjust_reason_cd
    ,sfa.adjust_amount
    ,sfa.disb_to_date
    ,sfa.auth_to_date
    ,COALESCE (sfa.accept_amount,0)    AS accept_amount
    ,COALESCE (sfa.disbursed_amount,0) AS disbursed_amount
    ,COALESCE
      (CASE
          WHEN UPPER (fa.award_description) ~~ '%PELL%' THEN sfa.disbursed_amount
          ELSE 0
        END,0)                        AS pell
    ,COALESCE
      (CASE
          WHEN fa.finaid_type = grant_fa_type.fieldvalue OR fa.finaid_type = scholarship_fa_type.fieldvalue THEN sfa.disbursed_amount
          ELSE 0
        END,0)                        AS grant_schol
    ,null                              AS fed_sub
    ,null                              AS fed_unsub
    ,null                              AS private_alt_loan
    ,COALESCE
      (CASE
          WHEN case when fa.finaid_cat = 'MISCL' then 'LOAN' else sfa.fin_aid_type end = loan_fa_type.fieldvalue THEN sfa.disbursed_amount
          ELSE 0
        END,0)                        AS loan
    ,COALESCE
      (CASE
          WHEN fa.finaid_type = workstudy_fa_type.fieldvalue THEN sfa.disbursed_amount
          ELSE 0
        END,0)                        AS workstudy
    ,COALESCE
      (CASE
          WHEN fa.finaid_type = waiver_fa_type.fieldvalue AND fa.fa_source = inst_fa_source.fieldvalue THEN sfa.disbursed_amount
          ELSE 0
        END,0)                        AS tuition_remission
    ,COALESCE
      (CASE
          WHEN fa.finaid_type = 'ASSISTANCE' AND fa.fa_source = 'MILITARY' THEN sfa.disbursed_amount
          ELSE 0
        END,0)                        AS military_ta
    ,COALESCE
      (CASE
          WHEN fa.finaid_type = va_benefits_fa_type.fieldvalue THEN sfa.disbursed_amount
          ELSE 0
        END,0)                        AS vet_ben
    ,sfa.aid_year                      AS aid_year
    ,fa.fa_source_descr                AS fa_source
    ,fa.finaid_type_descr              AS fa_type
    ,fa.need_based                     AS need_based
    ,fa.award_description              AS award_description
    ,fa.finaid_cat                     AS fin_aid_cat
    ,COALESCE
      (CASE
          WHEN fa.finaid_type = grant_fa_type.fieldvalue OR fa.finaid_type = scholarship_fa_type.fieldvalue THEN sfa.disbursed_amount
          ELSE 0
        END +
        CASE
          WHEN fa.finaid_type = waiver_fa_type.fieldvalue AND fa.fa_source = inst_fa_source.fieldvalue THEN sfa.disbursed_amount
          ELSE 0
        END +
        CASE
          WHEN fa.finaid_type = 'ASSISTANCE' AND fa.fa_source = 'MILITARY' THEN sfa.disbursed_amount
          ELSE 0
        END +
        CASE
          WHEN fa.finaid_type = va_benefits_fa_type.fieldvalue THEN sfa.disbursed_amount
          ELSE 0
        END,0)                        AS total_grant_schol_tuit_remis_amt
    ,COALESCE
      (CASE
          WHEN case when fa.finaid_cat = 'MISCL' then 'LOAN' else sfa.fin_aid_type end = loan_fa_type.fieldvalue THEN sfa.disbursed_amount
          ELSE 0
        END +
        CASE
          WHEN case when fa.finaid_cat = 'MISCL' then 'LOAN' else sfa.fin_aid_type end = workstudy_fa_type.fieldvalue THEN sfa.disbursed_amount
          ELSE 0
        END,0)                        AS total_loan_amt
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
    ,enrl.time_first_enrl_add_day_key
    ,dim_isir.current_fa_app_date      AS isir_current_fa_app_date
    ,dim_isir.first_fa_app_date        AS isir_first_fa_app_date
    ,dim_isir.pell_eligibility         AS isir_pell_eligibility
    ,dim_isir.adj_efc                  AS isir_adj_efc
    ,dim_isir.efc_status               AS isir_efc_status
    ,dim_isir.titleiv_elig             AS isir_titleiv_elig
    ,dim_isir.ssa_citizenshp_ind       AS isir_ssa_citizenshp_ind
    ,dim_isir.num_family_members       AS isir_num_family_members
    ,dim_isir.number_in_college        AS isir_number_in_college
    ,dim_isir.veteran                  AS isir_veteran
    ,dim_isir.graduate_student         AS isir_graduate_student
    ,dim_isir.married                  AS isir_married
    ,dim_isir.marital_stat             AS isir_marital_stat
    ,dim_isir.orphan                   AS isir_orphan
    ,dim_isir.dependents               AS isir_dependents
    ,dim_isir.agi                      AS isir_agi
    ,dim_isir.school_choice_1          AS isir_school_choice_1
    ,dim_isir.housing_code_1           AS isir_housing_code_1
    ,dim_isir.school_choice_2          AS isir_school_choice_2
    ,dim_isir.housing_code_2           AS isir_housing_code_2
    ,dim_isir.school_choice_3          AS isir_school_choice_3
    ,dim_isir.housing_code_3           AS isir_housing_code_3
    ,dim_isir.school_choice_4          AS isir_school_choice_4
    ,dim_isir.housing_code_4           AS isir_housing_code_4
    ,dim_isir.school_choice_5          AS isir_school_choice_5
    ,dim_isir.housing_code_5           AS isir_housing_code_5
    ,dim_isir.school_choice_6          AS isir_school_choice_6
    ,dim_isir.housing_code_6           AS isir_housing_code_6
    ,dim_isir.first_bach_degree        AS isir_first_bach_degree
    ,dim_isir.interested_in_ws         AS isir_interested_in_ws
    ,dim_isir.interested_in_sl         AS isir_interested_in_sl
    ,dim_isir.interested_in_plus       AS isir_interested_in_plus
    ,dim_isir.degree_certif            AS isir_degree_certif
    ,dim_isir.current_grade_lvl        AS isir_current_grade_lvl
    ,dim_isir.depndncy_stat            AS isir_depndncy_stat
    ,dim_isir.citizenship_status       AS isir_citizenship_status
    ,dim_isir.children                 AS isir_children
    ,dim_isir.sfa_active_duty          AS isir_sfa_active_duty
    ,dim_isir.parent_marital_stat      AS isir_parent_marital_stat
    ,dim_isir.parent_number_in_family  AS isir_parent_number_in_family
    ,dim_isir.parent_num_in_college    AS isir_parent_num_in_college
    ,dim_isir.parent_agi               AS isir_parent_agi
    ,dim_isir.father_grade_lvl         AS isir_father_grade_lvl
    ,dim_isir.mother_grade_lvl         AS isir_mother_grade_lvl
    ,dim_isir.parent_sfa_grant_aid     AS isir_parent_sfa_grant_aid
    ,dim_isir.fed_efc                  AS isir_fed_efc
    ,dim_isir.efc_status2              AS isir_efc_status2
    ,dim_isir.fed_need_base_aid        AS isir_fed_need_base_aid
    ,dim_isir.fed_year_coa             AS isir_fed_year_coa
    ,dim_isir.inst_year_coa            AS isir_inst_year_coa
    ,dim_isir.pell_year_coa            AS isir_pell_year_coa
    ,dim_isir.fed_need                 AS isir_fed_need
    ,dim_isir.inst_need                AS isir_inst_need
    ,dim_isir.fed_unmet_need           AS isir_fed_unmet_need
    ,dim_isir.inst_unmet_need          AS isir_inst_unmet_need
    ,dim_isir.fed_stdnt_contrb         AS isir_fed_stdnt_contrb
    ,dim_isir.isir_calc_sc
    ,dim_isir.isir_calc_efc
    ,enrl.courses_taken
    ,enrl.unt_billing
    ,enrl.unt_earned
    ,enrl.grade_points
    ,enrl.grade_points_per_unit
    ,enrl.term_end_dt
    ,enrl.term_begin_dt
    ,enrl.first_course_id
    ,enrl.first_crse_offer_nbr::varchar as first_crse_offer_nbr
    ,enrl.first_session_cd
    ,enrl.first_class_section
    ,enrl.first_class_grade_point
    ,enrl.time_min_enrl_drop_day_key
    ,enrl.courses_withdrawn
    ,student_term_extract.univ_attend_type_sd         AS stdnt_new_returning_desc
    ,student_term_extract.acad_level_bot_ld           AS stdnt_acad_level_ld
    ,student_term_extract.career_gpa_cum              AS stdnt_career_gpa_cum
    ,student_term_extract.career_tot_cumulative       AS stdnt_car_tot_cumulative
    ,student_term_extract.career_tot_test_credit      AS stdnt_car_tot_test_credit
    ,student_term_extract.career_tot_trnsfr           AS stdnt_car_tot_trnsfr
    ,student_term_extract.career_unt_audit            AS stdnt_car_unt_audit
    ,student_term_extract.career_unt_test_credit      AS stdnt_car_unt_test_credit
    ,student_term_extract.career_unt_trnsfr           AS stdnt_car_unt_trnsfr
    ,student_term_extract.tuition_residency_ld        AS stdnt_tuition_residency
    ,student_term_extract.full_part_time_ld           AS stdnt_full_part_time_ld
    ,COALESCE (student_term_extract.residency_ld,'*') AS stdnt_residency_ld
    ,student_term_extract.stdnt_campus_cd
    ,student_term_extract.stdnt_campus_ld
    ,dim_person_stdnt.ipeds_race_ethn_citz_ld         AS dem_ethnicity
    ,dim_person_stdnt.gender_ld                       AS dem_gender
    ,dim_person_stdnt.married_status_ld               AS dem_marital_status
    ,CASE
      WHEN dim_person_stdnt.birthdate_day = 0 THEN NULL
      ELSE TO_DATE (dim_person_stdnt.birthdate_day,'J')
    END                                              AS stdnt_birthdate_day
    ,sfa.emplid                                       AS stdnt_emplid
    ,dim_person_stdnt.first_name                      AS stdnt_first_name
    ,dim_person_stdnt.last_name                       AS stdnt_last_name
    ,CASE
      WHEN sfa.emplid IS NOT NULL THEN 'Y'
      ELSE 'N'
    END AS fin_aid_flag
    ,CASE
      WHEN (SUM (sfa.disbursed_amount) OVER (PARTITION BY sfa.acad_career,sfa.emplid,sfa.aid_year ROWS BETWEEN unbounded preceding AND unbounded following)) > 0 THEN 'Y'
      ELSE 'N'
    END AS fin_aid_disbursed_flag
    ,CASE
      WHEN (SUM (sfa.offer_amount) OVER (PARTITION BY sfa.acad_career,sfa.emplid,sfa.aid_year ROWS BETWEEN unbounded preceding AND unbounded following)) > 0 THEN 'Y'
      ELSE 'N'
    END AS fin_aid_offered_flag
    ,CASE
      WHEN (SUM (sfa.accept_amount) OVER (PARTITION BY sfa.acad_career,sfa.emplid,sfa.aid_year ROWS BETWEEN unbounded preceding AND unbounded following)) > 0 THEN 'Y'
      ELSE 'N'
    END                                                               AS fin_aid_accept_flag
    ,dim_address_perm.geog_dma_name                                    AS geo_dma
    ,dim_address_cascade_vw.state_ld                                   AS geo_state
    ,dim_address_cascade_vw.postal                                     AS geo_postal
    ,dim_address_cascade_vw.address1                                   AS geo_address1
    ,dim_address_cascade_vw.address2                                   AS geo_address2
    ,null                                                              AS geo_address_type
    ,dim_address_cascade_vw.country_ld                                 AS geo_country_ld
    ,dim_address_cascade_vw.county                                     AS geo_county
    ,dim_address_cascade_vw.city                                       AS geo_city
    ,"SUBSTRING" (dim_address_cascade_vw.postal,1,5)                   AS geo_zipcode
    ,dim_address_cascade_vw.state_cd                                   AS geo_state_cd
    ,dim_address_cascade_vw.state_ld                                   AS geo_state_ld
    ,null::numeric                                                     AS hs_final_gpa
    ,null::numeric                                                     AS hs_term_year
    ,null                                                              AS hs_highest_ed_lvl
    ,null::numeric                                                     AS hs_final_gpa_excl_last_strm
    ,null                                                              AS hs_last_year
    ,null                                                              AS hs_start_year
    ,null                                                              AS hs_years_attended
    ,null                                                              AS hs_class_pop
    ,null                                                              AS hs_rank
    ,null                                                              AS hs_unt_comp_total
    ,null                                                          AS hs_name
    ,null                                                          AS hs_short_name
    ,null                                                              AS hs_proprietorship
    ,null                                                       AS hs_address1
    ,null                                                       AS hs_address2
    ,null                                                          AS hs_city
    ,null                                                        AS hs_county
    ,null                                                         AS hs_state
    ,null                                                         AS hs_postal
    ,null                                                        AS hs_country
    ,NULL                                                              AS hs_lat
    ,NULL                                                              AS hs_long
    ,NULL                                         AS test_first_act_score
    ,NULL                                          AS test_last_act_score
    ,NULL                                          AS test_max_act_score
    ,NULL                                         AS test_avg_act_score
    ,NULL                                        AS test_num_act_attempts
    ,NULL                                        AS test_first_sat_score
    ,NULL                                         AS test_last_sat_score
    ,NULL                                          AS test_max_sat_score
    ,NULL                                         AS test_avg_sat_score
    ,NULL                                        AS test_num_sat_attempts
    ,null				                                               AS stdnt_cntct_latitude
    ,NULL                                                              AS stdnt_cntct_longitude
    ,adm.req_term
    ,adm.accepted_dt
    ,adm.stdnt_admit_type_cd
    ,adm.stdnt_admit_type_ld
    ,adm.admit_term
    ,adm.app_dt
    ,deg.completion_term
    ,grad_dim_term_vw.term_year_ld AS completion_term_year_ld
    ,grad_dim_term_vw.calendar_yr AS completion_term_time_calendar_yr
    ,grad_dim_term_vw.academic_yr AS completion_term_time_academic_yr
    ,grad_dim_term_vw.fiscal_yr AS completion_term_time_fiscal_yr
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
    ,dim_term_vw.term_year_ld AS time_term_year_ld
    ,dim_term_vw.fiscal_yr AS time_fiscal_yr
    ,sfa.disb_strm AS time_strm
    ,dim_term_vw.collapsed_strm AS time_collapsed_strm
    ,dim_term_vw.term_type_ld AS time_term_type
    ,CASE
      WHEN "SUBSTRING" (dim_term_vw.strm,4,1) = '1' THEN 'SPRING'
      ELSE dim_term_vw.term_type_ld
    END AS time_term_type_collapsed
    ,dim_term_vw.calendar_yr AS time_calendar_yr
    ,dim_term_vw.academic_yr AS time_academic_yr
    ,dim_term_vw.collapsed_term_year_ld AS time_collapsed_term_year_ld
    ,enrl.tot_courses_taken
    ,enrl.tot_courses_withdrawn
    ,enrl.tot_unt_billing
    ,enrl.tot_unt_earned
    ,enrl.tot_grade_points
    ,enrl.tot_grade_points_per_unit
    ,enrl.tot_term_end_dt
    ,enrl.tot_term_begin_dt
    ,enrl.tot_first_course_id
    ,enrl.tot_first_crse_offer_nbr
    ,enrl.tot_first_session_cd
    ,enrl.tot_first_class_section
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
    ,persis.reenrollment_type
    ,persis.persistence_flg
    ,"MAX" (rel_ret.retention_flag) OVER (PARTITION BY sfa.emplid,sfa.acad_career,sfa.aid_year ROWS BETWEEN unbounded preceding AND unbounded following)   AS retention_flag
    ,"MAX" (rel_ret.retention_status) OVER (PARTITION BY sfa.emplid,sfa.acad_career,sfa.aid_year ROWS BETWEEN unbounded preceding AND unbounded following) AS retention_status
    ,COALESCE (enrollment_address_vw.address1,'*')                                                                                                         AS enrollment_address1
    ,COALESCE (enrollment_address_vw.address2,'*')                                                                                                         AS enrollment_address2
    ,COALESCE (enrollment_address_vw.address_type_cd,'*')                                                                                                  AS enrollment_address_type
    ,COALESCE (enrollment_address_vw.city,'*')                                                                                                             AS enrollment_address_city
    ,COALESCE (enrollment_address_vw.country_ld,'*')                                                                                                       AS enrollment_address_country_ld
    ,COALESCE (enrollment_address_vw.county,'*')                                                                                                           AS enrollment_address_county
    ,COALESCE (enrollment_address_vw.postal,'*')                                                                                                           AS enrollment_address_postal
    ,COALESCE (enrollment_address_vw.state_cd,'*')                                                                                                         AS enrollment_address_state_cd
    ,COALESCE (enrollment_address_vw.state_ld,'*')                                                                                                         AS enrollment_address_state_ld
    ,COALESCE ("SUBSTRING" (enrollment_address_vw.postal,1,5),'*')                                                                                         AS enrollment_address_zipcode
    ,NULL                                                                                                                          AS enrollment_address_latitude
    ,NULL                                                                                                                          AS enrollment_address_longitude
    ,COALESCE (ret.strm,'*')                                                                                                                               AS ret_cohort_strm
    ,COALESCE (dim_car_prgrm_major_vw.bhe_api_hegis_ld,'*')                                                                                                AS plan_major
    ,NULL                                                                                                                             AS test_first_gre_score
    ,NULL                                                                                                                             AS test_last_gre_score
    ,NULL                                                                                                                              AS test_max_gre_score
    ,NULL                                                                                                                              AS test_avg_gre_score
    ,NULL                                                                                                                            AS test_num_gre_attempts
    ,NULL                                                                                                                            AS test_first_gmat_score
    ,NULL                                                                                                                             AS test_last_gmat_score
    ,NULL                                                                                                                              AS test_max_gmat_score
    ,NULL                                                                                                                              AS test_avg_gmat_score
    ,NULL                                                                                                                           AS test_num_gmat_attempts
    ,null as home_phone
    ,null as cell_phone
    ,NULL                     AS advisor_id
    ,NULL            AS advisor_name
    ,null                               AS stdnt_home_email
    ,null                               AS stdnt_business_email
    ,null                               AS stdnt_campus_email
    ,null                               AS advisor_business_email
    ,null                               AS advisor_campus_email
    ,null                               AS advisor_home_email
    ,null                               AS stdnt_grp_first_gen
    ,sfa.disbursement_id
    ,sfa.item_type
    ,NULL AS tuition_amount
    ,NULL AS fees_amount
    ,student_term_extract.pri_acad_plan_dept_cd
    ,student_term_extract.pri_acad_plan_dept_ld
    ,sfa.initial_offer_amount
    ,sfa.initial_accept_amount
    ,student_term_extract.urm_status AS urm_status
    ,(sysdate - 1) as perseus_load_time
    ,sfa.award_period_fed_efc
    ,sfa.award_period_fed_year_coa
    ,sfa.award_period_fed_need
    ,sfa.award_period_fed_unmet_need
    ,student_term_extract.cip2000_cd 	AS cip2000_cd
    ,student_term_extract.cip2000_ld 	AS cip2000_ld
    ,dim_person_stdnt.military_status_cd
    ,dim_person_stdnt.military_status_ld
    ,student_term_extract.honors_acad_prog_ind
    ,student_term_extract.acad_stndng_actn_cd_bot
    ,student_term_extract.acad_stndng_actn_ld_bot
    ,adm.stdnt_admit_type_group AS stdnt_admit_type_group
    ,NVL (student_term_extract.stdnt_dev_ed_engl_ind,0) AS stdnt_dev_ed_engl_ind
    ,NVL (student_term_extract.stdnt_dev_ed_math_ind,0) AS stdnt_dev_ed_math_ind
    ,NVL (student_term_extract.stdnt_dev_ed_ind,0) AS stdnt_dev_ed_ind
    ,student_term_extract.pri_bhe_api_hegis_cd
    ,student_term_extract.pri_bhe_api_hegis_ld
    ,student_term_extract.sec_bhe_api_hegis_cd
    ,student_term_extract.sec_bhe_api_hegis_ld
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
    ,fa.disburse_method AS disburse_method_cd
    ,fa.disburse_method_descrshort AS disburse_method_sd
    ,fa.disburse_method_descr AS disburse_method_ld
    ,student_term_extract.trn_transfer_inst_name_and_location
    ,student_term_extract.hs_inst_name_and_location
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
    ,student_term_extract.full_part_time_cd_calc
    ,student_term_extract.pri_reporting_acad_program
    ,student_term_extract.pri_reporting_acad_dept_cd
    ,student_term_extract.pri_reporting_acad_dept_ld
    ,student_term_extract.pri_reporting_acad_school
    ,null as isir_results_id
    ,student_term_extract.officially_registered_ind
    
FROM hercules_workday.fact_finaid sfa
LEFT JOIN perseus_workday.student_term_extract student_term_extract
   ON sfa.emplid = student_term_extract.emplid
  AND sfa.acad_career = student_term_extract.acad_career
  AND sfa.disb_strm   = student_term_extract.strm
LEFT JOIN hercules_workday.dim_career c
   ON c.acad_career_cd = sfa.acad_career
LEFT JOIN hercules_workday.dim_term dim_term_vw
   ON sfa.disb_strm = dim_term_vw.strm
LEFT JOIN hercules_workday.dim_address enrollment_address_vw
   ON student_term_extract.address_term_src_sys_cd = enrollment_address_vw.src_sys_cd
LEFT JOIN hercules_workday.dim_finaid_type fa
   ON sfa.item_type = fa.item_type
LEFT JOIN perseus_workday.cs_enrollments enrl
   ON sfa.emplid = enrl.emplid
  AND student_term_extract.strm = enrl.strm
  AND sfa.acad_career = enrl.acad_career
LEFT JOIN hercules_workday.fact_aos fact_aos_vw
   ON sfa.acad_career 				= fact_aos_vw.acad_career
  AND sfa.emplid 					= fact_aos_vw.emplid
  and sfa.strm 						= fact_aos_vw.strm
  and fact_aos_vw.plan_rn			= 1
  --AND student_term_extract.strm = fact_aos_vw.eff_strm
  --AND student_term_extract.primary_stdnt_car_nbr = fact_aos_vw.stdnt_car_nbr
  --AND fact_aos_vw.eff_strm <> '*'
--LEFT JOIN hercules_workday.dim_sub_plan dim_sub_plan_maj1
--   ON fact_aos_vw.acad_plan_maj = dim_sub_plan_maj1.acad_plan_cd
--  AND dim_sub_plan_maj1.acad_sub_plan_cd = fact_aos_vw.subplan_maj1
--LEFT JOIN hercules_workday.dim_sub_plan dim_sub_plan_maj2
--   ON fact_aos_vw.acad_plan_maj = dim_sub_plan_maj2.acad_plan_cd
--  AND dim_sub_plan_maj2.acad_sub_plan_cd = fact_aos_vw.subplan_maj2
--LEFT JOIN hercules_workday.dim_sub_plan dim_sub_plan_maj3
--   ON fact_aos_vw.acad_plan_maj = dim_sub_plan_maj3.acad_plan_cd
--  AND dim_sub_plan_maj3.acad_sub_plan_cd = fact_aos_vw.subplan_maj3
LEFT JOIN hercules_workday.dim_person dim_person_stdnt
   ON sfa.emplid = dim_person_stdnt.emplid
--LEFT JOIN hercules_workday.dim_person advisor
--   ON advisor.emplid = dim_person_stdnt.advisor_id
LEFT JOIN hercules_workday.dim_address_cascade dim_address_cascade_vw
   ON sfa.emplid = dim_address_cascade_vw.emplid
  AND dim_address_cascade_vw.address_type_cd = 'H'
LEFT JOIN hercules_workday.dim_address dim_address_perm
   ON sfa.emplid = dim_address_perm.emplid
  AND dim_address_perm.address_type_cd = 'PERM'
  AND dim_address_perm.active_flag = 'Y'
  AND dim_address_perm.emplid <> '*' 
LEFT JOIN perseus_workday.cs_admission adm
   ON sfa.emplid = adm.emplid
  AND sfa.acad_career = adm.acad_career
  AND dim_term_vw.rel_Strm >= adm.eff_strm 
  and dim_term_vw.rel_Strm  < adm.next_eff_strm
LEFT JOIN perseus_workday.cs_degrees deg
   ON sfa.emplid = deg.emplid
  AND sfa.acad_career = deg.acad_career
LEFT JOIN hercules_workday.dim_car_prgrm_plan dim_car_prgrm_major_vw
   ON fact_aos_vw.acad_plan_maj = dim_car_prgrm_major_vw.acad_plan_cd
  AND fact_aos_vw.acad_plan_maj_type_cd = dim_car_prgrm_major_vw.acad_plan_type_cd
LEFT JOIN hercules_workday.dim_isir dim_isir
   ON dim_isir.emplid = sfa.emplid
  AND dim_isir.aid_year = sfa.aid_year
LEFT JOIN perseus_workday.cs_retention_rel rel_ret
   ON sfa.disb_strm = rel_ret.strm
  AND sfa.emplid = rel_ret.emplid
  AND sfa.acad_career = rel_ret.acad_career
LEFT JOIN perseus_workday.cs_retention ret
   ON sfa.emplid = ret.emplid
  AND sfa.acad_career = ret.acad_career
  AND sfa.disb_strm = ret.strm
LEFT JOIN perseus_workday.cs_persistence persis
   ON sfa.disb_strm = persis.strm
  AND sfa.emplid = persis.emplid
  AND sfa.acad_career = persis.acad_career
LEFT JOIN hercules_workday.dim_term grad_dim_term_vw
   ON deg.completion_term = grad_dim_term_vw.strm
LEFT JOIN edwutil.perseus_lookup fed_fa_source
   ON fed_fa_source.fieldname = 'FEDERAL_FA_SOURCE'
LEFT JOIN edwutil.perseus_lookup inst_fa_source
   ON inst_fa_source.fieldname = 'INSTITUTIONAL_FA_SOURCE'
LEFT JOIN edwutil.perseus_lookup priv_fa_source
   ON priv_fa_source.fieldname = 'PRIVATE_FA_SOURCE'
LEFT JOIN edwutil.perseus_lookup oth_fa_source
   ON oth_fa_source.fieldname = 'OTHER_FA_SOURCE'
LEFT JOIN edwutil.perseus_lookup state_fa_source
   ON state_fa_source.fieldname = 'STATE_FA_SOURCE'
LEFT JOIN edwutil.perseus_lookup grant_fa_type
   ON grant_fa_type.fieldname = 'GRANT_FA_TYPE'
   AND grant_fa_type.fieldvalue = fa.finaid_type
LEFT JOIN edwutil.perseus_lookup scholarship_fa_type
   ON scholarship_fa_type.fieldname = 'SCHOLARSHIP_FA_TYPE'
   AND scholarship_fa_type.fieldvalue = fa.finaid_type
LEFT JOIN edwutil.perseus_lookup loan_fa_type
   ON loan_fa_type.fieldname = 'LOAN_FA_TYPE'
   AND loan_fa_type.fieldvalue = case when fa.finaid_cat = 'MISCL' then 'LOAN' else sfa.fin_aid_type end
LEFT JOIN edwutil.perseus_lookup workstudy_fa_type
   ON workstudy_fa_type.fieldname = 'WORKSTUDY_FA_TYPE'
   AND workstudy_fa_type.fieldvalue = case when fa.finaid_cat = 'MISCL' then 'LOAN' else sfa.fin_aid_type end
LEFT JOIN edwutil.perseus_lookup waiver_fa_type
   ON waiver_fa_type.fieldname = 'WAIVER_FA_TYPE'
   AND waiver_fa_type.fieldvalue = fa.finaid_type
LEFT JOIN edwutil.perseus_lookup fellowship_fa_type
   ON fellowship_fa_type.fieldname = 'FELLOWSHIP_FA_TYPE'
   AND fellowship_fa_type.fieldvalue = fa.finaid_type
LEFT JOIN edwutil.perseus_lookup va_benefits_fa_type
   ON va_benefits_fa_type.fieldname = 'VA_BENEFITS_FA_TYPE'
   AND va_benefits_fa_type.fieldvalue = fa.finaid_type
;
COMMIT;
ANALYZE perseus_workday.financial_aid_extract;
