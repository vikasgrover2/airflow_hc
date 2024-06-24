/*##########################################
Object type       : PERSEUS EXTRACT TABLE
Description       :
Grain             :
Target table name : COMPLETIONS_EXTRACT
Source table list : CS_ADMISSION (PERSEUS)
                    CS_ENROLLMENTS (PERSEUS)
                    CS_FINANCIAL_AID (PERSEUS)
                    CS_RETENTION (PERSEUS)
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
                    FACT_DEGREE
                    FACT_PI_API
                    FACT_TESTSCORE
                    FF_DEGREE
                    PERSEUS_LOOKUP (EDWUTIL)
                    STUDENT_TERM_EXTRACT (PERSEUS)
ETL load type     : FULL LOAD EVERYDAY
Date created      : 01-MARCH-2019
############################################*/

TRUNCATE TABLE perseus_workday.completions_extract;

INSERT INTO perseus_workday.completions_extract(
    extract_key
    ,acad_stndng_actn_cd
    ,acad_stndng_actn_ld
    ,term_career_unt_passd_gpa
    ,term_career_unt_passd_nogpa
    ,term_career_unt_taken_nogpa
    ,term_career_unt_taken_gpa
    ,stdnt_cntct_address1
    ,stdnt_cntct_address2
    ,stdnt_cntct_address_type
    ,stdnt_cntct_country_ld
    ,stdnt_cntct_county
    ,stdnt_cntct_city
    ,stdnt_cntct_zipcode
    ,stdnt_cntct_state_cd
    ,stdnt_cntct_state_ld
    ,stdnt_cntct_postal
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
    ,plan_prog_status_cd
    ,plan_degree_type
    ,plan_institution_cd
    ,plan_institution_ld
    ,acad_career
    ,acad_career_collapsed_cd
    ,acad_prog
    ,application_date
    ,career_tot_cumulative
    ,career_tot_test_credit
    ,career_tot_trnsfr
    ,career_unt_passd_gpa
    ,career_unt_passd_nogpa
    ,completion_term
    ,degr_chkout_stat_cd
    ,degr_chkout_stat_sd
    ,degr_confer_dt_key
    ,degr_status_dt_key
    ,degree_gpa
    ,stdnt_emplid
    ,time_calendar_year
    ,time_completion_collapsed_strm
    ,time_completion_collapsed_term_year_ld
    ,time_fiscal_year
    ,term_begin_day_key
    ,term_end_day_key
    ,term_seq_acad_yr
    ,term_seq_cal_yr
    ,time_completion_term
    ,time_completion_term_type
    ,time_completion_term_year
    ,time_academic_year
    ,time_academic_year_range
    ,full_part_time_cd
    ,full_part_time_ld
    ,honors_cd
    ,honors_ld
    ,ipeds_attend_type_cd
    ,ipeds_attend_type_ld
    ,ipeds_career_ind
    ,stdnt_instr_mode_ld
    ,stdnt_instr_mode_sd
    ,ipeds_taken_credits
    ,ipeds_total_classes
    ,bhe_residency_cd
    ,bhe_residency_ld
    ,prog_status_cd
    ,prog_status_ld
    ,stdnt_campus_cd
    ,stdnt_campus_ld
    ,tuition_residency_cd
    ,stdnt_tuition_residency
    ,univ_attend_type_cd
    ,univ_attend_type_ld
    ,stdnt_career_gpa_cum
    ,birthdate_day
    ,first_name
    ,gender_ld
    ,ipeds_race_ethn_citz_ld
    ,last_name
    ,married_status_ld
    ,bhe_birth_year
    ,name_first_last
    ,name_last_first
    ,career_gpa_cum
    ,hs_final_gpa
    ,hs_final_gpa_excl_last_strm
    ,hs_last_year
    ,hs_start_year
    ,hs_years_attended
    ,hs_name
    ,hs_short_name
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
    ,campus_latitude
    ,campus_longitude
    ,tot_strms
    ,tot_years
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
    ,scholarship_disbursed_amount
    ,vabenefits_disbursed_amount
    ,loan_disbursed_amount
    ,grant_disbursed_amount
    ,workstudy_disbursed_amount
    ,waiver_disbursed_amount
    ,fellowship_disbursed_amount
    ,other_disbursed_amount
    ,disbursed_amount
    ,fa_type
    ,aid_year
    ,req_term
    ,accepted_dt
    ,stdnt_admit_type_cd
    ,stdnt_admit_type_ld
    ,admit_term
    ,app_dt
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
    ,major_start_dt
    ,career_start_dt
    ,ret_first_strm
    ,plan_hegis
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
    ,stdnt_grp_first_gen
    ,urm_status
    ,degree_year
    ,cip2000_cd
    ,cip2000_ld
    ,acad_level_bot_ld
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
    ,gpa_plan
    ,degree_number
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
    ,perseus_load_date
    ,freeze_strm
    , school_group 
    , stdnt_type_cd 
    , stdnt_type_ld 
    , stdnt_status_cd 
    , stdnt_status_ld 
    , acad_career_collapsed_ld
    , stdnt_type_collapsed_ld
    , stdnt_type_collapsed_cd
    , area_of_study_acad_plan_ld
    , area_of_study_acad_plan_cd
    , area_of_study_degree_cd
    , area_of_study_degree_ld
    , area_of_study_acad_prog_ld
    , area_of_study_acad_prog_cd
    , area_of_study_school_ld
    , area_of_study_school_cd
    , area_of_study_specialization_ld
    , area_of_study_specialization_cd
    , area_of_study_education_level_ld
    , area_of_study_department_ld
    , area_of_study_department_cd
    , area_of_study_acad_plan_type_ld
    , area_of_study_plan_term_rank
    , area_of_study_acad_plan_type_cd
    , plan_acad_plan_code
	, location_collapsed_cd
	, location_collapsed_ld
	, time_term_desc
    , perseus_refresh_date
    , perseus_load_time  
    , aid_yr_range
    )
SELECT
    fact_degree.src_sys_cd as extract_key
    ,student_term_extract.acad_stndng_actn_cd
    ,student_term_extract.acad_stndng_actn_ld
    ,student_term_extract.career_unt_passd_gpa AS term_career_unt_passd_gpa
    ,student_term_extract.career_unt_passd_nogpa AS term_career_unt_passd_nogpa
    ,student_term_extract.career_unt_taken_nogpa AS term_career_unt_taken_nogpa
    ,student_term_extract.career_unt_taken_gpa AS term_career_unt_taken_gpa
    ,dim_address_cascade_vw.address1 AS stdnt_cntct_address1
    ,dim_address_cascade_vw.address2 AS stdnt_cntct_address2
    ,dim_address_cascade_vw.address_type_cd AS stdnt_cntct_address_type
    ,dim_address_cascade_vw.country_ld AS stdnt_cntct_country_ld
    ,dim_address_cascade_vw.geog_county_ld AS stdnt_cntct_county
    ,dim_address_cascade_vw.city AS stdnt_cntct_city
    ,"SUBSTRING" (dim_address_cascade_vw.postal,1,5) AS stdnt_cntct_zipcode
    ,dim_address_cascade_vw.state_cd AS stdnt_cntct_state_cd
    ,dim_address_cascade_vw.state_ld AS stdnt_cntct_state_ld
    ,dim_address_cascade_vw.postal AS stdnt_cntct_postal
    ,enrollment_address_vw.address1 AS enrollment_address1
    ,enrollment_address_vw.address2 AS enrollment_address2
    ,enrollment_address_vw.address_type_cd AS enrollment_address_type
    ,enrollment_address_vw.city AS enrollment_address_city
    ,enrollment_address_vw.country_ld AS enrollment_address_country_ld
    ,enrollment_address_vw.geog_county_ld AS enrollment_address_county
    ,enrollment_address_vw.postal AS enrollment_address_postal
    ,enrollment_address_vw.state_cd AS enrollment_address_state_cd
    ,enrollment_address_vw.state_ld AS enrollment_address_state_ld
    ,"SUBSTRING" (enrollment_address_vw.postal,1,5) AS enrollment_address_zipcode
    ,enrollment_zip.postal_loc_y AS enrollment_address_latitude
    ,enrollment_zip.postal_loc_x AS enrollment_address_longitude
    ,fact_degree.prog_status_cd AS plan_prog_status_cd
    ,dim_degree.education_level_ld AS plan_degree_type
    ,dim_program.institution_cd AS plan_institution_cd
    ,dim_program.institution_ld AS plan_institution_ld
    ,fact_degree.acad_career AS acad_career
    ,dim_career.collapsed_acad_career_cd as acad_career_collapsed_cd
    ,fact_degree.acad_prog
    ,fact_degree.application_date
    ,fact_degree.career_tot_cumulative
    ,fact_degree.career_tot_test_credit
    ,fact_degree.career_tot_trnsfr
    ,fact_degree.career_unt_passd_gpa
    ,fact_degree.career_unt_passd_nogpa
    ,fact_degree.completion_term
    ,fact_degree.degr_chkout_stat_cd
    ,fact_degree.degr_chkout_stat_sd
    ,TO_DATE
      (CASE
          WHEN fact_degree.degr_confer_dt_key <> 0 THEN fact_degree.degr_confer_dt_key
          ELSE NULL
        END,'J') AS degr_confer_dt_key
    ,TO_DATE
      (CASE
          WHEN fact_degree.degr_status_dt_key <> 0 THEN fact_degree.degr_status_dt_key
          ELSE NULL
        END,'J') AS degr_status_dt_key
    ,fact_degree.degree_gpa
    ,fact_degree.emplid AS stdnt_emplid
    ,dim_term.calendar_yr AS time_calendar_year
    ,dim_term.collapsed_strm AS time_completion_collapsed_strm
    ,dim_term.collapsed_term_year_ld AS time_completion_collapsed_term_year_ld
    ,dim_term.fiscal_yr AS time_fiscal_year
    ,dim_term.term_begin_day_key
    ,dim_term.term_end_day_key
    ,dim_term.term_seq_acad_yr
    ,dim_term.term_seq_cal_yr
    ,dim_term.term_type_cd AS time_completion_term
    ,dim_term.term_type_ld AS time_completion_term_type
    ,dim_term.term_year_ld AS time_completion_term_year
    ,dim_term.academic_yr AS time_academic_year
    ,dim_term.academic_yr_range AS time_academic_year_range,                                                       
    student_term_extract.full_part_time_cd
    ,student_term_extract.full_part_time_ld
    ,fact_degree.honors_cd
    ,fact_degree.honors_ld
    ,student_term_extract.ipeds_attend_type_cd
    ,student_term_extract.ipeds_attend_type_ld
    ,student_term_extract.ipeds_career_ind
    ,student_term_extract.stdnt_instr_mode_ld
    ,student_term_extract.stdnt_instr_mode_sd
    ,fact_degree.ipeds_taken_credits
    ,fact_degree.ipeds_total_classes
    ,student_term_extract.bhe_residency_cd
    ,student_term_extract.bhe_residency_ld
    ,fact_degree.prog_status_cd
    ,fact_degree.prog_status_ld
    ,COALESCE(fact_degree.stdnt_campus_cd,department_group.decoded_value_ld,school_group.decoded_value_ld,student_term_extract.stdnt_campus_cd) as stdnt_campus_cd
    ,COALESCE(fact_degree.stdnt_campus_ld,department_group.decoded_value_ld,school_group.decoded_value_ld,student_term_extract.stdnt_campus_ld) as stdnt_campus_ld
    ,student_term_extract.tuition_residency_cd
    ,COALESCE (fact_degree.tuition_residency_ld,student_term_extract.tuition_residency_ld,'*') AS stdnt_tuition_residency
    ,student_term_extract.univ_attend_type_cd
    ,student_term_extract.univ_attend_type_sd AS univ_attend_type_ld
    ,student_term_extract.career_gpa_cum AS stdnt_career_gpa_cum
    ,to_date(dim_person.birthdate_day,'J') AS birthdate_day
    ,dim_person.first_name
    ,dim_person.gender_ld
    ,dim_person.ipeds_race_ethn_citz_ld
    ,dim_person.last_name
    ,dim_person.married_status_ld
    ,dim_person.bhe_birth_year
    ,dim_person.name_first_last
    ,dim_person.name_last_first
    ,student_term_extract.career_gpa_cum
    ,hs.pi_gpa AS hs_final_gpa
    ,hs.pi_gpa AS hs_final_gpa_excl_last_strm
    ,TO_CHAR (hs.pi_end_Dt,'YYYY') AS hs_last_year
    ,TO_CHAR (hs.pi_start_dt,'YYYY') AS hs_start_year
    ,TO_CHAR (hs.pi_end_dt,'YYYY') - TO_CHAR (hs.pi_start_dt,'YYYY') AS hs_years_attended
    ,hs.descr AS hs_name
    ,hs.descr AS hs_short_name
    ,hs.address1 AS hs_address1
    ,hs.address2 AS hs_address2
    ,hs.city AS hs_city
    ,hs.county AS hs_county
    ,hs.state AS hs_state
    ,hs.postal AS hs_postal
    ,hs.country AS hs_country
    ,hs_zip_code.postal_loc_y AS hs_lat
    ,hs_zip_code.postal_loc_x AS hs_long
    ,dim_tests.first_act_score AS test_first_act_score
    ,dim_tests.last_act_score AS test_last_act_score
    ,dim_tests.max_act_score AS test_max_act_score
    ,dim_tests.avg_act_score AS test_avg_act_score
    ,dim_tests.num_act_attempts AS test_num_act_attempts
    ,dim_tests.first_sat_score AS test_first_sat_score
    ,dim_tests.last_sat_score AS test_last_sat_score
    ,dim_tests.max_sat_score AS test_max_sat_score
    ,dim_tests.avg_sat_score AS test_avg_sat_score
    ,dim_tests.num_sat_attempts AS test_num_sat_attempts
    ,geo_zip.postal_loc_y AS stdnt_cntct_latitude
    ,geo_zip.postal_loc_x AS stdnt_cntct_longitude
    ,campus_zip.postal_loc_y AS campus_latitude
    ,campus_zip.postal_loc_x AS campus_longitude
    ,enrl.tot_strms
    ,enrl.tot_years
    ,enrl.tot_courses_taken
    ,enrl.tot_courses_withdrawn
    ,enrl.tot_unt_billing
    ,student_term_extract.tot_unt_earned
    ,enrl.tot_grade_points
    ,enrl.tot_grade_points_per_unit
    ,enrl.tot_term_end_dt
    ,enrl.tot_term_begin_dt
    ,null as tot_first_course_id
    ,null as tot_first_crse_offer_nbr
    ,null as tot_first_session_cd
    ,null as tot_first_class_section
    ,null as tot_first_class_grade_point
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
    ,fin_aid.scholarship_disbursed_amount
    ,fin_aid.vabenefits_disbursed_amount
    ,fin_aid.loan_disbursed_amount
    ,fin_aid.grant_disbursed_amount
    ,fin_aid.workstudy_disbursed_amount
    ,fin_aid.waiver_disbursed_amount
    ,fin_aid.fellowship_disbursed_amount
    ,fin_aid.other_disbursed_amount
    ,fin_aid.disbursed_amount
    ,dim_term.aid_yr as aid_year
    ,fin_aid.fa_type
    ,adm.req_term
    ,adm.accepted_dt
    ,adm.stdnt_admit_type_cd
    ,adm.stdnt_admit_type_ld
    ,adm.admit_term
    ,adm.app_dt
    ,ret.retention_year_1_flag
    ,ret.retention_year_1_status
    ,ret.retention_year_2_flag
    ,ret.retention_year_2_status
    ,ret.retention_year_3_flag
    ,ret.retention_year_3_status
    ,ret.retention_year_4_flag
    ,ret.retention_year_4_status
    ,ret.retention_year_5_flag
    ,ret.retention_year_5_status
    ,ret.retention_year_6_flag
    ,ret.retention_year_6_status
    ,ret.retention_year_7_flag
    ,ret.retention_year_7_status
    ,ret.retention_year_8_flag
    ,ret.retention_year_8_status
    ,ret.retention_year_9_flag
    ,ret.retention_year_9_status
    ,ret.retention_year_10_flag
    ,ret.retention_year_10_status
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
    ,dim_isir.current_fa_app_date AS isir_current_fa_app_date
    ,dim_isir.first_fa_app_date AS isir_first_fa_app_date
    ,dim_isir.pell_eligibility AS isir_pell_eligibility
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
    ,enrl.courses_taken
    ,enrl.unt_billing
    ,enrl.unt_earned
    ,enrl.grade_points
    ,enrl.grade_points_per_unit
    ,enrl.term_end_dt
    ,enrl.term_begin_dt
    ,null as first_course_id
    ,null as first_crse_offer_nbr
    ,null as first_session_cd
    ,null as first_class_section
    ,enrl.first_class_grade_point
    ,enrl.time_min_enrl_drop_day_key
    ,enrl.courses_withdrawn
    ,student_term_extract.major_start_dt
    ,student_term_extract.career_start_dt
    ,ret.strm AS ret_first_strm
    ,dim_program.bhe_api_hegis_ld AS plan_hegis
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
    ,p_home.phone_email AS stdnt_home_phone
    ,p_cell.phone_email AS stdnt_cell_phone
    ,student_term_extract.advisor_emplid as advisor_id
    ,student_term_extract.advisor_name_first_last as advisor_name
    ,e_home.phone_email AS stdnt_home_email
    ,e_bus.phone_email AS stdnt_business_email
    ,e_camp.phone_email AS stdnt_campus_email
    ,0 AS stdnt_grp_first_gen
    ,dim_person.urm_status AS urm_status
    --,{degree_year} as degree_year
    ,dim_term.degree_yr as degree_year
    ,dim_program.acad_prog_cip_cd AS cip2000_cd
    ,dim_program.acad_prog_cip_cd AS cip2000_ld
    ,student_term_extract.acad_level_bot_ld AS acad_level_bot_ld
    ,0 AS honors_acad_prog_ind
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
    ,student_term_extract.career_gpa_cum AS gpa_plan
    ,fact_degree.stdnt_degr AS degree_number
    ,student_term_extract.trn_transfer_inst_name_and_location
    ,student_term_extract.hs_inst_name_and_location
    ,student_term_extract.orig_address_term_src_sys_cd
    ,student_term_extract.orig_home_address1
    ,student_term_extract.orig_home_address2
    ,student_term_extract.orig_home_address_type as orig_home_address_type
    ,student_term_extract.orig_home_city
    ,student_term_extract.orig_home_country_ld
    ,student_term_extract.orig_home_geog_county_ld
    ,student_term_extract.orig_home_postal
    ,student_term_extract.orig_home_state_cd
    ,student_term_extract.orig_home_state_ld
    ,student_term_extract.orig_home_postal_loc_y
    ,student_term_extract.orig_home_postal_loc_x
    ,trunc(sysdate)-1    as perseus_load_date
    ,dim_term.strm freeze_strm
    
    , COALESCE(department_group.decoded_value_ld,school_group.decoded_value_ld) as school_group 
    , student_term_extract.stdnt_type_cd
    , student_term_extract.stdnt_type_ld 
    , student_term_extract.stdnt_status_cd 
    , student_term_extract.stdnt_status_ld 
    , null as acad_career_collapsed_ld
    , student_term_extract.stdnt_type_collapsed_ld  as stdnt_type_collapsed_ld
    , student_term_extract.stdnt_type_collapsed_cd  as stdnt_type_collapsed_cd
    
    ,dim_plan.acad_plan_ld                             as area_of_study_acad_plan_ld
    ,fact_degree.car_prg_plan_maj                      as area_of_study_acad_plan_cd
    ,fact_degree.degree_cd                            as area_of_study_degree_cd
    ,dim_degree.degree_ld                            as area_of_study_degree_ld
    ,dim_program.acad_prog_ld                         as area_of_study_acad_prog_ld
    ,fact_degree.acad_prog                             as area_of_study_acad_prog_cd
    ,dim_program.school_ld                             as area_of_study_school_ld
    ,dim_program.school_cd                             as area_of_study_school_cd
    ,NULL                                             as area_of_study_specialization_ld
    ,NULL                                             as area_of_study_specialization_cd
    ,dim_degree.education_level_ld                     as area_of_study_education_level_ld
    ,dim_program.department_ld                         as area_of_study_department_ld
    ,dim_program.department_cd                         as area_of_study_department_cd
    ,dim_plan.plan_type_ld                             as area_of_study_acad_plan_type_ld
    ,fact_degree.acad_plan_seq_no                     as area_of_study_plan_term_rank
    ,fact_degree.acad_plan_type_cd                     as area_of_study_acad_plan_type_cd
    ,fact_degree.car_prg_plan_maj                       as plan_acad_plan_code
	, dim_location_campus.location_collapsed_cd
	, dim_location_campus.location_collapsed_ld
	,dim_term.term_desc as time_term_desc    
    ,trunc(sysdate) as  perseus_refresh_date
    ,sysdate-1 perseus_load_time
    ,(fact_degree.degree_year::int-1)||'-'||fact_degree.degree_year as aid_yr_range

FROM  hercules_workday.fact_degree fact_degree

left join   hercules_workday.dim_program
    on      fact_degree.acad_prog       = dim_program.acad_prog_cd

left join   hercules_workday.dim_plan
    on   fact_degree.car_prg_plan_maj  = dim_plan.acad_plan_cd
    and  fact_degree.acad_plan_type_cd = dim_plan.plan_type_cd

left join   hercules_workday.dim_degree
    on      fact_degree.degree_cd       = dim_degree.degree_cd


LEFT JOIN hercules_workday.dim_car_prgrm_plan dim_car_prgrm_plan_trans 
ON         fact_degree.acad_prog                         = dim_car_prgrm_plan_trans.acad_prog_Cd
    AND fact_degree.car_prg_plan_maj                     = dim_car_prgrm_plan_trans.acad_plan_cd
    AND fact_degree.acad_plan_type_cd                    = dim_car_prgrm_plan_trans.acad_plan_type_cd

left join hercules_workday.dim_career
    on dim_career.acad_career_cd = fact_degree.acad_career
  
LEFT JOIN hercules_workday.dim_person dim_person
   ON fact_degree.emplid = dim_person.emplid
   
LEFT JOIN hercules_workday.dim_address_cascade dim_address_cascade_vw
   ON dim_person.emplid = dim_address_cascade_vw.emplid

LEFT JOIN hercules_workday.dim_term dim_term
   ON fact_degree.completion_term = dim_term.strm
  
LEFT JOIN perseus_workday.student_term_extract student_term_extract
    ON  fact_degree.acad_career     = student_term_extract.acad_career
    AND fact_degree.emplid          = student_term_extract.emplid
    AND fact_degree.term			= student_term_extract.strm
  
LEFT JOIN hercules_workday.dim_address enrollment_address_vw
   ON student_term_extract.address_term_src_sys_cd = enrollment_address_vw.src_sys_cd

LEFT JOIN edwutil.dim_oth_raw_zip_data_t enrollment_zip
   ON LPAD ("LEFT" (enrollment_address_vw.postal,5),5,'0') = enrollment_zip.postal

LEFT JOIN hercules_workday.fact_pi_api hs
   ON fact_degree.completion_term = hs.strm
  AND fact_degree.acad_career = hs.acad_career_cd
  AND fact_degree.emplid = hs.emplid
  AND hs.pi_lpi_flag = 'Y'
  AND hs.ext_career = 'HS'

LEFT JOIN edwutil.dim_oth_raw_zip_data_t hs_zip_code
   ON "LEFT" (hs.postal,5) = hs_zip_code.postal

LEFT JOIN perseus_workday.cs_financial_aid fin_aid
   ON fact_degree.emplid = fin_aid.emplid
  AND dim_term.aid_yr = fin_aid.aid_year
  
LEFT JOIN hercules_workday.dim_campus dim_campus
   ON fact_degree.stdnt_campus_cd = dim_campus.campus_cd

LEFT JOIN edwutil.masterlookup school_group     
ON school_group.target_column_name = 'SCHOOL_CAMPUS'           
and school_group.sys_value = dim_program.school_cd

LEFT JOIN edwutil.masterlookup department_group 
ON department_group.target_column_name = 'DEPARTMENT_CAMPUS' 
and department_group.sys_value    = dim_program.department_cd

LEFT JOIN hercules_workday.dim_location dim_location_campus
   ON COALESCE(fact_degree.stdnt_campus_cd,department_group.decoded_value_ld,school_group.decoded_value_ld,student_term_extract.stdnt_campus_cd) = dim_location_campus.src_sys_cd

LEFT JOIN edwutil.dim_oth_raw_zip_data_t campus_zip
   ON "LEFT" (dim_location_campus.postal,5) = campus_zip.postal

LEFT JOIN edwutil.dim_oth_raw_zip_data_t geo_zip
   ON LPAD ("LEFT" (dim_address_cascade_vw.postal,5),5,'0') = geo_zip.postal

LEFT JOIN hercules_workday.fact_testscore dim_tests
   ON fact_degree.emplid = dim_tests.emplid

LEFT JOIN hercules_workday.dim_isir as dim_isir 
    ON student_term_extract.emplid = dim_isir.emplid
    AND dim_isir.aid_year = dim_term.aid_yr 

LEFT JOIN perseus_workday.cs_admission adm
    ON  fact_degree.emplid          =  adm.emplid
    AND fact_degree.acad_career     =  adm.acad_career
    AND dim_term.rel_strm		 	>= adm.eff_strm
    AND dim_term.rel_strm		 	<  adm.next_eff_strm

LEFT JOIN perseus_workday.cs_enrollments enrl
   ON student_term_extract.emplid 		= enrl.emplid
  AND student_term_extract.strm 		= enrl.strm
  AND student_term_extract.acad_career 	= enrl.acad_career
  
LEFT JOIN perseus_workday.cs_retention ret
   ON student_term_extract.emplid = ret.emplid
  AND student_term_extract.acad_career = ret.acad_career


LEFT JOIN edwutil.masterlookup acad_career_filter
   ON acad_career_filter.target_column_name = 'ACAD_CAREER_FILTER'
  AND acad_career_filter.sys_value = fact_degree.acad_career
  AND acad_career_filter.decoded_value_sd = '0'

left join   hercules_workday.dim_person_phone_email e_bus
    on      e_bus.emplid                = fact_degree.emplid 
    and     e_bus.phone_email_type_cd   = (select sys_code from edwutil.masterlookup where target_column_name = 'email_business' )
    
left join   hercules_workday.dim_person_phone_email e_camp
    on      e_camp.emplid               = fact_degree.emplid 
    and     e_camp.phone_email_type_cd  = (select sys_code from edwutil.masterlookup where target_column_name = 'email_campus' )
    
left join   hercules_workday.dim_person_phone_email e_home
    on      e_home.emplid               = fact_degree.emplid 
    and     e_home.phone_email_type_cd  = (select sys_code from edwutil.masterlookup where target_column_name = 'email_home' )
    
left join   hercules_workday.dim_person_phone_email p_bus
    on      p_bus.emplid                = fact_degree.emplid 
    and     p_bus.phone_email_type_cd   = (select sys_code from edwutil.masterlookup where target_column_name = 'phone_business' )
    
left join   hercules_workday.dim_person_phone_email p_cell
    on      p_cell.emplid               = fact_degree.emplid 
    and     p_cell.phone_email_type_cd  = (select sys_code from edwutil.masterlookup where target_column_name = 'phone_cell' )
    
left join   hercules_workday.dim_person_phone_email p_dorm
    on      p_dorm.emplid               = fact_degree.emplid 
    and     p_dorm.phone_email_type_cd  = (select sys_code from edwutil.masterlookup where target_column_name = 'phone_dorm' )
    
left join   hercules_workday.dim_person_phone_email p_home
    on      p_home.emplid               = fact_degree.emplid 
    and     p_home.phone_email_type_cd  = (select sys_code from edwutil.masterlookup where target_column_name = 'phone_home' )
    
left join   hercules_workday.dim_person_phone_email p_loc
    on      p_loc.emplid                = fact_degree.emplid 
    and     p_loc.phone_email_type_cd   = (select sys_code from edwutil.masterlookup where target_column_name = 'phone_local' )

WHERE 1 = 1
--{completion_filter}
;

COMMIT;
ANALYZE perseus_workday.completions_extract;