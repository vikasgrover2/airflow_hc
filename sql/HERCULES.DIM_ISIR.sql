
truncate table hercules_workday.dim_isir;

insert into hercules_workday.dim_isir 
(
 src_sys_id
,src_sys_cd
,src_sys_eff_dt
,src_sys_exp_dt
,eff_dt
,exp_dt
,emplid
,aid_year
,current_fa_app_date
,first_fa_app_date
,pell_eligibility
,adj_efc
,efc_status
,ssa_citizenshp_ind
,num_family_members
,number_in_college
,graduate_student
,married
,marital_stat
,dependents
,agi
,school_choice_1
,depndncy_stat
,citizenship_status
,children
,parent_marital_stat
,parent_number_in_family
,parent_num_in_college
,parent_agi
,fed_efc
,efc_status2
,total_income
,available_income
,descrtn_net_worth
,contrib_from_asset
,adj_available_inc
,total_par_contrib
,total_stu_contrib
,adj_par_contrib
,stu_total_inc
,contrib_avail_inc
,ifaf_p_tax_return_filed
,ifaf_p_father_inc
,ifaf_p_mother_inc
,ifaf_p_edu_credit
,ifaf_p_interest_income
,ifaf_p_ira_distrib
,ifaf_p_us_tax_pd
,ifaf_p_bus_net_worth
,ifaf_p_untaxed_pensions
,ifaf_p_other_untaxed_inc
,ires_par_calc_tax_stat
,ifaf_s_tax_return_filed
,ifaf_s_student_inc
,ifaf_spouse_inc
,ifaf_s_edu_credit
,ifaf_s_ira_paymts
,ifaf_s_interest_income
,ifaf_s_ira_distrib
,ifaf_s_child_sup_recv
,ifaf_s_us_tax_pd
,ifaf_s_bus_net_worth
,ifaf_s_untaxed_pensions
,ifaf_s_other_untaxed_inc
,ires_stu_calc_tax_stat
)
select distinct
'1' as src_sys_id
,it.student_reference_academic_person_id ||'~'||financial_aid_award_year_reference_financial_aid_award_year_id 		as src_sys_cd
      , date_trunc('day', CURRENT_TIMESTAMP) AS src_sys_eff_dt
      , date_trunc('day', CURRENT_TIMESTAMP)                                  AS src_sys_exp_dt
      , date_trunc('day', CURRENT_TIMESTAMP)                    AS eff_dt
      , date_trunc('day', CURRENT_TIMESTAMP)                                  AS exp_dt
,it.student_reference_academic_person_id		as emplid
,it.financial_aid_award_year_reference_financial_aid_award_year_id		as aid_year
,it.date_application_completed		as current_fa_app_date
,it.application_receipt_date		as first_fa_app_date
,it.pell_grant_eligibility_flag		as pell_eligibility
,it.primary_efc		as adj_efc
,it.expected_family_contribution_status_reference_expected_family_contribution_status_id		as efc_status
,it.ssa_citizenship_flag_reference_isir_ssa_citizenship_flag_id		as ssa_citizenshp_ind
,it.assumed_student_number_in_family		as num_family_members
,it.assumed_student_number_in_college		as number_in_college
,it.graduate_flag		as graduate_student
,it.assumed_student_marital_status_reference_student_marital_status_id		as married
,it.assumed_student_marital_status_reference_wid		as marital_stat
,it.assumed_have_other_legal_dependents_reference_wid		as dependents
,it.assumed_student_agi		as agi
,it.federal_school_code_reference_federal_school_code_rule_set_id		as school_choice_1
,it.student_dependency_status_reference_financial_aid_dependency_status_id		as depndncy_stat
,it.assumed_citizenship_reference_wid		as citizenship_status
,it.assumed_have_children_you_support_reference_wid		as children
,it.assumed_parents_marital_status_reference_financial_aid_marital_status_id		as parent_marital_stat
,it.assumed_parents_number_in_family		as parent_number_in_family
,it.assumed_parents_number_in_college		as parent_num_in_college
,it.assumed_parents_agi		as parent_agi
,it.secondary_efc		as fed_efc
,it.signature_reject_efc_calculated		as efc_status2
,it.ti_total_income		as total_income
,it.ai_available_income		as available_income
,it.sdnw_student_discretionary_net_worth		as descrtn_net_worth
,it.sca_student_contribution_from_assets		as contrib_from_asset
,it.aai_adjusted_available_income		as adj_available_inc
,it.tpc_total_parents_contribution		as total_par_contrib
,it.tsc_total_student_contribution		as total_stu_contrib
,it.pc_parents_contribution		as adj_par_contrib
,it.sec_sti_secondary_student_total_income		as stu_total_inc
,it.sec_cai_secondary_contribution_from_available_income		as contrib_avail_inc
,it.parent_irs_type_of_tax_return_data_field_flag_reference_wid		as ifaf_p_tax_return_filed
,it.assumed_parent_1_income_earned_from_work		as ifaf_p_father_inc
,it.assumed_parent_2_income_earned_from_work		as ifaf_p_mother_inc
,it.parent_irs_education_credits_data_field_flag_reference_wid		as ifaf_p_edu_credit
,it.parent_irs_interest_income_data_field_flag_reference_wid		as ifaf_p_interest_income
,it.parent_irs_ira_distributions_data_field_flag_reference_wid		as ifaf_p_ira_distrib
,it.assumed_parents_us_tax_paid		as ifaf_p_us_tax_pd
,it.nw_net_worth		as ifaf_p_bus_net_worth
,it.parent_irs_untaxed_pensions_data_field_flag_reference_wid		as ifaf_p_untaxed_pensions
,it.parents_untaxed_income_total_calculated_by_cps		as ifaf_p_other_untaxed_inc
,it.parent_irs_type_of_tax_return_data_field_flag_reference_wid		as ires_par_calc_tax_stat
,it.student_irs_tax_return_filing_status_data_field_flag_reference_wid		as ifaf_s_tax_return_filed
,it.assumed_student_income_from_work		as ifaf_s_student_inc
,it.assumed_spouse_income_from_work		as ifaf_spouse_inc
,it.student_irs_education_credits_data_field_flag_reference_wid		as ifaf_s_edu_credit
,it.student_irs_ira_payments_data_field_flag_reference_wid		as ifaf_s_ira_paymts
,it.student_irs_interest_income_data_field_flag_reference_wid		as ifaf_s_interest_income
,it.student_irs_ira_distributions_data_field_flag_reference_wid		as ifaf_s_ira_distrib
,it.assumed_have_children_you_support_reference_isir_response_id		as ifaf_s_child_sup_recv
,it.assumed_student_us_tax_paid		as ifaf_s_us_tax_pd
,it.sec_nw_secondary_net_worth		as ifaf_s_bus_net_worth
,it.student_irs_untaxed_pensions_data_field_flag_reference_wid		as ifaf_s_untaxed_pensions
,it.student_untaxed_income_total_calculated_by_cps		as ifaf_s_other_untaxed_inc
,it.student_irs_type_of_tax_return_data_field_flag_reference_wid		as ires_stu_calc_tax_stat
from workday_ods.isirtransactions it
where 1=1
;

commit;

anaylze hercules_workday.dim_isir;