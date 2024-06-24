truncate table hercules_workday.ff_applications_prog;

insert into hercules_workday.ff_applications_prog (
		emplid
		,acad_career
		,acad_prog
		,adm_appl_nbr
		,stdnt_car_nbr
		,prog_status_cd
		,prog_status_ld
		,prog_action_cd
		,prog_action_ld
		,effdt
		,effseq
		,action_dt
		,admit_term
		,req_term
		,prog_reason_cd
		,prog_reason_ld
		,residency
		,tuition_res
		,residency_ld
		,tuition_res_ld
		,stdnt_campus_ld
		,acad_plan_maj
		,prog_stage_cd
		,prog_stage_dt
		,rn
		,paid_flag
		,subplan_maj1
		,subplan_maj2
		,subplan_maj3
		,subplan_min1
		,subplan_min2
		,subplan_min3
		,acad_plan_maj_type_cd
		,acad_plan_min_type_cd
		,app_stage
		,appl_prog_nbr
		,adm_appl_nbr_reverse_seq
		,department_cd
		,last_applied_stage
		,appl_current_location
		,first_gen_flag
		,appl_rating
		,source_category
		,source_summary
		,source_memo
		,application_slate_id
		,workday_universal_id
		,workday_student_recruitment_id
		,workday_application_id
		,slate_external_id
		,gender
		,dem_gender
		,stdnt_first_gen_flag
		,eps_market
)
SELECT 
	substring(nvl(applications_prog.slate_reference_id,'*'),1,11)       as emplid
    ,'Undergraduate'                                                as acad_career
    ,applications_prog.applied_or_approved_major                    as acad_prog     
    ,NVL(applications_prog.workday_application_id, '*')                  as adm_appl_nbr  
    ,nvl(applications_prog.workday_application_id, '*')                  as stdnt_car_nbr
    ,'AP'                                                           as prog_status_cd
    ,'Applicant'                                                    as prog_status_ld
    ,NULL                                                           as prog_action_cd
    ,NULL                                                           as prog_action_ld
    ,applications_prog.app_date                             as effdt     
    ,1 :: int                                                             as effseq
    ,applications_prog.app_date::date                              as action_dt    
    ,nvl(applications_prog.app_term,dt1.collapsed_strm)             as admit_term 
    ,nvl(applications_prog.app_term,dt1.collapsed_strm)            as req_term 
    ,NULL                                                          as prog_reason_cd
    ,NULL                                                          as prog_reason_ld
    ,NULL                                                          as residency     
    ,NULL                                                          as tuition_res   
    ,NULL                                                          as residency_ld  
    ,NULL                                                          as tuition_res_ld
    ,null										                   as stdnt_campus_ld
    ,applications_prog.applied_or_approved_major                   as acad_plan_maj
    ,'AP'                                                          as prog_stage_cd
    ,applications_prog.app_date::date                             as prog_stage_dt
    ,1                                                             as rn
    ,NULL                                                          as paid_flag
    ,NULL                                                          as subplan_maj1
    ,NULL                                                          as subplan_maj2
    ,NULL                                                          as subplan_maj3
    ,NULL                                                          as subplan_min1
    ,NULL                                                          as subplan_min2
    ,NULL                                                          as subplan_min3
    ,NULL                                                         as acad_plan_maj_type_cd
    ,NULL                                                         as acad_plan_min_type_cd
    ,'APPLIED'                                                     as app_stage    
    ,NULL                                                          as appl_prog_nbr
    ,-2                                                            as adm_appl_nbr_reverse_seq
    ,NULL                                                          as department_cd
    ,NULL                                                          as last_applied_stage
    ,applications_prog.Campus_Loc_App                       as appl_current_location
    ,NULL                                                          as first_gen_flag  
    ,applications_prog.Decision_Rating                                 as appl_rating
    ,NULL                            							   as source_category
    ,NULL                           							   as source_summary
    ,NULL							                               as source_memo
	,applications_prog.application_slate_id                        as application_slate_id
	,applications_prog.workday_universal_id                         as workday_universal_id
	,applications_prog.workday_student_recruitment_id               as workday_student_recruitment_id
	,applications_prog.workday_application_id                       as workday_application_id     
	,applications_prog.slate_external_id							as slate_external_id
	,applications_prog.gender 										as gender
	,applications_prog.sex 											as dem_gender
	,applications_prog."first_gen_on_application?" 				    as stdnt_first_gen_flag
	,applications_prog.permanent_geomarket 						    as eps_market

FROM orion_ods.suffolk_uga_app_active_data applications_prog
 left join hercules_workday.dim_term dt1 on 
(CASE
        WHEN applications_prog.app_term LIKE '%Fall' THEN CONCAT('Fall ', left(applications_prog.app_term, 4))
        WHEN applications_prog.app_term LIKE '%Summer' THEN CONCAT('Summer ', left(applications_prog.app_term, 4))
        -- Add more cases for other term formats as needed
    end)
    = dt1.collapsed_strm and dt1.term_type_ld is not null
	
where app_date is not null

UNION ALL
--ADMITTED

SELECT 
    substring(nvl(applications_prog.slate_reference_id,'*'),1,11)       as emplid
    ,'Undergraduate'                                                as acad_career
    ,applications_prog.applied_or_approved_major                                      as acad_prog      
    ,NVL(applications_prog.workday_application_id, '*')                  as adm_appl_nbr  
    ,nvl(applications_prog.workday_application_id, '*')                  as stdnt_car_nbr
    ,'AD' as prog_status_cd
    ,'Admitted'                                                           as prog_status_ld
    ,NULL                                                           as prog_action_cd
    ,NULL                                                           as prog_action_ld
    ,applications_prog.acc_date::date                              as effdt     
    ,2                                                              as effseq
    ,applications_prog.acc_date::date                              as action_dt    
    ,nvl(applications_prog.app_term,dt1.collapsed_strm)             as admit_term 
    ,nvl(applications_prog.app_term,dt1.collapsed_strm)            as req_term 
    ,NULL                                                          as prog_reason_cd
    ,NULL                                                          as prog_reason_ld
    ,NULL                                                          as residency     
    ,NULL                                                          as tuition_res   
    ,NULL                                                          as residency_ld  
    ,NULL                                                          as tuition_res_ld
    ,null							 				             as stdnt_campus_ld
    ,applications_prog.applied_or_approved_major                  as acad_plan_maj
    ,'AD'                                                          as prog_stage_cd
    ,applications_prog.acc_date::date                             as prog_stage_dt
    ,1                                                             as rn
    ,NULL                                                          as paid_flag
    ,NULL                                                          as subplan_maj1
    ,NULL                                                          as subplan_maj2
    ,NULL                                                          as subplan_maj3
    ,NULL                                                          as subplan_min1
    ,NULL                                                          as subplan_min2
    ,NULL                                                          as subplan_min3
    ,NULL                                                         as acad_plan_maj_type_cd
    ,NULL                                                         as acad_plan_min_type_cd
    ,'ADMITTED'                                                     as app_stage    
    ,NULL                                                          as appl_prog_nbr
    ,-2                                                            as adm_appl_nbr_reverse_seq
    ,NULL                                                          as department_cd
    ,NULL                                                          as last_applied_stage
    ,applications_prog.Campus_Loc_App                       as appl_current_location
    ,NULL                                                          as first_gen_flag  
    ,applications_prog.Decision_Rating                                 as appl_rating
    ,NULL                            							   as source_category
    ,NULL                           							   as source_summary
    ,NULL							                               as source_memo
	,applications_prog.application_slate_id                        as application_slate_id
	,applications_prog.workday_universal_id                         as workday_universal_id
	,applications_prog.workday_student_recruitment_id               as workday_student_recruitment_id
	,applications_prog.workday_application_id                       as workday_application_id     
	,applications_prog.slate_external_id							as slate_external_id
	,applications_prog.gender 										as gender
	,applications_prog.sex 											as dem_gender
	,applications_prog."first_gen_on_application?" 				    as stdnt_first_gen_flag
	,applications_prog.permanent_geomarket 						    as eps_market

FROM orion_ods.suffolk_uga_app_active_data applications_prog
left join hercules_workday.dim_term dt1 on 
(CASE
        WHEN applications_prog.app_term LIKE '%Fall' THEN CONCAT('Fall ', left(applications_prog.app_term, 4))
        WHEN applications_prog.app_term LIKE '%Summer' THEN CONCAT('Summer ', left(applications_prog.app_term, 4))
        -- Add more cases for other term formats as needed
    end)
    = dt1.collapsed_strm and dt1.term_type_ld is not null

	
where acc_date is not null 

UNION ALL
--DEPOSITED

SELECT 
	substring(nvl(applications_prog.slate_reference_id,'*'),1,11)       as emplid
    ,'Undergraduate'                                                as acad_career
    ,applications_prog.applied_or_approved_major                                      as acad_prog      
    ,NVL(applications_prog.workday_application_id, '*')                  as adm_appl_nbr  
    ,nvl(applications_prog.workday_application_id, '*')                  as stdnt_car_nbr
    ,'DP'                                                           as prog_status_cd
    ,'Enrollment Deposit'                                                           as prog_status_ld
    ,NULL                                                           as prog_action_cd
    ,NULL                                                           as prog_action_ld
    ,applications_prog.dep_date::date                              as effdt     
    ,3                                                              as effseq
    ,applications_prog.dep_date::date                              as action_dt    
    ,nvl(applications_prog.app_term,dt1.collapsed_strm)             as admit_term 
    ,nvl(applications_prog.app_term,dt1.collapsed_strm)            as req_term 
    ,NULL                                                          as prog_reason_cd
    ,NULL                                                          as prog_reason_ld
    ,NULL                                                          as residency     
    ,NULL                                                          as tuition_res   
    ,NULL                                                          as residency_ld  
    ,NULL                                                          as tuition_res_ld
    ,NULL	              											as stdnt_campus_ld
    ,applications_prog.applied_or_approved_major                    as acad_plan_maj
    ,'DP'                                                          as prog_stage_cd
    ,applications_prog.dep_date::date                             as prog_stage_dt
    ,1                                                             as rn
    ,NULL                                                          as paid_flag
    ,NULL                                                          as subplan_maj1
    ,NULL                                                          as subplan_maj2
    ,NULL                                                          as subplan_maj3
    ,NULL                                                          as subplan_min1
    ,NULL                                                          as subplan_min2
    ,NULL                                                          as subplan_min3
    ,NULL                                                         as acad_plan_maj_type_cd
    ,NULL                                                         as acad_plan_min_type_cd
    ,'DEPOSIT'                                                     as app_stage    
    ,NULL                                                          as appl_prog_nbr
    ,-2                                                            as adm_appl_nbr_reverse_seq
    ,NULL                                                          as department_cd
    ,NULL                                                          as last_applied_stage
    ,applications_prog.Campus_Loc_App                       as appl_current_location
    ,NULL                                                          as first_gen_flag  
    ,applications_prog.Decision_Rating                                 as appl_rating
    ,NULL                            							   as source_category
    ,NULL                           							   as source_summary
    ,NULL							                               as source_memo
	,applications_prog.application_slate_id                        as application_slate_id
	,applications_prog.workday_universal_id                         as workday_universal_id
	,applications_prog.workday_student_recruitment_id               as workday_student_recruitment_id
	,applications_prog.workday_application_id                       as workday_application_id     
	,applications_prog.slate_external_id							as slate_external_id
	,applications_prog.gender 										as gender
	,applications_prog.sex 											as dem_gender
	,applications_prog."first_gen_on_application?" 				    as stdnt_first_gen_flag
	,applications_prog.permanent_geomarket 						    as eps_market

FROM orion_ods.suffolk_uga_app_active_data applications_prog
 left join hercules_workday.dim_term dt1 on 
(CASE
        WHEN applications_prog.app_term LIKE '%Fall' THEN CONCAT('Fall ', left(applications_prog.app_term, 4))
        WHEN applications_prog.app_term LIKE '%Summer' THEN CONCAT('Summer ', left(applications_prog.app_term, 4))
        -- Add more cases for other term formats as needed
    end)
    = dt1.collapsed_strm and dt1.term_type_ld is not null
	
where dep_date is not null

UNION ALL
--WITHDRAW BEFORE DECISION

SELECT 
	substring(nvl(applications_prog.slate_reference_id,'*'),1,11)       as emplid
    ,'Undergraduate'                                                             as acad_career
    ,applications_prog.applied_or_approved_major                                      as acad_prog    
    ,NVL(applications_prog.workday_application_id, '*')                  as adm_appl_nbr  
    ,nvl(applications_prog.workday_application_id, '*')                  as stdnt_car_nbr
    ,'WB'                                                           as prog_status_cd
    ,'Withdraw Before Decision'                                                           as prog_status_ld
    ,NULL                                                           as prog_action_cd
    ,NULL                                                           as prog_action_ld
    ,applications_prog.wd_before_dec_date::date                              as effdt     
    ,3                                                              as effseq
    ,applications_prog.wd_before_dec_date::date                              as action_dt    
    ,nvl(applications_prog.app_term,dt1.collapsed_strm)             as admit_term 
    ,nvl(applications_prog.app_term,dt1.collapsed_strm)            as req_term 
    ,NULL                                                          as prog_reason_cd
    ,NULL                                                          as prog_reason_ld
    ,NULL                                                          as residency     
    ,NULL                                                          as tuition_res   
    ,NULL                                                          as residency_ld  
    ,NULL                                                          as tuition_res_ld
    ,NULL                										as stdnt_campus_ld
    ,applications_prog.applied_or_approved_major                    as acad_plan_maj
    ,'WBD'                                                          as prog_stage_cd
    ,applications_prog.wd_before_dec_date::date                    as prog_stage_dt
    ,1                                                             as rn
    ,NULL                                                          as paid_flag
    ,NULL                                                          as subplan_maj1
    ,NULL                                                          as subplan_maj2
    ,NULL                                                          as subplan_maj3
    ,NULL                                                          as subplan_min1
    ,NULL                                                          as subplan_min2
    ,NULL                                                          as subplan_min3
    ,NULL                                                         as acad_plan_maj_type_cd
    ,NULL                                                         as acad_plan_min_type_cd
    ,'WITHDRAW BEFORE DECISION'                                    as app_stage    
    ,NULL                                                          as appl_prog_nbr
    ,-2                                                            as adm_appl_nbr_reverse_seq
    ,NULL                                                          as department_cd
    ,NULL                                                          as last_applied_stage
    ,applications_prog.Campus_Loc_App                       as appl_current_location
    ,NULL                                                          as first_gen_flag  
    ,applications_prog.Decision_Rating                                 as appl_rating
    ,NULL                            							   as source_category
    ,NULL                           							   as source_summary
    ,NULL							                               as source_memo
	,applications_prog.application_slate_id                        as application_slate_id
	,applications_prog.workday_universal_id                         as workday_universal_id
	,applications_prog.workday_student_recruitment_id               as workday_student_recruitment_id
	,applications_prog.workday_application_id                       as workday_application_id     
	,applications_prog.slate_external_id							as slate_external_id
	,applications_prog.gender 										as gender
	,applications_prog.sex 											as dem_gender
	,applications_prog."first_gen_on_application?" 				    as stdnt_first_gen_flag
	,applications_prog.permanent_geomarket 						    as eps_market

FROM orion_ods.suffolk_uga_app_active_data applications_prog
 left join hercules_workday.dim_term dt1 on 
(CASE
        WHEN applications_prog.app_term LIKE '%Fall' THEN CONCAT('Fall ', left(applications_prog.app_term, 4))
        WHEN applications_prog.app_term LIKE '%Summer' THEN CONCAT('Summer ', left(applications_prog.app_term, 4))
        -- Add more cases for other term formats as needed
    end)
    = dt1.collapsed_strm and dt1.term_type_ld is not null
	
where wd_before_dec_date is not null

UNION ALL
--WITHDRAW after DECISION

SELECT 
	substring(nvl(applications_prog.slate_reference_id,'*'),1,11)       as emplid
    ,'Undergraduate'                                                             as acad_career
    ,applications_prog.applied_or_approved_major                                      as acad_prog     
    ,NVL(applications_prog.workday_application_id, '*')                  as adm_appl_nbr  
    ,nvl(applications_prog.workday_application_id, '*')                  as stdnt_car_nbr
    ,'WA'                                                           as prog_status_cd
    ,'Withdraw after decision'                                                           as prog_status_ld
    ,NULL                                                           as prog_action_cd
    ,NULL                                                           as prog_action_ld
    ,applications_prog.wd_after_acc_date::date                              as effdt     
    ,4                                                              as effseq
    ,applications_prog.wd_after_acc_date::date                              as action_dt    
    ,nvl(applications_prog.app_term,dt1.collapsed_strm)             as admit_term 
    ,nvl(applications_prog.app_term,dt1.collapsed_strm)            as req_term 
    ,NULL                                                          as prog_reason_cd
    ,NULL                                                          as prog_reason_ld
    ,NULL                                                          as residency     
    ,NULL                                                          as tuition_res   
    ,NULL                                                          as residency_ld  
    ,NULL                                                          as tuition_res_ld
    ,Null                  											as stdnt_campus_ld
    ,applications_prog.applied_or_approved_major                     as acad_plan_maj
    ,'WAD'                                                          as prog_stage_cd
    ,applications_prog.wd_after_acc_date::date                             as prog_stage_dt
    ,1                                                             as rn
    ,NULL                                                          as paid_flag
    ,NULL                                                          as subplan_maj1
    ,NULL                                                          as subplan_maj2
    ,NULL                                                          as subplan_maj3
    ,NULL                                                          as subplan_min1
    ,NULL                                                          as subplan_min2
    ,NULL                                                          as subplan_min3
    ,NULL                                                         as acad_plan_maj_type_cd
    ,NULL                                                         as acad_plan_min_type_cd
    ,'WITHDRAW AFTER DECISION'                                                     as app_stage    
    ,NULL                                                          as appl_prog_nbr
    ,-2                                                            as adm_appl_nbr_reverse_seq
    ,NULL                                                          as department_cd
    ,NULL                                                          as last_applied_stage
    ,applications_prog.Campus_Loc_App                       as appl_current_location
    ,NULL                                                          as first_gen_flag  
    ,applications_prog.Decision_Rating                                 as appl_rating
    ,NULL                            							   as source_category
    ,NULL                           							   as source_summary
    ,NULL							                               as source_memo
	,applications_prog.application_slate_id                        as application_slate_id
	,applications_prog.workday_universal_id                         as workday_universal_id
	,applications_prog.workday_student_recruitment_id               as workday_student_recruitment_id
	,applications_prog.workday_application_id                       as workday_application_id     
	,applications_prog.slate_external_id							as slate_external_id
	,applications_prog.gender 										as gender
	,applications_prog.sex 											as dem_gender
	,applications_prog."first_gen_on_application?" 				    as stdnt_first_gen_flag
	,applications_prog.permanent_geomarket 						    as eps_market

FROM orion_ods.suffolk_uga_app_active_data applications_prog
 left join hercules_workday.dim_term dt1 on 
(CASE
        WHEN applications_prog.app_term LIKE '%Fall' THEN CONCAT('Fall ', left(applications_prog.app_term, 4))
        WHEN applications_prog.app_term LIKE '%Summer' THEN CONCAT('Summer ', left(applications_prog.app_term, 4))
        -- Add more cases for other term formats as needed
    end)
    = dt1.collapsed_strm and dt1.term_type_ld is not null
	
where wd_after_acc_date is not null

union all

SELECT 
	substring(nvl(applications_prog.slate_reference_id,'*'),1,11)       as emplid
    ,'Undergraduate'                                                             as acad_career
    ,applications_prog.applied_or_approved_major                                      as acad_prog     
    ,NVL(applications_prog.workday_application_id, '*')                  as adm_appl_nbr  
    ,nvl(applications_prog.workday_application_id, '*')                  as stdnt_car_nbr
    ,'ND'                                                           as prog_status_cd
    ,'Net Desposit'                                                           as prog_status_ld
    ,NULL                                                           as prog_action_cd
    ,NULL                                                           as prog_action_ld
    ,applications_prog.wd_after_acc_date::date                              as effdt     
    ,5                                                              as effseq
    ,applications_prog.wd_after_acc_date::date                              as action_dt    
    ,nvl(applications_prog.app_term,dt1.collapsed_strm)             as admit_term 
    ,nvl(applications_prog.app_term,dt1.collapsed_strm)            as req_term 
    ,NULL                                                          as prog_reason_cd
    ,NULL                                                          as prog_reason_ld
    ,NULL                                                          as residency     
    ,NULL                                                          as tuition_res   
    ,NULL                                                          as residency_ld  
    ,NULL                                                          as tuition_res_ld
    ,Null                  											as stdnt_campus_ld
    ,applications_prog.applied_or_approved_major                     as acad_plan_maj
    ,'DP'                                                          as prog_stage_cd
    ,applications_prog.wd_after_acc_date::date                             as prog_stage_dt
    ,1                                                             as rn
    ,NULL                                                          as paid_flag
    ,NULL                                                          as subplan_maj1
    ,NULL                                                          as subplan_maj2
    ,NULL                                                          as subplan_maj3
    ,NULL                                                          as subplan_min1
    ,NULL                                                          as subplan_min2
    ,NULL                                                          as subplan_min3
    ,NULL                                                         as acad_plan_maj_type_cd
    ,NULL                                                         as acad_plan_min_type_cd
    ,'DEPOSIT'                                                     as app_stage    
    ,NULL                                                          as appl_prog_nbr
    ,-2                                                            as adm_appl_nbr_reverse_seq
    ,NULL                                                          as department_cd
    ,NULL                                                          as last_applied_stage
    ,applications_prog.Campus_Loc_App                       as appl_current_location
    ,NULL                                                          as first_gen_flag  
    ,applications_prog.Decision_Rating                                 as appl_rating
    ,NULL                            							   as source_category
    ,NULL                           							   as source_summary
    ,NULL							                               as source_memo
	,applications_prog.application_slate_id                        as application_slate_id
	,applications_prog.workday_universal_id                         as workday_universal_id
	,applications_prog.workday_student_recruitment_id               as workday_student_recruitment_id
	,applications_prog.workday_application_id                       as workday_application_id     
	,applications_prog.slate_external_id							as slate_external_id
	,applications_prog.gender 										as gender
	,applications_prog.sex 											as dem_gender
	,applications_prog."first_gen_on_application?" 				    as stdnt_first_gen_flag
	,applications_prog.permanent_geomarket 						    as eps_market

FROM orion_ods.suffolk_uga_app_active_data applications_prog
 left join hercules_workday.dim_term dt1 on 
(CASE
        WHEN applications_prog.app_term LIKE '%Fall' THEN CONCAT('Fall ', left(applications_prog.app_term, 4))
        WHEN applications_prog.app_term LIKE '%Summer' THEN CONCAT('Summer ', left(applications_prog.app_term, 4))
        -- Add more cases for other term formats as needed
    end)
    = dt1.collapsed_strm and dt1.term_type_ld is not null
	
where net_dep_date is not null;


COMMIT;
ANALYZE hercules_workday.ff_applications_prog;