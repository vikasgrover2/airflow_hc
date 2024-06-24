TRUNCATE TABLE hercules_workday.ff_applications;

INSERT INTO hercules_workday.ff_applications
(
        src_sys_cd
        ,emplid
        ,acad_career
        ,stdnt_car_nbr
        ,adm_appl_nbr
        ,application_slate_id
		,workday_universal_id
		,workday_student_recruitment_id
		,workday_application_id
		,slate_external_id
        ,admit_type_cd
        ,admit_type_ld
        ,admit_type_group
        ,entry_term
        ,fin_aid_interest
        ,appl_fee_status_cd
        ,appl_fee_status_ld
        ,appl_fee_dt
        ,notification_plan_cd
        ,notification_plan_ld
        ,adm_creation_dt
        ,adm_updated_dt
        ,adm_appl_dt
        ,prior_appl
        ,appl_fee_type_cd
        ,appl_fee_type_ld
        ,adm_appl_ctr
        ,adm_creation_by
        ,logipaddress
        ,date1
        ,eff_dt
        ,application_type
        ,appl_prog_status_cd
         ,appl_prog_status_ld
        ,stdnt_type_cd
        ,stdnt_type_ld
        ,optout_ind
        ,expir_date
        ,stdnt_approved_acad_load_cd
        ,stdnt_approved_acad_load_ld
		,gender
		,dem_gender
		,stdnt_first_gen_flag
		,appl_current_location
		,eps_market
		,appl_rating
)
SELECT 
        NVL(applications.slate_reference_id, '*')||'~'||
        NVL(applications.workday_application_id::varchar(10), '*') ||'~'||
        NVL(dt1.collapsed_strm , '*')                                              as src_sys_cd
        ,nvl(applications.slate_reference_id,'*')                                  as emplid
        ,'Undergraduate'                                                                 as acad_career  --no prefix needed
        ,applications.workday_application_id                                             as stdnt_car_nbr
        ,NVL(applications.workday_application_id, '*')                                   as adm_appl_nbr 
	    ,applications.application_slate_id                                           as application_slate_id
		,applications.workday_universal_id                                           as workday_universal_id
		,applications.workday_student_recruitment_id                                 as workday_student_recruitment_id
		,applications.workday_application_id                                         as workday_application_id     
		,applications.slate_external_id												 as slate_external_id
        ,NULL                                                                        as admit_type_cd
        ,applications.entry_type_app                                                 as admit_type_ld
        ,NULL                                                                        as admit_type_group
        ,nvl(dt1.collapsed_strm )                                                     as entry_term
        ,NULL                                                                          as fin_aid_interest
        ,NULL                                                                           as appl_fee_status_cd      
        ,NULL                                                                           as appl_fee_status_ld      
        ,NULL::date                                                                     as appl_fee_dt             
        ,NULL                                                                           as notification_plan_cd    
        ,NULL                                                                           as notification_plan_ld    
        ,NVL(applications.app_date::date)                                  			   as adm_creation_dt         
        ,applications.app_date::date                                       as adm_updated_dt          
        ,NVL(applications.app_date::date)                                  as adm_appl_dt             
        ,NULL                                                                           as prior_appl              
        ,NULL                                                                           as appl_fee_type_cd        
        ,NULL                                                                           as appl_fee_type_ld        
        ,NULL                                                                        as adm_appl_ctr            
        ,NULL                                                                           as adm_creation_by         
        ,NULL                                                                           as logipaddress            
        ,NULL::date                                                                     as date1                   
        ,applications.app_date::date                                               as eff_dt                  
        ,NULL                                                                           as application_type        
        ,''                                                                         as appl_prog_status_cd     
        ,NULL                                                                    as appl_prog_status_ld    
        ,NULL                                                                           as stdnt_type_cd           
        ,NULL                                                                           as stdnt_type_ld           
        ,null                                                                           as optout_ind              
        ,NULL::date                                                                           as expir_date              
        ,applications."full-time_intent?"                                                 as stdnt_approved_acad_load_cd
        ,case when applications."full-time_intent?" = '1' then  'Full Time Student' 
		when applications."full-time_intent?" = '0' then  'Part Time Student'  
		else null end                                                                   as stdnt_approved_acad_load_ld    
		,applications.gender as gender
		,applications.sex as dem_gender
		,applications."first_gen_on_application?" as stdnt_first_gen_flag
		,applications.Campus_Loc_App as appl_current_location
		,applications.permanent_geomarket as eps_market
		,applications.Decision_Rating as appl_rating

FROM orion_ods.suffolk_uga_app_active_data applications
 left join hercules_workday.dim_term dt1 on 
(CASE
        WHEN applications.app_term LIKE '%Fall' THEN CONCAT('Fall ', left(applications.app_term, 4))
        WHEN applications.app_term LIKE '%Summer' THEN CONCAT('Summer ', left(applications.app_term, 4))
        -- Add more cases for other term formats as needed
    end)
    = dt1.collapsed_strm and dt1.term_type_ld is not null
where app_date is not null
;

COMMIT;
ANALYZE hercules_workday.ff_applications;