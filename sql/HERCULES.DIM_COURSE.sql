/*
   Object type      : Dimension (SCD Type 1)
   Description      : Records course catalog setup information.
   Grain            : Course

   Target table name: DIM_COURSE

   Source table list: courses   -- Primary source table to retrieve course catalog details.
                      

   ETL load type    : Full load everyday

   Date created     : 11/29/2023

*/


TRUNCATE TABLE hercules_workday.dim_course;

INSERT INTO hercules_workday.dim_course
(
     src_sys_cd
    ,src_sys_eff_dt
    ,eff_dt
    ,exp_dt
    ,crse_id
    ,course_sd
    ,course_ld
    ,equiv_crse_id
    ,consent_cd
    ,consent_sd
    ,consent_ld
    ,allow_mult_enroll
    ,units_minimum
    ,units_maximum
    ,units_acad_prog
    ,units_finaid_prog
    ,crse_repeatable
    ,units_repeat_limit
    ,crse_repeat_limit
    ,grading_basis
    ,grading_basis_sd
    ,grading_basis_ld
    ,component_cd
    ,component_sd
    ,component_ld
    ,component_primary
    ,lst_mult_trm_crs
    ,crse_contact_hrs
    ,crse_count
    ,instructor_edit
    ,fees_exist
    ,enrl_un_ld_clc_typ
    ,eff_status
    ,CRSE_WORKDAY_ID
)
SELECT 
           courses.course_id ||'~' ||courses.effective_date		AS SRC_SYS_CD,
           NULL													AS SRC_SYS_EFF_DT,
           courses.effective_date								AS EFF_DT,
           null													AS EXP_DT,
           courses.course_id 									AS CRSE_ID,
           courses.course_abbreviated_title 					AS COURSE_SD,
           courses.course_title									AS COURSE_LD,
           NULL													AS EQUIV_CRSE_ID,
           NULL													AS CONSENT_CD,
           NULL 												AS CONSENT_SD,
           NULL													AS CONSENT_LD,
           NULL 												AS ALLOW_MULT_ENROLL,
           courses.minimum_units								AS UNITS_MINIMUM,
           courses.maximum_units								AS UNITS_MAXIMUM,
           NULL													AS UNITS_ACAD_PROG,
           null													AS UNITS_FINAID_PROG,
           case when courses.repeatable THEN 'Y' ELSE 'N' END  	AS CRSE_REPEATABLE,
           courses.repeat_maximum_units							AS UNITS_REPEAT_LIMIT,
           courses.repeat_maximim_attempts						AS CRSE_REPEAT_LIMIT,
           null      											AS GRADING_BASIS,
           null													AS GRADING_BASIS_SD, 
           null 												AS GRADING_BASIS_LD,
           null								    				AS COMPONENT_CD,
           null                                 				AS COMPONENT_SD,
           null                                 				AS COMPONENT_LD,
           null  												AS COMPONENT_PRIMARY,
           null   												AS LST_MULT_TRM_CRS,
           courses.contact_hours                        		AS CRSE_CONTACT_HRS,
           null                 								AS CRSE_COUNT,
           null 												as instructor_edit,
           null                         						AS FEES_EXIST,
           NULL  												AS ENRL_UN_LD_CLC_TYP,
           null 												AS EFF_STATUS,
           courses.course_id									AS CRSE_WORKDAY_ID
FROM workday_full_test.courses
WHERE 1 =1
;

commit;

analyze hercules_workday.dim_course;