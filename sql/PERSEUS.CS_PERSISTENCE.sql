/*############################################
Object type       : CROSS SEEDING TABLE
Description       :
Grain             :
Target table name : CS_PERSISTENCE
Source table list : FF_STUDENT_TERM_TRENDS_RE
ETL load type     : FULL LOAD EVERYDAY
Date created      : 01-MARCH-2019
###############################################*/

TRUNCATE TABLE perseus_workday.cs_persistence;

INSERT INTO perseus_workday.cs_persistence 
 (
     strm
   , emplid
   , acad_career
   , reenrollment_type
   , persistence_flg
   , pers_next_fall_flg
   , pers_next_acad_yr_flg
   , persistence_term_ld
   , persistence_graduated_flg
   , next_term_major
   , next_fall_major
   , next_acad_yr_major
 )
SELECT 
     ff_student_term_trends_re_tbl.strm as strm,
     ff_student_term_trends_re_tbl.emplid as emplid,
     ff_student_term_trends_re_tbl.acad_career as acad_career,
     max(case when ff_student_term_trends_re_tbl.reenrollment_type in ('Winter-Spring', 'Fall-Spring', 'Summer-Fall', 'Spring-Fall') then ff_student_term_trends_re_tbl.reenrollment_type end) as reenrollment_type,
     max(case when ff_student_term_trends_re_tbl.reenrollment_type in ('Winter-Spring', 'Fall-Spring', 'Summer-Fall', 'Spring-Fall') then ff_student_term_trends_re_tbl.campusi_flag else 0 end) AS persistence_flg
   , max(case when ff_student_term_trends_re_tbl.reenrollment_type in ('Fall-Fall') then ff_student_term_trends_re_tbl.campusi_flag else 0 end)   as pers_next_fall_flg
   , max(case when ff_student_term_trends_re_tbl.reenrollment_type in ('Next-Acad-Year') then ff_student_term_trends_re_tbl.campusi_flag else 0 end)   as pers_next_acad_yr_flg
   , max(case when ff_student_term_trends_re_tbl.reenrollment_type in ('Winter-Spring', 'Fall-Spring', 'Summer-Fall', 'Spring-Fall') then t.term_year_ld end)   AS persistence_term_ld
   , max(case when ff_student_term_trends_re_tbl.reenrollment_type in ('Winter-Spring', 'Fall-Spring', 'Summer-Fall', 'Spring-Fall') then ff_student_term_trends_re_tbl.graduated_flag end) AS persistence_graduated_flg
   , max(case when ff_student_term_trends_re_tbl.reenrollment_type in ('Winter-Spring', 'Fall-Spring', 'Summer-Fall', 'Spring-Fall') then next_dim_car_prgrm_major_vw.acad_plan_ld || '('||next_dim_car_prgrm_major_vw.acad_plan_cd||')' end)  as next_term_major
   , max(next_fall_dim_car_prgrm_major_vw.acad_plan_ld || '('||next_fall_dim_car_prgrm_major_vw.acad_plan_cd||')') as next_fall_major
   , max(next_acad_yr_dim_car_prgrm_major_vw.acad_plan_ld || '('||next_acad_yr_dim_car_prgrm_major_vw.acad_plan_cd||')') as next_acad_yr_major
FROM 	  hercules_workday.ff_student_term_trends_re ff_student_term_trends_re_tbl

left join hercules_workday.dim_term t
on  ff_student_term_trends_re_tbl.strm          = t.strm 

LEFT JOIN hercules_workday.fact_aos next_fact_aos_vw 
ON         ff_student_term_trends_re_tbl.ACAD_CAREER     	= next_fact_aos_vw.ACAD_CAREER -- JJB 2020-03-14  added for Banner
    AND ff_student_term_trends_re_tbl.emplid         		= next_fact_aos_vw.emplid
    AND ff_student_term_trends_re_tbl.next_strm     		= next_fact_aos_vw.strm
    AND next_fact_aos_vw.plan_type_cd                 		= 'PROG' 
    and next_fact_aos_vw.plan_type_rn                       = 1


LEFT JOIN hercules_workday.dim_car_prgrm_plan next_dim_car_prgrm_major_vw 
ON         next_fact_aos_vw.acad_plan                 = next_dim_car_prgrm_major_vw.acad_plan_cd
    AND next_fact_aos_vw.plan_type_cd             = next_dim_car_prgrm_major_vw.acad_plan_type_cd -- JJB 2020-03-14  Changed to MAJ for banner
    AND next_fact_aos_vw.acad_career             = next_dim_car_prgrm_major_vw.acad_career_cd
    
LEFT JOIN (
    select fact_aos_scd.*, t.academic_yr - 1 as prev_academic_yr
    from hercules_workday.fact_aos fact_aos_scd
    join hercules_workday.dim_Term t 
    on  t.strm                 = fact_aos_scd.strm
    
    where 1=1
    and t.term_type_ld = 'Fall'
	and fact_aos_scd.plan_type_cd	='PROG'
	and fact_aos_scd.plan_type_rn	= 1
    ) as next_fall_fact_aos_vw
ON         ff_student_term_trends_re_tbl.ACAD_CAREER     = next_fall_fact_aos_vw.ACAD_CAREER -- JJB 2020-03-14  added for Banner
    AND ff_student_term_trends_re_tbl.emplid         	 = next_fall_fact_aos_vw.emplid
    AND t.academic_yr                                 	 = next_fall_fact_aos_vw.prev_academic_yr
    
LEFT JOIN hercules_workday.dim_car_prgrm_plan next_fall_dim_car_prgrm_major_vw 
ON         next_fall_fact_aos_vw.acad_plan             = next_fall_dim_car_prgrm_major_vw.acad_plan_cd
    AND next_fall_fact_aos_vw.plan_type_cd             = next_fall_dim_car_prgrm_major_vw.acad_plan_type_cd -- JJB 2020-03-14  Changed to MAJ for banner
    AND next_fall_fact_aos_vw.acad_career              = next_fall_dim_car_prgrm_major_vw.acad_career_cd
    
LEFT JOIN (
    select fact_aos_scd.*, t.academic_yr - 1 as prev_academic_yr, 
    row_number() over (partition by fact_aos_scd.acad_career, fact_aos_scd.emplid, t.academic_yr order by to_date(t.term_begin_day_key,'J') desc) rn
    from hercules_workday.fact_aos fact_aos_scd
    join hercules_workday.dim_Term t 
    on  t.strm                 = fact_aos_scd.strm
    
    where 1=1
    AND fact_aos_scd.plan_type_cd             = 'PROG'
    AND fact_aos_scd.plan_type_rn             = 1
    ) as next_acad_yr_fact_aos_vw
ON         ff_student_term_trends_re_tbl.ACAD_CAREER     	= next_acad_yr_fact_aos_vw.ACAD_CAREER -- JJB 2020-03-14  added for Banner
    AND ff_student_term_trends_re_tbl.emplid         		= next_acad_yr_fact_aos_vw.emplid
    AND 1                                             		= next_acad_yr_fact_aos_vw.rn
    
LEFT JOIN hercules_workday.dim_car_prgrm_plan next_acad_yr_dim_car_prgrm_major_vw 
ON         next_acad_yr_fact_aos_vw.acad_plan                 = next_acad_yr_dim_car_prgrm_major_vw.acad_plan_cd
    AND next_acad_yr_fact_aos_vw.plan_type_cd             = next_acad_yr_dim_car_prgrm_major_vw.acad_plan_type_cd -- JJB 2020-03-14  Changed to MAJ for banner
    AND next_acad_yr_fact_aos_vw.acad_career             = next_acad_yr_dim_car_prgrm_major_vw.acad_career_cd
    
WHERE 1 = 1
   AND ff_student_term_trends_re_tbl.reenrollment_type IS NOT NULL
   group by 
    ff_student_term_trends_re_tbl.strm,
    ff_student_term_trends_re_tbl.emplid ,
    ff_student_term_trends_re_tbl.acad_career
 ;

COMMIT;

ANALYZE perseus_workday.cs_persistence;