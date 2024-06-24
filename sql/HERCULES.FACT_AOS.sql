TRUNCATE TABLE hercules_workday.fact_aos;

INSERT INTO hercules_workday.fact_aos
(
  emplid
, strm
, acad_career
, plan_rank_maj
, plan_rank_min
, stdnt_car_nbr
, trans_day_key
, src_sys_cd
, stdnt_campus_alt_ld
, stdnt_campus_cd
, stdnt_campus_ld
, stdnt_campus_sd
, prog_status_cd
, prog_status_ld
, enrlmtr_deg_seeking_ind
, enrlmtr_deg_seeking_ld
, plan_sequence_maj
, plan_sequence_min
, degr_chkout_stat_cd
, degr_chkout_stat_sd
, acad_plan_maj
, acad_plan_min
, acad_prog
, eff_dt
--, is_primary
, admit_term
, eff_strm
, expir_strm
, exp_grad_term
, completion_term
, src_sys_eff_dt
, prog_rank_dw
, adm_appl_nbr
, appl_prog_nbr
, prog_rank
, term_active_ind
, ipeds_deg_seeking_ind
, ipeds_deg_seeking_ld
, src_sys_exp_dt
, prog_action_cd
, prog_action_ld
, prog_reason_cd
, prog_reason_ld
, subplan_maj1
, subplan_maj2
, subplan_maj3
, subplan_min1
, subplan_min2
, subplan_min3
, action_dt
, prog_status_census
, prog_status_term_start
, acad_plan_maj_census
, acad_plan_maj_term_start
, acad_plan_min_census
, acad_plan_min_term_start
, acad_prog_census
, acad_prog_term_start
, eff_dt_census
, eff_dt_term_start
, action_dt_census
, action_dt_term_start
, major_start_dt
, career_start_dt
, acad_career_desc
, acad_plan_maj_type_cd
, acad_plan_min_type_cd
, req_term
, adm_appl_nbr_adj
, subplan_maj4
, subplan_min4
, app_dt
, admit_dt
, matric_dt
, deposit_paid_dt
, ter_plan
, ter_plan_census
, ter_plan_sequence
, acad_plan_ter_type_cd
, honors_ind
, honors_acad_plan
, honors_acad_sub_plan
, effseq
, quat_plan
, quin_plan
, acad_plan_quat_type_cd
, acad_plan_quin_type_cd
, subplan_ter1
, subplan_ter2
, subplan_ter3
, subplan_ter4
, subplan_quat1
, subplan_quat2
, subplan_quat3
, subplan_quat4
, subplan_quin1
, subplan_quin2
, subplan_quin3
, subplan_quin4
, subplan_maj1_term_start
, department_cd
, department_ld
, stdnt_school_cd
, stdnt_school_ld
, acad_plan_maj1_2
, acad_plan_min1_2
, acad_plan
, plan_type_cd
, plan_type_rn
, addnl_plan_flg
, acad_degree
, effdt
, plan_rn
, deg_rn
, degree_seeking_ind
)
--drop table workday_ods.hercules_fact_aos ;
--create table workday_ods.hercules_fact_aos
--as
WITH FACT_AOS_STG1 AS (
SELECT 
  src1.student_wid
, src1.academic_person_id
, src1.student_id
, src1.universal_id
, src1.academic_record_wid
, src1.student_record_id
, src1.program_order
, src1.effective_date
, src1.program_of_study_wid
, src1.program_of_study_id
, src1.is_primary
, src1.program_of_study_record_status_wid
, src1.program_of_study_record_status_id
, src1.expected_completion_date
, src1.program_of_study_bundle_wid
, src1.program_of_study_bundle_id
, src1.program_of_study_record_status_reason_wid
, src1.program_of_study_record_status_reason_tenanted_id
, src1.program_of_study_record_status_reason_id
, src1.date_of_determination
FROM (workday_ods.Program_of_Study_Academic_Record_Data a
		left join workday_ods.Program_of_Study_Assignment_Subedit b
			on a.subtable_index = b.subtable_index) src1
WHERE 1=1
UNION ALL 
SELECT
  DISTINCT
  src1.student_wid
, src1.academic_person_id
, src1.student_id
, src1.universal_id
, src1.academic_record_wid
, src1.student_record_id
, src2.program_order||'~'||src1.program_order as program_order
, src1.effective_date
, src1.program_of_study_wid
, src1.program_of_study_id
, src1.is_primary
, src2.program_of_study_record_status_wid
, src2.program_of_study_record_status_id
, src1.expected_completion_date
, src1.program_of_study_bundle_wid
, src1.program_of_study_bundle_id
, src1.program_of_study_record_status_reason_wid
, src1.program_of_study_record_status_reason_tenanted_id
, src1.program_of_study_record_status_reason_id
, src1.date_of_determination
FROM (workday_ods.Program_of_Study_Academic_Record_Data a
		left join workday_ods.Program_of_Study_Assignment_Subedit b
			on a.subtable_index = b.subtable_index) src1
join (workday_ods.Program_of_Study_Academic_Record_Data a
		left join workday_ods.Program_of_Study_Assignment_Subedit b
			on a.subtable_index = b.subtable_index) src2
on  src1.student_wid	=  src2.student_wid
and src1.effective_date	=  src2.effective_date
and src1.program_order			<> src2.program_order
and src1.program_of_study_record_status_wid				<> src2.program_of_study_record_status_wid
and src1.program_of_study_wid							<> src2.program_of_study_wid
left join (workday_ods.Program_of_Study_Academic_Record_Data a
		left join workday_ods.Program_of_Study_Assignment_Subedit b
			on a.subtable_index = b.subtable_index) src3
on  src1.student_wid										=  src3.student_wid
and src1.effective_date	=  src3.effective_date
and src2.program_of_study_record_status_wid				=  src3.program_of_study_record_status_wid
and src1.program_of_study_wid								=  src3.program_of_study_wid
where 1=1
and src3.student_wid is null
)
, FACT_AOS_STG2 as (
SELECT 
  src1.student_wid
, src1.academic_person_id
, src1.student_id
, src1.universal_id
, src1.academic_record_wid
, src1.student_record_id
, src1.program_order
, src1.effective_date
, src1.program_of_study_wid
, src1.program_of_study_id
, src1.is_primary
, src1.program_of_study_record_status_wid
, src1.program_of_study_record_status_id
, src1.expected_completion_date
, src1.program_of_study_bundle_wid
, src1.program_of_study_bundle_id
, src1.program_of_study_record_status_reason_wid
, src1.program_of_study_record_status_reason_tenanted_id
, src1.program_of_study_record_status_reason_id
, src1.date_of_determination
FROM FACT_AOS_STG1 src1
WHERE 1=1
UNION ALL 
SELECT 
  src1.student_wid
, src1.academic_person_id
, src1.student_id
, src1.universal_id
, src1.academic_record_wid
, src1.student_record_id
, src2.program_order||'~'||src1.program_order as program_order
, src1.effective_date
, src1.program_of_study_wid
, src1.program_of_study_id
, src1.is_primary
, src1.program_of_study_record_status_wid
, src1.program_of_study_record_status_id
, src1.expected_completion_date
, src1.program_of_study_bundle_wid
, src1.program_of_study_bundle_id
, src1.program_of_study_record_status_reason_wid
, src1.program_of_study_record_status_reason_tenanted_id
, src1.program_of_study_record_status_reason_id
, src1.date_of_determination
FROM FACT_AOS_STG1 src1
join (
	SELECT DISTINCT
	  bund.student_wid
	, bund.program_of_study_wid
	, bund.program_of_study_id
	, bund.is_primary
	, bund.program_order
	, max(bund.program_of_study_bundle_wid) over (partition by bund.student_wid, bund.effective_date ) as program_of_study_bundle_wid
	FROM FACT_AOS_STG1 bund	
	WHERE 1=1
	and bund.program_order not like '%~%'
		) src2
on  src1.student_wid										=  src2.student_wid
and src1.program_of_study_bundle_wid						=  src2.program_of_study_bundle_wid
and src1.program_of_study_wid								<> src2.program_of_study_wid
left join FACT_AOS_STG1 src3
on  src1.student_wid										=  src3.student_wid
and src1.effective_date	=  src3.effective_date
and src1.program_of_study_record_status_wid				    =  src3.program_of_study_record_status_wid
and src2.program_of_study_wid								=  src3.program_of_study_wid
WHERE 1=1
and src3.student_wid is null
and src1.program_order not like '%~%'
)
, FACT_AOS_STG_NEXT as (
SELECT 
	src2.student_wid
, 	src2.program_order
, 	src2.effective_date
, 	nvl(lead(src2.effective_date::date) 
	  over (partition by src2.student_wid,src2.student_record_id order by src2.effective_date ,src2.program_order),'2099-12-31'::DATE) next_effective_date
FROM
	(	SELECT 
		  distinct 
		  src1.student_wid
		, src1.student_record_id
		, left(src1.program_order,1) as program_order
		, src1.effective_date
		FROM FACT_AOS_STG2 src1
		WHERE 1=1
	) src2
where 1=1
)
SELECT  
  a.student_id as emplid
, st.strm as strm
, st.acad_career as acad_career
, null as plan_rank_maj
, null as plan_rank_min
, st.stdnt_car_nbr::int as stdnt_car_nbr
, to_char(a.effective_date::date,'J') as trans_day_key
, a.academic_person_id ||'~'|| a.program_of_study_id||'~'||a.effective_date 
						  ||'~'|| a.program_of_study_record_status_id
						  ||'~'|| a.program_order 
						  ||'~'|| st.acad_career ||'~'|| st.strm as src_sys_cd
, null as stdnt_campus_alt_ld
, null as stdnt_campus_cd
, null as stdnt_campus_ld
, null as stdnt_campus_sd
, a.program_of_study_record_status_wid as prog_status_cd
, a.program_of_study_record_status_id as prog_status_ld
, null as enrlmtr_deg_seeking_ind
, null as enrlmtr_deg_seeking_ld
, null as plan_sequence_maj
, null as plan_sequence_min
, case when a.program_of_study_record_status_id = 'COMPLETE' then 'AW'
	   when a.program_of_study_record_status_id = 'PENDING_COMPLETION' then 'AP'
	   end as degr_chkout_stat_cd
, case when a.program_of_study_record_status_id = 'COMPLETE' then 'Awarded'
	   when a.program_of_study_record_status_id = 'PENDING_COMPLETION' then 'Applied' 
	   end as degr_chkout_stat_sd
, a.program_of_study_wid as acad_plan_maj
, null as acad_plan_min
, a.program_of_study_id as acad_prog
, a.effective_date::date as eff_dt
--, a.is_primary
, null as admit_term
, null as eff_strm
, null as expir_strm
, to_char(a.expected_completion_date,'YYYYMMDD') as exp_grad_term
, to_char(a.expected_completion_date,'YYYYMMDD') as completion_term
, a.effective_date::date as src_sys_eff_dt
, null as prog_rank_dw
, null as adm_appl_nbr
, null as appl_prog_nbr
, null as prog_rank
, st.officially_registered_ind as term_active_ind
, null as ipeds_deg_seeking_ind
, null as ipeds_deg_seeking_ld
, null as src_sys_exp_dt
, a.program_of_study_record_status_reason_tenanted_id as prog_action_cd
, a.program_of_study_record_status_reason_tenanted_id as prog_action_ld
, a.program_of_study_record_status_reason_wid as prog_reason_cd
, a.program_of_study_record_status_reason_id as prog_reason_ld
, null as subplan_maj1
, null as subplan_maj2
, null as subplan_maj3
, null as subplan_min1
, null as subplan_min2
, null as subplan_min3
, a.effective_date::date as action_dt
, null as prog_status_census
, null as prog_status_term_start
, null as acad_plan_maj_census
, null as acad_plan_maj_term_start
, null as acad_plan_min_census
, null as acad_plan_min_term_start
, null as acad_prog_census
, null as acad_prog_term_start
, null as eff_dt_census
, null as eff_dt_term_start
, null as action_dt_census
, null as action_dt_term_start
, null as major_start_dt
, null as career_start_dt
, null as acad_career_desc
, case replace(program_of_study_type_id,'PROGRAM_OF_STUDY_TYPE_','')
	when 'Law_Concentration' then 'LAW'
	when 'Undeclared' then 'UNDE'
	when 'Certificate' then 'CERT'
	when 'Undergraduate_Concentration' then 'UGCN'
	when 'Pathway' then 'PATH'
	when 'Academic_Standing' then 'AS'
	when 'Program' then 'PROG'
	when 'Non_Degree' then 'ND'
	when 'Major' then 'MAJ'
	when 'Minor' then 'MIN'
	when 'Graduate_Concentration' then 'GRCN'
	else 'UNKN' end as acad_plan_maj_type_cd
, null as acad_plan_min_type_cd
, null as req_term
, null as adm_appl_nbr_adj
, null as subplan_maj4
, null as subplan_min4
, MIN(case when a.Program_of_Study_Record_Status_id = 'IN_PROGRESS'
			then a.effective_date::date end) 
	over (partition by a.student_wid, a.program_of_study_wid) as app_dt
, MIN(case when a.Program_of_Study_Record_Status_id = 'IN_PROGRESS'
			then a.effective_date::date end) 
	over (partition by a.student_wid, a.program_of_study_wid) as admit_dt
, MIN(case when a.Program_of_Study_Record_Status_id = 'MATRICULATED'
			then a.effective_date::date end) 
	over (partition by a.student_wid, a.program_of_study_wid) as matric_dt
, MIN(case when a.Program_of_Study_Record_Status_id = 'MATRICULATED'
			then a.effective_date::date end) 
	over (partition by a.student_wid, a.program_of_study_wid) deposit_paid_dt
, null as ter_plan
, null as ter_plan_census
, null as ter_plan_sequence
, null as acad_plan_ter_type_cd
, null as honors_ind
, null as honors_acad_plan
, null as honors_acad_sub_plan
, row_number() over (partition by a.student_wid, a.effective_date, 
					  left(a.program_order,1)) effseq
, null as quat_plan
, null as quin_plan
, null as acad_plan_quat_type_cd
, null as acad_plan_quin_type_cd
, null as subplan_ter1
, null as subplan_ter2
, null as subplan_ter3
, null as subplan_ter4
, null as subplan_quat1
, null as subplan_quat2
, null as subplan_quat3
, null as subplan_quat4
, null as subplan_quin1
, null as subplan_quin2
, null as subplan_quin3
, null as subplan_quin4
, null as subplan_maj1_term_start
, null as department_cd
, null as department_ld
, null as stdnt_school_cd
, null as stdnt_school_ld
, null as acad_plan_maj1_2
, null as acad_plan_min1_2
, a.program_of_study_id as acad_plan
, case replace(program_of_study_type_id,'PROGRAM_OF_STUDY_TYPE_','')
	when 'Law_Concentration' then 'LAW'
	when 'Undeclared' then 'UNDE'
	when 'Certificate' then 'CERT'
	when 'Undergraduate_Concentration' then 'UGCN'
	when 'Pathway' then 'PATH'
	when 'Academic_Standing' then 'AS'
	when 'Program' then 'PROG'
	when 'Non_Degree' then 'ND'
	when 'Major' then 'MAJ'
	when 'Minor' then 'MIN'
	when 'Graduate_Concentration' then 'GRCN'
	else 'UNKN' end as plan_type_cd
, row_number() over (partition by a.student_wid, st.acad_career, st.strm, plan_type_cd 
					order by case replace(program_of_study_type_id,'PROGRAM_OF_STUDY_TYPE_','')
						when 'Law_Concentration' then 1
						when 'Undeclared' then 100
						when 'Certificate' then 50
						when 'Undergraduate_Concentration' then 40
						when 'Pathway' then 40
						when 'Academic_Standing' then 60
						when 'Program' then 10
						when 'Non_Degree' then 200
						when 'Major' then 15
						when 'Minor' then 20
						when 'Graduate_Concentration' then 30
						else  200 end, a.program_of_study_wid) as  plan_type_rn
, null as addnl_plan_flg
, null as acad_degree
, a.effective_date::date as effdt
, row_number() over (partition by a.student_wid, st.acad_career, st.strm order by
                        case replace(program_of_study_type_id,'PROGRAM_OF_STUDY_TYPE_','')
						when 'Law_Concentration' then 1
						when 'Undeclared' then 100
						when 'Certificate' then 50
						when 'Undergraduate_Concentration' then 40
						when 'Pathway' then 40
						when 'Academic_Standing' then 60
						when 'Program' then 10
						when 'Non_Degree' then 200
						when 'Major' then 15
						when 'Minor' then 20
						when 'Graduate_Concentration' then 30
						else  200 end
					 , a.program_of_study_wid) as plan_rn
, null as deg_rn	
, case replace(program_of_study_type_id,'PROGRAM_OF_STUDY_TYPE_','')
						when 'Law_Concentration' then 'Y'
						when 'Undeclared' then 'Y'
						when 'Certificate' then 'N'
						when 'Undergraduate_Concentration' then 'Y'
						when 'Pathway' then 'Y'
						when 'Academic_Standing' then 'Y'
						when 'Program' then 'Y'
						when 'Non_Degree' then 'N'
						when 'Major' then 'Y'
						when 'Minor' then 'Y'
						when 'Graduate_Concentration' then 'Y'
						else  'N'						end as degree_seeking_ind
FROM FACT_AOS_STG2 a 

join FACT_AOS_STG_NEXT next_effdt
on  a.student_wid										= next_effdt.student_wid
AND left(a.program_order,1)	= next_effdt.program_order
AND a.effective_date	= next_effdt.effective_date

left join workday_ods.program_of_study prog 
on 	prog.program_of_study_id = a.program_of_study_id
-- and prog.expir_timestamp is null 


JOIN hercules_workday.ff_student_term st
   on  st.emplid 		 = a.student_id
   and  decode(st.acad_career,'Law','LAW','Non_Credit','NC','Undergraduate','UG','Graduate','GR', 'UN')					
		= decode(prog.academic_level_id,'Law','LAW','Non_Credit','NC','Undergraduate','UG','Graduate','GR', 'UN')
   and a.effective_date::date  		<= st.term_end_date
   and next_effdt.next_effective_date  > st.term_end_date
   
where 1=1
and ((a.program_of_study_record_status_id in ( 'IN_PROGRESS','MATRICULATED') )
or (a.program_of_study_record_status_id not in ( 'IN_PROGRESS','MATRICULATED') 
AND a.effective_date::date  >= st.term_start_date-10 
AND a.effective_date::date  <= st.term_end_date))
;

ANALYZE hercules_workday.fact_aos;
