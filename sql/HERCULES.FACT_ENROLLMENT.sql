/*
   Object type      : Fact
   Description      : Records student class enrollment.
   Grain            : Emplid, STRM, Class number

   Target table name: FACT_ENROLLMENT

   Source table list:                    

   ETL load type    : Full load everyday

   Date created     : 
   Date updated     : 
   Update notes     : 
*/

-- student_acad_cred_nf contains all enrollments including courses that were transfered in 
-- for transfer courses data is inconsistently populated, sometimes an equivalent student course section id is given (stc_student_equiv_eval), other times just the title is populated (stc_title)
-- grain of student_acad_cred_nf is stc_person_id (emplid), stc_course (acad_career), scs_course_section (class_nbr), and stc_student_course_sec (additional key to student_course_sections table -- I am pulling the latest student_course_section id based on student_course_sec_chgdate)
-- this temp table allows us to get one row per emplid, course_id, class_nbr, and strm



TRUNCATE TABLE hercules_workday.fact_enrollment;

INSERT INTO hercules_workday.fact_enrollment
(
    src_sys_cd,
	uid,
	emplid,
	unt_billing,
	grade_points,
	unt_earned,
	--earned_grade_points,
	grade_points_per_unit,
	trans_date,
	enrl_add_day_key,
	enrl_drop_day_key,
	grade_day_key,
	emplid_stdnt,
	course_id,
	crse_offer_nbr,  
	class_nbr,
	class_stat,
	strm,
	session_cd,
	tuition,
	fees,
	class_section,
	tot_payment_amt,
	tuition_payment,
	fee_payment,
	acad_career,
	eff_dt,
	stdnt_enrl_status,
	enrl_status_reason,
	class_tuition_res_cat_cd,
	crse_count,
	unt_taken,
	enrollment_reason,
	sub_term,
	crse_grade_off,
	tuition_campus_cd,
	crse_completion_flag,
	crse_success_flag
)

select 
	NVL(enrl.student_id, '*')||'~'||
		NVL(crse.academic_period_id, '*')||'~'||NVL(crse.course_id , '*')||'~'||
		NVL(enrl.student_course_section_id, '*')||'~'||student_record_id as src_sys_cd,
	enrl.universal_id as uid,
	enrl.student_id as emplid,
	enrl.billing_unconverted_credits as unt_billing,
	enrl.grade_points as grade_points,
	enrl.earned_credits as unt_earned,
	--enrl.earned_grade_points as earned_grade_points,
	CASE 
		WHEN enrl.credits::NUMERIC = 0 THEN 0
				ELSE NVL(enrl.grade_points:: NUMERIC, 0) / NVL(enrl.credits:: NUMERIC, 1)::NUMERIC(9,3) 
		END as grade_points_per_unit,
	enrl.registration_date as trans_date,
	NVL(TO_CHAR(enrl.registration_date::date, 'J'),'0'):: NUMERIC as enrl_add_day_key,
	NVL(TO_CHAR(TO_DATE(enrl.drop_date, 'YYYY-MM-DD')::date, 'J'),'0'):: NUMERIC   as enrl_drop_day_key,
	NVL(TO_CHAR(TO_DATE(enrl.graded_date, 'YYYY-MM-DD')::date, 'J'),'0'):: NUMERIC   as grade_day_key,
	enrl.student_id   as emplid_stdnt,
	crse.course_id  as course_id,
	crse.course_id   as crse_offer_nbr,  
	enrl.student_course_section_id as class_nbr,
	crse.section_status_offering_status_id as class_stat,
	replace(crse.academic_period_id,'_',' ')  as strm,
	crse.academic_period_id as session_cd,
	null as tuition,
	null as fees,
	crse.course_listing_section_number    as class_section,
	null as tot_payment_amt,
	null as tuition_payment,
	null as fee_payment,
	CASE 
        WHEN POSITION('_' IN student_record_id) > 0 THEN
            CASE 
                WHEN POSITION('_' IN SUBSTRING(student_record_id, POSITION('_' IN student_record_id) + 1)) > 0 THEN 
                    SUBSTRING(student_record_id, POSITION('_' IN student_record_id) + 1, POSITION('_' IN SUBSTRING(student_record_id, POSITION('_' IN student_record_id) + 1)) - 1)
                ELSE
                    SUBSTRING(student_record_id, POSITION('_' IN student_record_id) + 1)
            END
        ELSE
            student_record_id
    END as acad_career,
	crse.start_date as eff_dt,
	case when enrl.registration_status_descriptor IN ('Completed', 'Registered', 'Withdrawn') then 'E' else 'D'
	end as stdnt_enrl_status,
	enrl.registration_status_descriptor as enrl_status_reason,
	NULL as class_tuition_res_cat_cd,
	crse.section_capacity as crse_count,
	enrl.credits as unt_taken,
	NULL as enrollment_reason,
	NULL as sub_term,
	enrl.grade_descriptor as crse_grade_off,
	NULL as tuition_campus_cd,
	CASE 
			WHEN enrl.student_grade_id similar to  '\\d\\d' THEN CASE
																WHEN enrl.student_grade_id  >= '60' THEN 1 
																ELSE 0
															END 
			ELSE CASE 
						WHEN enrl.student_grade_id not like '%F%' 
						and enrl.student_grade_id not like '%W%' THEN 1
						ELSE 0
					END
		END    as crse_completion_flag,
	CASE 
			WHEN enrl.student_grade_id similar to  '\\d\\d' THEN CASE 
																WHEN enrl.student_grade_id  >= '70' THEN 1
																ELSE 0
															END
			ELSE  CASE 
						WHEN enrl.student_grade_id not like '%F%' 
						and enrl.student_grade_id not like '%W%' 
						and enrl.student_grade_id not like '%D%' THEN 1
						ELSE 0
					END
		END  as crse_success_flag
		
from workday_ods.raas_student_section_enrollment enrl
left join workday_ods.coursesections crse
	on crse.course_section_id = enrl.student_course_section_id 
	and crse.course_listing_id = enrl.course_listing_id 
WHERE 1=1
;

commit; 

analyze hercules_workday.fact_enrollment;