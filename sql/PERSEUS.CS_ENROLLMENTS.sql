/*##############################################################################
Object type       : CROSS SEEDING TABLE
Description       :
Grain             :
Target table name : CS_ENROLLMENTS
Source table list : DIM_CLASS
                    DIM_TERM
                    FACT_AOS
                    FACT_ENROLLMENT
                    FF_STUDENT_TERM
                    FF_STUDENT_CLASS
ETL load type     : FULL LOAD EVERYDAY
Date created      : 02/13/2024
Date updated      :
Update notes      :
##############################################################################*/

TRUNCATE TABLE perseus_workday.cs_enrollments;

INSERT INTO perseus_workday.cs_enrollments (
      acad_career
     ,emplid
	 ,uid
     ,strm
     ,first_enrl_add_day
     ,first_enrl_drop_day
     ,courses_taken
     ,courses_dropped
     ,courses_withdrawn
     ,courses_passed
     ,courses_failed
     ,courses_with_p_grades
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
     ,fin_tot_payment_amt
     ,fin_tuition
     ,fin_tuition_payment
     ,fin_fees
     ,fin_fee_payment
     ,fin_total_charges
     ,fin_credit_hours
     ,fin_corporate_waiver_amt
     ,time_enrl_add_day_key
     ,time_min_enrl_drop_day_key
     ,time_enrl_drop_day_key
     ,time_first_enrl_add_day_key
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
     ,first_term_flag
     ,tot_strms
     ,tot_years
     ,dev_ed_flag
     ,gen_ed_flag
     ,next_strm
     ,stdnt_dev_ed_engl_ind
     ,stdnt_dev_ed_math_ind
     ,stdnt_dev_ed_ind
	 ,max_su_adv_flag
)
SELECT
   src.acad_career,
   src.emplid,
   src.uid,
   src.strm,
   src.first_enrl_add_day,
   src.first_enrl_drop_day,
   src.courses_taken,
   src.courses_dropped,
   src.courses_withdrawn,
   src.courses_passed,
   src.courses_failed,
   src.courses_with_p_grades,
   src.unt_billing,
   src.unt_earned,
   src.grade_points,
   src.grade_points_per_unit,
   src.term_end_dt,
   src.term_begin_dt,
   src.first_course_id,
   src.first_crse_offer_nbr,
   src.first_session_cd,
   src.first_class_section,
   src.first_class_grade_point,
   src.fin_tot_payment_amt,
   src.fin_tuition,
   src.fin_tuition_payment,
   src.fin_fees,
   src.fin_fee_payment,
   src.fin_total_charges,
   src.fin_credit_hours,
   src.fin_corporate_waiver_amt,
   src.time_enrl_add_day_key,
   src.time_min_enrl_drop_day_key,
   src.time_enrl_drop_day_key,
   src.time_first_enrl_add_day_key,
   SUM(src.courses_taken) OVER(PARTITION BY src.acad_career,src.emplid ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_courses_taken,
   SUM(src.courses_withdrawn) OVER(PARTITION BY src.acad_career,src.emplid ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_courses_withdrawn,
   SUM(src.unt_billing) OVER(PARTITION BY src.acad_career,src.emplid ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_unt_billing,
   SUM(src.unt_earned) OVER(PARTITION BY src.acad_career,src.emplid ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_unt_earned,
   SUM(src.grade_points) OVER(PARTITION BY src.acad_career,src.emplid ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_grade_points,
   SUM(src.grade_points_per_unit) OVER(PARTITION BY src.acad_career,src.emplid ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_grade_points_per_unit,
   "MAX"(src.term_end_dt) OVER(PARTITION BY src.acad_career,src.emplid ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_term_end_dt,
   MIN(src.term_begin_dt) OVER(PARTITION BY src.acad_career,src.emplid ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_term_begin_dt,
   "FIRST_VALUE"(src.first_course_id) OVER(PARTITION BY src.acad_career,src.emplid ORDER BY src.term_begin_dt,src.first_course_id ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_first_course_id,
   "FIRST_VALUE"(src.first_crse_offer_nbr) OVER(PARTITION BY src.acad_career,src.emplid ORDER BY src.term_begin_dt,src.first_course_id ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_first_crse_offer_nbr,
   "FIRST_VALUE"(src.first_session_cd) OVER(PARTITION BY src.acad_career,src.emplid ORDER BY src.term_begin_dt,src.first_course_id ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_first_session_cd,
   "FIRST_VALUE"(src.first_class_section) OVER(PARTITION BY src.acad_career,src.emplid ORDER BY src.term_begin_dt,src.first_course_id ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_first_class_section,
   "FIRST_VALUE"(src.first_class_grade_point) OVER(PARTITION BY src.acad_career,src.emplid ORDER BY src.term_begin_dt,src.first_course_id ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_first_class_grade_point,
   SUM(src.fin_tot_payment_amt) OVER(PARTITION BY src.acad_career,src.emplid ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_fin_tot_payment_amt,
   SUM(src.fin_tuition) OVER(PARTITION BY src.acad_career,src.emplid ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_fin_tuition,
   SUM(src.fin_tuition_payment) OVER(PARTITION BY src.acad_career,src.emplid ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_fin_tuition_payment,
   SUM(src.fin_fees) OVER(PARTITION BY src.acad_career,src.emplid ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_fin_fees,
   SUM(src.fin_fee_payment) OVER(PARTITION BY src.acad_career,src.emplid ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_fin_fee_payment,
   SUM(src.fin_total_charges) OVER(PARTITION BY src.acad_career,src.emplid ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_fin_total_charges,
   SUM(src.fin_credit_hours) OVER(PARTITION BY src.acad_career,src.emplid ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_fin_credit_hours,
   SUM(src.fin_corporate_waiver_amt) OVER(PARTITION BY src.acad_career,src.emplid ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_fin_corporate_waiver_amt,
   MIN(src.time_enrl_add_day_key) OVER(PARTITION BY src.acad_career,src.emplid ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_time_enrl_add_day_key,
   MIN(src.time_min_enrl_drop_day_key) OVER(PARTITION BY src.acad_career,src.emplid ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_time_min_enrl_drop_day_key,
   "MAX"(src.time_enrl_drop_day_key) OVER(PARTITION BY src.acad_career,src.emplid ROWS BETWEEN unbounded preceding AND unbounded following) AS tot_time_enrl_drop_day_key,
   CASE
      WHEN src.strm = ("FIRST_VALUE"(src.strm) OVER(PARTITION BY src.acad_career,src.emplid
                                                 ORDER BY
                                                    CASE
                                                       WHEN src.term_type_ld != 'Spring' AND src.term_type_ld != 'Fall'
                                                       THEN 2 ELSE 1
                                                    END,
                                                    src.term_begin_dt ROWS BETWEEN unbounded preceding AND unbounded following))
      THEN 'Y' ELSE 'N'
   END AS first_term_flag,
   pg_catalog.dense_rank() OVER(PARTITION BY src.acad_career,src.emplid ORDER BY src.term_begin_dt) AS tot_strms,
   pg_catalog.dense_rank() OVER(PARTITION BY src.acad_career,src.emplid ORDER BY src.calendar_yr) AS tot_years,
   src.dev_ed_flag,
   src.gen_ed_flag,
   nvl(lead(strm) over (partition BY src.acad_career,src.emplid ORDER BY src.term_begin_dt),'9999') next_strm,
   src.stdnt_dev_ed_engl_ind,
   src.stdnt_dev_ed_math_ind,
   src.stdnt_dev_ed_ind,
   src.max_su_adv_flag
FROM (
    SELECT
        fact_enrollment_vw.acad_career,
      fact_enrollment_vw.emplid,
	  fact_enrollment_vw.uid,
      fact_enrollment_vw.strm,
      TO_DATE(fact_enrollment_vw.first_enrl_add_day_key,'J') AS first_enrl_add_day,
      TO_DATE(CASE
         WHEN fact_enrollment_vw.first_enrl_drop_day_key <> 0
         THEN fact_enrollment_vw.first_enrl_drop_day_key ELSE NULL
      END,'J') AS first_enrl_drop_day,
      COUNT(DISTINCT fact_enrollment_vw.course_id) AS courses_taken,
      COUNT(DISTINCT
      CASE
         WHEN fact_enrollment_vw.stdnt_enrl_status in ('D')
         THEN fact_enrollment_vw.course_id ELSE NULL
      END) AS courses_dropped,
      COUNT(DISTINCT
      CASE
         WHEN fact_enrollment_vw.stdnt_enrl_status in ('D') --fact_enrollment_vw.time_enrl_drop_day_key IS NOT NULL
         THEN fact_enrollment_vw.course_id ELSE NULL
      END) AS courses_withdrawn,
      COUNT(DISTINCT
      CASE
         WHEN fact_enrollment_vw.stdnt_enrl_status in ('E')  AND fact_enrollment_vw.course_completion_flag = 1
         THEN fact_enrollment_vw.course_id ELSE NULL
      END) AS courses_passed,
      COUNT(DISTINCT
      CASE
         WHEN fact_enrollment_vw.crse_grade_off in ('F','F*')
         THEN fact_enrollment_vw.course_id ELSE NULL
      END) AS courses_failed,
      
      COUNT(DISTINCT
      CASE
         WHEN fact_enrollment_vw.crse_grade_off in ('P')
         THEN fact_enrollment_vw.course_id ELSE NULL
      END) AS courses_with_p_grades,
      
      SUM(fact_enrollment_vw.unt_billing) AS unt_billing,
      SUM(fact_enrollment_vw.unt_earned) AS unt_earned,
      SUM(fact_enrollment_vw.grade_points) AS grade_points,
      SUM(fact_enrollment_vw.grade_points_per_unit) AS grade_points_per_unit,
      MAX(fact_enrollment_vw.term_end_dt) AS term_end_dt,
      MIN(fact_enrollment_vw.term_begin_dt) AS term_begin_dt,
      fact_enrollment_vw.first_course_id,
      fact_enrollment_vw.first_crse_offer_nbr,
      fact_enrollment_vw.first_session_cd,
      fact_enrollment_vw.first_class_section,
      fact_enrollment_vw.first_class_grade_point,
      SUM(fact_enrollment_vw.fin_tot_payment_amt) AS fin_tot_payment_amt,
      SUM(fact_enrollment_vw.fin_tuition) AS fin_tuition,
      SUM(fact_enrollment_vw.fin_tuition_payment) AS fin_tuition_payment,
      SUM(fact_enrollment_vw.fin_fees) AS fin_fees,
      SUM(fact_enrollment_vw.fin_fee_payment) AS fin_fee_payment,
      SUM(fact_enrollment_vw.fin_total_charges) AS fin_total_charges,
      SUM(fact_enrollment_vw.fin_credit_hours) AS fin_credit_hours,
      SUM(fact_enrollment_vw.fin_corporate_waiver_amt) AS fin_corporate_waiver_amt,
      MIN(fact_enrollment_vw.time_enrl_add_day_key) AS time_enrl_add_day_key,
      MIN(fact_enrollment_vw.time_enrl_drop_day_key) AS time_min_enrl_drop_day_key,
      MAX(fact_enrollment_vw.time_enrl_drop_day_key) AS time_enrl_drop_day_key,
      MIN(fact_enrollment_vw.time_first_enrl_add_day_key) AS time_first_enrl_add_day_key,
      MAX(fact_enrollment_vw.dev_ed_flag) AS dev_ed_flag,
      MAX(fact_enrollment_vw.gen_ed_flag) AS gen_ed_flag,
      MAX(fact_enrollment_vw.calendar_yr) AS calendar_yr,
      MAX(fact_enrollment_vw.term_type_ld) AS term_type_ld,
      max(fact_enrollment_vw.stdnt_dev_ed_engl_ind) as stdnt_dev_ed_engl_ind,
      max(fact_enrollment_vw.stdnt_dev_ed_math_ind) as stdnt_dev_ed_math_ind,
      max(fact_enrollment_vw.stdnt_dev_ed_ind) as stdnt_dev_ed_ind,
	  max(max_su_adv_flag) as max_su_adv_flag
   FROM (
    SELECT
         fact_enrollment_vw.acad_career,
         fact_enrollment_vw.emplid_stdnt AS emplid,
		 fact_enrollment_vw.uid as uid ,
         fact_enrollment_vw.strm AS strm,
         fact_enrollment_vw.tot_payment_amt AS fin_tot_payment_amt,    
         fact_enrollment_vw.tuition AS fin_tuition,
         fact_enrollment_vw.tuition_payment AS fin_tuition_payment,
         fact_enrollment_vw.fees AS fin_fees,
         fact_enrollment_vw.fee_payment AS fin_fee_payment,
         fact_enrollment_vw.fees + fact_enrollment_vw.tuition AS fin_total_charges,
         fact_enrollment_vw.unt_billing AS fin_credit_hours,
         NULL AS fin_corporate_waiver_amt,
         fact_enrollment_vw.unt_billing,
         fact_enrollment_vw.unt_earned,
         fact_enrollment_vw.grade_points,
         fact_enrollment_vw.grade_points_per_unit,
         fact_enrollment_vw.course_id,
         fact_enrollment_vw.stdnt_enrl_status ,
         fact_enrollment_vw.crse_grade_off,
         null as enrl_status_reason_cd, --ff_student_class_vw.enrl_status_reason_cd,
         fact_enrollment_vw.crse_completion_flag  as course_completion_flag, --ff_student_class_vw.course_completion_flag,
         CASE
            WHEN dim_term_vw.term_end_day_key = 0
            THEN NULL ELSE TO_DATE(dim_term_vw.term_end_day_key,'J')
         END AS term_end_dt,
         CASE
            WHEN dim_term_vw.term_begin_day_key = 0
            THEN NULL ELSE TO_DATE(dim_term_vw.term_begin_day_key,'J')
         END AS term_begin_dt,
         "FIRST_VALUE"(fact_enrollment_vw.enrl_add_day_key) OVER(
                                                              PARTITION BY fact_enrollment_vw.emplid_stdnt,
                                                                 fact_enrollment_vw.acad_career,
                                                                 fact_enrollment_vw.strm
                                                              ORDER BY fact_enrollment_vw.enrl_add_day_key,
                                                                 fact_enrollment_vw.course_id,
                                                                 fact_enrollment_vw.class_nbr ROWS BETWEEN unbounded preceding AND unbounded following) AS first_enrl_add_day_key,
         "FIRST_VALUE"(fact_enrollment_vw.enrl_drop_day_key) OVER(
                                                               PARTITION BY fact_enrollment_vw.emplid_stdnt,
                                                                  fact_enrollment_vw.acad_career,
                                                                  fact_enrollment_vw.strm
                                                               ORDER BY fact_enrollment_vw.enrl_drop_day_key,
                                                                  fact_enrollment_vw.course_id,
                                                                  fact_enrollment_vw.class_nbr ROWS BETWEEN unbounded preceding AND unbounded following) AS first_enrl_drop_day_key,
         "FIRST_VALUE"(fact_enrollment_vw.grade_points_per_unit) OVER(
                                                                   PARTITION BY fact_enrollment_vw.emplid_stdnt,
                                                                      fact_enrollment_vw.acad_career
                                                                   ORDER BY fact_enrollment_vw.enrl_add_day_key,
                                                                      fact_enrollment_vw.course_id,
                                                                      fact_enrollment_vw.class_nbr ROWS BETWEEN unbounded preceding AND unbounded following) AS first_class_grade_point,
         "FIRST_VALUE"(fact_enrollment_vw.course_id) OVER(
                                                       PARTITION BY fact_enrollment_vw.emplid_stdnt,
                                                          fact_enrollment_vw.acad_career
                                                       ORDER BY fact_enrollment_vw.enrl_add_day_key,
                                                          fact_enrollment_vw.course_id,
                                                          fact_enrollment_vw.class_nbr ROWS BETWEEN unbounded preceding AND unbounded following) AS first_course_id,
         "FIRST_VALUE"(fact_enrollment_vw.crse_offer_nbr) OVER(
                                                            PARTITION BY fact_enrollment_vw.emplid_stdnt,
                                                               fact_enrollment_vw.acad_career
                                                            ORDER BY fact_enrollment_vw.enrl_add_day_key,
                                                               fact_enrollment_vw.course_id,
                                                               fact_enrollment_vw.class_nbr ROWS BETWEEN unbounded preceding AND unbounded following) AS first_crse_offer_nbr,
         "FIRST_VALUE"(fact_enrollment_vw.session_cd) OVER(
                                                        PARTITION BY fact_enrollment_vw.emplid_stdnt,
                                                           fact_enrollment_vw.acad_career
                                                        ORDER BY fact_enrollment_vw.enrl_add_day_key,
                                                           fact_enrollment_vw.course_id,
                                                           fact_enrollment_vw.class_nbr ROWS BETWEEN unbounded preceding AND unbounded following) AS first_session_cd,
         "FIRST_VALUE"(fact_enrollment_vw.class_section) OVER(
                                                           PARTITION BY fact_enrollment_vw.emplid_stdnt,
                                                              fact_enrollment_vw.acad_career
                                                           ORDER BY fact_enrollment_vw.enrl_add_day_key,
                                                              fact_enrollment_vw.course_id,
                                                              fact_enrollment_vw.class_nbr ROWS BETWEEN unbounded preceding AND unbounded following) AS first_class_section,
         CASE
            WHEN fact_enrollment_vw.enrl_add_day_key = 0
            THEN NULL ELSE TO_DATE(fact_enrollment_vw.enrl_add_day_key,'J')
         END AS time_enrl_add_day_key,
         CASE
            WHEN fact_enrollment_vw.enrl_drop_day_key = 0
            THEN NULL ELSE TO_DATE(fact_enrollment_vw.enrl_drop_day_key,'J')
         END AS time_enrl_drop_day_key,
         MIN(TO_DATE(fact_enrollment_vw.enrl_add_day_key,'J') ) OVER(
                                                                  PARTITION BY fact_enrollment_vw.emplid_stdnt,
                                                                     fact_enrollment_vw.acad_career ROWS BETWEEN unbounded preceding AND unbounded following) AS time_first_enrl_add_day_key,
         'TBD' AS dev_ed_flag,
         'TBD' AS gen_ed_flag,
         dim_term_vw.calendar_yr as calendar_yr,
         dim_term_vw.term_type_ld,
         null::int as stdnt_dev_ed_engl_ind, --max(class_dev_ed_engl_ind) over (partition by fact_enrollment_vw.emplid_stdnt,fact_enrollment_vw.acad_career) as stdnt_dev_ed_engl_ind,
         null::int as stdnt_dev_ed_math_ind, --max(class_dev_ed_math_ind) over (partition by fact_enrollment_vw.emplid_stdnt,fact_enrollment_vw.acad_career) as stdnt_dev_ed_math_ind,
         null::int as stdnt_dev_ed_ind --max(class_dev_ed_ind) over (partition by fact_enrollment_vw.emplid_stdnt,fact_enrollment_vw.acad_career) as stdnt_dev_ed_ind
		 , CASE WHEN (dim_class_vw.crse_subject,dim_class_vw.catalog_nbr ) in (('CAS','101'),('SBS','100'))
			     AND dim_class_vw.class_section in ('CLAS','CLAS1','CLAS2','CLAS3','CLAS4','BLC01','BLC03')
				 AND fact_enrollment_vw.stdnt_enrl_status in ('E', 'D')
				 AND ff_student_term_vw.stdnt_status_cd in ('R', 'T') then 1 else 0 end max_su_adv_flag

	FROM 		hercules_workday.fact_enrollment fact_enrollment_vw
      LEFT JOIN hercules_workday.dim_class dim_class_vw
            on fact_enrollment_vw.class_nbr 		= dim_class_vw.class_nbr
      LEFT JOIN hercules_workday.ff_student_term ff_student_term_vw
         ON     fact_enrollment_vw.emplid_stdnt 	= ff_student_term_vw.emplid
            AND fact_enrollment_vw.acad_career  	= ff_student_term_vw.acad_career
            AND fact_enrollment_vw.strm 			= ff_student_term_vw.strm
      LEFT JOIN hercules_workday.dim_term dim_term_vw
            ON fact_enrollment_vw.strm 				= dim_term_vw.strm
      WHERE true
      and fact_enrollment_vw.stdnt_enrl_status in ('E','D')
      ) fact_enrollment_vw
   GROUP BY fact_enrollment_vw.acad_career,fact_enrollment_vw.emplid,fact_enrollment_vw.strm,fact_enrollment_vw.first_course_id,fact_enrollment_vw.first_crse_offer_nbr,fact_enrollment_vw.first_session_cd,fact_enrollment_vw.first_class_section,fact_enrollment_vw.first_class_grade_point,TO_DATE(fact_enrollment_vw.first_enrl_add_day_key,'J'),TO_DATE( CASE
         WHEN fact_enrollment_vw.first_enrl_drop_day_key <> 0
         THEN fact_enrollment_vw.first_enrl_drop_day_key ELSE NULL
      END,'J')
      ,fact_enrollment_vw.term_begin_dt
	  ,fact_enrollment_vw.uid
      ) src
      where 1=1
;
	  
COMMIT;

ANALYZE perseus_workday.cs_enrollments;
