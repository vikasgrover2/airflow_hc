/*#######################################
Object type       : CROSS SEEDING TABLE
Description       :
Grain             :
Target table name : CS_DEGREES
Source table list : FACT_DEGREE
ETL load type     : FULL LOAD EVERYDAY
Date created      : 01-MARCH-2019
#########################################*/

TRUNCATE TABLE perseus_workday.cs_degrees;

INSERT INTO perseus_workday.cs_degrees
(
   emplid,
   acad_career,
   car_prg_plan_maj,
   acad_prog,
   completion_term,
   acad_plan_maj,
   application_date,
   honors_cd,
   honors_ld,
   prog_status_cd,
   prog_status_ld,
   degr_chkout_stat_cd,
   degr_chkout_stat_sd,
   degr_confer_dt,
   degr_status_dt,
   degree_gpa
   , stdnt_campus_cd
)
SELECT fact_degree_vw.emplid,
   fact_degree_vw.acad_career,
   fact_degree_vw.car_prg_plan_maj AS car_prg_plan_maj,
   fact_degree_vw.acad_prog AS acad_prog,
   fact_degree_vw.completion_term AS completion_term,
   fact_degree_vw.acad_plan_maj AS acad_plan_maj,
   fact_degree_vw.application_date AS application_date,
   fact_degree_vw.honors_cd AS honors_cd,
   fact_degree_vw.honors_ld AS honors_ld,
   fact_degree_vw.prog_status_cd AS prog_status_cd,
   fact_degree_vw.prog_status_ld AS prog_status_ld,
   fact_degree_vw.degr_chkout_stat_cd AS degr_chkout_stat_cd,
   fact_degree_vw.degr_chkout_stat_sd AS degr_chkout_stat_sd,
   fact_degree_vw.degr_confer_dt AS degr_confer_dt,
   fact_degree_vw.degr_status_dt AS degr_status_dt,
   fact_degree_vw.degree_gpa AS degree_gpa
   , fact_degree_vw.stdnt_campus_cd
FROM ( SELECT fact_degree_vw.emplid,
      fact_degree_vw.acad_career,
      fact_degree_vw.car_prg_plan_maj AS car_prg_plan_maj,
      fact_degree_vw.acad_prog AS acad_prog,
      fact_degree_vw.completion_term AS completion_term,
      fact_degree_vw.acad_plan_maj AS acad_plan_maj,
      fact_degree_vw.application_date AS application_date,
      fact_degree_vw.honors_cd AS honors_cd,
      fact_degree_vw.honors_ld AS honors_ld,
      fact_degree_vw.prog_status_cd AS prog_status_cd,
      fact_degree_vw.prog_status_ld AS prog_status_ld,
      fact_degree_vw.degr_chkout_stat_cd AS degr_chkout_stat_cd,
      fact_degree_vw.degr_chkout_stat_sd AS degr_chkout_stat_sd,
      CASE
         WHEN fact_degree_vw.degr_confer_dt_key = 0
         THEN NULL ELSE TO_DATE(fact_degree_vw.degr_confer_dt_key,'J')
      END AS degr_confer_dt,
      CASE
         WHEN fact_degree_vw.degr_status_dt_key = 0
         THEN NULL ELSE TO_DATE(fact_degree_vw.degr_status_dt_key,'J')
      END AS degr_status_dt,
      fact_degree_vw.degree_gpa AS degree_gpa,
      ROW_NUMBER() OVER(
                     PARTITION BY fact_degree_vw.emplid,
                        fact_degree_vw.acad_career
                     ORDER BY fact_degree_vw.completion_term,
                        fact_degree_vw.application_date,
                        fact_degree_vw.degree_gpa DESC ) AS rn
        , fact_degree_vw.stdnt_campus_cd
   FROM hercules_workday.fact_degree fact_degree_vw
   WHERE 1 = 1
     -- AND fact_degree_vw.acad_degr_status_cd = 'A' ---acad_degr_status_cd does not exist
     -- JJB 2020-02-28 : Banner does not have non-awarded completions
     ) 
     fact_degree_vw
WHERE 1 = 1
   AND fact_degree_vw.rn = 1;

COMMIT;

ANALYZE perseus_workday.cs_degrees;