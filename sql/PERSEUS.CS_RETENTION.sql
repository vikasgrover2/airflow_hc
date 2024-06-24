/*##########################################
Object type       : CROSS SEEDING TABLE
Description       :
Grain             :
Target table name : CS_RETENTION
Source table list : FF_RETENTION_GRAD_PROG
ETL load type     : FULL LOAD EVERYDAY
Date created      : 01-MARCH-2019
#############################################*/

TRUNCATE TABLE perseus_workday.cs_retention;

INSERT INTO perseus_workday.cs_retention -- JJB 2020-02-29 : Added implicit insert column list
(
   emplid,
   acad_career,
   strm,
   retention_year_1_flag,
   retention_year_1_status,
   retention_year_2_flag,
   retention_year_2_status,
   retention_year_3_flag,
   retention_year_3_status,
   retention_year_4_flag,
   retention_year_4_status,
   retention_year_5_flag,
   retention_year_5_status,
   retention_year_6_flag,
   retention_year_6_status,
   retention_year_7_flag,
   retention_year_7_status,
   retention_year_8_flag,
   retention_year_8_status,
   retention_year_9_flag,
   retention_year_9_status,
   retention_year_10_flag,
   retention_year_10_status
)
SELECT a.emplid,
   a.student_career AS acad_career,
   a.cohort_strm AS strm,
   MAX( CASE
         WHEN a.status_year_cd = 'STATUS_TERM_2' AND (a.academic_year_status = 0 OR a.academic_year_status IS NULL)
         THEN 0 ELSE 1
      END) AS retention_year_1_flag,
   MAX( CASE
         WHEN a.status_year_cd = 'STATUS_TERM_2'
         THEN
            CASE
               WHEN a.academic_year_status = 1
               THEN 'ENROLLED'
               WHEN a.academic_year_status = 0
               THEN 'NOT ENROLLED'
               WHEN a.academic_year_status = 2
               THEN 'ENROLLED ALT PROGRAM'
               WHEN a.academic_year_status = 3
               THEN 'GRADUATED'
               WHEN a.academic_year_status = 4
               THEN 'GRADUATED ALT PROGRAM' ELSE 'N/A'
            END ELSE NULL
      END) AS retention_year_1_status,
   MAX( CASE
         WHEN a.status_year_cd = 'STATUS_TERM_4' AND (a.academic_year_status = 0 OR a.academic_year_status IS NULL)
         THEN 0 ELSE 1
      END) AS retention_year_2_flag,
   MAX( CASE
         WHEN a.status_year_cd = 'STATUS_TERM_4'
         THEN
            CASE
               WHEN a.academic_year_status = 1
               THEN 'ENROLLED'
               WHEN a.academic_year_status = 0
               THEN 'NOT ENROLLED'
               WHEN a.academic_year_status = 2
               THEN 'ENROLLED ALT PROGRAM'
               WHEN a.academic_year_status = 3
               THEN 'GRADUATED'
               WHEN a.academic_year_status = 4
               THEN 'GRADUATED ALT PROGRAM' ELSE 'N/A'
            END ELSE NULL
      END) AS retention_year_2_status,
   MAX( CASE
         WHEN a.status_year_cd = 'STATUS_TERM_6' AND (a.academic_year_status = 0 OR a.academic_year_status IS NULL)
         THEN 0 ELSE 1
      END) AS retention_year_3_flag,
   MAX( CASE
         WHEN a.status_year_cd = 'STATUS_TERM_6'
         THEN
            CASE
               WHEN a.academic_year_status = 1
               THEN 'ENROLLED'
               WHEN a.academic_year_status = 0
               THEN 'NOT ENROLLED'
               WHEN a.academic_year_status = 2
               THEN 'ENROLLED ALT PROGRAM'
               WHEN a.academic_year_status = 3
               THEN 'GRADUATED'
               WHEN a.academic_year_status = 4
               THEN 'GRADUATED ALT PROGRAM' ELSE 'N/A'
            END ELSE NULL
      END) AS retention_year_3_status,
   MAX( CASE
         WHEN a.status_year_cd = 'STATUS_TERM_8' AND (a.academic_year_status = 0 OR a.academic_year_status IS NULL)
         THEN 0 ELSE 1
      END) AS retention_year_4_flag,
   MAX( CASE
         WHEN a.status_year_cd = 'STATUS_TERM_8'
         THEN
            CASE
               WHEN a.academic_year_status = 1
               THEN 'ENROLLED'
               WHEN a.academic_year_status = 0
               THEN 'NOT ENROLLED'
               WHEN a.academic_year_status = 2
               THEN 'ENROLLED ALT PROGRAM'
               WHEN a.academic_year_status = 3
               THEN 'GRADUATED'
               WHEN a.academic_year_status = 4
               THEN 'GRADUATED ALT PROGRAM' ELSE 'N/A'
            END ELSE NULL
      END) AS retention_year_4_status,
   MAX( CASE
         WHEN a.status_year_cd = 'STATUS_TERM_10' AND (a.academic_year_status = 0 OR a.academic_year_status IS NULL)
         THEN 0 ELSE 1
      END) AS retention_year_5_flag,
   MAX( CASE
         WHEN a.status_year_cd = 'STATUS_TERM_10'
         THEN
            CASE
               WHEN a.academic_year_status = 1
               THEN 'ENROLLED'
               WHEN a.academic_year_status = 0
               THEN 'NOT ENROLLED'
               WHEN a.academic_year_status = 2
               THEN 'ENROLLED ALT PROGRAM'
               WHEN a.academic_year_status = 3
               THEN 'GRADUATED'
               WHEN a.academic_year_status = 4
               THEN 'GRADUATED ALT PROGRAM' ELSE 'N/A'
            END ELSE NULL
      END) AS retention_year_5_status,
   MAX( CASE
         WHEN a.status_year_cd = 'STATUS_TERM_12' AND (a.academic_year_status = 0 OR a.academic_year_status IS NULL)
         THEN 0 ELSE 1
      END) AS retention_year_6_flag,
   MAX( CASE
         WHEN a.status_year_cd = 'STATUS_TERM_12'
         THEN
            CASE
               WHEN a.academic_year_status = 1
               THEN 'ENROLLED'
               WHEN a.academic_year_status = 0
               THEN 'NOT ENROLLED'
               WHEN a.academic_year_status = 2
               THEN 'ENROLLED ALT PROGRAM'
               WHEN a.academic_year_status = 3
               THEN 'GRADUATED'
               WHEN a.academic_year_status = 4
               THEN 'GRADUATED ALT PROGRAM' ELSE 'N/A'
            END ELSE NULL
      END) AS retention_year_6_status,
   MAX( CASE
         WHEN a.status_year_cd = 'STATUS_TERM_14' AND (a.academic_year_status = 0 OR a.academic_year_status IS NULL)
         THEN 0 ELSE 1
      END) AS retention_year_7_flag,
   MAX( CASE
         WHEN a.status_year_cd = 'STATUS_TERM_14'
         THEN
            CASE
               WHEN a.academic_year_status = 1
               THEN 'ENROLLED'
               WHEN a.academic_year_status = 0
               THEN 'NOT ENROLLED'
               WHEN a.academic_year_status = 2
               THEN 'ENROLLED ALT PROGRAM'
               WHEN a.academic_year_status = 3
               THEN 'GRADUATED'
               WHEN a.academic_year_status = 4
               THEN 'GRADUATED ALT PROGRAM' ELSE 'N/A'
            END ELSE NULL
      END) AS retention_year_7_status,
   MAX( CASE
         WHEN a.status_year_cd = 'STATUS_TERM_16' AND (a.academic_year_status = 0 OR a.academic_year_status IS NULL)
         THEN 0 ELSE 1
      END) AS retention_year_8_flag,
   MAX( CASE
         WHEN a.status_year_cd = 'STATUS_TERM_16'
         THEN
            CASE
               WHEN a.academic_year_status = 1
               THEN 'ENROLLED'
               WHEN a.academic_year_status = 0
               THEN 'NOT ENROLLED'
               WHEN a.academic_year_status = 2
               THEN 'ENROLLED ALT PROGRAM'
               WHEN a.academic_year_status = 3
               THEN 'GRADUATED'
               WHEN a.academic_year_status = 4
               THEN 'GRADUATED ALT PROGRAM' ELSE 'N/A'
            END ELSE NULL
      END) AS retention_year_8_status,
   MAX( CASE
         WHEN a.status_year_cd = 'STATUS_TERM_18' AND (a.academic_year_status = 0 OR a.academic_year_status IS NULL)
         THEN 0 ELSE 1
      END) AS retention_year_9_flag,
   MAX( CASE
         WHEN a.status_year_cd = 'STATUS_TERM_18'
         THEN
            CASE
               WHEN a.academic_year_status = 1
               THEN 'ENROLLED'
               WHEN a.academic_year_status = 0
               THEN 'NOT ENROLLED'
               WHEN a.academic_year_status = 2
               THEN 'ENROLLED ALT PROGRAM'
               WHEN a.academic_year_status = 3
               THEN 'GRADUATED'
               WHEN a.academic_year_status = 4
               THEN 'GRADUATED ALT PROGRAM' ELSE 'N/A'
            END ELSE NULL
      END) AS retention_year_9_status,
   MAX( CASE
         WHEN a.status_year_cd = 'STATUS_TERM_20' AND (a.academic_year_status = 0 OR a.academic_year_status IS NULL)
         THEN 0 ELSE 1
      END) AS retention_year_10_flag,
   MAX( CASE
         WHEN a.status_year_cd = 'STATUS_TERM_20'
         THEN
            CASE
               WHEN a.academic_year_status = 1
               THEN 'ENROLLED'
               WHEN a.academic_year_status = 0
               THEN 'NOT ENROLLED'
               WHEN a.academic_year_status = 2
               THEN 'ENROLLED ALT PROGRAM'
               WHEN a.academic_year_status = 3
               THEN 'GRADUATED'
               WHEN a.academic_year_status = 4
               THEN 'GRADUATED ALT PROGRAM' ELSE 'N/A'
            END ELSE NULL
      END) AS retention_year_10_status
FROM hercules_workday.ff_retention_grad_prog a
WHERE 1 = 1
   AND (a.student_career = 'Undergraduate' and a.first_strm_flag = 'Y')
GROUP BY a.emplid,a.student_career,a.cohort_strm
;

COMMIT;

ANALYZE perseus_workday.cs_retention;