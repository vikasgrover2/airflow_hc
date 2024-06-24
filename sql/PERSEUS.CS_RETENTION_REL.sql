/*##########################################
Object type       : CROSS SEEDING TABLE
Description       :
Grain             :
Target table name : CS_RETENTION_REL
Source table list : FF_RETENTION_GRAD_PROG
ETL load type     : FULL LOAD EVERYDAY
Date created      : 01-MARCH-2019
#############################################*/

TRUNCATE TABLE perseus_workday.cs_retention_rel;

INSERT INTO perseus_workday.cs_retention_rel (
        emplid
       ,acad_career
       ,strm
       ,cohort_strm
       ,retention_flag
       ,retention_status
)
SELECT a.emplid,
   a.student_career AS acad_career,
   a.strm_plus_0 AS strm,
   a.cohort_strm,
   MAX( CASE
         WHEN a.academic_year_status = 0 OR a.academic_year_status IS NULL AND 0 IS NULL
         THEN 0 ELSE 1
      END) AS retention_flag,
   MAX( CASE
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
      END) AS retention_status
FROM hercules_workday.ff_retention_grad_prog a
WHERE 1 = 1
   AND a.cohort_flag = 'Y'
GROUP BY a.emplid,a.student_career,a.strm_plus_0,a.cohort_strm;

COMMIT;

ANALYZE perseus_workday.cs_retention_rel;