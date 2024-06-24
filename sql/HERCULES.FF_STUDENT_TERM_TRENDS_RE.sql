/*
   Object type      : Reporting table.
   Description      : Records student re-enrollment information across each university campus for each strm by re-enrollment type.
   Grain            : Emplid, Academic career, STRM (Cohort)

   Target table name: FF_STUDENT_TERM_TRENDS_RE

   Source table list: FF_STUDENT_TERM      -- Used to retrieve student re-enrollment information by each strm.

   ETL load type    : Full load everyday

   Date created     : 03/04/2020
   Date updated     :
*/
DROP TABLE IF EXISTS student_term_completers_stg;
CREATE TEMP TABLE student_term_completers_stg AS
select
    ffst.acad_career AS acad_career,
    ffst.strm AS strm,
    ffst.emplid,
    DECODE (distinct_degrees.emplid,null,'N','Y') AS completer_ind,
    distinct_degrees.strm as degree_strm,
    distinct_degrees.degree_cd as ff_degree_cd,
    row_number() over (partition by ffst.emplid,ffst.strm,ffst.acad_career order by distinct_degrees.strm) as rn
from hercules_workday.ff_student_term as ffst
left outer join (
    select distinct ff_degree.completion_term AS strm,
          ff_degree.acad_career AS acad_career,
          ff_degree.emplid as emplid,
          ff_degree.degree_cd,
          to_date(ff_degree.degr_confer_dt_key,'J') as deg_confer_dt,
          row_number() over (partition by ff_degree.acad_career,ff_degree.emplid,ff_degree.completion_term order by case when ff_degree.degree_cd IN ('AA','CERT') then 2 else 1 end,ff_degree.degree_cd) as rn
    from hercules_workday.fact_degree as ff_degree
    where true 
--    and ff_degree.acad_degr_status_cd = 'A'
)  distinct_degrees
    ON ffst.emplid = distinct_degrees.emplid
    AND ffst.acad_career = distinct_degrees.acad_career
    AND ffst.term_end_date <= distinct_degrees.deg_confer_dt
    AND distinct_degrees.rn = 1
WHERE true;



TRUNCATE TABLE hercules_workday.ff_student_term_trends_re;

INSERT INTO hercules_workday.ff_student_term_trends_re (
    src_sys_cd,
    strm,
    emplid,
    acad_career,
    primary_stdnt_car_nbr,
    graduated_flag,
    degree_strm,
    primary_acad_prog,
    stdnt_campus_cd,
    stdnt_campus_ld,
    reenrollment_type,
    exclusion_type,
    campusi_flag,
    campusii_flag,
    campusiii_flag,
    fts_ind,
    next_strm
)
SELECT
    strm ||'~'||emplid ||'~'||acad_career ||'~'||reenrollment_type AS src_sys_cd,
    strm,
    emplid,
    acad_career,
    primary_stdnt_car_nbr,
    graduated_flag,
    degree_strm,
    primary_acad_prog,
    stdnt_campus_cd,
    stdnt_campus_ld,
    reenrollment_type,
    exclusion_type,
    campusi_flag,
    campusii_flag,
    campusiii_flag,
    fts_ind,
    next_strm
FROM (
    SELECT DISTINCT
        reenrollment.ff_strm AS strm,
        reenrollment.ff_emplid AS emplid,
        reenrollment.acad_career AS acad_career,
        reenrollment.primary_stdnt_car_nbr AS primary_stdnt_car_nbr,
        reenrollment.graduated_flag     AS graduated_flag,
        student_term_completers_stg.degree_strm AS degree_strm,
        reenrollment.primary_acad_prog AS primary_acad_prog,
        fa.stdnt_campus_cd AS stdnt_campus_cd,
        fa.stdnt_campus_ld AS stdnt_campus_ld,
        reenrollment.reenrollment_type AS reenrollment_type,
        'No Exclusion' AS exclusion_type,
        reenrollment.reenrollment_ind AS campusi_flag,
        null::int AS campusii_flag,
        null::int AS campusiii_flag,
        DECODE (ff_applications.emplid,NULL,'N','Y') AS fts_ind,
        reenrollment.next_strm AS next_strm,
        row_number() OVER(PARTITION BY reenrollment.ff_emplid, reenrollment.ff_strm, reenrollment.acad_career, reenrollment.reenrollment_type ORDER BY CASE WHEN fa.prog_status_cd  in ('A', 'G') THEN 1 ELSE 2 END, fa.eff_dt desc) aos_rn
    FROM (
        SELECT
               fs.reenrollment_type AS reenrollment_type
              ,decode(fs.enrolled_ind,'Y',1,0) AS reenrollment_ind
              ,CASE WHEN COALESCE(deg.emplid, deg2.emplid, 'N') <> 'N' THEN 1 ELSE 0 END AS graduated_flag
			  ,fs.next_strm
			  ,FS.ff_strm
              ,fs.ff_emplid
              ,fs.acad_career
              ,fs.primary_stdnt_car_nbr
              ,fs.primary_acad_prog
        FROM (
--2
            SELECT distinct
                f1.ff_emplid,
				f1.acad_career,
                f1.ff_strm,
                f1.term_type_ld,
                f1.term_type_cd,
				f1.term_begin_day,
				f1.term_end_day,
				f1.primary_stdnt_car_nbr,
				f1.primary_acad_prog,
                f1.next_strm,
				f1.reenrollment_type,
                --max(f2.{enrolled_ind}) enrolled_ind 
				max(f2.officially_registered_ind) enrolled_ind 
            FROM (
--1
                SELECT DISTINCT
                    f.emplid ff_emplid,
					f.acad_career,
                    f.strm ff_strm,
                    f.term_start_date,
                    dt.term_type_cd,
                    dt.term_type_ld,
					f.primary_stdnt_car_nbr,
					f.primary_acad_prog,
					to_date(dt.term_begin_day_key,'J') as term_begin_day,
					to_date(dt.term_end_day_key,'J') as term_end_day,
                    first_value(nxt_spr.strm) over(
                                partition by f.emplid,f.acad_career,ff_strm,dt.term_type_cd,dt.term_type_ld
                                order by to_date(nxt_spr.term_begin_day_key,'J') rows between unbounded preceding and unbounded following)
                    AS next_strm,
					persist_types.decoded_value_ld as reenrollment_type
                FROM hercules_workday.ff_student_term f
				left join edwutil.masterlookup persist_types
				on 		1=1
					and persist_types.module_name = 'Student'
					and persist_types.target_column_name = 'persistence_term_types'
                JOIN hercules_workday.dim_term dt
                    ON f.strm = dt.strm
                JOIN hercules_workday.dim_term nxt_spr
                    on  f.term_start_date < to_date(nxt_spr.term_begin_day_key,'J')
                    AND UPPER (nxt_spr.term_type_cd) = persist_types.sys_value
					AND nxt_spr.cal_type = 'SEM'
                 WHERE 1=1
				 --and f.{enrolled_ind} = 'Y'
                 and f.officially_registered_ind = 'Y'
                 AND UPPER (dt.term_type_cd) = persist_types.sys_code
            ) f1
			left join hercules_workday.dim_term as dim_term
				ON	dim_term.strm 	= f1.next_strm
            LEFT JOIN hercules_workday.ff_student_term f2
                ON  f1.ff_emplid 	= f2.emplid
                AND dim_term.strm 	= f2.strm
                AND f1.acad_career 	= f2.acad_career
			WHERE 1=1
			group by f1.ff_emplid,
				f1.acad_career,
                f1.ff_strm,
                f1.term_type_ld,
                f1.term_type_cd,
                f1.next_strm,
				f1.term_begin_day,
				f1.term_end_day,
				f1.primary_stdnt_car_nbr,
				f1.primary_acad_prog,
				f1.reenrollment_type
        ) fs
        LEFT JOIN(
            SELECT
                emplid,
                acad_career,
                completion_term
            FROM hercules_workday.fact_degree
            WHERE true
            GROUP BY emplid,acad_career,completion_term
        ) deg
            ON  fs.ff_emplid 	= deg.emplid
            AND fs.ff_strm 		= deg.completion_term
            AND fs.acad_career 	= deg.acad_career
        LEFT JOIN(
            SELECT
                emplid,
                acad_career,
                to_date(degr_confer_dt_key,'J') degr_confer_dt
            FROM hercules_workday.fact_degree
            WHERE true
            GROUP BY emplid,acad_career,to_date(degr_confer_dt_key,'J')
        ) deg2
            ON  fs.ff_emplid 		= 	deg2.emplid
            AND fs.acad_career 		= 	deg2.acad_career
			AND fs.term_begin_day	<= 	deg2.degr_confer_dt
			AND fs.term_end_day		>=	deg2.degr_confer_dt
        WHERE 1=1
UNION
--'Next-Acad-Year' START
    SELECT 'Next-Acad-Year' as reenrollment_type
          ,decode(FS.next_academic_yr, 'Y', 1,0) as reenrollment_ind
          ,CASE WHEN COALESCE(deg.emplid, deg1.emplid, deg2.emplid, deg3.emplid, deg4.emplid, 'N') != 'N' THEN 1 ELSE 0 END AS graduated_flag
          ,least(case   when ff_academic_year_1 = next_academic_yr then ff_strm_1
             when ff_academic_year_2 = next_academic_yr then ff_strm_2
             when ff_academic_year_3 = next_academic_yr then ff_strm_3
             when ff_academic_year_4 = next_academic_yr then ff_strm_4
         end) as next_strm
          ,FS.ff_strm
          ,fs.ff_emplid
          ,fs.acad_career
          ,fs.primary_stdnt_car_nbr
          ,fs.primary_acad_prog
    FROM (SELECT F1.ff_emplid
                ,F1.ff_strm
                ,F1.f_next_strm
                ,F1.f_next_strm_2
                ,F1.term_type_ld
                ,F1.term_type_cd
                ,F1.next_academic_yr
				,F2.acad_career
				,f2.primary_stdnt_car_nbr
				,f2.primary_acad_prog
				,F2.emplid
                ,lead(f1.ff_academic_yr,1) over (   partition by f1.ff_emplid, f1.acad_career   order by f1.term_start_date) as ff_academic_year_1
                ,lead(f1.ff_academic_yr,2) over (   partition by f1.ff_emplid, f1.acad_career   order by f1.term_start_date) as ff_academic_year_2
                ,lead(f1.ff_academic_yr,3) over (   partition by f1.ff_emplid, f1.acad_career   order by f1.term_start_date) as ff_academic_year_3
                ,lead(f1.ff_academic_yr,4) over (   partition by f1.ff_emplid, f1.acad_career   order by f1.term_start_date) as ff_academic_year_4
                ,lead(f1.ff_strm,1) over ( partition by f1.ff_emplid, f1.acad_career    order by f1.term_start_date) as ff_strm_1
                ,lead(f1.ff_strm,2) over ( partition by f1.ff_emplid, f1.acad_career    order by f1.term_start_date) as ff_strm_2
                ,lead(f1.ff_strm,3) over ( partition by f1.ff_emplid, f1.acad_career    order by f1.term_start_date) as ff_strm_3
                ,lead(f1.ff_strm,4) over ( partition by f1.ff_emplid, f1.acad_career    order by f1.term_start_date) as ff_strm_4
                
        FROM (SELECT f.emplid as ff_emplid
                    ,f.strm as ff_strm
                    ,f.term_start_date
                    ,dt.academic_yr ff_academic_yr
                    ,MIN(f2.strm) AS f_next_strm
                    ,MIN(f2.strm) AS f_next_strm_2
                    ,dt.term_type_ld
                    ,dt.term_type_cd
                    ,MIN(nxt_yr.academic_yr) AS next_academic_yr
                    ,f.acad_career
                    --,row_number() over (partition by f.emplid,f.acad_career,ff_strm,dt.term_type_cd,dt.term_type_ld,tsap.campus order by t2.term_begin_day_key) rn
              FROM hercules_workday.ff_student_term f 
              JOIN hercules_workday.dim_term dt 
			  ON f.strm = dt.strm
              LEFT JOIN hercules_workday.dim_term t2
                    on  dt.strm 		< t2.strm
					AND t2.cal_type 	= 'SEM'
                    AND dt.academic_yr 	= t2.academic_yr
              LEFT JOIN hercules_workday.dim_term nxt_yr 
                    ON  dt.academic_yr  < nxt_yr.academic_yr
					AND nxt_yr.cal_type = 'SEM'
              LEFT JOIN hercules_workday.ff_student_Term f2
                    ON  1=1
					and f2.officially_registered_ind = 'Y'
                    --and f2.{enrolled_ind} = 'Y'
                    and f.emplid = f2.emplid
                    and f.strm < f2.strm
                    and f2.strm = t2.strm
                  where 1=1 
				  and f.officially_registered_ind = 'Y'
                  --and f.{enrolled_ind} = 'Y'
                  GROUP BY f.emplid,f.acad_career,ff_strm,dt.term_type_cd,dt.term_type_ld,dt.academic_yr,f.term_start_date
                  ORDER BY ff_strm    
            )F1      
            JOIN hercules_workday.ff_student_Term F2
                ON F1.ff_emplid = F2.emplid
                AND F1.ff_strm = F2.strm
                AND F1.acad_career = F2.acad_career
            ) FS
        LEFT JOIN (select emplid, acad_career, completion_term                                                                                                  
                  from hercules_workday.fact_degree                                                                                                                  
                  WHERE true                                                                                                             
                  group by emplid, acad_career, completion_term) deg on FS.emplid = deg.emplid and  FS.ff_strm = deg.completion_term and  FS.acad_career = deg.acad_career
       LEFT JOIN (select emplid, acad_career, completion_term                                                                                                  
                  from hercules_workday.fact_degree                                                                                                                  
                  WHERE true                                                                                                                 
                  group by emplid, acad_career, completion_term) deg1 on FS.emplid = deg1.emplid and  FS.ff_strm_1 = deg1.completion_term and  FS.acad_career = deg1.acad_career
       LEFT JOIN (select emplid, acad_career, completion_term                                                                                                  
                  from hercules_workday.fact_degree                                                                                                                  
                  WHERE true                                                                                                                  
                  group by emplid, acad_career, completion_term) deg2 on FS.emplid = deg2.emplid and  FS.ff_strm_2 = deg2.completion_term and  FS.acad_career = deg2.acad_career
       LEFT JOIN (select emplid, acad_career, completion_term                                                                                                  
                  from hercules_workday.fact_degree                                                                                                                  
                  WHERE true                                                                                                                   
                  group by emplid, acad_career, completion_term) deg3 on FS.emplid = deg3.emplid and  FS.ff_strm_3 = deg3.completion_term and  FS.acad_career = deg3.acad_career
       LEFT JOIN (select emplid, acad_career, completion_term                                                                                                  
                  from hercules_workday.fact_degree                                                                                                                  
                  WHERE true                                                                                                                  
                  group by emplid, acad_career, completion_term) deg4 on FS.emplid = deg4.emplid and  FS.ff_strm_4 = deg4.completion_term and  FS.acad_career = deg4.acad_career
      WHERE FS.term_type_cd in ('FA')      
    ) AS reenrollment
    
    left join hercules_workday.dim_term re_term
    on reenrollment.ff_strm = re_term.strm
    LEFT JOIN(
        SELECT DISTINCT 
            emplid,
            acad_career
        FROM hercules_workday.ff_applications
        WHERE admit_type_cd = 'FTS'
    ) ff_applications
        ON (ff_applications.emplid = reenrollment.ff_emplid
        AND ff_applications.acad_career = reenrollment.acad_career)
    LEFT JOIN student_term_completers_stg
        ON reenrollment.acad_career = student_term_completers_stg.acad_career
        AND reenrollment.ff_strm = student_term_completers_stg.strm
        AND reenrollment.ff_emplid = student_term_completers_stg.emplid
        AND student_term_completers_stg.rn = 1
    LEFT JOIN   hercules_workday.fact_aos fa
        ON      reenrollment.ff_emplid      = fa.emplid
        AND     reenrollment.acad_career    = fa.acad_career
        and     fa.strm                     = re_term.strm
        and     fa.plan_type_cd             = 'PROG'
        and     fa.plan_type_rn             = 1
)
WHERE aos_rn = 1
;

COMMIT;

ANALYZE hercules_workday.ff_student_term_trends_re;

