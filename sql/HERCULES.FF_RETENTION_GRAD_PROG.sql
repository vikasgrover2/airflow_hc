/*
   Object type      : Reporting table.
   Description      : Records student retention and graduation over 10 academic year's and next three strm's from cohort strm.
   Grain            : Emplid, Academic career, ACAD_PROG_CD, ACAD_PLAN_CD, COHORT STRM

   Target table name: FF_RETENTION_GRAD_PROG

   Source table list: FF_APPLICATIONS             -- Used to retrieve FTS students.
                      FF_STUDENT_TERM             -- Used to retrieve student term enrollment and registration information.
                      FACT_AOS                    -- Used to retrieve student academic program plan registration information.
                      DIM_CAR_PRGRM_PLAN          -- Used to retrieve University academic program plan setup information.
                      DIM_TERM                    -- Used to retrieve Term definition information.
                      fact_degree                   -- Used to retrieve student degree information.

   ETL load type    : Full load everyday

   Date created     : 03/02/20
   Date updated     : 

Custom Notes:
         Usually our retention base code will define cohorts based off students program data...
         Suffolk defines cohorts using their admissions data...I have added a join to their admissions data to populate the fts_ind
         fts_ind = 'Y' indicates that it is the strm/program combination that definines a student's cohort
*/



drop table if exists stg_cohort_by_prog_stg;

create temp table stg_cohort_by_prog_stg as 
    SELECT
         STDNT_PROG_ENRLMT.emplid                                                              AS EMPLID
        ,STDNT_PROG_ENRLMT.cohort_strm                                                         AS STRM
        ,STDNT_PROG_ENRLMT.acad_career                                                         AS ACAD_CAREER
        ,STDNT_PROG_ENRLMT.primary_acad_prog_cd                                                AS PRIMARY_ACAD_PROG
        ,STDNT_PROG_ENRLMT.acad_plan_cd                                                        AS ACAD_PLAN
        ,STDNT_PROG_ENRLMT.stdnt_campus_cd                                                     AS STDNT_CAMPUS_CD
        ,STDNT_PROG_ENRLMT.univ_attend_type                                                    AS UNIV_ATTEND_TYPE_SD
        ,STDNT_PROG_ENRLMT.deg_level                                                           AS DEG_LEVEL
        ,STDNT_PROG_ENRLMT.program_cd                                                          AS PROGRAM_CD
        ,STDNT_PROG_ENRLMT.school_cd                                                           AS SCHOOL_CD
        ,Lead(STDNT_PROG_ENRLMT.program_cd, 1) over( PARTITION BY STDNT_PROG_ENRLMT.emplid, STDNT_PROG_ENRLMT.acad_career ORDER BY STDNT_PROG_ENRLMT.emplid DESC, STDNT_PROG_ENRLMT.cohort_strm DESC, STDNT_PROG_ENRLMT.acad_career DESC, STDNT_PROG_ENRLMT.primary_acad_prog_cd DESC )        AS PRIOR_PROGRAM_CD
        ,Lead(STDNT_PROG_ENRLMT.school_cd, 1) over( PARTITION BY STDNT_PROG_ENRLMT.emplid, STDNT_PROG_ENRLMT.acad_career ORDER BY STDNT_PROG_ENRLMT.emplid DESC, STDNT_PROG_ENRLMT.cohort_strm DESC, STDNT_PROG_ENRLMT.acad_career DESC, STDNT_PROG_ENRLMT.primary_acad_prog_cd DESC )         AS PRIOR_SCHOOL_CD
        ,Lag(STDNT_PROG_ENRLMT.program_cd, 1) over( PARTITION BY STDNT_PROG_ENRLMT.emplid, STDNT_PROG_ENRLMT.acad_career ORDER BY STDNT_PROG_ENRLMT.emplid DESC, STDNT_PROG_ENRLMT.cohort_strm DESC, STDNT_PROG_ENRLMT.acad_career DESC, STDNT_PROG_ENRLMT.primary_acad_prog_cd DESC )         AS NEXT_PROGRAM_CD
        ,Lag(STDNT_PROG_ENRLMT.school_cd, 1) over( PARTITION BY STDNT_PROG_ENRLMT.emplid, STDNT_PROG_ENRLMT.acad_career ORDER BY STDNT_PROG_ENRLMT.emplid DESC, STDNT_PROG_ENRLMT.cohort_strm DESC, STDNT_PROG_ENRLMT.acad_career DESC, STDNT_PROG_ENRLMT.primary_acad_prog_cd DESC )          AS NEXT_SCHOOL_CD
        ,Lead(STDNT_PROG_ENRLMT.acad_plan_cd, 1) over( PARTITION BY STDNT_PROG_ENRLMT.emplid, STDNT_PROG_ENRLMT.acad_career ORDER BY STDNT_PROG_ENRLMT.emplid DESC, STDNT_PROG_ENRLMT.cohort_strm DESC, STDNT_PROG_ENRLMT.acad_career DESC, STDNT_PROG_ENRLMT.primary_acad_prog_cd DESC )      AS PRIOR_PLAN_CD
        ,Lag(STDNT_PROG_ENRLMT.acad_plan_cd, 1) over( PARTITION BY STDNT_PROG_ENRLMT.emplid, STDNT_PROG_ENRLMT.acad_career ORDER BY STDNT_PROG_ENRLMT.emplid DESC, STDNT_PROG_ENRLMT.cohort_strm DESC, STDNT_PROG_ENRLMT.acad_career DESC, STDNT_PROG_ENRLMT.primary_acad_prog_cd DESC )       AS NEXT_PLAN_CD
        ,STDNT_PROG_ENRLMT.FTS_IND
        ,t.academic_yr as academic_yr
        ,t.rel_strm
        ,STDNT_PROG_ENRLMT.APPL_CURRENT_LOCATION as APPL_CURRENT_LOCATION
        ,stdnt_prog_enrlmt.orig_cohort_strm as orig_cohort_strm
		,STDNT_PROG_ENRLMT.stdnt_type_cd
		,STDNT_PROG_ENRLMT.degree_seeking_ind
		,t.cal_type
FROM (
---1
    SELECT
         ff_student_term.emplid                  AS EMPLID
        ,ff_student_term.strm                   as orig_cohort_strm
        ,dim_term.strm                			as cohort_strm
        ,ff_student_term.acad_career            AS ACAD_CAREER
        ,fact_aos.acad_prog                     AS PRIMARY_ACAD_PROG_CD
        ,fact_aos.acad_plan                     AS ACAD_PLAN_CD
        ,CASE
            WHEN dim_degree.degree_type_cd IN ( 'DOC', 'BACC' )
            THEN 1
            WHEN dim_degree.degree_type_cd IN ( 'MAS', 'ASC' )
            THEN 2
            WHEN dim_degree.degree_type_cd IN ( 'MBA' )
            THEN 3
            ELSE 4
        END                                                         AS DEG_LEVEL
        ,fact_aos.stdnt_campus_cd                                    AS STDNT_CAMPUS_CD
        ,ff_student_term.univ_attend_type_sd                         AS UNIV_ATTEND_TYPE
        ,Row_number() over( PARTITION BY
                                 ff_student_term.emplid
                                ,ff_student_term.acad_career
                                ,dim_program.acad_prog_cd
                            ORDER BY
                                 decode(ff_student_term.official_credit_registered_ind,'Y',ff_student_term.term_start_date,null)
                                ,ff_student_term.plan_attend_type_cd
                                ,ff_student_term.term_start_date
        ) AS COHORT_RN
        ,ff_student_term.plan_attend_type_cd            AS PLAN_ATTEN_TYPE_CD 
        ,dim_program.acad_prog_cd                       AS PROGRAM_CD
        ,dim_program.school_cd                          AS SCHOOL_CD
        ,null                                           AS FTS_IND
        ,null                                           AS APPL_CURRENT_LOCATION
		,ff_student_term.stdnt_type_cd					AS stdnt_type_cd
		,fact_aos.degree_seeking_ind					AS degree_seeking_ind
    from        hercules_workday.ff_student_term             as ff_student_term
    
    JOIN        hercules_workday.dim_term
    on      	ff_student_term.strm    = dim_term.strm
	and     	dim_term.term_type_ld           ~ ('Fall|Spring')
    
    join    	hercules_workday.fact_aos as fact_aos
    on  ff_student_term.emplid 		= fact_aos.emplid
    and ff_student_term.acad_career = fact_aos.acad_career
    and ff_student_term.strm 		= fact_aos.strm
	and fact_aos.plan_type_cd		= 'PROG'
	and fact_aos.plan_rn			= 1
    
    join hercules_workday.dim_program
    on  fact_aos.acad_prog = dim_program.acad_prog_cd

    join hercules_workday.dim_degree
    on dim_program.degree_cd = dim_degree.degree_cd
    
    WHERE true
	and ff_student_term.officially_registered_ind = 'Y'
    --and ff_student_term.{enrolled_ind} = 'Y'
---1
) STDNT_PROG_ENRLMT
join hercules_workday.dim_term t 
on t.strm = t.strm
and STDNT_PROG_ENRLMT.cohort_strm = t.strm
where 1=1
and STDNT_PROG_ENRLMT.cohort_rn = 1 

order by
     STDNT_PROG_ENRLMT.emplid
    ,STDNT_PROG_ENRLMT.primary_acad_prog_cd
    ,STDNT_PROG_ENRLMT.cohort_strm
;

/* List all distinct enrollments and graduation until current term for each student and academic career, program and plan */
drop table if exists stg_distinct_enrollment_prog_stg;

create temp table stg_distinct_enrollment_prog_stg as 
SELECT 
    stg_cohort_by_prog_stg.emplid                                                                                                               AS emplid
    ,stg_cohort_by_prog_stg.strm                                                                                                                AS cohort_strm
    ,dim_term.strm                                                                                                                    			AS strm
    ,dim_term.academic_yr                                                                                                                       AS academic_yr
    ,NVL(dim_term.next_strm_1,'*')                                                                                                              AS strm1
    ,NVL(dim_term.next_strm_2,'*')                                                                                                              AS strm2
    ,NVL(dim_term.next_strm_3,'*')                                                                                                              AS strm3
    ,NVL(dim_term.next_strm_4,'*')                                                                                                              AS strm4
    ,stg_cohort_by_prog_stg.acad_career                                                                                                         AS acad_career
    ,stg_cohort_by_prog_stg.primary_acad_prog                                                                                                   AS primary_acad_prog
    ,stg_cohort_by_prog_stg.acad_plan                                                                                                           AS acad_plan
    ,case when fact_aos.emplid is not null and ff_student_term.official_credit_registered_ind = 'Y' then 'Y' else 'N' end                       AS enrolled_ind
    ,CASE                                                                                                                                                                                                                  
      WHEN stdnt_prog_degree.emplid IS NULL                                                                                                                                                                               
      THEN 'N'                                                                                                                                                                                                            
      ELSE 'Y'                                                                                                                                                                                                            
    END                                                                                                                                         AS grad_ind
    ,NVL(ff_student_term.official_credit_registered_ind,'N')                																	AS enrolled_cr_ind
    ,MAX(enrolled_ind) OVER (
        PARTITION BY stg_cohort_by_prog_stg.emplid,dim_term.academic_yr,stg_cohort_by_prog_stg.acad_career,stg_cohort_by_prog_stg.primary_acad_prog) AS enrolled_ay_ind
    ,MAX(NVL (ff_student_term.official_credit_registered_ind,'N')) OVER (
        PARTITION BY stg_cohort_by_prog_stg.emplid,dim_term.academic_yr,stg_cohort_by_prog_stg.acad_career,stg_cohort_by_prog_stg.primary_acad_prog)                                        AS enrolled_cr_ay_ind
    ,(CASE WHEN stdnt_car_degree.emplid IS NULL THEN 'N' ELSE 'Y' END)                                      									   AS grad_cr_ind
    ,MAX(CASE WHEN stdnt_prog_degree.emplid IS NULL THEN 'N' ELSE 'Y' END) OVER (
        PARTITION BY stg_cohort_by_prog_stg.emplid,dim_term.academic_yr,stg_cohort_by_prog_stg.acad_career,stg_cohort_by_prog_stg.primary_acad_prog)                                        AS grad_ay_ind
    ,MAX(CASE WHEN stdnt_prog_degree.emplid IS NULL THEN 'N' ELSE 'Y' END) OVER (
        PARTITION BY stg_cohort_by_prog_stg.emplid,dim_term.academic_yr,stg_cohort_by_prog_stg.acad_career,stg_cohort_by_prog_stg.primary_acad_prog)                                        AS grad_sp_ay_ind
    ,MAX (CASE WHEN stdnt_car_degree.emplid IS NULL THEN 'N' ELSE 'Y' END) OVER (
        PARTITION BY stg_cohort_by_prog_stg.emplid,dim_term.academic_yr,stg_cohort_by_prog_stg.acad_career,stg_cohort_by_prog_stg.primary_acad_prog)                                        AS grad_cr_ay_ind
    ,CASE WHEN stdnt_prog_degree.emplid IS NULL THEN 'N' ELSE 'Y' END 																			   AS grad_sp_ind
    ,ROW_NUMBER() OVER (
        PARTITION BY
            stg_cohort_by_prog_stg.emplid
            ,dim_term.collapsed_strm
            ,stg_cohort_by_prog_stg.acad_career
            ,stg_cohort_by_prog_stg.primary_acad_prog
        ORDER BY
            dim_term.rel_strm
    ) AS rn
    ,ROW_NUMBER() OVER (
        PARTITION BY
            stg_cohort_by_prog_stg.emplid
            ,stg_cohort_by_prog_stg.acad_career
            ,dim_term.academic_yr
            ,stg_cohort_by_prog_stg.primary_acad_prog
        ORDER BY
            dim_term.rel_strm - stg_cohort_by_prog_stg.rel_strm desc
            ,dim_term.term_begin_day_key desc
    ) AS rn_ay
    ,row_number() over(
        partition by
            stg_cohort_by_prog_stg.emplid
            ,stg_cohort_by_prog_stg.acad_career
            ,stg_cohort_by_prog_stg.primary_acad_prog
        order by
            dim_term.rel_strm
    ) as rn_mod
    ,mod(
        row_number() over(
            partition by
                stg_cohort_by_prog_stg.emplid
                ,stg_cohort_by_prog_stg.acad_career
                ,stg_cohort_by_prog_stg.primary_acad_prog
            order by
                dim_term.rel_strm
        )
        ,2
    ) as mod_yr
    ,dim_term.degree_yr
    ,dim_term.rel_strm
    ,stg_cohort_by_prog_stg.deg_level   as deg_level_cohort
    ,stdnt_car_degree.deg_level     as deg_level_degree
	,stg_cohort_by_prog_stg.degree_seeking_ind

FROM        stg_cohort_by_prog_stg

INNER JOIN  hercules_workday.dim_term
    ON      dim_term.rel_strm               >= stg_cohort_by_prog_stg.rel_strm
    AND     dim_term.calendar_yr            <= TO_CHAR (SYSDATE,'YYYY')::int + 1
    --and     dim_term.strm                   = dim_term.strm
    and     dim_term.term_type_ld           ~ ('Fall|Spring')
    --and 	dim_term.strm 					~  {dim_term_strm_logic}
	and		dim_term.cal_type				= stg_cohort_by_prog_stg.cal_type


LEFT JOIN   hercules_workday.ff_student_term
    on      ff_student_term.emplid      = stg_cohort_by_prog_stg.emplid
    and     ff_student_term.acad_career = stg_cohort_by_prog_stg.acad_career
    and     ff_student_term.strm        = dim_term.strm

LEFT JOIN   hercules_workday.fact_aos
    on      ff_student_term.emplid      		= fact_aos.emplid
    and     ff_student_term.acad_career 		= fact_aos.acad_career
    and     ff_student_term.strm        		= fact_aos.strm
	and     stg_cohort_by_prog_stg.acad_plan 	= fact_aos.acad_plan
	and fact_aos.plan_type_cd		= 'PROG'
	and fact_aos.plan_rn			= 1


LEFT JOIN   (
--1
    SELECT
        fact_degree.emplid
        --,deg_strm.strm
        ,fact_degree.acad_career
        ,fact_degree.acad_prog
        ,0 AS deg_level
        ,min(to_date(fact_degree.degr_confer_dt_key,'J')) as deg_term_dt
    
    FROM        hercules_workday.fact_degree
    LEFT JOIN   hercules_workday.dim_degree
        on      fact_degree.degree_cd                   = dim_degree.degree_cd
            
    left JOIN        hercules_workday.dim_term deg_strm
        on      fact_degree.completion_term             = deg_strm.strm

    WHERE true
    group by
        fact_degree.emplid
        ,fact_degree.acad_career
        --,deg_strm.strm
        ,fact_degree.acad_prog
--1
) stdnt_prog_degree
    ON      stg_cohort_by_prog_stg.emplid               = stdnt_prog_degree.emplid
    AND     stg_cohort_by_prog_stg.acad_career          = stdnt_prog_degree.acad_career
    AND     stg_cohort_by_prog_stg.primary_acad_prog    = stdnt_prog_degree.acad_prog
	AND     to_date(dim_term.term_end_day_key,'J') 		>= stdnt_prog_degree.deg_term_dt-115
    AND     stdnt_prog_degree.deg_level             	<= stg_cohort_by_prog_stg.deg_level
LEFT JOIN   (
--1
    SELECT
        fact_degree.emplid
        --,deg_strm.strm
        ,fact_degree.acad_career
        ,0 AS deg_level
        ,min(to_date(fact_degree.degr_confer_dt_key,'J')) as deg_term_dt
    
    FROM        hercules_workday.fact_degree
    LEFT JOIN   hercules_workday.dim_degree
        on      fact_degree.degree_cd                   = dim_degree.degree_cd
            
    left JOIN   hercules_workday.dim_term deg_strm
        on      fact_degree.completion_term             = deg_strm.strm

    WHERE true
    group by
        fact_degree.emplid
        ,fact_degree.acad_career
        --,deg_strm.strm
--1
) stdnt_car_degree
    ON      stg_cohort_by_prog_stg.emplid               = stdnt_car_degree.emplid
    AND     stg_cohort_by_prog_stg.acad_career          = stdnt_car_degree.acad_career
    AND     to_date(dim_term.term_end_day_key,'J') 		>= stdnt_car_degree.deg_term_dt-125
    AND     stdnt_car_degree.deg_level              	<= stg_cohort_by_prog_stg.deg_level
WHERE true
;



/* Derive student's enrollment and graduation status in every academic career, program and plan */
drop table if exists stg_ff_ret_grad_prog_stg;

create temp table stg_ff_ret_grad_prog_stg as 
SELECT *
FROM (
    SELECT
        d1.emplid                       aS emplid
        ,d1.acad_career                 AS acad_career
        ,d1.primary_acad_prog           AS primary_acad_prog
        ,FIRST_VALUE (d1.academic_yr) OVER (
            PARTITION BY
                d1.emplid
                ,d1.acad_career
                ,d1.primary_acad_prog
            ORDER BY
                d1.academic_yr
                ,d1.strm
                ,d1.primary_acad_prog
            ROWS BETWEEN unbounded preceding AND CURRENT ROW
        )                               as cohort_academic_yr
        ,d1.strm                        AS strm_plus_0
        ,d1.academic_yr                 AS academic_yr_plus_0
        ,MIN (CASE
            WHEN 		d1.enrolled_ind = 'N'
                and     d1.grad_ind = 'N'
                and     d1.enrolled_cr_ind = 'N'
                and     d1.grad_cr_ind = 'N'
                    THEN 100
            WHEN d1.grad_sp_ind = 'Y'
                    THEN 3
            WHEN d1.grad_cr_ind = 'Y'
                    THEN 4
            WHEN d1.enrolled_ind = 'Y'
                and     d1.grad_ind = 'N'
                    THEN 1
            WHEN d1.enrolled_cr_ind = 'Y'
                and     d1.grad_cr_ind = 'N'
                    THEN 2
            ELSE NULL
          END) status_plus_0
        ,null AS strm_plus_2
        ,null AS academic_yr_plus_2
        ,min (COALESCE(NULL::INTEGER, 0)) AS status_plus_2
        ,null AS strm_plus_4
        ,null AS academic_yr_plus_4
        ,min (COALESCE(NULL::INTEGER, 0)) AS status_plus_4
        ,MIN(CASE
            WHEN 		d1.enrolled_ind = 'N'
                and     d1.grad_ind = 'N'
                and     d1.enrolled_cr_ind = 'N'
                and     d1.grad_cr_ind = 'N'
                    THEN 100
            WHEN 		d1.grad_sp_ay_ind = 'Y'
                    THEN 3
            WHEN 		d1.grad_cr_ay_ind = 'Y'
                    THEN 4
            WHEN 		d1.enrolled_ind = 'Y'
                and     d1.grad_ind = 'N'
                    THEN 1
            WHEN 		d1.enrolled_cr_ind = 'Y'
                and     d1.grad_cr_ind = 'N'
                    THEN 2
            ELSE NULL
          END)  OVER (
                PARTITION BY d1.emplid,d1.academic_yr,d1.acad_career,d1.primary_acad_prog) AS status_academic_yr
        ,ROW_NUMBER() OVER (
                PARTITION BY d1.emplid,d1.academic_yr,d1.acad_career,d1.primary_acad_prog
                ORDER BY (
                    decode(regexp_replace(split_part(d1.strm,'/',1),'([^0-9.])',''),'',null,regexp_replace(split_part(d1.strm,'/',1),'([^0-9.])','') )::int 
                        -
                    decode(regexp_replace(split_part(d1.cohort_strm,'/',1),'([^0-9.])',''),'',null,regexp_replace(split_part(d1.cohort_strm,'/',1),'([^0-9.])','') )::int
                )
                ,dt1.rel_strm
                ,d1.primary_acad_prog
        ) AS rn
        ,'STATUS_TERM_' + CAST (row_number() OVER (
                PARTITION BY d1.emplid,d1.acad_career,d1.primary_acad_prog
                ORDER BY d1.academic_yr,d1.primary_acad_prog) AS VARCHAR
            ) AS status_yr
		, d1.degree_seeking_ind

FROM        stg_distinct_enrollment_prog_stg d1

join        hercules_workday.dim_term dt1
    on      d1.strm = dt1.strm
    --and     d1.mod_yr = 1
	
WHERE true
--and     d1.rn = 1
 GROUP BY
    d1.emplid,d1.strm, dt1.rel_strm, d1.acad_career, d1.primary_acad_prog,d1.enrolled_ind,d1.grad_ind,d1.enrolled_cr_ind,d1.grad_cr_ind,d1.grad_sp_ind,d1.academic_yr
    ,d1.cohort_strm,d1.grad_sp_ay_ind, d1.grad_cr_ay_ind, d1.degree_seeking_ind
)
WHERE status_yr IN ('STATUS_TERM_1','STATUS_TERM_2','STATUS_TERM_3','STATUS_TERM_4','STATUS_TERM_5','STATUS_TERM_6','STATUS_TERM_7','STATUS_TERM_8','STATUS_TERM_9','STATUS_TERM_10',
					'STATUS_TERM_11','STATUS_TERM_12','STATUS_TERM_13','STATUS_TERM_14','STATUS_TERM_15','STATUS_TERM_16','STATUS_TERM_17','STATUS_TERM_18','STATUS_TERM_19','STATUS_TERM_20','STATUS_TERM_21'
					) -- Filter to just show the first 10 years
;



drop table if exists stg_ff_retention_grad_prog_tmp_pre;

create temp table stg_ff_retention_grad_prog_tmp_pre(
     src_sys_cd
    ,emplid
    ,cohort_strm
    ,cohort_academic_yr
    ,acad_career
    ,student_campus
    ,acad_plan_cd
    ,primary_acad_prog
    ,univ_attend_type_sd
    ,program_cd
    ,school_cd
    ,prior_plan_cd
    ,prior_program_cd
    ,prior_school_cd
    ,next_plan_cd
    ,next_program_cd
    ,next_school_cd
    ,fts_ind
    ,strm_plus_0
    ,academic_yr_plus_0
    ,status_plus_0
    ,strm_plus_2
    ,academic_yr_plus_2
    ,status_plus_2
    ,strm_plus_4
    ,academic_yr_plus_4
    ,status_plus_4
    ,academic_year_status
    ,status_year_cd
    ,first_strm_flag
    ,stdnt_car_nbr
    ,appl_current_location
    ,student_career
    ,cohort_flag
    ,inst_cohort_ind
    ,exclusion_flag
    ,orig_cohort_strm
) as 
SELECT
    emplid
        ||'~'
        ||'~'||acad_career
        ||'~'||cohort_strm
        ||'~'||primary_acad_prog
        ||'~'||strm_plus_0 as src_sys_cd
    ,emplid
    ,cohort_strm
    ,cohort_academic_yr
    ,acad_career
    ,student_campus
    ,acad_plan_cd
    ,primary_acad_prog
    ,univ_attend_type_sd
    ,program_cd
    ,school_cd
    ,prior_plan_cd
    ,prior_program_cd
    ,prior_school_cd
    ,next_plan_cd
    ,next_program_cd
    ,next_school_cd
    ,fts_ind
    ,strm_plus_0
    ,academic_yr_plus_0
    ,status_plus_0
    ,strm_plus_2
    ,academic_yr_plus_2
    ,status_plus_2
    ,strm_plus_4
    ,academic_yr_plus_4
    ,status_plus_4
    ,academic_year_status
    ,status_year_cd
    ,first_strm_flag
    ,stdnt_car_nbr
    ,appl_current_location
    ,student_career
    ,cohort_flag
    ,inst_cohort_ind
    ,exclusion_flag
    ,orig_cohort_Strm
FROM (
    select
        --{cohort_flag} AS cohort_flag
		case
           WHEN cohort.degree_seeking_ind = 'Y'
                    AND (cohort.acad_plan =
                        FIRST_VALUE (cohort.acad_plan) OVER (
                            PARTITION BY
                                cohort.emplid,cohort.acad_career
                            ORDER BY
                                 decode(cohort.degree_seeking_ind,'Y',1,2)
                                ,cohort.rel_strm
                                ,decode(cohort.univ_attend_type_sd,'Returning',2,'New',1,100)
                                ,cohort.primary_acad_prog
                                ,cohort.acad_plan ROWS BETWEEN unbounded preceding AND unbounded following
                        )
                    )
                    AND (cohort.strm =
                        FIRST_VALUE (cohort.strm)  OVER (
                            PARTITION BY
                                cohort.emplid,cohort.acad_career
                            ORDER BY
                                 decode(cohort.degree_seeking_ind,'Y',1,2)
                                ,cohort.rel_strm
                                ,decode(cohort.univ_attend_type_sd,'Returning',2,'New',1,100)
                                ,cohort.primary_acad_prog
                                ,cohort.acad_plan ROWS BETWEEN unbounded preceding AND unbounded following
                        )
                    )
                THEN 'Y'
            ELSE 'N'
        END as cohort_flag
        ,cohort.emplid AS emplid
        ,cohort.strm AS cohort_strm
        ,cohort.academic_yr AS cohort_academic_yr
        ,cohort.acad_career as acad_career
        ,cohort.stdnt_campus_cd AS student_campus
        ,cohort.acad_plan AS acad_plan_cd
        ,cohort.primary_acad_prog AS primary_acad_prog
        ,cohort.univ_attend_type_sd AS univ_attend_type_sd
        ,cohort.program_cd AS program_cd
        ,cohort.school_cd AS school_cd
        ,cohort.prior_plan_cd AS prior_plan_cd
        ,cohort.prior_program_cd AS prior_program_cd
        ,cohort.prior_school_cd AS prior_school_cd
        ,cohort.next_plan_cd AS next_plan_cd
        ,cohort.next_program_cd AS next_program_cd
        ,cohort.next_school_cd AS next_school_cd
        ,cohort.fts_ind AS fts_ind
        ,rgp_stg.strm_plus_0
        ,rgp_stg.academic_yr_plus_0
        ,DECODE (rgp_stg.status_plus_0,100,0,rgp_stg.status_plus_0)
        ,rgp_stg.strm_plus_2
        ,rgp_stg.academic_yr_plus_2
        ,DECODE (rgp_stg.status_plus_2,100,0,rgp_stg.status_plus_2)
        ,rgp_stg.strm_plus_4
        ,rgp_stg.academic_yr_plus_4
        ,DECODE (rgp_stg.status_plus_4,100,0,rgp_stg.status_plus_4)
        ,DECODE (rgp_stg.status_academic_yr,100,0,rgp_stg.status_academic_yr) AS academic_year_status
        ,rgp_stg.status_yr AS status_year_cd
        ,CASE
          WHEN cohort.strm = FIRST_VALUE (cohort.strm) OVER (PARTITION BY cohort.emplid,cohort.acad_career ORDER BY
                                                         CASE 
                                                           WHEN cohort.fts_ind = 'Y'
                                                           THEN 1
                                                           ELSE 2
                                                         END
    ,                                             CASE
                                                           WHEN cohort.univ_attend_type_sd = 'New'
                                                           THEN 1
                                                           WHEN cohort.univ_attend_type_sd = 'Returning'
                                                           THEN 3
                                                           ELSE 2
                                                         END
                                                         ,cohort.strm 
                                                         ,rgp_stg.strm_plus_0
                                                         ROWS BETWEEN unbounded preceding AND unbounded following)
        AND cohort.primary_acad_prog||'~'||cohort.acad_plan = FIRST_VALUE (cohort.primary_acad_prog||'~'||cohort.acad_plan) OVER (PARTITION BY cohort.emplid,cohort.acad_career ORDER BY
                                                                CASE 
                                                                  WHEN cohort.fts_ind = 'Y'
                                                                  THEN 1
                                                                  ELSE 2
                                                                END
    ,                                                    CASE
                                                                  WHEN cohort.univ_attend_type_sd = 'New'
                                                                  THEN 1
                                                                  WHEN cohort.univ_attend_type_sd = 'Returning'
                                                                  THEN 3
                                                                  ELSE 2
                                                                END
                                                                ,cohort.strm
                                                                ,rgp_stg.strm_plus_0
                                                                ROWS BETWEEN unbounded preceding AND unbounded following)
          THEN 'Y'
          ELSE 'N'
        END                                                 AS first_strm_flag
        ,null::INT                      as stdnt_car_nbr
        ,cohort.appl_current_location        as appl_current_location
        ,cohort.acad_career                  as student_career
        ,'N'                            as inst_cohort_ind
        ,null::varchar(3)                as exclusion_flag
        ,cohort.orig_cohort_strm 		as orig_cohort_Strm
    from stg_ff_ret_grad_prog_stg rgp_stg
    join stg_cohort_by_prog_stg cohort
       on cohort.emplid = rgp_stg.emplid
      and cohort.acad_career = rgp_stg.acad_career
      and cohort.primary_acad_prog = rgp_stg.primary_acad_prog
      and cohort.academic_yr = rgp_stg.cohort_academic_yr
    where true
    --and rgp_stg.rn = 1
)
;
truncate  hercules_workday.ff_retention_grad_prog;

insert into hercules_workday.ff_retention_grad_prog(
     src_sys_cd
    ,strm_plus_0
    ,emplid
    ,cohort_strm
    ,status_year_cd
    ,academic_year_status
    ,term_status
    ,student_career
    ,acad_plan_cd
    ,primary_acad_prog
    ,cohort_flag
    ,student_campus
    ,first_strm_flag
    ,first_degree_seeking_strm
    ,ret_term
    ,ret_year
    ,ret_year_status
    ,ret_year_status_collapsed
    ,ret_status
    ,ret_status_collapsed
    ,orig_cohort_strm
    ,EXCLUSION_FLAG
    ,stdnt_car_nbr
    ,cohort_academic_yr
)
SELECT
     x.emplid
        ||'~'
        ||x.student_career
        ||'~'
        ||x.cohort_strm
        ||'~'
        ||x.student_career
        ||'~'
        ||x.strm_plus_0
        ||'~'
        ||x.primary_acad_prog
        ||'~'
        ||x.acad_plan_cd        as src_sys_cd
    ,x.strm_plus_0
    ,x.emplid
    ,x.cohort_strm
    ,x.status_year_cd
    ,x.academic_year_status
    ,x.status_plus_0            AS term_status
    ,x.student_career
    ,x.acad_plan_cd
    ,x.primary_acad_prog
    ,x.cohort_flag
    ,x.student_campus
    ,x.first_strm_flag
    ,(MIN( CASE
         WHEN x.first_strm_flag = 'Y'
         THEN x.cohort_strm ELSE NULL
      END) OVER(
             PARTITION BY x.emplid
    ,             x.student_career ROWS BETWEEN unbounded preceding AND unbounded following)) AS first_degree_seeking_strm
    , CASE
      WHEN status_year_cd = 'STATUS_TERM_1'
      THEN '1 TERM'
      WHEN status_year_cd = 'STATUS_TERM_2'
      THEN '2 TERMS'
      WHEN status_year_cd = 'STATUS_TERM_3'
      THEN '3 TERMS'
      WHEN status_year_cd = 'STATUS_TERM_4'
      THEN '4 TERMS'
      WHEN status_year_cd = 'STATUS_TERM_5'
      THEN '5 TERMS'
      WHEN status_year_cd = 'STATUS_TERM_6'
      THEN '6 TERMS'
      WHEN status_year_cd = 'STATUS_TERM_7'
      THEN '7 TERMS'
      WHEN status_year_cd = 'STATUS_TERM_8'
      THEN '8 TERMS'
      WHEN status_year_cd = 'STATUS_TERM_9'
      THEN '9 TERMS'
      WHEN status_year_cd = 'STATUS_TERM_10'
      THEN '10 TERMS' 
	  WHEN status_year_cd = 'STATUS_TERM_11'
      THEN '11 TERM'
      WHEN status_year_cd = 'STATUS_TERM_12'
      THEN '12 TERMS'
      WHEN status_year_cd = 'STATUS_TERM_13'
      THEN '13 TERMS'
      WHEN status_year_cd = 'STATUS_TERM_14'
      THEN '14 TERMS'
      WHEN status_year_cd = 'STATUS_TERM_15'
      THEN '15 TERMS'
      WHEN status_year_cd = 'STATUS_TERM_16'
      THEN '16 TERMS'
      WHEN status_year_cd = 'STATUS_TERM_17'
      THEN '17 TERMS'
      WHEN status_year_cd = 'STATUS_TERM_18'
      THEN '18 TERMS'
      WHEN status_year_cd = 'STATUS_TERM_19'
      THEN '19 TERMS'
      WHEN status_year_cd = 'STATUS_TERM_20'
      THEN '20 TERMS' 
	  WHEN status_year_cd = 'STATUS_TERM_21'
      THEN '21 TERMS' 
	  ELSE NULL
   END AS ret_term
   , CASE
      WHEN status_year_cd = 'STATUS_TERM_1'
      THEN '1ST YEAR'
      WHEN status_year_cd = 'STATUS_TERM_2'
      THEN '1ST YEAR'
      WHEN status_year_cd = 'STATUS_TERM_3'
      THEN '2ND YEAR'
      WHEN status_year_cd = 'STATUS_TERM_4'
      THEN '2ND YEAR'
      WHEN status_year_cd = 'STATUS_TERM_5'
      THEN '3RD YEAR'
      WHEN status_year_cd = 'STATUS_TERM_6'
      THEN '3RD YEAR'
      WHEN status_year_cd = 'STATUS_TERM_7'
      THEN '4TH YEAR'
      WHEN status_year_cd = 'STATUS_TERM_8'
      THEN '4TH YEAR'
      WHEN status_year_cd = 'STATUS_TERM_9'
      THEN '5TH YEAR'
      WHEN status_year_cd = 'STATUS_TERM_10'
      THEN '5TH YEAR'
      WHEN status_year_cd = 'STATUS_TERM_11'
      THEN '6TH YEAR'
      WHEN status_year_cd = 'STATUS_TERM_12'
      THEN '6TH YEAR'
      WHEN status_year_cd = 'STATUS_TERM_13'
      THEN '7TH YEAR'
      WHEN status_year_cd = 'STATUS_TERM_14'
      THEN '7TH YEAR'
      WHEN status_year_cd = 'STATUS_TERM_15'
      THEN '8TH YEAR'
      WHEN status_year_cd = 'STATUS_TERM_16'
      THEN '8TH YEAR'
      WHEN status_year_cd = 'STATUS_TERM_17'
      THEN '9TH YEAR'
      WHEN status_year_cd = 'STATUS_TERM_18'
      THEN '9TH YEAR'
      WHEN status_year_cd = 'STATUS_TERM_19'
      THEN '10TH YEAR' 
	  WHEN status_year_cd = 'STATUS_TERM_20'
      THEN '10TH YEAR' 
	  WHEN status_year_cd = 'STATUS_TERM_21'
      THEN '11TH YEAR' 
	  ELSE NULL
   END AS ret_year
    ,CASE
      WHEN x.academic_year_status = 0 AND nsc_st.emplid IS NOT NULL
      THEN 'TRANSFER'
      WHEN x.academic_year_status = 0 AND nsc_st.emplid IS NULL
      THEN 'NOT ENROLLED'
      WHEN x.academic_year_status = 1
      THEN 'ENROLLED'
      WHEN x.academic_year_status = 2
      THEN 'ENROLLED ALT PROGRAM'
      WHEN x.academic_year_status = 3
      THEN 'GRADUATED'
      WHEN x.academic_year_status = 4
      THEN 'GRADUATED ALT PROGRAM' ELSE 'N/A'
   END AS ret_year_status
    ,CASE
      WHEN x.academic_year_status = 0 AND nsc_st.emplid IS NOT NULL
      THEN 'TRANSFER'
      WHEN x.academic_year_status = 0 AND nsc_st.emplid IS NULL
      THEN 'NOT ENROLLED'
      WHEN x.academic_year_status = 1
      THEN 'ENROLLED'
      WHEN x.academic_year_status = 2
      THEN 'ENROLLED'
      WHEN x.academic_year_status = 3
      THEN 'GRADUATED'
      WHEN x.academic_year_status = 4
      THEN 'GRADUATED' ELSE 'N/A'
   END AS ret_year_status_collapsed
    ,CASE
      WHEN x.status_plus_0 = 0 AND nsc_st.emplid IS NOT NULL
      THEN 'TRANSFER'
      WHEN x.status_plus_0 = 0 AND nsc_st.emplid IS NULL
      THEN 'NOT ENROLLED'
      WHEN x.status_plus_0 = 1
      THEN 'ENROLLED'
      WHEN x.status_plus_0 = 2
      THEN 'ENROLLED ALT PROGRAM'
      WHEN x.status_plus_0 = 3
      THEN 'GRADUATED'
      WHEN x.status_plus_0 = 4
      THEN 'GRADUATED ALT PROGRAM' ELSE 'N/A'
   END AS ret_status
    ,CASE
      WHEN x.status_plus_0 = 0 AND nsc_st.emplid IS NOT NULL
      THEN 'TRANSFER'
      WHEN x.status_plus_0 = 0 AND nsc_st.emplid IS NULL
      THEN 'NOT ENROLLED'
      WHEN x.status_plus_0 = 1
      THEN 'ENROLLED'
      WHEN x.status_plus_0 = 2
      THEN 'ENROLLED'
      WHEN x.status_plus_0 = 3
      THEN 'GRADUATED'
      WHEN x.status_plus_0 = 4
      THEN 'GRADUATED' ELSE 'N/A'
   END AS ret_status_collapsed
   , x.orig_cohort_strm
   , CASE WHEN x.orig_cohort_strm <> x.cohort_strm 
     AND first_value(x.status_plus_0) over ( partition by 1 order by x.strm_plus_0  ROWS BETWEEN unbounded preceding AND unbounded following) = 0
     THEN 'Y' 
     ELSE 'N'
     end EXCLUSION_FLAG
    , x.stdnt_car_nbr
    ,x.cohort_academic_yr
FROM stg_ff_retention_grad_prog_tmp_pre x
   LEFT JOIN hercules_workday.ff_student_tracker nsc_st
      ON 
             nsc_st.pri_enrl_college_code_branch <> null
         AND x.strm_plus_0 = nsc_st.enrl_strm
         AND x.emplid = nsc_st.emplid
WHERE 1 = 1
 
 ;
 
 
commit;


analyze hercules_workday.ff_retention_grad_prog;