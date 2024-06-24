/*
   Object type      : Dimension (SCD Type 1)
   Description      : Records university academic term definition information.
   Grain            : terms_id

   Target table name: DIM_TERM

   Source table list: terms_nf
                      
                      

   ETL load type    : Full load everyday

   Date created     : 
   Date updated     : 
*/

drop table if exists aid_years;


create temp table aid_years as
select *
from (
  select  
         terms.id as strm
      ,  right(terms.academic_period_name,4)::int +
         case when terms.academic_period_id ~ 'Fall' or terms.academic_period_id ~ 'Summer' then 1 else 0 end as aid_year
      ,  row_number() OVER (PARTITION BY  terms.id ORDER BY (SUBSTRING(rf.academic_period_id FROM POSITION('-' IN rf.academic_period_id) + 1 for 4)) desc ) as rn
  from workday_ods.raas_student_financial_aid rf
  left join workday_ods.maintained_academic_period terms
  on terms.academic_period_id  = rf.academic_period_id
  --or terms.id                   = SPLIT_PART(f19.id, '*', 3)
  where 1=1
  )
where rn = 1
;

TRUNCATE TABLE hercules_workday.dim_term;

INSERT INTO hercules_workday.dim_term
(
     src_sys_cd
    --,src_sys_eff_dt
    --,src_sys_exp_dt
    --,eff_dt
    --,exp_dt
    ,strm
    ,alt_term_cd
    ,academic_yr
    ,bhe_eis_term_cd
    ,last_enrl_day_key
    ,term_year_ld
    ,year_term_ld
    ,bhe_tss_term_cd
    ,term_end_day_key
    ,term_begin_day_key
    ,std_end_day_key
    ,bhe_eis_report_year
    ,term_type_ld
    ,term_seq_acad_yr
    ,first_enrl_day_key
    ,fiscal_yr
    ,std_start_day_key
    ,academic_yr_range
    ,calendar_yr
    ,term_type_cd
    ,term_seq_cal_yr
    ,acad_career
    ,adam_term
    ,institution
    ,session_code
    ,collapsed_strm
    ,collapsed_term_year_ld
    ,collapsed_year_term_ld
    ,aid_yr
    ,ssr_trmac_last_dt
    ,ssr_ssenrldisp_bdt
    ,ssr_ssenrldisp_edt
    ,ssr_ssplnrdisp_bdt
    ,ssr_ssplnrdisp_edt
    ,ssr_sswhifadvr_bdt
    ,ssr_sswhifadvr_edt
    ,ssr_sswhifprem_bdt
    ,ssr_sswhifprem_edt
    ,ssr_sswhifstd_bdt
    ,ssr_sswhifstd_edt
    ,year_term_sd
    ,sixty_pct_dt
    ,weeks_of_instruct
    ,term_category
    ,cal_begin_day
    ,cal_end_day
    ,next_yr_strm
    ,last_yr_strm
    ,next_term_begin_day_key
    ,lst_wd_wo_pen_dt
    ,census_dt_key
    ,fin_aid_collapsed_strm
    ,completion_collapsed_strm
    ,degree_yr
	,cal_type
	,rel_strm
    ,next_strm_1
	,next_strm_2
	,next_strm_3
	,next_strm_4
)
select term.id as src_sys_cd,
	--	term.load_timestamp as src_sys_eff_dt,
	--	term.expir_timestamp as src_sys_exp_dt,
	--  term.load_timestamp as eff_dt,
	--	term.expir_timestamp as exp_dt,
	replace(term.id,'_',' ') as strm,
	LPAD(ROW_NUMBER() OVER (PARTITION BY RIGHT(term.academic_period_name,4) order by term.academic_period_start_date, term.academic_period_end_date)::varchar, 2 ,'0') 
	AS alt_term_cd,
	case 
		when SUBSTRING(term.academic_period_name from length(term.academic_period_name)-3)::TEXT =
	EXTRACT(YEAR FROM TO_DATE(term.academic_period_start_date, 'YYYY-MM-DD'))::TEXT then SUBSTRING(term.academic_period_name from length(term.academic_period_name)-3)::TEXT
	else EXTRACT(YEAR FROM TO_DATE(term.academic_period_start_date, 'YYYY-MM-DD'))::TEXT
	end as academic_yr,
	DECODE ( (CASE
		WHEN POSITION('/' IN term.id) > 0 THEN
		SUBSTRING(term.id, POSITION('/' IN id) + 1)
		ELSE
		term.id
	END)
	,'FA','01','WS','02','SP','03','SU','04','*')  AS bhe_eis_term_cd,
	to_char(term.academic_period_end_date::date, 'J')::numeric  AS last_enrl_day_key,
	term.academic_period_name as term_year_ld,
	NULL  AS year_term_ld,
	DECODE ((CASE
		WHEN POSITION('/' IN term.id) > 0 THEN
		SUBSTRING(id, POSITION('/' IN term.id) + 1)
		ELSE
		term.id
	END),'FA','01','WS','02','SP','03','SU','04','*')   AS bhe_tss_term_cd,
	to_char(term.academic_period_end_date::date, 'J')::numeric  as term_end_day_key,
	to_char(term.academic_period_start_date::date, 'J')::numeric  as term_begin_day_key,
	to_char(term.academic_period_end_date::date, 'J')::numeric  AS std_end_day_key,
	NULL   AS bhe_eis_report_year,
	term.Academic_periods_offered_type_id as term_type_ld,
	NULL AS term_seq_acad_yr,
	to_char(term.academic_period_start_date::date, 'J')::numeric AS first_enrl_day_key,
	case 
		when to_char(term.academic_period_start_date::date,'MM')::int between 7 and 12 then  to_char(term.academic_period_start_date::date,'YYYY')::int+1 
		else to_char(term.academic_period_start_date::date,'YYYY')::int 
		end AS fiscal_yr,
	to_char(term.academic_period_standard_start_date::date, 'J')::numeric  as std_start_day_key,
	NULL as academic_yr_range,
	case 
		when SUBSTRING(term.academic_period_name from length(term.academic_period_name)-3)::TEXT =
	EXTRACT(YEAR FROM TO_DATE(term.academic_period_start_date, 'YYYY-MM-DD'))::TEXT then SUBSTRING(term.academic_period_name from length(term.academic_period_name)-3)::TEXT
	else EXTRACT(YEAR FROM TO_DATE(term.academic_period_start_date, 'YYYY-MM-DD'))::TEXT
	end as calender_yr,
	left(upper(split_part(term.academic_periods_offered_type_id,' ', 1)),2) as term_type_cd,
	0 as term_seq_cal_yr,
	'*' as acad_career,
	NULL as adam_term,
	'SUFLK' as Institution,
	NULL as session_code,
	term.academic_period_name AS collapsed_strm,
	term.academic_period_name AS collapsed_term_year_ld,
	NULL AS collapsed_year_term_ld,
	aid_years.aid_year AS aid_yr,
	NULL AS ssr_trmac_last_dt,
	NULL AS ssr_ssenrldisp_bdt,
	NULL AS ssr_ssenrldisp_edt,
	NULL AS ssr_ssplnrdisp_bdt,
	NULL AS ssr_ssplnrdisp_edt,
	NULL AS ssr_sswhifadvr_bdt,
	NULL AS ssr_sswhifadvr_edt,
	NULL AS ssr_sswhifprem_bdt,
	NULL AS ssr_sswhifprem_edt,
	NULL AS ssr_sswhifstd_bdt,
	NULL AS ssr_sswhifstd_edt,
	NULL AS year_term_sd,
	NULL AS sixty_pct_dt,
	NULL AS weeks_of_instruct,
	NULL AS term_category,
	term.academic_period_start_date::date as cal_begin_day,
	term.academic_period_end_date::date as cal_end_day,

 CASE 
       WHEN term.id similar to '[A-Za-z]%' THEN '*'
       ELSE LPAD((split_part(term.id, '/', 1)::int + 1)::varchar, 2, '0')||'/'||split_part(term.id, '/', 2)                          
    END       AS next_yr_strm
    ,CASE 
       WHEN term.id similar to '[A-Za-z]%' THEN '*'
       ELSE LPAD((split_part(term.id, '/', 1)::int - 1)::varchar, 2, '0')||'/'||split_part(term.id, '/', 2)                        
    END     AS last_yr_strm
    ,NULL AS next_term_begin_day_key
	 ,NULL AS lst_wd_wo_pen_dt
	 ,NULL AS census_dt_key
	 ,NULL AS fin_aid_collapsed_strm
	 ,NULL AS completion_collapsed_strm
	 ,NULL as degree_yr
	 ,case  when coalesce(term.academic_periods_offered_type_id, term.academic_period_name) ~ 'Law' then 'LAW'
			when coalesce(term.academic_periods_offered_type_id, term.academic_period_name) ~ 'Quarter' then 'QTR'
			when coalesce(term.academic_periods_offered_type_id, term.academic_period_name) ~ 'Continuing Education' then 'CON'
			when coalesce(term.academic_periods_offered_type_id, term.academic_period_name) ~ 'Intersession' then 'INT'
			when coalesce(term.academic_periods_offered_type_id, term.academic_period_name) ~ 'Fall 2|Spring 2|Summer 2' then 'SES'
			when coalesce(term.academic_periods_offered_type_id, term.academic_period_name) ~ 'Fall 1|Spring 1|Summer 1' then 'SES'
			when coalesce(term.academic_periods_offered_type_id, term.academic_period_name) ~ 'Fiscal Year' then 'SES'
			when term.id ~ '/' then 'LEG'
			else 'SEM'
			end as cal_type
	 ,ROW_NUMBER() OVER (ORDER BY term.academic_period_start_date) AS rel_strm
     ,lead(replace(term.id,'_',' '), 1) over (partition by cal_type order by term.academic_period_start_date) as next_strm_1
	 ,lead(replace(term.id,'_',' '), 2) over (partition by cal_type order by term.academic_period_start_date) as next_strm_2
	 ,lead(replace(term.id,'_',' '), 3) over (partition by cal_type order by term.academic_period_start_date) as next_strm_3
	 ,lead(replace(term.id,'_',' '), 4) over (partition by cal_type order by term.academic_period_start_date) as next_strm_4
from workday_ods.maintained_academic_period term

left join aid_years aid_years
    on term.id = aid_years.strm
where 1=1
;

commit;

analyze hercules_workday.dim_term;
