

truncate table hercules_workday.fact_finaid ;

insert into hercules_workday.fact_finaid
(
 src_sys_cd,
 src_sys_eff_dt,
 emplid,
 uid,
 acad_career, 
 aid_year,
 strm,
 item_type,
 award_status,
 offer_amount,
 accept_amount,
 currency_cd,
 cancel_decline_amt,
 offered_dt,
 award_strm,
 disb_strm,
 fin_aid_type,
 fed_ind
)
select 
 rf.student_wid ||'~'|| rf.academic_period_id ||'~'|| rf.student_award_item_id as src_sys_cd,
 rf.load_date as src_sys_eff_dt,
 dp.emplid as emplid,
 rf.universal_id as uid,
 decode(rf.academic_level_financial_aid_descriptor,'Undergraduate', 'UG','Non_Credit','NC','Graduate','GRAD','Law','Law', null,'*') as acad_career, 
 SUBSTRING(rf.financial_aid_year_descriptor FROM POSITION('-' IN rf.financial_aid_year_descriptor) + 1 for 4) as aid_year,
 replace(rf.academic_period_id,'_',' ') as strm,
 rf.student_award_item_id as item_type,
 rf.student_award_status_id as award_status,
 rf.student_award_amount as offer_amount,
 case when rf.student_award_status_id in ('Accepted') then rf.student_award_amount else 0 end as accept_amount,
 sai.currency_id as currency_cd,
 case when rf.student_award_status_id in ('Cancelled','Declined') then rf.student_award_amount else 0 end as cancel_decline_amt,
 rf.package_date as offered_dt,
 rf.academic_period_id as award_strm,
 rf.academic_period_id as disb_strm,
 sai.student_award_type_id as fin_aid_type,
 sai.source_id as fed_ind
 from workday_ods.raas_student_financial_aid rf
left join hercules_workday.dim_person dp
on rf.universal_id = dp.uid --99512
left JOIN 	workday_ods.student_award_item sai 
ON rf.student_award_item_id::text = sai.student_award_item_id::text
;

commit;

analyze hercules_workday.fact_finaid ;
