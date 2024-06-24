/*##############################################################################
Object type       : CROSS SEEDING TABLE
Description       :
Grain             : EMPLID, ACAD_CAREER, AID_YEAR
Target table name : CS_FINANCIAL_AID
Source table list : DIM_FINAID_TYPE
                    FACT_FINAID
                    PERSEUS_LOOKUP
ETL load type     : FULL LOAD EVERYDAY
Date created      : 01-MARCH-2019
Date updated      :
Update notes      :
##############################################################################*/

truncate table perseus_workday.cs_financial_aid;

INSERT INTO perseus_workday.cs_financial_aid (
     emplid
    ,acad_career
    ,aid_year
    ,first_term_flag
    ,fa_type
    ,scholarship_disbursed_amount
    ,vabenefits_disbursed_amount
    ,loan_disbursed_amount
    ,grant_disbursed_amount
    ,workstudy_disbursed_amount
    ,waiver_disbursed_amount
    ,fellowship_disbursed_amount
    ,other_disbursed_amount
    ,federal_grant_disbursed_amount
    ,institutional_scholarship_disbursed_amount
    ,private_grant_disbursed_amount
    ,private_scholarship_disbursed_amount
    ,private_loan_disbursed_amount
    ,other_scholarship_disbursed_amount
    ,institutional_other_disbursed_amount
    ,institutional_grant_disbursed_amount
    ,federal_workstudy_disbursed_amount
    ,state_grant_disbursed_amount
    ,federal_loan_disbursed_amount
    ,state_scholarship_disbursed_amount
    ,disbursed_amount
    ,federal_grant_accept_amount
    ,institutional_scholarship_accept_amount
    ,private_grant_accept_amount
    ,private_scholarship_accept_amount
    ,private_loan_accept_amount
    ,other_scholarship_accept_amount
    ,institutional_other_accept_amount
    ,institutional_grant_accept_amount
    ,federal_workstudy_accept_amount
    ,state_grant_accept_amount
    ,federal_loan_accept_amount
    ,state_scholarship_accept_amount
    ,accept_amount
    ,federal_grant_offer_amount
    ,institutional_scholarship_offer_amount
    ,private_grant_offer_amount
    ,private_scholarship_offer_amount
    ,private_loan_offer_amount
    ,other_scholarship_offer_amount
    ,institutional_other_offer_amount
    ,institutional_grant_offer_amount
    ,federal_workstudy_offer_amount
    ,state_grant_offer_amount
    ,federal_loan_offer_amount
    ,state_scholarship_offer_amount
    ,offer_amount
    ,tot_fa_type
    ,tot_scholarship_disbursed_amount
    ,tot_vabenefits_disbursed_amount
    ,tot_loan_disbursed_amount
    ,tot_grant_disbursed_amount
    ,tot_workstudy_disbursed_amount
    ,tot_waiver_disbursed_amount
    ,tot_fellowship_disbursed_amount
    ,tot_other_disbursed_amount
    ,tot_disbursed_amount
    ,federal_sub_load_offer_amount
    ,federal_sub_load_disbursed_amount
    ,other_grant_offer_amount
    ,other_grant_disbursed_amount
    ,fa_pell_disb_flag
)
SELECT a.emplid
    ,a.acad_career
    ,a.aid_year
    ,CASE
      WHEN a.aid_year = (MIN(a.aid_year) OVER(PARTITION BY a.emplid,a.acad_career ROWS BETWEEN unbounded preceding AND unbounded following))
      THEN 'Y' ELSE 'N'
   END AS first_term_flag
    ,CASE
      WHEN a.scholarship_disbursed_balance = GREATEST(a.scholarship_disbursed_balance,a.vabenefits_disbursed_balance,a.loan_disbursed_balance,a.grant_disbursed_balance,a.workstudy_disbursed_balance,a.waiver_disbursed_balance,a.fellowship_disbursed_balance,a.other_disbursed_balance)
      THEN 'SCHOLARSHIP'
      WHEN a.vabenefits_disbursed_balance = GREATEST(a.scholarship_disbursed_balance,a.vabenefits_disbursed_balance,a.loan_disbursed_balance,a.grant_disbursed_balance,a.workstudy_disbursed_balance,a.waiver_disbursed_balance,a.fellowship_disbursed_balance,a.other_disbursed_balance)
      THEN 'VA BENEFITS'
      WHEN a.loan_disbursed_balance = GREATEST(a.scholarship_disbursed_balance,a.vabenefits_disbursed_balance,a.loan_disbursed_balance,a.grant_disbursed_balance,a.workstudy_disbursed_balance,a.waiver_disbursed_balance,a.fellowship_disbursed_balance,a.other_disbursed_balance)
      THEN 'LOANS'
      WHEN a.grant_disbursed_balance = GREATEST(a.scholarship_disbursed_balance,a.vabenefits_disbursed_balance,a.loan_disbursed_balance,a.grant_disbursed_balance,a.workstudy_disbursed_balance,a.waiver_disbursed_balance,a.fellowship_disbursed_balance,a.other_disbursed_balance)
      THEN 'GRANT'
      WHEN a.workstudy_disbursed_balance = GREATEST(a.scholarship_disbursed_balance,a.vabenefits_disbursed_balance,a.loan_disbursed_balance,a.grant_disbursed_balance,a.workstudy_disbursed_balance,a.waiver_disbursed_balance,a.fellowship_disbursed_balance,a.other_disbursed_balance)
      THEN 'WORKSTUDY'
      WHEN a.waiver_disbursed_balance = GREATEST(a.scholarship_disbursed_balance,a.vabenefits_disbursed_balance,a.loan_disbursed_balance,a.grant_disbursed_balance,a.workstudy_disbursed_balance,a.waiver_disbursed_balance,a.fellowship_disbursed_balance,a.other_disbursed_balance)
      THEN 'WAIVER'
      WHEN a.fellowship_disbursed_balance = GREATEST(a.scholarship_disbursed_balance,a.vabenefits_disbursed_balance,a.loan_disbursed_balance,a.grant_disbursed_balance,a.workstudy_disbursed_balance,a.waiver_disbursed_balance,a.fellowship_disbursed_balance,a.other_disbursed_balance)
      THEN 'FELLOWSHIP'
      WHEN a.other_disbursed_balance = GREATEST(a.scholarship_disbursed_balance,a.vabenefits_disbursed_balance,a.loan_disbursed_balance,a.grant_disbursed_balance,a.workstudy_disbursed_balance,a.waiver_disbursed_balance,a.fellowship_disbursed_balance,a.other_disbursed_balance)
      THEN 'OTHER' ELSE 'NO FINANCIAL AID'
   END AS fa_type
    ,a.scholarship_disbursed_balance                          as scholarship_disbursed_amount
    ,a.vabenefits_disbursed_balance                           as vabenefits_disbursed_amount
    ,a.loan_disbursed_balance                                 as loan_disbursed_amount
    ,a.grant_disbursed_balance                                as grant_disbursed_amount
    ,a.workstudy_disbursed_balance                            as workstudy_disbursed_amount
    ,a.waiver_disbursed_balance                               as waiver_disbursed_amount
    ,a.fellowship_disbursed_balance                           as fellowship_disbursed_amount
    ,a.other_disbursed_balance                                as other_disbursed_amount
    ,a.federal_grant_disbursed_balance                        as federal_grant_disbursed_amount
    ,a.institutional_scholarship_disbursed_balance            as institutional_scholarship_disbursed_amount
    ,a.private_grant_disbursed_balance                        as private_grant_disbursed_amount
    ,a.private_scholarship_disbursed_balance                  as private_scholarship_disbursed_amount
    ,a.private_loan_disbursed_balance                         as private_loan_disbursed_amount
    ,a.other_scholarship_disbursed_balance                    as other_scholarship_disbursed_amount
    ,a.institutional_other_disbursed_balance                  as institutional_other_disbursed_amount
    ,a.institutional_grant_disbursed_balance                  as institutional_grant_disbursed_amount
    ,a.federal_workstudy_disbursed_balance                    as federal_workstudy_disbursed_amount
    ,a.state_grant_disbursed_balance                          as state_grant_disbursed_amount
    ,a.federal_loan_disbursed_balance                         as federal_loan_disbursed_amount
    ,a.state_scholarship_disbursed_balance                    as state_scholarship_disbursed_amount
    ,a.disbursed_balance                                      as disbursed_amount
    ,a.federal_grant_accept_amount
    ,a.institutional_scholarship_accept_amount
    ,a.private_grant_accept_amount
    ,a.private_scholarship_accept_amount
    ,a.private_loan_accept_amount
    ,a.other_scholarship_accept_amount
    ,a.institutional_other_accept_amount
    ,a.institutional_grant_accept_amount
    ,a.federal_workstudy_accept_amount
    ,a.state_grant_accept_amount
    ,a.federal_loan_accept_amount
    ,a.state_scholarship_accept_amount
    ,a.accept_amount
    ,a.federal_grant_offer_amount
    ,a.institutional_scholarship_offer_amount
    ,a.private_grant_offer_amount
    ,a.private_scholarship_offer_amount
    ,a.private_loan_offer_amount
    ,a.other_scholarship_offer_amount
    ,a.institutional_other_offer_amount
    ,a.institutional_grant_offer_amount
    ,a.federal_workstudy_offer_amount
    ,a.state_grant_offer_amount
    ,a.federal_loan_offer_amount
    ,a.state_scholarship_offer_amount
    ,a.offer_amount
    ,CASE
        WHEN a1.scholarship_disbursed_balance = GREATEST(a1.scholarship_disbursed_balance,a1.vabenefits_disbursed_balance,a1.loan_disbursed_balance,a1.grant_disbursed_balance,a1.workstudy_disbursed_balance,a1.waiver_disbursed_balance,a1.fellowship_disbursed_balance,a1.other_disbursed_balance)
        THEN 'SCHOLARSHIP'
        WHEN a1.vabenefits_disbursed_balance = GREATEST(a1.scholarship_disbursed_balance,a1.vabenefits_disbursed_balance,a1.loan_disbursed_balance,a1.grant_disbursed_balance,a1.workstudy_disbursed_balance,a1.waiver_disbursed_balance,a1.fellowship_disbursed_balance,a1.other_disbursed_balance)
        THEN 'VA BENEFITS'
        WHEN a1.loan_disbursed_balance = GREATEST(a1.scholarship_disbursed_balance,a1.vabenefits_disbursed_balance,a1.loan_disbursed_balance,a1.grant_disbursed_balance,a1.workstudy_disbursed_balance,a1.waiver_disbursed_balance,a1.fellowship_disbursed_balance,a1.other_disbursed_balance)
        THEN 'LOANS'
        WHEN a1.grant_disbursed_balance = GREATEST(a1.scholarship_disbursed_balance,a1.vabenefits_disbursed_balance,a1.loan_disbursed_balance,a1.grant_disbursed_balance,a1.workstudy_disbursed_balance,a1.waiver_disbursed_balance,a1.fellowship_disbursed_balance,a1.other_disbursed_balance)
        THEN 'GRANT'
        WHEN a1.workstudy_disbursed_balance = GREATEST(a1.scholarship_disbursed_balance,a1.vabenefits_disbursed_balance,a1.loan_disbursed_balance,a1.grant_disbursed_balance,a1.workstudy_disbursed_balance,a1.waiver_disbursed_balance,a1.fellowship_disbursed_balance,a1.other_disbursed_balance)
        THEN 'WORKSTUDY'
        WHEN a1.waiver_disbursed_balance = GREATEST(a1.scholarship_disbursed_balance,a1.vabenefits_disbursed_balance,a1.loan_disbursed_balance,a1.grant_disbursed_balance,a1.workstudy_disbursed_balance,a1.waiver_disbursed_balance,a1.fellowship_disbursed_balance,a1.other_disbursed_balance)
        THEN 'WAIVER'
        WHEN a1.fellowship_disbursed_balance = GREATEST(a1.scholarship_disbursed_balance,a1.vabenefits_disbursed_balance,a1.loan_disbursed_balance,a1.grant_disbursed_balance,a1.workstudy_disbursed_balance,a1.waiver_disbursed_balance,a1.fellowship_disbursed_balance,a1.other_disbursed_balance)
        THEN 'FELLOWSHIP'
        WHEN a1.other_disbursed_balance = GREATEST(a1.scholarship_disbursed_balance,a1.vabenefits_disbursed_balance,a1.loan_disbursed_balance,a1.grant_disbursed_balance,a1.workstudy_disbursed_balance,a1.waiver_disbursed_balance,a1.fellowship_disbursed_balance,a1.other_disbursed_balance)
        THEN 'OTHER' ELSE 'NO FINANCIAL AID'
    END AS tot_fa_type
    ,a1.scholarship_disbursed_balance AS tot_scholarship_disbursed_amount
    ,a1.vabenefits_disbursed_balance  AS tot_vabenefits_disbursed_amount
    ,a1.loan_disbursed_balance        AS tot_loan_disbursed_amount
    ,a1.grant_disbursed_balance       AS tot_grant_disbursed_amount
    ,a1.workstudy_disbursed_balance   AS tot_workstudy_disbursed_amount
    ,a1.waiver_disbursed_balance      AS tot_waiver_disbursed_amount
    ,a1.fellowship_disbursed_balance  AS tot_fellowship_disbursed_amount
    ,a1.other_disbursed_balance       AS tot_other_disbursed_amount
    ,a1.disbursed_balance             AS tot_disbursed_amount
    ,a.federal_sub_load_offer_amount
    ,a.federal_sub_load_disbursed_amount
    ,a.other_grant_offer_amount
    ,a.other_grant_disbursed_amount
    ,case
        when a.pell_disb > 0
            then 'Y'
        else 'N'
    end fa_pell_disb_flag
    
FROM (SELECT fa.aid_year
    ,   fa.emplid
    ,   fa.acad_career
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'FEDERAL' AND fa.fin_aid_type = 'GRANT'
         THEN fa.offer_amount ELSE NULL
      END) AS federal_grant_offer_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'INSTITUTIONAL' AND fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.offer_amount ELSE NULL
      END) AS institutional_scholarship_offer_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'PRIVATE' AND fa.fin_aid_type = 'GRANT'
         THEN fa.offer_amount ELSE NULL
      END) AS private_grant_offer_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'PRIVATE' AND fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.offer_amount ELSE NULL
      END) AS private_scholarship_offer_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'PRIVATE' AND fa.fin_aid_type = 'LOAN'
         THEN fa.offer_amount ELSE NULL
      END) AS private_loan_offer_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr in ('MILITARY','FOUNDATION') AND fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.offer_amount ELSE NULL
      END) AS other_scholarship_offer_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'INSTITUTIONAL' AND ( fa.fin_aid_type != 'GRANT' AND fa.fin_aid_type != 'SCHOLARSHIP' AND fa.fin_aid_type != 'LOAN' AND fa.fin_aid_type != 'WORK_STUDY' AND fa.fin_aid_type != 'WAIVER' AND fa.fin_aid_type != 'FELLOWSHIP' AND fa.fin_aid_type != 'VA_BENEFIT')
         THEN fa.offer_amount ELSE NULL
      END) AS institutional_other_offer_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'INSTITUTIONAL' AND fa.fin_aid_type = 'GRANT'
         THEN fa.offer_amount ELSE NULL
      END) AS institutional_grant_offer_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'FEDERAL' AND fa.fin_aid_type = 'WORK_STUDY'
         THEN fa.offer_amount ELSE NULL
      END) AS federal_workstudy_offer_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'STATE' AND fa.fin_aid_type = 'GRANT'
         THEN fa.offer_amount ELSE NULL
      END) AS state_grant_offer_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'FEDERAL' AND fa.fin_aid_type = 'LOAN'
         THEN fa.offer_amount ELSE NULL
      END) AS federal_loan_offer_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'STATE' AND fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.offer_amount ELSE NULL
      END) AS state_scholarship_offer_amount
    ,   SUM(fa.offer_amount) AS offer_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'FEDERAL' AND fa.fin_aid_type = 'GRANT'
         THEN fa.accept_amount ELSE NULL
      END) AS federal_grant_accept_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'INSTITUTIONAL' AND fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.accept_amount ELSE NULL
      END) AS institutional_scholarship_accept_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'PRIVATE' AND fa.fin_aid_type = 'GRANT'
         THEN fa.accept_amount ELSE NULL
      END) AS private_grant_accept_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'PRIVATE' AND fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.accept_amount ELSE NULL
      END) AS private_scholarship_accept_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'PRIVATE' AND fa.fin_aid_type = 'LOAN'
         THEN fa.accept_amount ELSE NULL
      END) AS private_loan_accept_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr in ('MILITARY','FOUNDATION') AND fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.accept_amount ELSE NULL
      END) AS other_scholarship_accept_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'INSTITUTIONAL' AND ( fa.fin_aid_type != 'GRANT' AND fa.fin_aid_type != 'SCHOLARSHIP' AND fa.fin_aid_type != 'LOAN' AND fa.fin_aid_type != 'WORK_STUDY' AND fa.fin_aid_type != 'WAIVER' AND fa.fin_aid_type != 'FELLOWSHIP' AND fa.fin_aid_type != 'VA_BENEFIT')
         THEN fa.accept_amount ELSE NULL
      END) AS institutional_other_accept_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'INSTITUTIONAL' AND fa.fin_aid_type = 'GRANT'
         THEN fa.accept_amount ELSE NULL
      END) AS institutional_grant_accept_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'FEDERAL' AND fa.fin_aid_type = 'WORK_STUDY'
         THEN fa.accept_amount ELSE NULL
      END) AS federal_workstudy_accept_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'STATE' AND fa.fin_aid_type = 'GRANT'
         THEN fa.accept_amount ELSE NULL
      END) AS state_grant_accept_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'FEDERAL' AND fa.fin_aid_type = 'LOAN'
         THEN fa.accept_amount ELSE NULL
      END) AS federal_loan_accept_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'STATE' AND fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.accept_amount ELSE NULL
      END) AS state_scholarship_accept_amount
    ,   SUM(fa.accept_amount) AS accept_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'FEDERAL' AND fa.fin_aid_type = 'GRANT'
         THEN fa.disbursed_amount ELSE NULL
      END) AS federal_grant_disbursed_balance
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'INSTITUTIONAL' AND fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.disbursed_amount ELSE NULL
      END) AS institutional_scholarship_disbursed_balance
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'PRIVATE' AND fa.fin_aid_type = 'GRANT'
         THEN fa.disbursed_amount ELSE NULL
      END) AS private_grant_disbursed_balance
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'PRIVATE' AND fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.disbursed_amount ELSE NULL
      END) AS private_scholarship_disbursed_balance
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'PRIVATE' AND fa.fin_aid_type = 'LOAN'
         THEN fa.disbursed_amount ELSE NULL
      END) AS private_loan_disbursed_balance
    ,   SUM( CASE
         WHEN faa.fa_source_descr in ('MILITARY','FOUNDATION') AND fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.disbursed_amount ELSE NULL
      END) AS other_scholarship_disbursed_balance
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'INSTITUTIONAL' AND ( fa.fin_aid_type != 'GRANT' AND fa.fin_aid_type != 'SCHOLARSHIP' AND fa.fin_aid_type != 'LOAN' AND fa.fin_aid_type != 'WORK_STUDY' AND fa.fin_aid_type != 'WAIVER' AND fa.fin_aid_type != 'FELLOWSHIP' AND fa.fin_aid_type != 'VA_BENEFIT')
         THEN fa.disbursed_amount ELSE NULL
      END) AS institutional_other_disbursed_balance
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'INSTITUTIONAL' AND fa.fin_aid_type = 'GRANT'
         THEN fa.disbursed_amount ELSE NULL
      END) AS institutional_grant_disbursed_balance
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'FEDERAL' AND fa.fin_aid_type = 'WORK_STUDY'
         THEN fa.disbursed_amount ELSE NULL
      END) AS federal_workstudy_disbursed_balance
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'STATE' AND fa.fin_aid_type = 'GRANT'
         THEN fa.disbursed_amount ELSE NULL
      END) AS state_grant_disbursed_balance
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'FEDERAL' AND fa.fin_aid_type = 'LOAN'
         THEN fa.disbursed_amount ELSE NULL
      END) AS federal_loan_disbursed_balance
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'STATE' AND fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.disbursed_amount ELSE NULL
      END) AS state_scholarship_disbursed_balance
    ,   SUM( CASE
         WHEN fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.disbursed_amount ELSE NULL
      END) AS scholarship_disbursed_balance
    ,   SUM( CASE
         WHEN fa.fin_aid_type = 'VA_BENEFIT'
         THEN fa.disbursed_amount ELSE NULL
      END) AS vabenefits_disbursed_balance
    ,   SUM( CASE
         WHEN fa.fin_aid_type = 'LOAN'
         THEN fa.disbursed_amount ELSE NULL
      END) AS loan_disbursed_balance
    ,   SUM( CASE
         WHEN fa.fin_aid_type = 'GRANT'
         THEN fa.disbursed_amount ELSE NULL
      END) AS grant_disbursed_balance
    ,   SUM( CASE
         WHEN fa.fin_aid_type = 'WORK_STUDY'
         THEN fa.disbursed_amount ELSE NULL
      END) AS workstudy_disbursed_balance
    ,   SUM( CASE
         WHEN fa.fin_aid_type = 'WAIVER'
         THEN fa.disbursed_amount ELSE NULL
      END) AS waiver_disbursed_balance
    ,   SUM( CASE
         WHEN fa.fin_aid_type = 'FELLOWSHIP'
         THEN fa.disbursed_amount ELSE NULL
      END) AS fellowship_disbursed_balance
    ,   SUM( CASE
         WHEN (fa.fin_aid_type != 'GRANT' AND fa.fin_aid_type != 'SCHOLARSHIP' AND fa.fin_aid_type != 'LOAN' AND fa.fin_aid_type != 'WORK_STUDY' AND fa.fin_aid_type != 'WAIVER' AND fa.fin_aid_type != 'FELLOWSHIP' AND fa.fin_aid_type != 'VA_BENEFIT')
         THEN fa.disbursed_amount ELSE NULL
      END) AS other_disbursed_balance
    ,   SUM(fa.disbursed_amount) AS disbursed_balance
    --ADD A FEDERAL_SUB_LOAN_DISBURSED AND FEDERAL_SUB_LOAN_OFFER TO CS.FINACIAL_AID_TERM
    ,sum( case when faa.finaid_cat = ('GSL') then fa.offer_amount end) federal_sub_load_offer_amount
    ,sum( case when faa.finaid_cat = ('GSL') then fa.disbursed_amount end) federal_sub_load_disbursed_amount
    --ADD OTHER GRANTS TO CS.FINANCIAL_AID_TERM (uses the perseus_lookups)
    ,SUM( CASE
             WHEN faa.fa_source_descr in ('MILITARY','FOUNDATION') AND fa.fin_aid_type = 'GRANT'
             THEN fa.offer_amount ELSE NULL
          END
    ) AS other_grant_offer_amount
    ,SUM( CASE
             WHEN faa.fa_source_descr in ('MILITARY','FOUNDATION') AND fa.fin_aid_type = 'GRANT'
             THEN fa.disbursed_amount ELSE NULL
          END
    ) AS other_grant_disbursed_amount
    ,sum(
        case
            when lower(faa.award_description) ~~ '%pell%'
                    then fa.disbursed_amount
            else 0
        end
    )                                                               as  pell_disb
   FROM hercules_workday.fact_finaid fa
   JOIN hercules_workday.dim_finaid_type faa
      ON fa.item_type = faa.item_type
   GROUP BY fa.aid_year,fa.emplid,fa.acad_career) a
JOIN (SELECT fa.emplid
    ,   fa.acad_career
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'FEDERAL' AND fa.fin_aid_type = 'GRANT'
         THEN fa.offer_amount ELSE NULL
      END) AS federal_grant_offer_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'INSTITUTIONAL' AND fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.offer_amount ELSE NULL
      END) AS institutional_scholarship_offer_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'PRIVATE' AND fa.fin_aid_type = 'GRANT'
         THEN fa.offer_amount ELSE NULL
      END) AS private_grant_offer_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'PRIVATE' AND fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.offer_amount ELSE NULL
      END) AS private_scholarship_offer_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'PRIVATE' AND fa.fin_aid_type = 'LOAN'
         THEN fa.offer_amount ELSE NULL
      END) AS private_loan_offer_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr in ('MILITARY','FOUNDATION') AND fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.offer_amount ELSE NULL
      END) AS other_scholarship_offer_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'INSTITUTIONAL' AND ( fa.fin_aid_type != 'GRANT' AND fa.fin_aid_type != 'SCHOLARSHIP' AND fa.fin_aid_type != 'LOAN' AND fa.fin_aid_type != 'WORK_STUDY' AND fa.fin_aid_type != 'WAIVER' AND fa.fin_aid_type != 'FELLOWSHIP' AND fa.fin_aid_type != 'VA_BENEFIT')
         THEN fa.offer_amount ELSE NULL
      END) AS institutional_other_offer_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'INSTITUTIONAL' AND fa.fin_aid_type = 'GRANT'
         THEN fa.offer_amount ELSE NULL
      END) AS institutional_grant_offer_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'FEDERAL' AND fa.fin_aid_type = 'WORK_STUDY'
         THEN fa.offer_amount ELSE NULL
      END) AS federal_workstudy_offer_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'STATE' AND fa.fin_aid_type = 'GRANT'
         THEN fa.offer_amount ELSE NULL
      END) AS state_grant_offer_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'FEDERAL' AND fa.fin_aid_type = 'LOAN'
         THEN fa.offer_amount ELSE NULL
      END) AS federal_loan_offer_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'STATE' AND fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.offer_amount ELSE NULL
      END) AS state_scholarship_offer_amount
    ,   SUM(fa.offer_amount) AS offer_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'FEDERAL' AND fa.fin_aid_type = 'GRANT'
         THEN fa.accept_amount ELSE NULL
      END) AS federal_grant_accept_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'INSTITUTIONAL' AND fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.accept_amount ELSE NULL
      END) AS institutional_scholarship_accept_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'PRIVATE' AND fa.fin_aid_type = 'GRANT'
         THEN fa.accept_amount ELSE NULL
      END) AS private_grant_accept_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'PRIVATE' AND fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.accept_amount ELSE NULL
      END) AS private_scholarship_accept_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'PRIVATE' AND fa.fin_aid_type = 'LOAN'
         THEN fa.accept_amount ELSE NULL
      END) AS private_loan_accept_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr in ('MILITARY','FOUNDATION') AND fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.accept_amount ELSE NULL
      END) AS other_scholarship_accept_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'INSTITUTIONAL' AND ( fa.fin_aid_type != 'GRANT' AND fa.fin_aid_type != 'SCHOLARSHIP' AND fa.fin_aid_type != 'LOAN' AND fa.fin_aid_type != 'WORK_STUDY' AND fa.fin_aid_type != 'WAIVER' AND fa.fin_aid_type != 'FELLOWSHIP' AND fa.fin_aid_type != 'VA_BENEFIT')
         THEN fa.accept_amount ELSE NULL
      END) AS institutional_other_accept_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'INSTITUTIONAL' AND fa.fin_aid_type = 'GRANT'
         THEN fa.accept_amount ELSE NULL
      END) AS institutional_grant_accept_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'FEDERAL' AND fa.fin_aid_type = 'WORK_STUDY'
         THEN fa.accept_amount ELSE NULL
      END) AS federal_workstudy_accept_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'STATE' AND fa.fin_aid_type = 'GRANT'
         THEN fa.accept_amount ELSE NULL
      END) AS state_grant_accept_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'FEDERAL' AND fa.fin_aid_type = 'LOAN'
         THEN fa.accept_amount ELSE NULL
      END) AS federal_loan_accept_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'STATE' AND fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.accept_amount ELSE NULL
      END) AS state_scholarship_accept_amount
    ,   SUM(fa.accept_amount) AS accept_amount
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'FEDERAL' AND fa.fin_aid_type = 'GRANT'
         THEN fa.disbursed_amount ELSE NULL
      END) AS federal_grant_disbursed_balance
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'INSTITUTIONAL' AND fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.disbursed_amount ELSE NULL
      END) AS institutional_scholarship_disbursed_balance
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'PRIVATE' AND fa.fin_aid_type = 'GRANT'
         THEN fa.disbursed_amount ELSE NULL
      END) AS private_grant_disbursed_balance
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'PRIVATE' AND fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.disbursed_amount ELSE NULL
      END) AS private_scholarship_disbursed_balance
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'PRIVATE' AND fa.fin_aid_type = 'LOAN'
         THEN fa.disbursed_amount ELSE NULL
      END) AS private_loan_disbursed_balance
    ,   SUM( CASE
         WHEN faa.fa_source_descr in ('MILITARY','FOUNDATION') AND fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.disbursed_amount ELSE NULL
      END) AS other_scholarship_disbursed_balance
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'INSTITUTIONAL' AND ( fa.fin_aid_type != 'GRANT' AND fa.fin_aid_type != 'SCHOLARSHIP' AND fa.fin_aid_type != 'LOAN' AND fa.fin_aid_type != 'WORK_STUDY' AND fa.fin_aid_type != 'WAIVER' AND fa.fin_aid_type != 'FELLOWSHIP' AND fa.fin_aid_type != 'VA_BENEFIT')
         THEN fa.disbursed_amount ELSE NULL
      END) AS institutional_other_disbursed_balance
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'INSTITUTIONAL' AND fa.fin_aid_type = 'GRANT'
         THEN fa.disbursed_amount ELSE NULL
      END) AS institutional_grant_disbursed_balance
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'FEDERAL' AND fa.fin_aid_type = 'WORK_STUDY'
         THEN fa.disbursed_amount ELSE NULL
      END) AS federal_workstudy_disbursed_balance
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'STATE' AND fa.fin_aid_type = 'GRANT'
         THEN fa.disbursed_amount ELSE NULL
      END) AS state_grant_disbursed_balance
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'FEDERAL' AND fa.fin_aid_type = 'LOAN'
         THEN fa.disbursed_amount ELSE NULL
      END) AS federal_loan_disbursed_balance
    ,   SUM( CASE
         WHEN faa.fa_source_descr = 'STATE' AND fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.disbursed_amount ELSE NULL
      END) AS state_scholarship_disbursed_balance
    ,   SUM( CASE
         WHEN fa.fin_aid_type = 'SCHOLARSHIP'
         THEN fa.disbursed_amount ELSE NULL
      END) AS scholarship_disbursed_balance
    ,   SUM( CASE
         WHEN fa.fin_aid_type = 'VA_BENEFIT'
         THEN fa.disbursed_amount ELSE NULL
      END) AS vabenefits_disbursed_balance
    ,   SUM( CASE
         WHEN fa.fin_aid_type = 'LOAN'
         THEN fa.disbursed_amount ELSE NULL
      END) AS loan_disbursed_balance
    ,   SUM( CASE
         WHEN fa.fin_aid_type = 'GRANT'
         THEN fa.disbursed_amount ELSE NULL
      END) AS grant_disbursed_balance
    ,   SUM( CASE
         WHEN fa.fin_aid_type = 'WORK_STUDY'
         THEN fa.disbursed_amount ELSE NULL
      END) AS workstudy_disbursed_balance
    ,   SUM( CASE
         WHEN fa.fin_aid_type = 'WAIVER'
         THEN fa.disbursed_amount ELSE NULL
      END) AS waiver_disbursed_balance
    ,   SUM( CASE
         WHEN fa.fin_aid_type = 'FELLOWSHIP'
         THEN fa.disbursed_amount ELSE NULL
      END) AS fellowship_disbursed_balance
    ,   SUM( CASE
         WHEN (fa.fin_aid_type != 'GRANT' AND fa.fin_aid_type != 'SCHOLARSHIP' AND fa.fin_aid_type != 'LOAN' AND fa.fin_aid_type != 'WORK_STUDY' AND fa.fin_aid_type != 'WAIVER' AND fa.fin_aid_type != 'FELLOWSHIP' AND fa.fin_aid_type != 'VA_BENEFIT')
         THEN fa.disbursed_amount ELSE NULL
      END) AS other_disbursed_balance
    ,   SUM(fa.disbursed_amount) AS disbursed_balance
    --ADD A FEDERAL_SUB_LOAN_DISBURSED AND FEDERAL_SUB_LOAN_OFFER TO CS.FINACIAL_AID_TERM
    ,sum( case when faa.finaid_cat = ('GSL') then fa.offer_amount end) federal_sub_load_offer_amount
    ,sum( case when faa.finaid_cat = ('GSL') then fa.disbursed_amount end) federal_sub_load_disbursed_amount
    --ADD OTHER GRANTS TO CS.FINANCIAL_AID_TERM (uses the perseus_lookups)
    ,SUM( CASE
             WHEN faa.fa_source_descr in ('MILITARY','FOUNDATION') AND fa.fin_aid_type = 'GRANT'
             THEN fa.offer_amount ELSE NULL
          END
    ) AS other_grant_offer_amount
    ,SUM( CASE
             WHEN faa.fa_source_descr in ('MILITARY','FOUNDATION') AND fa.fin_aid_type = 'GRANT'
             THEN fa.disbursed_amount ELSE NULL
          END
    ) AS other_grant_disbursed_amount
    ,sum(
        case
            when lower(faa.award_description) ~~ '%pell%'
                    then fa.disbursed_amount
            else 0
        end
    )                                                               as  pell_disb
   FROM hercules_workday.fact_finaid fa
   JOIN hercules_workday.dim_finaid_type faa
      ON fa.item_type = faa.item_type
   GROUP BY fa.emplid,fa.acad_career) a1
   ON a.emplid = a1.emplid
      AND a.acad_career = a1.acad_career;
	  
commit;

analyze perseus_workday.cs_financial_aid;
