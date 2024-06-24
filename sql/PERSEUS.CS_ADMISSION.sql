/*##############################################################################
Object type          : CROSS SEEDING TABLE
Description          : 
Grain                : 
Target table name    : CS_ADMISSION
Source table list    : FF_APPLICATIONS
                       FF_APPLICATIONS_PROG
ETL load type        : FULL LOAD EVERYDAY
Date created         : 01-MARCH-2019
Date updated         :
Update notes         :
##############################################################################*/

TRUNCATE TABLE perseus_workday.cs_admission;

INSERT INTO perseus_workday.cs_admission(
     src_sys_cd
    ,emplid
    ,acad_career
    ,adm_appl_nbr
    ,admit_term
    ,eff_strm
    ,acad_prog
    ,app_dt
    ,admit_dt
    ,accepted_dt
    ,fin_aid_interest
    ,stdnt_admit_type_cd
    ,stdnt_admit_type_ld
    ,stdnt_admit_type_group
    ,stdnt_campus_cd
	,next_eff_strm
)
select
    x.emplid
        ||'~'
        ||x.acad_career
        ||'~'
        ||x.eff_strm                                                                        as src_sys_cd
    ,x.emplid
    ,x.acad_career
    ,x.adm_appl_nbr
    ,x.admit_term
    ,x.eff_strm
    ,x.acad_prog
    ,x.app_dt
    ,x.admit_dt
    ,x.accepted_dt
    ,x.fin_aid_interest
    ,x.stdnt_admit_type_cd
    ,x.stdnt_admit_type_ld
    ,x.stdnt_admit_type_group
    ,x.stdnt_campus_cd
	,nvl(lead(x.eff_strm) over (partition by x.emplid, x.acad_career order by x.eff_strm),9999) next_eff_strm
from (
---1
    select
         prog.emplid                                                                        as emplid
        ,prog.acad_career                                                                   as acad_career
        ,prog.adm_appl_nbr                                                                  as adm_appl_nbr
        ,prog.admit_term                                                                    as admit_term
        ,dt_admit.rel_strm                                                                  as eff_strm
        ,prog.acad_prog                                                                     as acad_prog
        ,ap.adm_appl_dt                                                                     as app_dt
        ,min(case
                when
                    prog.app_stage in ('ADMITTED')
                        then prog.effdt
                else null
            end
        ) over(
            partition by
                prog.emplid
                ,prog.acad_career
                ,dt_admit.strm
            order by
                dt_admit.rel_strm desc
                ,prog.effdt
                ,prog.adm_appl_nbr desc
                ,case
                    when
                        prog.app_stage in ('ADMITTED')
                            then prog.effdt
                    else null
                end nulls last
            rows between unbounded preceding and current row
        )                                                                                   as admit_dt
        ,min(case
                when
                    prog.app_stage in ('DEPOSIT_PAID')
                        then prog.effdt
                else null
            end
        ) over(
            partition by
                prog.emplid
                ,prog.acad_career
                ,dt_admit.strm
            order by
                dt_admit.rel_strm desc
                ,prog.effdt
                ,prog.adm_appl_nbr desc
                ,case
                    when
                        prog.app_stage in ('DEPOSIT_PAID')
                            then prog.effdt
                    else null
                end nulls last
            rows between unbounded preceding and current row
        )                                                                                   as accepted_dt
        ,ap.fin_aid_interest                                                                as fin_aid_interest
        ,ap.admit_type_cd                                                                   as stdnt_admit_type_cd
        ,ap.admit_type_ld                                                                   as stdnt_admit_type_ld
        ,ap.admit_type_group                                                                as stdnt_admit_type_group
        ,prog.stdnt_campus_cd                                                               as stdnt_campus_cd
        ,row_number() over(
            partition by
                prog.emplid
                ,prog.acad_career
                ,dt_admit.strm
            order by 
                dt_admit.rel_strm desc
                ,prog.effdt desc
                ,prog.adm_appl_nbr desc
        ) rn
        
        ,dt_admit.rel_strm
    from hercules_workday.ff_applications_prog prog
    
    left join hercules_workday.ff_applications ap
    on prog.adm_appl_nbr = ap.adm_appl_nbr
    
    left join hercules_workday.dim_term dt_admit
    on prog.admit_term = dt_admit.strm

    where 1=1
--and prog.emplid = ''
--and prog.adm_appl_nbr = ''

---1
) x
where x.rn = 1
;

COMMIT;

ANALYZE perseus_workday.cs_admission;