/*
   Object type      : Dimension
   Description      : 
   Grain            : 
   
   Target table name: DIM_DEGREE
   
   Source table list: ps_degree_tbl
                        ,psxlatitem
   
   ETL load type    : Full load everyday
   
   Date created     : 12/04/2023
   Date updated     : 01/16/2023
*/
truncate table hercules_workday.dim_degree;

insert into hercules_workday.dim_degree (
	src_sys_cd,
    degree_cd,
    degree_sd,
    degree_ld,
    education_level_ld,
	bhe_degree_level_cd
)
select DISTINCT
	wpos.default_educational_credential_wid src_sys_cd,
	wpos.default_educational_credential_wid as degree_cd,
	wpos.default_educational_credential_id as degree_sd,
	wpos.default_educational_credential_id as degree_ld,
	null as education_level_ld,
	null as bhe_degree_level_cd
from workday_full_test.program_of_study wpos
where 1=1
and wpos.default_educational_credential_id <> ''				  
;

commit;

ANALYZE hercules_workday.dim_degree;