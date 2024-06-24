/*
   Object type      : Dimension (SCD Type 1)
   Description      : Records university academic career definition information.
   Grain            :  Career  
   
   Target table name: DIM_CAREER
   
   Source table list: acad_levels_nf                   -- Student Level Table.
                      
                      
   
   ETL load type    : Full load everyday
   
   Date created     : 01/27/2020
*/


truncate table hercules_workday.dim_career;

insert into hercules_workday.dim_career
(
  src_sys_cd
--, src_sys_eff_dt
--, src_sys_exp_dt
--, eff_dt
--, exp_dt
, acad_career_cd
, acad_career_ld
, collapsed_acad_career_cd
, collapsed_acad_career_ld
)
select distinct program_of_study.academic_level_wid as src_sys_cd,
--load_timestamp as eff_dt,
--expir_timestamp  as exp_dt
program_of_study.academic_level_wid as acad_career_cd,
program_of_study.academic_level_id as acad_career_ld,
program_of_study.academic_level_wid as collapsed_acad_career_cd,
program_of_study.academic_level_id as collapsed_acad_career_ld
from workday_ods.program_of_study
where 1=1
;

commit;

analyze hercules_workday.dim_career;