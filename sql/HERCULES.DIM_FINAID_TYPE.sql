/*
   Object type      : Dimension (SCD Type 1)
   Description      : Records financial aid source item type information.
   Grain            : Location code

   Target table name: DIM_FINAID_TYPE

   Source table list: PS_ITEM_TYPE_FA      -- Primary source table to retrieve financial aid item type details.
                      PS_EXT_ORG_TBL       -- Used to retrieve financial aid source details.
                      PS_ITEM_TYPE_TBL     -- Used to retrieve item type details.
                      PS_ITEM_SF           -- Used to retrieve student financial transactions associated with financial aid.

   ETL load type    : Full load everyday

   Date created     : 04/07/16
   Date updated     : 09/09/19
   Updated By: Trevor Filipiak
   Update reason: Added disburse_method, disburse_method_descrshort, disburse_method_descr per UDEL-123
*/

TRUNCATE TABLE hercules_workday.DIM_FINAID_TYPE;
INSERT INTO hercules_workday.DIM_FINAID_TYPE
(
     src_sys_id
    ,src_sys_cd
    ,src_sys_eff_dt
    ,src_sys_exp_dt
    ,eff_dt
    ,exp_dt
    ,item_type
    ,aid_year
    ,fa_source
    ,fa_source_descrshort
    ,fa_source_descr
    ,finaid_type
    ,finaid_type_descrshort
    ,finaid_type_descr
    ,need_based
    ,federal_id
    ,award_description
    ,finaid_cat
    ,disburse_method
    ,disburse_method_descrshort
    ,disburse_method_descr
    ,keyword1
    ,keyword2
    ,keyword3
)

---Notes for Workday: the workday student award items is effective dated 
---and contains all awards defined, not necessarily associating them with an aid year
---this has implications for all the joins down the line


SELECT 
       student_award_item.wid                                  AS src_sys_id
      ,student_award_item.id  ||'~' || student_award_item.code     AS src_sys_cd            
      ,null AS src_sys_eff_dt
      ,NULL                                  AS src_sys_exp_dt
      ,student_award_item.effective_date                    AS eff_dt
      ,NULL                                  AS exp_dt
      ,student_award_item.code AS ITEM_TYPE
      ,NULL 
      ,student_award_item.source_wid AS FA_SOURCE
      ,student_award_item.source_id AS FA_SOURCE_DESCRSHORT
      ,student_award_item.source_id AS FA_SOURCE_DESCR
      ,student_award_item.student_award_type_wid  AS finaid_type
      ,student_award_item.student_award_type_id           AS FINAID_TYPE_DESCRSHORT
      ,student_award_item.student_award_type_id AS FINAID_TYPE_DESCR
      ,NULL AS need_based
      ,student_award_item.federal_program_id_wid AS FEDERAL_ID  --SOME will have this AND OTHERS won't
      ,student_award_item.Name                             AS AWARD_DESCRIPTION
      ,student_award_item.federal_program_id_id                               AS FINAID_CAT
      ,NULL AS DISBURSE_METHOD
      ,NULL AS DISBURSE_METHOD_DSCRSHORT
      ,NULL AS DISBURSE_METHOD_DESCR
   ---could consider using these for some of the worktags   
      ,NULL AS keyword1
      ,NULL AS keyword2
      ,NULL AS keyword3
FROM workday_ods.student_award_item
WHERE 1=1
  --AND student_award_item.EXPIR_TIMESTAMP IS NULL
  --AND student_award_item.Inactive = 0 
;

COMMIT;

ANALYZE hercules_workday.DIM_FINAID_TYPE;
