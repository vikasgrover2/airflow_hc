/*
   Object type      : Reporting Dimension
   Description      : Records one of the student/faculty/staff address among HOME/PERMANENT/MAIL/BUSINESS/DORMITORY/BILLING based on defined order.
   Grain            : Person, Address type

   Target table name: DIM_ADDRESS_CASCADE

   Source table list: DIM_ADDRESS       -- Primary source table to retrieve person address details.

   ETL load type    : Full load everyday

   Date created     : 04/07/16
   Date updated     : 03/07/18
*/

TRUNCATE TABLE hercules_workday.dim_address_cascade;

INSERT INTO hercules_workday.dim_address_cascade
(
     eff_dt
    ,exp_dt
    ,emplid
    ,address_cd
    ,active_flag
    ,address_type_cd
    ,address_type_ld
    ,address1
    ,address2
    ,city
    ,state_cd
    ,state_ld
    ,country_cd
    ,country_ld
    ,postal
    ,county
    ,geog_state_ld
    ,geog_md_va_dc_regional_ld
    ,geog_md_va_dc_regional_ind
    ,geog_postal_loc_y
    ,geog_country_ld
    ,geog_county_ld
    ,geog_postal_loc_x
    ,geog_md_va_dc_regional_cd
    ,geog_city
    ,geog_country_cd
    ,geog_fips_county_cd
    ,geog_postal_group_sd
    ,geog_postal_match_code
    ,geog_country_sd
    ,geog_alias_city
    ,geog_fips_combined_cd
    ,geog_post_office_name
    ,geog_rec_type
    ,geog_state_cd
    ,geog_fips_state_cd
    ,geog_mil_regional_ind
    ,geog_metro_service_area
    ,geog_postal_group_ld
    ,geog_postal
    ,geog_dma_name
    ,geog_dma_select
    ,eff_status
    ,rn
)
SELECT 
     add.eff_dt
    ,add.exp_dt
    ,add.emplid
    ,add.address_cd
    ,add.active_flag
    ,add.address_type_cd
    ,add.address_type_ld
    ,add.address1
    ,add.address2
    ,add.city
    ,add.state_cd
    ,add.state_ld
    ,add.country_cd
    ,add.country_ld
    ,add.postal
    ,add.county
    ,add.geog_state_ld
    ,add.geog_md_va_dc_regional_ld
    ,add.geog_md_va_dc_regional_ind
    ,add.geog_postal_loc_y
    ,add.geog_country_ld
    ,add.geog_county_ld
    ,add.geog_postal_loc_x
    ,add.geog_md_va_dc_regional_cd
    ,add.geog_city
    ,add.geog_country_cd
    ,add.geog_fips_county_cd
    ,add.geog_postal_group_sd
    ,add.geog_postal_match_code
    ,add.geog_country_sd
    ,add.geog_alias_city
    ,add.geog_fips_combined_cd
    ,add.geog_post_office_name
    ,add.geog_rec_type
    ,add.geog_state_cd
    ,add.geog_fips_state_cd
    ,add.geog_mil_regional_ind
    ,add.geog_metro_service_area
    ,add.geog_postal_group_ld
    ,add.geog_postal
    ,add.geog_dma_name
    ,add.geog_dma_select
    ,add.eff_status
    ,add.rn
FROM (
    SELECT 
        d.eff_dt
        ,d.exp_dt
        ,d.emplid
        ,d.address_cd
        ,d.active_flag
        ,d.address_type_cd
        ,d.address_type_ld
        ,d.address1
        ,d.address2
        ,d.city
        ,d.state_cd
        ,d.state_ld
        ,d.country_cd
        ,d.country_ld
        ,d.postal
        ,d.county
        ,d.geog_state_ld
        ,d.geog_md_va_dc_regional_ld
        ,d.geog_md_va_dc_regional_ind
        ,d.geog_postal_loc_y
        ,d.geog_country_ld
        ,d.geog_county_ld
        ,d.geog_postal_loc_x
        ,d.geog_md_va_dc_regional_cd
        ,d.geog_city
        ,d.geog_country_cd
        ,d.geog_fips_county_cd
        ,d.geog_postal_group_sd
        ,d.geog_postal_match_code
        ,d.geog_country_sd
        ,d.geog_alias_city
        ,d.geog_fips_combined_cd
        ,d.geog_post_office_name
        ,d.geog_rec_type
        ,d.geog_state_cd
        ,d.geog_fips_state_cd
        ,d.geog_mil_regional_ind
        ,d.geog_metro_service_area
        ,d.geog_postal_group_ld
        ,d.geog_postal
        ,d.geog_dma_name
        ,d.geog_dma_select
        ,d.eff_status_cd as eff_status
        ,ROW_NUMBER() OVER(
            PARTITION BY
                d.emplid
               ,d.address_type_cd
            ORDER BY
                decode(d.eff_status_cd,'C',0,'L',5,9)
                ,d.eff_dt desc
                ,d.address_cd desc
        ) AS rn
    FROM hercules_workday.dim_address d
    WHERE d.active_flag = 'Y'
) add
WHERE add.rn = 1
;

COMMIT;

ANALYZE hercules_workday.DIM_ADDRESS_CASCADE
;
