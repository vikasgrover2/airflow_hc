/*
   Object type      : Dimension (SCD Type 1)
   Description      : Records student/faculty/staff personal and demographic information.
   Grain            : Person

   Target table name: DIM_PERSON

   Source table list:

   ETL load type    : Full load everyday

   Date created     : 11/25/19
   Date updated     :
*/

Truncate table hercules_workday.dim_person;

Insert into hercules_workday.dim_person 
(
src_sys_cd                                
 	,emplid                               
 	,uid                                  
 	,src_sys_eff_dt                       
 	,src_sys_exp_dt                       
 	,eff_dt                               
 	,exp_dt                               
 	,first_name                           
 	,last_name                            
 	,name_last_first                      
 	,name_first_last                      
 	,middle_initial                       
 	,middle_name                          
 	,name_prefix                          
 	,duplicate_emplid_ind                 
 	,name_suffix                          
 	,last_name_match                      
 	,name_type_ld                         
 	,name_initials                        
 	,first_name_match                     
 	,natl_id_country_ld                   
 	,natl_id_country_cd                   
 	,date_of_birth                        
 	,natl_id_country_sd                   
 	,birthdate_day                        
 	,first_gen_flg                        
 	,deceased                             
 	,deceased_date                        
 	,ethnicity_id                         
 	,ethnicity                            
 	,race_white_flag                      
 	,race_hawaiian_ind                    
 	,race_amer_indian_ind                 
 	,race_black_ind                       
 	,race_asian_ind                       
 	,race_hispanic_ind                    
 	,foreign_ind                          
 	,race_not_specified_ind               
 	,ethnicity_ld   
 	,ipeds_race_ethn_citz_cd
 	,ipeds_race_ethn_citz_ld
 	,asian_citz_ind
    ,amer_indian_citz_ind
    ,white_citz_ind
 	,usa_citizenship_status_code          
 	,usa_citizenship_status               
 	,gender_cd                            
 	,visa_permit_type                     
 	,visa_permit_type_code                
 	,citizenship_country_sd               
 	,citizenship_country_ld               
 	,citizenship_country_status_cd        
 	,citizenship_country_cd               
 	,bhe_birth_year                       
 	,married_status_cd                    
 	,married_status_ld                    
 	,highest_degree_cd                    
 	,highest_degree_ld                    
 )
--create table hercules_workday.dim_person  as 
select ssd.student_reference_student_wid as src_sys_cd,
ssd.student_id as emplid,
ssd.student_universal_id as uid,
ssd.load_date as src_sys_eff_dt,
null::date as src_sys_exp_dt,
ssd.load_date as eff_dt,
null::date as exp_dt,
ssd.name_legal_first_name as first_name,
DECODE(Trim(ssd.name_legal_last_name),'','*',NULL,'*',Trim(ssd.name_legal_last_name))      AS last_name,
Trim(NVL(Trim(ssd.name_legal_last_name)||','||Trim(CASE WHEN Trim(ssd.name_legal_first_name) = '' THEN NULL ELSE Trim(ssd.name_legal_first_name) END)||' '||Trim(ssd.name_legal_last_name),'')) AS name_last_first,
DECODE(Trim(Trim(ssd.name_legal_last_name)||' '||CASE WHEN Trim(ssd.name_legal_first_name) = '' THEN NULL ELSE Trim(ssd.name_legal_first_name) END)||' '||Trim(ssd.name_legal_last_name),
              '','*',
              NULL,'*',
              Trim(Trim(ssd.name_legal_last_name)||' '||CASE WHEN Trim(ssd.name_legal_first_name) = '' THEN NULL ELSE Trim(ssd.name_legal_first_name) END)||' '||Trim(ssd.name_legal_last_name))       AS name_first_last,
DECODE(Trim(Substring(ssd.name_legal_middle_name,1,1)),'','*',NULL,'*',Trim(Substring(ssd.name_legal_middle_name,1,1)))   AS middle_initial,
nvl(ssd.name_legal_middle_name,'*') as middle_name,
NULL AS name_prefix,
NULL AS duplicate_emplid_ind,
NULL AS name_suffix,
NULL AS last_name_match,
NULL AS name_type_ld,
NULL AS name_initials,
NULL AS first_name_match,
--ssd.national_id_type_code as national_id_type_cd,
--ssd. national_id_type_code as national_id_type_ld,
--ssd.national_id_data_id as primary_nid,
ssd.country_iso_alpha3_code as natl_id_country_ld,
ssd.country_iso_alpha2_code as natl_id_country_cd,
--ssd.Student__Custom_Id_Type_Id as sas_id,
ssd.date_of_birth as date_of_birth,
DECODE(Trim(ssd.country_iso_alpha3_code),'','*',NULL,'USA',Trim(ssd.country_iso_alpha3_code)) AS natl_id_country_sd,
EXTRACT(DAY FROM TO_DATE(ssd.date_of_birth, 'YYYY-MM-DD'))  as birthdate_day,
CASE WHEN  ssd.first_gen_college= 1 THEN 'Y'
            ELSE 'N'
       END  as first_gen_flg,
case when ssd.student_is_deceased=1 then 'Y' else 'N' end as deceased,
NULL as deceased_date,
NULL as ethnicity_id,
NULL as ethnicity,
ssd_ethnicity.race_white_ind as race_white_flag,
ssd_ethnicity.race_hawaiian_ind as race_hawaiian_ind,
ssd_ethnicity.race_amer_indian_ind as race_amer_indian_ind,
ssd_ethnicity.race_black_ind as race_black_ind,
ssd_ethnicity.race_asian_ind as race_asian_ind,
ssd_ethnicity.race_hispanic_ind as race_hispanic_ind,
CASE WHEN ssd.country_iso_alpha3_code = 'USA' then 'N' else 'Y' end AS foreign_ind,
CASE WHEN ssd_ethnicity.race_black_ind       = 'N'
            AND ssd_ethnicity.race_hawaiian_ind    = 'N'
            AND ssd_ethnicity.race_amer_indian_ind = 'N'
            AND ssd_ethnicity.race_white_ind       = 'N'
            AND ssd_ethnicity.race_asian_ind       = 'N'
            AND ssd_ethnicity.race_hispanic_ind    = 'N' THEN 'Y'
           ELSE 'N'                                                        
       END  AS race_not_specified_ind,
 CASE WHEN ssd_ethnicity.hispanic_or_latino_ind = 'Y' THEN 'Hispanic/Latino'
            ELSE 'Non Hispanic/Latino'
       END    AS ethnicity_ld,     
null as ipeds_race_ethn_citz_cd,
null as ipeds_race_ethn_citz_ld,
null as asian_citz_ind,
null as amer_indian_citz_ind,
null as white_citz_ind,
ssd_citzen.Citizenship_Status_Code as usa_citizenship_status_code,
CASE
WHEN ssd_citzen.citizenship_status_code LIKE 'Citizen_%' THEN SUBSTRING(ssd_citzen.citizenship_status_code,1, 8)
WHEN ssd_citzen.citizenship_status_code LIKE 'Permanent_Resident_%' THEN 'PR'
WHEN ssd_citzen.citizenship_status_code LIKE 'Naturalized_Citizen_%' THEN SUBSTRING(ssd_citzen.citizenship_status_code,1, 21)
WHEN ssd_citzen.citizenship_status_code LIKE 'International_%' THEN SUBSTRING(ssd_citzen.citizenship_status_code,1, 13)
ELSE ssd_citzen.Citizenship_Status_Code
END AS usa_citizenship_status,
ssd.gender_code as gender_cd,
ssd_visa.visa_type_id as visa_permit_type,
ssd_visa.visa_type_wid as visa_permit_type_code,
case when (ssd_citzen.Citizenship_Status_Code='' or ssd_citzen.Citizenship_Status_Code is null) then
Decode(ssd_citzen.Citizenship_Status_Code,'','*',null,'*') else 
SUBSTRING(ssd_citzen.Citizenship_Status_Code FROM POSITION('_' IN ssd_citzen.Citizenship_Status_Code) + 1)  
end AS citizenship_country_sd,
case when (ssd_citzen.Citizenship_Status_Code='' or ssd_citzen.Citizenship_Status_Code is null) then
Decode(ssd_citzen.Citizenship_Status_Code,'','*',null,'*') else 
SUBSTRING(ssd_citzen.Citizenship_Status_Code FROM POSITION('_' IN ssd_citzen.Citizenship_Status_Code) + 1)  
end AS citizenship_country_ld,
case when (ssd_citzen.Citizenship_Status_Code='' or ssd_citzen.Citizenship_Status_Code is null) then
Decode(ssd_citzen.Citizenship_Status_Code,'','*',null,'*') else 
SUBSTRING(ssd_citzen.Citizenship_Status_Code FROM POSITION('_' IN ssd_citzen.Citizenship_Status_Code) + 1)  
end AS citizenship_country_status_cd,
case when (ssd_citzen.Citizenship_Status_Code='' or ssd_citzen.Citizenship_Status_Code is null) then
Decode(ssd_citzen.Citizenship_Status_Code,'','*',null,'*') else 
SUBSTRING(ssd_citzen.Citizenship_Status_Code FROM POSITION('_' IN ssd_citzen.Citizenship_Status_Code) + 1)  
end AS citizenship_country_cd,
DECODE(Trim(CASE WHEN ssd.date_of_birth IS NULL THEN '0000' ELSE EXTRACT(YEAR FROM TO_DATE(ssd.date_of_birth, 'YYYY-MM-DD')) END),'','*',NULL,'*',Trim(CASE WHEN ssd.date_of_birth IS NULL THEN '0000' ELSE EXTRACT(YEAR FROM TO_DATE(ssd.date_of_birth, 'YYYY-MM-DD')) END)) AS bhe_birth_year,
'*' AS married_status_cd,
'*'    AS married_status_ld,
NULL   AS highest_degree_cd,
'*'    AS highest_degree_ld
 from workday_ods.student ssd
left join (
		select student_id,MAX(CASE WHEN split_part(ssd_ethnicity.ethnicity_id, '_', 1) = 'White' THEN 'Y' ELSE 'N' end) as race_white_ind,
		MAX(CASE WHEN split_part(ssd_ethnicity.ethnicity_id, '_', 1) = 'Native' THEN 'Y' ELSE 'N' end) as race_hawaiian_ind,
		MAX(CASE WHEN split_part(ssd_ethnicity.ethnicity_id, '_', 1) = 'American' THEN 'Y' ELSE 'N' end) as race_amer_indian_ind,
		MAX(CASE WHEN split_part(ssd_ethnicity.ethnicity_id, '_', 1) = 'Black' THEN 'Y' ELSE 'N' end) as race_black_ind,
		MAX(CASE WHEN split_part(ssd_ethnicity.ethnicity_id, '_', 1) = 'Asian' THEN 'Y' ELSE 'N' end) as race_asian_ind,
		MAX(CASE WHEN split_part(ssd_ethnicity.ethnicity_id, '_', 1) = 'Hispanic' THEN 'Y' ELSE 'N' end) as race_hispanic_ind,
		MAX(CASE WHEN ssd_ethnicity.hispanic_or_latino = 1 then 'Y' else 'N' end) as hispanic_or_latino_ind
		from workday_ods.ethnicity_reference ssd_ethnicity
		group by student_id) ssd_ethnicity
     on ssd.student_id = ssd_ethnicity.student_id
left join (select *,
		row_number() over (partition by student_id, student_universal_id 
		order by (case when citizenship_status_code like 'International%' then 1 else 9 end)) as rn
		from workday_ods.citizenship_status_reference) ssd_citzen
	  on ssd.student_id = ssd_citzen.student_id
      and ssd_citzen.rn = 1
left join (select distinct student_id,visa_type_id,visa_type_wid from workday_ods.visa_id) ssd_visa
      on ssd.student_id = ssd_visa.student_id
;

commit;

analyze hercules_workday.dim_person;