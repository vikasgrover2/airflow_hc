

TRUNCATE TABLE hercules_workday.dim_address ;

INSERT INTO hercules_workday.dim_address 
(
                                                                                                                                    
     src_sys_cd                                                                                                                  
   , src_sys_eff_dt                                                                                                             
   , src_sys_exp_dt                                                                                                             
   , eff_dt                                                                                                                     
   , exp_dt                                                                                                                     
   , emplid                                                                                                                     
   , uid                                                                                                               
   , address_cd                                                                                                                 
   , active_flag                                                                                                                
   , address_type_ld                                                                                                             
 --  , address1                                                                                                                    
   , state_ld                                                                                                                    
   , state_cd  
   , county   
   , postal                                                                                                                      
   , country_cd
   , country_ld
   , geog_fips_county_cd  
   , postal_zip5                                                                                                        
                                                                                                                                
    
)
select src_sys_cd,
       src_sys_eff_dt,
       src_sys_exp_dt,
       eff_dt,
       exp_dt,
       emplid,
       universal_id as uid,
       address_cd,
       active_flag,
       address_type_ld,
       state_ld,
       state_cd,
       county,
       postal,
       country_cd,
       country_ld,
       geog_fips_county_cd,
       postal_zip5
from (
select  
  std_address.home_reference_address_wid || '~' || 'HOME'         as src_sys_cd                                                                                                                  
, load_date      as src_sys_eff_dt                                                                                                            
, null::date           as src_sys_exp_dt                                                                                                             
, load_date      as eff_dt                                                                                                                     
, NULL::date           as exp_dt                                                                                                                     
, std_address.student_id            as emplid                                                                                                                     
, std_address.student_universal_id	 as universal_id                                                                                                               
, std_address.home_reference_address_wid				 as address_cd                                                                                                                 
,'Y'				 as active_flag                                                                                                                
, 'HOME'								           as address_type_ld                                                                                                             
--, std_address.address_line_data											           as address1                                                                                                                    
, std_address.home_country_region_descriptor					           as state_ld                                                                                                                    
, std_address.home_country_region_reference_iso_3166_2_code		           as state_cd  
, std_address.home_municipality as county
, std_address.home_postal_code		  as postal       
, std_address.home_address_data_iso_3166_1_alpha_2_code as country_cd	 
, std_address.home_address_data_iso_3166_1_alpha_3_code as country_ld	                                                                                     
, std_address.home_address_data_iso_3166_1_numeric_3_code	           as geog_fips_county_cd                                                                                                         
, nvl(std_address.home_postal_code ,'*')         as postal_zip5 
, row_number() over (partition by home_reference_address_wid) as rn
from  workday_ods.address_information_data std_address 
where 1=1
union all
select  nvl(wrk_address.address_reference_wid,wrk_address.student_id) || '~' || 'WORK'        as src_sys_cd                                                                                                                  
, load_date      as src_sys_eff_dt                                                                                                            
, null::date           as src_sys_exp_dt                                                                                                             
, load_date      as eff_dt                                                                                                                     
, NULL::date           as exp_dt                                                                                                                     
, wrk_address.student_id            as emplid                                                                                                                     
, wrk_address.student_universal_id	 as universal_id                                                                                                               
, wrk_address.address_reference_wid				 as address_cd                                                                                                                 
,'Y'				 as active_flag                                                                                                                
, 'WORK'			as address_type_ld                                                                                                             
--, std_address.address_line_data											           as address1                                                                                                                    
, wrk_address.address_country_descriptor					           as state_ld                                                                                                                    
, wrk_address.country_region_2_code		           as state_cd  
, wrk_address.address_municipality as county
, wrk_address.address_postal_code	  as postal       
, wrk_address.country_iso_alpha2_code as country_cd	 
, wrk_address.country_iso_alpha3_code as country_ld	                                                                                     
, wrk_address.country_iso_numeric_code	           as geog_fips_county_cd                                                                                                         
, nvl(wrk_address.address_postal_code ,'*')         as postal_zip5 
, row_number() over (partition by student_id) as rn
from  workday_ods.student wrk_address
where 1=1
)s
where s.rn = 1
;

commit;

analyze hercules_workday.dim_address ;

    
	