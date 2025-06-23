

{{
   config(
        materialized='table'
   )
 
}}

WITH SAC_ORIGINAL AS (

SELECT id,id, first_name, last_name, gender, City, JobTitle, Salary1, Latitude, Longitude,Incm_tax
FROM {{ ref("sac_original") }} 

)
SELECT id,id, first_name, last_name, gender, City, JobTitle, Salary1, Latitude, Longitude,Incm_tax,current_timestamp() as load_time FROM SAC_ORIGINAL

