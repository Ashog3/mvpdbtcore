

{{
   config(
        materialized='incremental',
        on_schema_change='fail'
   )
 
}}

WITH SAC_ORIGINAL AS (

SELECT id,id, first_name, last_name, gender, City, JobTitle, Salary1, Latitude, Longitude
FROM {{ ref("sac_original") }} 

)
SELECT id,id, first_name, last_name, gender, City, JobTitle, Salary1, Latitude, Longitude,Incm_tax,current_timestamp() as load_time FROM SAC_ORIGINAL

{% if is_incremental() %}
where id > ( select max(id) from {{this}})
{% endif %}
