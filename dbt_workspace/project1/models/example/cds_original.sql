

{{
   config(
        materialized='incremental',
        on_schema_change='fail'
   )
 
}}

WITH STG_ORIGINAL AS (

SELECT id,first_name, last_name, gender, City, JobTitle, Salary1, Latitude, Longitude
FROM {{ ref("sac_original") }} 

)
SELECT id,first_name, last_name, gender, City, JobTitle, Salary1, Latitude, Longitude,current_timestamp() as load_time FROM STG_ORIGINAL

{% if is_incremental() %}
where id > ( select max(id) from {{this}})
{% endif %}
