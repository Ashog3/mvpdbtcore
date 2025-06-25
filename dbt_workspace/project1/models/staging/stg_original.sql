

{{
   config(
        materialized='table',
        tags=['no_default_run']
   )
 
}}

WITH STG_ORIGINAL AS (

SELECT CAST(NULL AS INTEGER) AS id, CAST(NULL AS STRING) AS first_name, CAST(NULL AS STRING) AS last_name, CAST(NULL AS STRING) AS gender, CAST(NULL AS STRING) AS City, CAST(NULL AS STRING) AS JobTitle, CAST(NULL AS STRING) AS Salary1, CAST(NULL AS STRING) AS Latitude, CAST(NULL AS STRING) AS Longitude
LIMIT 0

)
SELECT id, first_name, last_name, gender, City, JobTitle, Salary1, Latitude, Longitude FROM  stg_ORIGINAL


