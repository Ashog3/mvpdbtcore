

{{
   config(
        materialized='table'
   )
 
}}

WITH STG_ORIGINAL AS (

SELECT first_name, CAST(NULL AS STRING)last_name, CAST(NULL AS STRING)gender, CAST(NULL AS STRING)City, CAST(NULL AS STRING)JobTitle, CAST(NULL AS STRING)Salary1, CAST(NULL AS STRING)Latitude, CAST(NULL AS STRING)Longitude
LIMIT 0

)
SELECT first_name, last_name, gender, City, JobTitle, Salary1, Latitude, Longitude FROM STG_ORIGINAL


