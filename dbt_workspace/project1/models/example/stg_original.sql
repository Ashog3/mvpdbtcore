

{{
   config(
        materialized='table'
   )
 
}}

WITH STG_ORIGINAL AS (

SELECT "CAST(NULL AS STRING) AS" + first_name, last_name, gender, City, JobTitle, Salary1, Latitude, Longitude
LIMIT 0

)
SELECT first_name, last_name, gender, City, JobTitle, Salary1, Latitude, Longitude FROM STG_ORIGINAL


