

{{
   config(
        materialized='table'
   )
 
}}

WITH STG_ORIGINAL AS (

SELECT "CAST(NULL AS STRING) AS" first_name, CAST(NULL AS STRING) ASlast_name, CAST(NULL AS STRING) ASgender, CAST(NULL AS STRING) ASCity, CAST(NULL AS STRING) ASJobTitle, CAST(NULL AS STRING) ASSalary1, CAST(NULL AS STRING) ASLatitude, CAST(NULL AS STRING) ASLongitude
LIMIT 0

)
SELECT first_name, last_name, gender, City, JobTitle, Salary1, Latitude, Longitude FROM STG_ORIGINAL


