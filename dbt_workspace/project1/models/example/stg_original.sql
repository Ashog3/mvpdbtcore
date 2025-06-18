

{{
   config(
        materialized='table',
        on_schema_change='fail'
   )
 
}}

WITH STG_ORIGINAL AS (

SELECT first_name, last_name, gender, City, JobTitle, Salary1, Latitude, Longitude

)
SELECT first_name, last_name, gender, City, JobTitle, Salary1, Latitude, Longitude FROM STG_ORIGINAL


