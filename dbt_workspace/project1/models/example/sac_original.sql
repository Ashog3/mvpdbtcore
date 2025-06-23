 
{{ config(materialized='table') }}

with SOURCE_DATA as (
SELECT id,first_name,last_name,gender,City,JobTitle,cast(REPLACE(Salary1,"'","")as INT64) as Salary1
,cast(REPLACE(Latitude,"'","") as FLOAT64) as Latitude
,cast(REPLACE(Longitude,"'","") as FLOAT64) as Longitude,cast(REPLACE(Salary1,"'","") as FLOAT64)*0.3 As Incm_tax 
FROM `i-ier1-6j336sl3-h9urmye1jqo7ms.dbt_lend.STG_ORIGINAL dt`
)

SELECT * FROM SOURCE_DATA

