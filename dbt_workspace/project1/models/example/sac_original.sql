 
{{ config(materialized='table') }}

with SOURCE_DATA as (
SELECT dt.*,Salary1*0.3 As Incm_tax FROM `i-ier1-6j336sl3-h9urmye1jqo7ms.dbt_lend.STG_ORIGINAL dt`
)

SELECT * FROM SOURCE_DATA

