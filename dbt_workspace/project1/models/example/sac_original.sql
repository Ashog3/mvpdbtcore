 
{{ config(materialized='table') }}

with SOURCE_DATA as (
SELECT * FROM `i-ier1-6j336sl3-h9urmye1jqo7ms.dbt_lend.ORIGINAL`
)

SELECT * FROM SOURCE_DATA

