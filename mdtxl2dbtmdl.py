import pandas as pd
import sys

# Filepath of the Excel file containing the sheets
excel_file = "MetaSheet_crd_rsk_1.xlsx"

# Read the sheets into DataFrames
sourcefile_df = pd.read_excel(excel_file, sheet_name="Sourcefield")
targettable_df = pd.read_excel(excel_file, sheet_name="TargetTable")

# Ensure the required columns exist in both sheets
if "Source_table" not in sourcefile_df.columns or "Target_table" not in targettable_df.columns:
    raise ValueError("Required columns 'Sourcefile' or 'Targettable' are missing in the sheets.")

# Extract the source table and columns
source_table = sourcefile_df["Source_table"].iloc[0]
source_columns = sourcefile_df["Field_name"].iloc[1:].tolist()
# source_columns = source_columns.append('current_timestamp() as load_time')

# Extract the target table and columns
target_table = targettable_df["Target_table"].iloc[0]
target_columns = targettable_df["Field_name"].iloc[1:].tolist()

sql_statement1= f""" 
SELECT * FROM `dbtwithgcp.mydbtproject.{source_table}`
"""

# Generate the SQL statement
sql_statement2 = f"""

{{{{
   config(
        materialized='incremental',
        on_schema_change='fail'
   )
 
}}}}

WITH STG_{source_table} AS (

SELECT id,{', '.join(source_columns)}
FROM {{{{ ref("sac_original") }}}} 

)
SELECT id,{', '.join(source_columns)},current_timestamp() as load_time FROM STG_{source_table}

{{% if is_incremental() %}}
where id > ( select max(id) from {{{{this}}}})
{{% endif %}}
"""

# Output the SQL statement
print("Generated SQL Statements:")
print("SAC \n",sql_statement1)
print("CDS \n", sql_statement2)

root_path = sys.argv[1]
# Write the SQL statement to a file
output_file = f"{root_path}/models/example/sac_{source_table.lower()}.sql"
with open(output_file, "w") as file:
    file.write(sql_statement1)

# Write the SQL statement to a file
output_file = f"{root_path}/models/example/cds_{source_table.lower()}.sql"
with open(output_file, "w") as file:
    file.write(sql_statement2)

print(f"SQL statement has been written to {output_file}")
