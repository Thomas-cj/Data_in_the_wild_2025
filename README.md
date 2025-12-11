## Gender and Leadership in Research: Trends in Female-Led Studies Across EU and US Presidential Eras

### Data in wild 2025

#### To reproduce our results:

1. Connect to our google drive using the link available on request and download raw snapshot data (~500 GB)

2. Download raw data into the project directory and run *1_Extract_works.py*, *2_Extract_Top_Level_Top-Cons.py* and *3_Add_Names_and_Genders.py* to create 

3. Run *4_gender_database_creation.ipynb* to generate duckdb database
    - Optionally download the database $gender_db.duckdb$ into the project directory from our google drive directly (~ 16GB) and skip database creation in the notebook

4. Unknown gender analysis can be reproduced by running *Unknown_gender_analysis.ipynb* with *gender_db.duckdb* in the directory

5. Annotation validation can be reproduced by running *validation.ipynb* in the validation directory

6. Statistical analysis can be reproduced by running