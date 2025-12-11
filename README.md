## Gender and Leadership in Research: Trends in Female-Led Studies Across EU and US Presidential Eras

### Data in wild 2025

#### To reproduce our results from database (~16 GB download):

1. Install requirements.txt in a python environment with python >= 3.13.5

2. Connect to our google drive using the link available on request and download the database *gender_db.duckdb* into the project directory (~ 16GB) from the folder *duckdb_database* and skip database creation cells in the project notebook
    - Link is available as a text file *drive.txt* in the project submission

3. Unknown gender analysis can be reproduced by running *Unknown_gender_analysis.ipynb* with *gender_db.duckdb* in the main project directory

4. Annotation validation can be reproduced by running *validation.ipynb* in the validation directory

5. Statistical analysis can be reproduced by running 

#### To reproduce our results from raw data (~500 GB download):

1. Install requirements.txt in a python environment with python >= 3.13.5

2. Connect to our google drive using the link available on request and download raw snapshot data (~500 GB) from the folder *data/raw*
    - Link is available as a text file *drive.txt* in the project submission
    
3. Copy raw data into the project directory and run *1_Extract_works.py*, *2_Extract_Top_Level_Top-Cons.py* and *3_Add_Names_and_Genders.py* to create annotated data

4. Run *4_gender_database_creation.ipynb* to generate duckdb database using the cells marked with database creation

5. Unknown gender analysis can be reproduced by running *Unknown_gender_analysis.ipynb* with *gender_db.duckdb* in the main project directory

6. Annotation validation can be reproduced by running *validation.ipynb* in the validation directory

7. Statistical analysis can be reproduced by running 
