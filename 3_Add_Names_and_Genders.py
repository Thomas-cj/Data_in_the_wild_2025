import dask.dataframe as dd
import re
from dask.diagnostics import ProgressBar
import glob
from names_dataset import NameDataset, NameWrapper
from typing import Dict, List, Any, Optional
import os
import sys


#input and output file names / directories
CSV_FILES: Dict[str, Dict[str, Any]] = {
    "US": {
        "input": "works_with_raw_domains_US.csv",
        "output_dir": "openalex_us_final_labeledupd/"
    },
    "EU": {
        "input": "works_with_raw_domains_EU.csv",
        "output_dir": "openalex_eu_final_labeledupd/"
    }
}

# Final column order, with primary_author_first_name, primary_author_gender, 
# last_author_first_name, last_author_gender added
DESIRED_COLUMNS = [
    "work_id", 
    "title", 
    "publication_year", 
    "country_codes", 
    "primary_author_name", 
    "primary_author_id", 
    "primary_author_first_name",
    "primary_author_gender",    
    "last_author_name", 
    "last_author_id", 
    "last_author_first_name",  
    "last_author_gender",
    "source_id", 
    "source_name", 
    "raw_topic_domains", 
    "raw_concept_domains"
]


# Initialize the NameDataset globally
nd = NameDataset()

#extract first name using capitalisation and spaces
def first_name_from_caps(name: str) -> str:
    if not isinstance(name, str) or not name:
        return ""
    
    # Find first space (if exists)
    space_pos: Optional[int] = name.find(" ") if " " in name else None

    # Find capital letter positions
    capitals: List[int] = [m.start() for m in re.finditer(r"[A-Z]", name)] #list of all capital letter positions
    second_cap_pos: Optional[int] = capitals[1] if len(capitals) > 1 else None #second capital position from list

    # Determine cutoff: smallest valid positive index
    cut_positions = [p for p in [space_pos, second_cap_pos] if p is not None and p != -1] #list of positions of spaces and the second capital letter
    cutoff = min(cut_positions) if cut_positions else len(name) #if space or second capital exists gives position to cut first name from full name, else gives position at end of name

    # Extract substring and clean it
    first_name = name[:cutoff].strip()

    # Keep only Unicode letters
    first_name = re.sub(r"[^A-Za-zÀ-ÖØ-öø-ÿ]", "", first_name)

    return first_name


#use NameDataSet to annotate gender of first name
def infer_gender(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        return "unknown" #unknown if no name
    try:
        # NameDataset returns a NameWrapper object
        result = NameWrapper(nd.search(name))
        #return either gender of name, or if gender returns None / an empty string return Unknown
        return result.gender.lower() if result.gender else "unknown"
    except Exception:
        # Fallback for any internal library errors
        return "unknown"

#process a regions data (us/eu)
def process_region(region: str, input_path: str, output_dir: str):
    print(f"Processing {region} data from {input_path}...")

    # 1. Lazy read the partitioned file
    try:
        ddf = dd.read_csv(input_path, assume_missing=True, dtype={
            'primary_author_name': 'object',
            'last_author_name': 'object',
            'raw_topic_domains': 'object', 
            # Explicitly set object dtypes for the columns that often contain mixed types (IDs/Names)
            'source_id': 'object',
            'source_name': 'object'
        })
    except Exception as e:
        print(f"Error reading CSV file {input_path}: {e}", file=sys.stderr)
        return

    # Ensure author name columns are clean strings for the next map operation
    ddf["primary_author_name"] = ddf["primary_author_name"].astype(str).fillna("")
    ddf["last_author_name"] = ddf["last_author_name"].astype(str).fillna("")
    
    # 2. Apply robust first-name extraction (Dask Map)
    print("Extracting first names...")
    ddf["primary_author_first_name"] = ddf["primary_author_name"].map(
        first_name_from_caps, meta=("primary_author_first_name", "object")
    )
    ddf["last_author_first_name"] = ddf["last_author_name"].map(
        first_name_from_caps, meta=("last_author_first_name", "object")
    )

    # 3. Create Gender Mapping (Dask Optimization)
    # Extract unique names
    print("Extracting unique first names for gender mapping...")
    with ProgressBar():
        unique_primary_names = ddf["primary_author_first_name"].drop_duplicates().compute() #extract unique first author names
        unique_last_names = ddf["last_author_first_name"].drop_duplicates().compute() #extract uunique last author names

    # Create dictionaries of names mapped to genders to prevent repeated running of the same names on different people
    print(f"Inferring genders for {len(unique_primary_names) + len(unique_last_names)} unique names...")
    primary_gender_map = {name: infer_gender(name) for name in unique_primary_names} #dict for first author names
    last_gender_map = {name: infer_gender(name) for name in unique_last_names} #dict for last author names

    #Map Gender back to the full Dask DataFrame 
    ddf["primary_author_gender"] = ddf["primary_author_first_name"].map(
        primary_gender_map, meta=("primary_author_gender", "object") #map gender back to all first authors
    )
    ddf["last_author_gender"] = ddf["last_author_first_name"].map(
        last_gender_map, meta=("last_author_gender", "object") #map gender bck to all last authors
    )

    #reorder columns to put info for each author together
    ddf_reordered = ddf[DESIRED_COLUMNS]



    with ProgressBar(): #using a  progress tracker
        # Ensure the directory exists
        os.makedirs(output_dir, exist_ok=True)
        # Write the updated DataFrame
        ddf_reordered.to_csv(output_dir + "part_*.csv", index=False)

    print(f"✅ Finished processing {region}. Output saved to {output_dir}")


# ---------- MAIN EXECUTION ----------
if __name__ == "__main__":
    for region, paths in CSV_FILES.items():
        # Ensure the output directory exists
        os.makedirs(paths["output_dir"], exist_ok=True)
        
        # Run the combined pipeline
        process_region(region, paths["input"], paths["output_dir"])