import csv
import json
import os
import sys
from typing import List, Set, Dict


#input and output filenames
OUTPUT_FILE_US = "works_with_raw_domains_US.csv" 
OUTPUT_FILE_EU = "works_with_raw_domains_EU.csv"

#New columns to add
NEW_FIELDNAMES = [
    "raw_topic_domains", 
    "raw_concept_domains"
]
# Columns to remove
COLUMNS_TO_DROP = ["topics", "concepts"]

#function to read json string and extract top level concepts and topics
def extract_raw_domains(json_string: str, classification_type: str) -> List[str]:
    if not json_string or json_string.strip() in ['[]', '']:
        return [] #no domains if json doesn't exit
        
    extracted_names: Set[str] = set() #create set for extracted topics or domains
    
    try:
        data = json.loads(json_string) #load json string 
        
        if isinstance(data, list):
            for item in data:
                if classification_type == 'concept': #if concepts
                    if item.get("level") == 0 and item.get("display_name"): # Find all Level 0 Concepts
                        extracted_names.add(item["display_name"]) #store in set
                        
                elif classification_type == 'topic':  #if topics
                    domain_obj = item.get("domain", {}) # extract domain (top level topic) info
                    domain_name = domain_obj.get("display_name") #extract domain name
                    if domain_name:
                        extracted_names.add(domain_name) #store domain name in set
                        
    except json.JSONDecodeError: #skip errors
        pass 
        
    return sorted(list(extracted_names)) #return sorted list of topics or concepts



# extract and store top level concept and topics, add and remove required columns from csv
def process_raw_level0_extraction(input_path: str, output_path: str):
    
    if not os.path.exists(input_path):
        print(f"Error: Input file '{input_path}' not found. Skipping.", file=sys.stderr)
        return

    print(f"\n--- Starting raw extraction for: '{input_path}' ---")
    print(f"Outputting data to '{output_path}'.")

    total_rows = 0
    
    # Open input file for reading
    with open(input_path, 'r', encoding='utf-8', newline='') as infile:
        
        reader = csv.DictReader(infile)
        
        #Get existing fieldnames, excluding the ones we want to drop
        existing_fieldnames = [
            name for name in reader.fieldnames if name not in COLUMNS_TO_DROP
        ] if reader.fieldnames else []
        
        # Add new fieldnames
        all_fieldnames = existing_fieldnames + NEW_FIELDNAMES
        
        # Open output file, using comma as the delimiter
        with open(output_path, 'w', encoding='utf-8', newline='') as outfile:
            
            writer = csv.DictWriter(outfile, fieldnames=all_fieldnames, delimiter=',') 
            writer.writeheader()
            
            #track roaws processed for progeress tracking
            for i, row in enumerate(reader, 1):
                total_rows = i

                #print tracking tom terminal every 100,000 rows
                if i % 100000 == 0:
                    sys.stdout.write(
                        f"\rProcessed {i:,} rows." + " " * 50 
                    )
                    sys.stdout.flush()
                    
                #topic extraction
                topics_json_str = row.get("topics", "") #take whole topics json string
                topic_domains = extract_raw_domains(topics_json_str, 'topic') #extract top level topics from json
                
                #concept extraction
                concepts_json_str = row.get("concepts", "") #take whole concepts json string
                concept_domains = extract_raw_domains(concepts_json_str, 'concept') #extract top level concepts


                #drop old columns containing whole json strings, to massively reduce file size
                for col in COLUMNS_TO_DROP:
                    if col in row:
                        del row[col]

                # Add the new data as pipe seperateted strings (smaller than adding as json string)
                row["raw_topic_domains"] = "|".join(topic_domains)
                row["raw_concept_domains"] = "|".join(concept_domains)

                writer.writerow(row)

    # progress complete printout
    print(f"\rProcessed {total_rows:,} rows. Data extraction complete.")
    print("-" * 50)
    print(f"Output saved to '{output_path}'. (Delimiter is ',')")
    
if __name__ == "__main__":
    print("--- Starting Two-File Raw Domain Extraction ---")
    
    # Process US file
    process_raw_level0_extraction(INPUT_FILE_US, OUTPUT_FILE_US)
    
    # Process EU file
    process_raw_level0_extraction(INPUT_FILE_EU, OUTPUT_FILE_EU)
    
    print("\nAll tasks finished.")