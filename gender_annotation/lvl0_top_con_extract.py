import csv
import json
import os
import sys
from typing import List, Set, Dict

# -------- CONFIGURATION --------

# 1. INPUT/OUTPUT FILE NAMES (Handles two files)
INPUT_FILE_US = "openalex_us_works_upd100.csv"
INPUT_FILE_EU = "openalex_eu_works_upd100.csv"
OUTPUT_FILE_US = "works_with_raw_domains_US.csv"
OUTPUT_FILE_EU = "works_with_raw_domains_EU.csv"

# 2. FIELD DEFINITIONS
# These are the two new columns we will add to the end of the CSV.
NEW_FIELDNAMES = [
    "raw_topic_domains",
    "raw_concept_domains"
]
# Columns to remove from the output
COLUMNS_TO_DROP = ["topics", "concepts"]


# -------------------------------

def extract_raw_domains(json_string: str, classification_type: str) -> List[str]:
    """
    Safely loads the JSON string and extracts all top-level classification names.
    """
    if not json_string or json_string.strip() in ['[]', '']:
        return []

    extracted_names: Set[str] = set()

    try:
        data = json.loads(json_string)

        if isinstance(data, list):
            for item in data:
                if classification_type == 'concept':
                    # Find all Level 0 Concepts
                    if item.get("level") == 0 and item.get("display_name"):
                        extracted_names.add(item["display_name"])

                elif classification_type == 'topic':
                    # Find all Domain names (Level 0 equivalent)
                    domain_obj = item.get("domain", {})
                    domain_name = domain_obj.get("display_name")
                    if domain_name:
                        extracted_names.add(domain_name)

    except json.JSONDecodeError:
        pass

    return sorted(list(extracted_names))


def process_raw_level0_extraction(input_path: str, output_path: str):
    """
    Reads the input CSV, extracts raw top-level categories from JSON columns,
    and appends them to the output file, dropping the original JSON columns.
    """

    if not os.path.exists(input_path):
        print(f"Error: Input file '{input_path}' not found. Skipping.", file=sys.stderr)
        return

    print(f"\n--- Starting raw extraction for: '{input_path}' ---")
    print(f"Outputting data to '{output_path}'.")

    total_rows = 0

    # Open input file for reading
    with open(input_path, 'r', encoding='utf-8', newline='') as infile:

        reader = csv.DictReader(infile)

        # 1. Get existing fieldnames, excluding the ones we want to drop
        existing_fieldnames = [
            name for name in reader.fieldnames if name not in COLUMNS_TO_DROP
        ] if reader.fieldnames else []

        # 2. Determine the complete set of column names for the output
        all_fieldnames = existing_fieldnames + NEW_FIELDNAMES

        # Open output file, using the standard comma (,) as the delimiter
        with open(output_path, 'w', encoding='utf-8', newline='') as outfile:

            # Use standard comma delimiter
            writer = csv.DictWriter(outfile, fieldnames=all_fieldnames, delimiter=',')
            writer.writeheader()

            # --- Iterate and Process Data Rows ---
            for i, row in enumerate(reader, 1):
                total_rows = i

                if i % 100000 == 0:
                    sys.stdout.write(
                        f"\rProcessed {i:,} rows." + " " * 50
                    )
                    sys.stdout.flush()

                # --- PROCESSING AND EXTRACTION ---

                topics_json_str = row.get("topics", "")
                topic_domains = extract_raw_domains(topics_json_str, 'topic')

                concepts_json_str = row.get("concepts", "")
                concept_domains = extract_raw_domains(concepts_json_str, 'concept')

                # Fix: Remove the columns we don't want in the output.
                for col in COLUMNS_TO_DROP:
                    if col in row:
                        del row[col]

                # Add the new data in PIPE-SEPARATED STRING format (SMALLEST SIZE)
                row["raw_topic_domains"] = "|".join(topic_domains)
                row["raw_concept_domains"] = "|".join(concept_domains)

                writer.writerow(row)

    # Final report
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