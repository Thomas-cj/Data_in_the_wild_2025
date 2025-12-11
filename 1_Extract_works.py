import os
import gzip
import json
import csv
import glob
import time

#file locations
SNAPSHOT_DIR = r"C:\Users\Gareth\open\openalex-snapshot\data\works" 
OUT_US = "openalex_us_works_upd100.csv"
OUT_EU = "openalex_eu_works_upd100.csv"

#year and country codes for extracction
MIN_YEAR = 1980
EU_COUNTRY_CODES = {
    "AT","BE","BG","HR","CY","CZ","DK","EE","FI","FR","DE",
    "GR","HU","IE","IT","LV","LT","LU","MT","NL","PL","PT",
    "RO","SK","SI","ES","SE"
}
US_CODE = "US"


#fieldnames fpr extraction
FIELDNAMES = [
    "work_id", "title", "publication_year", "country_codes",
    "primary_author_name", "primary_author_id",
    "last_author_name", "last_author_id",
    "topics", "concepts", "source_id", "source_name"
]

#extract first and last authors for a work
def extract_author(authorships, position="first"):
    if not authorships:
        return {"name": "", "id": ""}
    #extarct first author id "first", else last author
    a = authorships[0] if position == "first" else authorships[-1]
    #extract name, id, and display name for current author
    author = a.get("author") or {}
    return {"name": author.get("display_name",""), "id": author.get("id","")}

#extract country codes of a work
def work_country_codes(work):
    codes = set()
    for auth in work.get("authorships") or []: #get authors in work
        for inst in auth.get("institutions") or []: #get institutions for each author
            cc = inst.get("country_code") or inst.get("geo",{}).get("country_code") #get country code for each institution
            if cc:
                codes.add(cc.upper()) #add codes in uppercase
    host = (work.get("primary_location") or {}).get("source") or {} #extract listed primary location
    if host.get("country_code"):
        codes.add(host["country_code"].upper()) #add country code for primary location
    return codes

def process_file(gz_path):
    #Process a gzip file and extract US/EU works
    us_rows = []
    eu_rows = []
    
    #extarct work
    with gzip.open(gz_path, "rt", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                work = json.loads(line)
            except json.JSONDecodeError:
                continue
            
            #extract publication year, and discard work if publication year not listed or too early.
            year = work.get("publication_year")
            if not isinstance(year, int) or year < MIN_YEAR: 
                continue
            
            #extract country codes for work
            countries = work_country_codes(work)
            if not countries:
                continue
            
            #check if country codes from us or eu, if not discard work
            has_us = US_CODE in countries
            has_eu = any(c in EU_COUNTRY_CODES for c in countries)
            if not (has_us or has_eu):
                continue
            

            #extarct first author, last author, and source
            first_author = extract_author(work.get("authorships") or [], "first")
            last_author = extract_author(work.get("authorships") or [], "last")
            source = (work.get("primary_location") or {}).get("source") or {}

            #extract entire concepts and topics jsons for later processing
            topics_json = json.dumps(work.get("topics", []), ensure_ascii=False)
            concepts_json = json.dumps(work.get("concepts", []), ensure_ascii=False)

            #prepare row as dictionary containing all info wanted for extraction
            row = {
                "work_id": work.get("id", ""),
                "title": (work.get("display_name") or "").replace("\n", " ").strip(),
                "publication_year": year,
                "country_codes": ";".join(sorted(countries)),
                "primary_author_name": first_author["name"],
                "primary_author_id": first_author["id"],
                "last_author_name": last_author["name"],
                "last_author_id": last_author["id"],
                "topics": topics_json,
                "concepts": concepts_json,
                "source_id": source.get("id", ""),
                "source_name": source.get("display_name", "")
            }

            #add row to relevant dataset 
            if has_us:
                us_rows.append(row)
            if has_eu:
                eu_rows.append(row)

    return us_rows, eu_rows #return all rows ready for storage

#function to write rows to csvs
def write_csv(filename, rows):
    if not rows: #if nothing to write, stop
        return
    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if f.tell() == 0: #if file is empty add header
            writer.writeheader()
        writer.writerows(rows) #write rows into csv

def main():
    # Remove old outputs
    for fpath in [OUT_US, OUT_EU]:
        if os.path.exists(fpath):
            os.remove(fpath)

    #assemble snapshot files for processing
    gz_files = sorted(glob.glob(os.path.join(SNAPSHOT_DIR, "*", "*.gz")))

    #prepare info for progress tracking whilst function runs
    total_files = len(gz_files)
    totals = {"us": 0, "eu": 0, "files": 0}
    start_time = time.time()


    for gz_path in gz_files:
        us_rows, eu_rows = process_file(gz_path) #process files, extract relevant info

        write_csv(OUT_US, us_rows) #write us works csv
        write_csv(OUT_EU, eu_rows) #write eu works csv

        #update info for progress tracking
        totals["us"] += len(us_rows)
        totals["eu"] += len(eu_rows)
        totals["files"] += 1

        # Progress tracking (ballpark, very not precise / reliable)
        elapsed = time.time() - start_time
        avg_per_file = elapsed / totals["files"]
        remaining_files = total_files - totals["files"]
        eta = remaining_files * avg_per_file
        eta_str = time.strftime("%H:%M:%S", time.gmtime(eta))

        print(
            f"Processed {totals['files']}/{total_files} files → "
            f"US={totals['us']}, EU={totals['eu']} | ETA: {eta_str}",
            end="\r"
        )

    #extraction complete update
    print("\nExtraction complete.")
    print(f"US works CSV: {OUT_US} ({totals['us']} records)")
    print(f"EU works CSV: {OUT_EU} ({totals['eu']} records)")

if __name__ == "__main__":
    main()
