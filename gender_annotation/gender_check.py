import dask.dataframe as dd
import os
import glob
from dask.diagnostics import ProgressBar
from typing import Dict, Any, Tuple

# ---------- CONFIG ----------
# Define the directories containing the partitioned output CSVs
PROCESSED_DATA_DIRS: Dict[str, Dict[str, Any]] = {
    "US": {
        # This MUST match the output_dir from your full_author_labeler.py script
        "input_dir": "openalex_us_final_labeled/",
        "label": "US"
    },
    "EU": {
        "input_dir": "openalex_eu_final_labeled/",
        "label": "EU"
    }
}


# ---------- FUNCTIONS ----------

def calculate_gender_stats(input_dir: str, label: str):
    """
    Loads partitioned Dask DataFrame and calculates the counts and percentages of
    'male', 'female', 'unknown', and 'other' gender classifications for primary and last authors.
    """

    # Check if the output directory contains partitioned files
    file_pattern = os.path.join(input_dir, "part_*.csv")
    if not glob.glob(file_pattern):
        print(f"Skipping {label}: No partitioned files found in '{input_dir}'.")
        return

    print(f"\n--- Starting Statistical Analysis for {label} Data ---")

    # Lazy load the partitioned data
    ddf = dd.read_csv(file_pattern, assume_missing=True, usecols=[
        "work_id",
        "primary_author_gender",
        "last_author_gender"
    ])

    # Calculate total works
    print("Calculating total works...")
    # FIX: Use the Dask method to get the length lazily, returning a Dask Scalar object
    total_works_dask = ddf.shape[0]

    with ProgressBar():
        total_works_computed = total_works_dask.compute()

    if total_works_computed == 0:
        print(f"No records found in {label} data.")
        return

    print(f"Total Works Loaded for {label}: {total_works_computed:,}")

    # --- Helper to calculate all four gender counts for a given position ---
    def calculate_counts_for_position(ddf, position_col, total_works):
        print(f"Calculating {position_col} counts...")

        # Calculate counts for male, female, and unknown
        male_count_dask = (ddf[position_col] == 'male').sum()
        female_count_dask = (ddf[position_col] == 'female').sum()
        unknown_count_dask = (ddf[position_col] == 'unknown').sum()

        # Calculate 'other' count: anything that is not male, female, or unknown
        is_known = (ddf[position_col] == 'male') | (ddf[position_col] == 'female') | (ddf[position_col] == 'unknown')
        other_count_dask = (~is_known).sum()

        # Compute all results together for efficiency
        # The compute call now returns a 4-element tuple
        with ProgressBar():
            results: Tuple[int, int, int, int] = dd.compute(male_count_dask, female_count_dask, unknown_count_dask,
                                                            other_count_dask)

        male_count, female_count, unknown_count, other_count = results

        # Calculate percentages
        male_percent = (male_count / total_works) * 100
        female_percent = (female_count / total_works) * 100
        unknown_percent = (unknown_count / total_works) * 100
        other_percent = (other_count / total_works) * 100

        return {
            'male_count': male_count, 'male_percent': male_percent,
            'female_count': female_count, 'female_percent': female_percent,
            'unknown_count': unknown_count, 'unknown_percent': unknown_percent,
            'other_count': other_count, 'other_percent': other_percent
        }

    # ------------------------------------------------------------------------

    # --- Calculation 1: Primary Author Stats ---
    primary_stats = calculate_counts_for_position(ddf, 'primary_author_gender', total_works_computed)

    # --- Calculation 2: Last Author Stats ---
    last_stats = calculate_counts_for_position(ddf, 'last_author_gender', total_works_computed)

    # --- Report ---
    print("\n--- RESULTS ---")
    print(f"Dataset: {label} ({total_works_computed:,} records)")
    print("-" * 30)

    # Primary Author Report
    print(f"PRIMARY AUTHOR GENDER DISTRIBUTION:")
    print(f"| Male:    {primary_stats['male_count']:,} ({primary_stats['male_percent']:.2f}%)")
    print(f"| Female:  {primary_stats['female_count']:,} ({primary_stats['female_percent']:.2f}%)")
    print(f"| Unknown: {primary_stats['unknown_count']:,} ({primary_stats['unknown_percent']:.2f}%)")
    print(f"| Other:   {primary_stats['other_count']:,} ({primary_stats['other_percent']:.2f}%)")
    print("-" * 30)

    # Last Author Report
    print(f"LAST AUTHOR GENDER DISTRIBUTION:")
    print(f"| Male:    {last_stats['male_count']:,} ({last_stats['male_percent']:.2f}%)")
    print(f"| Female:  {last_stats['female_count']:,} ({last_stats['female_percent']:.2f}%)")
    print(f"| Unknown: {last_stats['unknown_count']:,} ({last_stats['unknown_percent']:.2f}%)")
    print(f"| Other:   {last_stats['other_count']:,} ({last_stats['other_percent']:.2f}%)")
    print("-" * 30)


# ---------- MAIN EXECUTION ----------
if __name__ == "__main__":
    for region, paths in PROCESSED_DATA_DIRS.items():
        calculate_gender_stats(paths["input_dir"], paths["label"])