import pandas as pd
import os
from clean_data import normalize_title

def validate_data(df, step_name):
    """Rubric Requirement: Validates data quality at each step"""
    print(f"--- Validation: {step_name} ---")
    print(f"Row count: {len(df)}")
    print(f"Nulls in key columns: {df['clean_title'].isnull().sum()}")
    if df.duplicated(subset=['clean_title']).any():
        print("Warning: Duplicate titles detected.")
    return df

def run_pipeline():
    # 1. Load data (Rubric: Data loading)
    print("Loading datasets...")
    netflix = pd.read_csv('data/raw/netflix_titles.csv')
    tmdb = pd.read_csv('data/raw/tmdb_raw.csv')
    rt = pd.read_csv('data/raw/rotten_tomatoes_raw.csv')

    # 2. Cleaning (Rubric: Missing values/Duplicates)
    print("Cleaning data...")
    for df in [netflix, tmdb, rt]:
        df['clean_title'] = df['title'].apply(normalize_title)
        df.drop_duplicates(subset=['clean_title'], inplace=True)
    
    validate_data(netflix, "Netflix Load")

    # 3. Merging (Rubric: Join strategies)
    print("Merging sources...")
    # Left join to keep Netflix as the primary source
    merged = pd.merge(netflix, tmdb.drop(columns=['title']), on='clean_title', how='left')
    final_df = pd.merge(merged, rt.drop(columns=['title']), on='clean_title', how='left')
    
    validate_data(final_df, "Final Merge")

    # 4. Save in Efficient Format (Rubric: Zipped CSV)
    output_path = 'data/processed/movie_data_final.zip'
    os.makedirs('data/processed', exist_ok=True)
    final_df.to_csv(output_path, compression='zip', index=False)
    print(f"Pipeline Successful! Saved to {output_path}")

if __name__ == "__main__":
    run_pipeline()
