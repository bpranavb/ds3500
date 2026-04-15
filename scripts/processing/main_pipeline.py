import pandas as pd
import os
import sys


sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from clean_data import normalize_title

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def normalize_title(title):
    if not isinstance(title, str): return ""
    return "".join(e for e in title.lower() if e.isalnum())

def run_pipeline():
    print("Starting Data Pipeline...")
    

    try:
        netflix = pd.read_csv('data/raw/netflix_titles.csv')
        tmdb = pd.read_csv('data/raw/tmdb_raw.csv')
        rt = pd.read_csv('data/raw/rotten_tomatoes_raw.csv')
    except FileNotFoundError as e:
        print(f"Error: Missing raw data files. {e}")
        return

    for df in [netflix, tmdb, rt]:
        df['clean_title'] = df['title'].apply(normalize_title)
        df.drop_duplicates(subset=['clean_title'], inplace=True)

    print("Merging Netflix, TMDB, and Rotten Tomatoes data...")
    merged = pd.merge(netflix, tmdb.drop(columns=['title']), on='clean_title', how='left')
    final_df = pd.merge(merged, rt.drop(columns=['title']), on='clean_title', how='left')

    final_df = final_df.dropna(subset=['title']) 
    final_df = final_df.drop_duplicates(subset=['show_id'])

    output_dir = 'data/processed'
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, 'merged_movie_data.zip')
    
    final_df.to_csv(output_file, compression='zip', index=False)
    print(f"Success! Final dataset saved to: {output_file}")

if __name__ == "__main__":
    run_pipeline()