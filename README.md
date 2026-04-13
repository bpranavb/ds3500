Milestone 2: Data Pipeline & Integration

Setup Environment: 
pip install pandas pytest

Execute Pipeline: Run the master script from the root directory:

python scripts/main_pipeline.py
This will load raw data from data/raw/, perform a three-way merge, and export the result to data/processed/.

Run Tests: Execute the test suite to verify data integrity:

Bash
pytest test_pipeline.py
Data Cleaning & Decisions
To create a high-quality unified dataset, we made the following engineering choices:

Normalization: We implemented a regex-based normalize_title function. This was essential because movie titles vary across platforms (e.g., "The Matrix" vs "Matrix, The" or "Spider-Man" vs "Spiderman"). Stripping special characters and lowercasing allowed for a significantly higher match rate.

Join Strategy: We used a Left Join anchored on the Netflix dataset. This ensures we keep 100% of our primary source data (Netflix) while enriching it with supplemental metadata from TMDB and Rotten Tomatoes where available.

Deduplication: Before merging, we removed duplicate titles within the individual TMDB and RT datasets to prevent "row explosion" during the join process.

Data Quality & Validation
We integrated validation checks at each step of the pipeline:

Ingestion Check: The pipeline verifies the existence of all three raw CSVs before execution.

Schema Validation: We ensure that the primary join key (clean_title) exists in all sources.

Range Check: We validated that numerical scores (like popularity) fall within expected non-negative ranges.

Final Integrity: The script performs a final count check to ensure the row count matches the original Netflix library.

Known Quality Issues:

Missing Ratings: Not all Netflix titles exist on Rotten Tomatoes. These rows contain NaN for RT-specific columns. We chose to keep these rows to maintain a complete catalog rather than dropping them.

API Constraints: During fetching, we implemented time.sleep intervals to respect TMDB rate limits and avoid 429 errors.

Efficient Storage
The final dataset is saved as a Zipped CSV (merged_movie_data.zip). 

