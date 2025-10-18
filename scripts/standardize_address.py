import pandas as pd
import requests
import time
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()


# Claude helps my with this script


# --- Configuration ---
INPUT_FILENAME = os.getenv("input_filename")
OUTPUT_FILENAME = os.getenv("output_filename")
PROPERTY_ID_COL = 'property_id'
ADDRESS_COL = 'property_address'
SAVE_INTERVAL = 500
RATE_LIMIT_DELAY = 0.25

def create_retry_session():
    """Creates a requests Session with automatic retry logic."""
    session = requests.Session()
    retry_strategy = Retry(
        total=3, status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"], backoff_factor=1
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def geocode_address(session, address, property_id):
    """Geocodes a single address, returning the standardized label."""
    if not address or pd.isna(address):
        return "Missing Address"
    try:
        base_url = "https://geosearch.planninglabs.nyc/v2/search"
        response = session.get(base_url, params={'text': address}, timeout=(5, 15))
        response.raise_for_status()
        data = response.json()
        return data['features'][0]['properties']['label'] if data.get('features') else "Address Not Found in NYC"
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Property ID '{property_id}': Request failed for '{address}'. Error: {e}")
        return "API Request Error"

def print_progress(current, total, start_time):
    """Prints progress indicator with percentage and estimated time remaining."""
    elapsed = time.time() - start_time
    percent = (current / total) * 100
    
    if current > 0:
        avg_time_per_item = elapsed / current
        remaining = total - current
        eta_seconds = avg_time_per_item * remaining
        eta_minutes = eta_seconds / 60
        
        print(f"Progress: {current}/{total} ({percent:.1f}%) | "
              f"Elapsed: {elapsed/60:.1f}m | ETA: {eta_minutes:.1f}m")
    else:
        print(f"Progress: {current}/{total} ({percent:.1f}%)")

def main():
    """Main function to run the resumable address standardization process."""
    print(f"Reading data from '{INPUT_FILENAME}'...")
    try:
        df_full_input = pd.read_csv(INPUT_FILENAME, low_memory=False)
    except FileNotFoundError:
        print(f"ERROR: Input file '{INPUT_FILENAME}' not found.")
        return

    processed_ids = set()
    # --- RESUME LOGIC ---
    if os.path.exists(OUTPUT_FILENAME):
        print(f"Found existing output file: '{OUTPUT_FILENAME}'. Resuming from last run.")
        try:
            df_processed = pd.read_csv(OUTPUT_FILENAME)
            if not df_processed.empty:
                processed_ids = set(df_processed[PROPERTY_ID_COL])
                print(f"Loaded {len(processed_ids)} already processed IDs.")
            else:
                print("Output file exists but is empty. Starting fresh.")
        except pd.errors.EmptyDataError:
            print("Output file exists but is empty. Starting fresh.")

    # Filter out the rows that have already been processed
    df_to_process = df_full_input[~df_full_input[PROPERTY_ID_COL].isin(processed_ids)].copy()

    if df_to_process.empty:
        print("All addresses have already been processed. Nothing to do.")
        return

    total_to_process = len(df_to_process)
    print(f"Processing {total_to_process} remaining addresses...")
    
    api_session = create_retry_session()
    results = []
    start_time = time.time()
    processed_count = 0

    for index, row in df_to_process.iterrows():
        current_property_id = row[PROPERTY_ID_COL]
        current_address = row[ADDRESS_COL]
        
        standardized = geocode_address(api_session, current_address, current_property_id)
        results.append({
            PROPERTY_ID_COL: current_property_id,
            ADDRESS_COL: current_address,
            'standardized_address': standardized
        })
        
        processed_count += 1
        
        # Add rate limiting delay
        time.sleep(RATE_LIMIT_DELAY)
        
        # --- PERIODIC SAVE LOGIC ---
        if len(results) % SAVE_INTERVAL == 0 and len(results) > 0:
            print_progress(processed_count, total_to_process, start_time)
            print(f"  ...saving progress...")
            df_batch = pd.DataFrame(results)
            # Append to the CSV. Write header only if the file is new.
            df_batch.to_csv(
                OUTPUT_FILENAME, 
                mode='a', 
                header=not os.path.exists(OUTPUT_FILENAME), 
                index=False
            )
            results = [] # Clear the list after saving

    # --- FINAL SAVE ---
    # Save any remaining results that are left over
    if results:
        print_progress(processed_count, total_to_process, start_time)
        print("Saving final batch of results...")
        df_batch = pd.DataFrame(results)
        df_batch.to_csv(
            OUTPUT_FILENAME, 
            mode='a', 
            header=not os.path.exists(OUTPUT_FILENAME), 
            index=False
        )

    print("\nProcessing complete.")
    print(f"All standardized addresses are in '{OUTPUT_FILENAME}'")
    total_time = time.time() - start_time
    print(f"Total time: {total_time/60:.1f} minutes")

if __name__ == "__main__":
    main()