"""
Part 2: API Data Fetching
Fetches population data from DataUSA API and stores in S3
Uses utils helper functions for data fetching and storage
"""

from utils.helperFunctions import fetch_population_data, save_to_s3

def main():
    """Main function that fetches and stores population data"""
    print("Starting population data fetch...")
    
    try:
        # Fetch data from API using helper function
        data = fetch_population_data()
        print(f"Successfully fetched {len(data.get('data', []))} records from API")
        
        # Save to S3 with timestamp using helper function
        timestamped_key = save_to_s3(data)
        if timestamped_key:
            print(f"Saved timestamped data to: {timestamped_key}")
        
        # Save as latest using helper function
        latest_key = save_to_s3(data, 'api-data/population_data_latest.json')
        if latest_key:
            print(f"Saved latest data to: {latest_key}")
        
        print(f"Population data fetch completed successfully!")
        print(f"Timestamped file: {timestamped_key}")
        print(f"Latest file: {latest_key}")
        
    except Exception as e:
        print(f"Error in population data fetch: {e}")
        raise

if __name__ == "__main__":
    main()
