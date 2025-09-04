import boto3
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import hashlib
from utils import constants
import json
from datetime import datetime
import logging
import os
logging.basicConfig(level=logging.INFO)

# Constants
api_url = constants.api_url
base_url = constants.base_url
headers = {
        'User-Agent': 'DataPipeline/1.0 (kramkrishnaachary01@gmail.com)',
        'Referer': base_url
    }

# Initialize S3 client
s3_client = boto3.client('s3')
bucket_name = os.environ.get('S3_BUCKET_NAME', constants.bucket_name)
prefix = constants.prefix
metadata_key = f"{prefix}/sync_metadata.json"

def get_file_list_from_bls():
    """Fetch list of files from BLS website"""
    try:
        response = requests.get(base_url, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        files = {}

        for link in soup.find_all('a'):
            href = link.get('href')
            if href and not href.endswith('/') and not href.startswith('?'):
                file_url = urljoin(base_url, href)
                files[href] = {
                    'url': file_url,
                    'last_modified': 'N/A',
                    'size': 'N/A'
                }

        logging.info(f"Found {len(files)} files on BLS website")
        return files

    except requests.RequestException as e:
        logging.error(f"Error fetching BLS file list: {e}")
        raise

def get_s3_metadata():
    """Get sync metadata from S3"""
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=metadata_key)
        return json.loads(response['Body'].read())
    except s3_client.exceptions.NoSuchKey:
        logging.info("No existing metadata found, starting fresh sync")
        return {'files': {}, 'last_sync': None}
    except Exception as e:
        logging.error(f"Error reading metadata: {e}")
        raise e

def save_s3_metadata(metadata):
    """Save sync metadata to S3"""
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=metadata_key,
            Body=json.dumps(metadata, indent=2),
            ContentType='application/json'
            )
        logging.info("Metadata saved successfully")
    except Exception as e:
        logging.error(f"Error saving metadata: {e}")
        raise e

def calculate_file_hash(content: bytes) -> str:
    """Calculate MD5 hash of file content"""
    return hashlib.md5(content).hexdigest()

def should_update_file(filename: str, file_info: dict, existing_metadata: dict) -> bool:
    """Check if file should be updated by comparing hashes"""
    try:
        # Download file content to check hash
        response = requests.get(file_info['url'], headers=headers)
        response.raise_for_status()
        
        current_hash = calculate_file_hash(response.content)
        stored_hash = existing_metadata.get('file_hash', '')
        
        # Update if hashes are different
        return current_hash != stored_hash
        
    except Exception as e:
        logging.warning(f"Could not check hash for {filename}, will update: {e}")
        return True

def download_and_upload_file(filename, file_info):
        """Download a file from BLS and upload to S3"""
        try:
            # Download file
            logging.info(f"Downloading {filename}")
            response = requests.get(file_info['url'], headers=headers)
            response.raise_for_status()

            content = response.content
            file_hash = calculate_file_hash(content)

            # Upload to S3
            # Clean filename to avoid double slashes
            clean_filename = filename.lstrip('/')
            s3_key = f"{prefix}/{clean_filename}"
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=content,
                Metadata={
                    'source_url': file_info['url'],
                    'last_modified': file_info['last_modified'],
                    'file_hash': file_hash
                }
            )
            logging.info(f"Uploaded {filename} to s3://{bucket_name}/{s3_key}")
            return file_hash

        except Exception as e:
            logging.error(f"Error processing {filename}: {e}")
            return False

def delete_from_s3(filename: str):
    """Delete file from S3"""
    try:
        clean_filename = filename.lstrip('/')
        s3_key = f"{prefix}/{clean_filename}"
        s3_client.delete_object(Bucket=bucket_name, Key=s3_key)
        logging.info(f"Deleted {filename} from S3")
    except Exception as e:
        logging.error(f"Error deleting {filename}: {e}")

def fetch_population_data() -> dict:
    """Fetch population data from DataUSA API"""
    api_url = constants.api_url
    params = {
        'cube': 'acs_yg_total_population_1',
        'drilldowns': 'Year,Nation',
        'locale': 'en',
        'measures': 'Population'
    }
    try:
        logging.info(f"Fetching data from API: {api_url}")
        response = requests.get(api_url, params=params)
        response.raise_for_status()

        data = response.json()
        logging.info(f"Successfully fetched {len(data.get('data', []))} records")
        return data

    except requests.RequestException as e:
        logging.error(f"Error fetching API data: {e}")
        raise


def save_to_s3(data: dict, filename: str = None):
    """Save JSON data to S3"""
    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'api-data/population_data_{timestamp}.json'

    s3_key = filename

    try:
        # Add metadata to the data
        enriched_data = {
            'metadata': {
                'source': api_url,
                'fetched_at': datetime.now().isoformat(),
                'record_count': len(data.get('data', []))
            },
            'data': data
        }
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(enriched_data, indent=2),
            ContentType='application/json',
            Metadata={
                'source': 'datausa-api',
                'fetched_at': datetime.now().isoformat()
            }
        )
        logging.info(f"Saved data to s3://{bucket_name}/{s3_key}")
        return s3_key

    except Exception as e:
        logging.error(f"Error saving to S3: {e}")
        return None

