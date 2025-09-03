"""
Lambda handler for data synchronization (Part 1 & 2)
"""

import json
import os
import boto3
import requests
from bs4 import BeautifulSoup
import hashlib
from datetime import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

BUCKET_NAME = os.environ['S3_BUCKET_NAME']
QUEUE_URL = os.environ.get('QUEUE_URL', '')

def sync_bls_data():
    """Sync BLS data to S3"""
    base_url = 'https://download.bls.gov/pub/time.series/pr/'
    prefix = 'bls-data/'
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; DataPipeline/1.0; +admin@example.com)'
    }
    
    try:
        logger.info(f"Fetching BLS file list from: {base_url}")
        # Get file list from BLS
        response = requests.get(base_url, headers=headers)
        response.raise_for_status()
        logger.info(f"BLS response status: {response.status_code}")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a')
        logger.info(f"Found {len(links)} links on BLS page")
        
        files_synced = 0
        
        for link in links:
            href = link.get('href')
            logger.info(f"Processing link: {href}")
            if href and not href.startswith('?') and not href.endswith('/'):
                # Construct the correct URL - href already contains the full path
                file_url = 'https://download.bls.gov' + href
                logger.info(f"Downloading file: {file_url}")
                try:
                    file_response = requests.get(file_url, headers=headers)
                    file_response.raise_for_status()
                    
                    # Clean the href to remove the prefix for S3 key
                    clean_href = href.replace('/pub/time.series/pr/', '')
                    s3_key = prefix + clean_href
                    s3_client.put_object(
                        Bucket=BUCKET_NAME,
                        Key=s3_key,
                        Body=file_response.content,
                        Metadata={
                            'source_url': file_url,
                            'synced_at': datetime.now().isoformat()
                        }
                    )
                    files_synced += 1
                    logger.info(f"Synced {href} to S3 as {s3_key}")
                    
                except Exception as e:
                    logger.error(f"Error syncing {href}: {e}")
            else:
                logger.info(f"Skipping link: {href}")
        
        logger.info(f"BLS sync completed. Total files synced: {files_synced}")
        return files_synced
        
    except Exception as e:
        logger.error(f"Error in BLS sync: {e}")
        raise

def fetch_population_data():
    """Fetch population data from API"""
    api_url = 'https://honolulu-api.datausa.io/tesseract/data.jsonrecords'
    params = {
        'cube': 'acs_yg_total_population_1',
        'drilldowns': 'Year,Nation',
        'locale': 'en',
        'measures': 'Population'
    }
    
    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        # Save to S3
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        s3_key = f'api-data/population_data_{timestamp}.json'
        
        enriched_data = {
            'metadata': {
                'source': api_url,
                'fetched_at': datetime.now().isoformat(),
                'record_count': len(data.get('data', []))
            },
            'data': data
        }
        
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(enriched_data, indent=2),
            ContentType='application/json'
        )
        
        # Also save as latest
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key='api-data/population_data_latest.json',
            Body=json.dumps(enriched_data, indent=2),
            ContentType='application/json'
        )
        
        logger.info(f"Saved population data to {s3_key}")
        return s3_key
        
    except Exception as e:
        logger.error(f"Error fetching population data: {e}")
        raise

def handler(event, context):
    """Lambda handler function"""
    logger.info(f"Starting data sync - Event: {json.dumps(event)}")
    
    results = {
        'statusCode': 200,
        'timestamp': datetime.now().isoformat()
    }
    
    try:
        # Sync BLS data
        logger.info("Starting BLS data sync...")
        bls_files = sync_bls_data()
        logger.info(f"BLS sync completed. Files synced: {bls_files}")
        results['bls_files_synced'] = bls_files
        
        # Fetch population data
        logger.info("Starting population data fetch...")
        population_key = fetch_population_data()
        logger.info(f"Population data fetch completed. Key: {population_key}")
        results['population_data_key'] = population_key
        
        # Send message to SQS if queue URL is configured
        if QUEUE_URL:
            message = {
                'type': 'data_sync_complete',
                'timestamp': datetime.now().isoformat(),
                'bls_files': bls_files,
                'population_key': population_key
            }
            
            sqs_client.send_message(
                QueueUrl=QUEUE_URL,
                MessageBody=json.dumps(message)
            )
            logger.info("Sent completion message to SQS")
        
        results['message'] = 'Data sync completed successfully'
        
    except Exception as e:
        logger.error(f"Error in handler: {e}")
        results['statusCode'] = 500
        results['error'] = str(e)
        results['message'] = 'Data sync failed'
    
    return results
