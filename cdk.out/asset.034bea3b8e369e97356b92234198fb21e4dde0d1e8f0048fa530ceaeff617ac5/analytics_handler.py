"""
Lambda handler for data analytics (Part 3)
Triggered by SQS messages when new data arrives
"""

import json
import os
import boto3
import pandas as pd
import numpy as np
from io import StringIO
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
BUCKET_NAME = os.environ['S3_BUCKET_NAME']

def load_bls_data():
    """Load BLS data from S3"""
    try:
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key='bls-data/pr.data.0.Current')
        csv_content = obj['Body'].read().decode('utf-8')
        
        df = pd.read_csv(StringIO(csv_content), sep='\t')
        df.columns = df.columns.str.strip()
        
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].str.strip()
        
        logger.info(f"Loaded BLS data: {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Error loading BLS data: {e}")
        raise

def load_population_data():
    """Load population data from S3"""
    try:
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key='api-data/population_data_latest.json')
        json_content = obj['Body'].read().decode('utf-8')
        
        data = json.loads(json_content)
        
        if 'data' in data and 'data' in data['data']:
            df = pd.DataFrame(data['data']['data'])
        else:
            df = pd.DataFrame(data.get('data', data))
        
        logger.info(f"Loaded population data: {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Error loading population data: {e}")
        raise

def perform_analytics():
    """Perform all analytics tasks"""
    results = {}
    
    # Load data
    bls_df = load_bls_data()
    population_df = load_population_data()
    
    # Analysis 1: Population statistics
    filtered_pop = population_df[(population_df['Year'] >= 2013) & (population_df['Year'] <= 2018)]
    filtered_pop['Population'] = pd.to_numeric(filtered_pop['Population'], errors='coerce')
    
    mean_population = filtered_pop['Population'].mean()
    std_population = filtered_pop['Population'].std()
    
    results['population_stats'] = {
        'mean': float(mean_population),
        'std': float(std_population),
        'years_analyzed': list(filtered_pop['Year'].unique())
    }
    
    logger.info(f"Population Stats - Mean: {mean_population:,.0f}, Std: {std_population:,.0f}")
    
    # Analysis 2: Best year per series
    bls_df['value'] = pd.to_numeric(bls_df['value'], errors='coerce')
    quarterly_df = bls_df[bls_df['period'].str.startswith('Q')]
    
    yearly_sums = quarterly_df.groupby(['series_id', 'year'])['value'].sum().reset_index()
    yearly_sums.columns = ['series_id', 'year', 'total_value']
    
    best_years = yearly_sums.loc[yearly_sums.groupby('series_id')['total_value'].idxmax()]
    best_years = best_years[['series_id', 'year', 'total_value']]
    
    results['best_years'] = {
        'total_series': len(best_years),
        'sample': best_years.head(5).to_dict('records')
    }
    
    logger.info(f"Best Years - Analyzed {len(best_years)} series")
    
    # Analysis 3: Combined report for PRS30006032
    filtered_bls = bls_df[
        (bls_df['series_id'] == 'PRS30006032') & 
        (bls_df['period'] == 'Q01')
    ].copy()
    
    filtered_bls['year'] = filtered_bls['year'].astype(int)
    population_df['Year'] = population_df['Year'].astype(int)
    
    combined_df = filtered_bls.merge(
        population_df[['Year', 'Population']], 
        left_on='year', 
        right_on='Year', 
        how='left'
    )
    
    combined_df = combined_df[['series_id', 'year', 'period', 'value', 'Population']]
    
    results['combined_report'] = {
        'total_records': len(combined_df),
        'records_with_population': combined_df['Population'].notna().sum(),
        'sample': combined_df.head(5).to_dict('records')
    }
    
    logger.info(f"Combined Report - {len(combined_df)} records")
    
    return results

def handler(event, context):
    """Lambda handler function"""
    logger.info(f"Analytics handler triggered - Event: {json.dumps(event)}")
    
    results = {
        'statusCode': 200,
        'processed_records': 0
    }
    
    try:
        # Process SQS messages
        for record in event.get('Records', []):
            message_body = json.loads(record.get('body', '{}'))
            logger.info(f"Processing message: {message_body}")
            
            # Perform analytics
            analytics_results = perform_analytics()
            
            # Log results
            logger.info("=" * 60)
            logger.info("ANALYTICS RESULTS")
            logger.info("=" * 60)
            logger.info(f"Population Mean: {analytics_results['population_stats']['mean']:,.0f}")
            logger.info(f"Population Std Dev: {analytics_results['population_stats']['std']:,.0f}")
            logger.info(f"Series Analyzed: {analytics_results['best_years']['total_series']}")
            logger.info(f"Combined Report Records: {analytics_results['combined_report']['total_records']}")
            logger.info("=" * 60)
            
            results['analytics'] = analytics_results
            results['processed_records'] += 1
        
        results['message'] = 'Analytics completed successfully'
        
    except Exception as e:
        logger.error(f"Error in analytics handler: {e}")
        results['statusCode'] = 500
        results['error'] = str(e)
        results['message'] = 'Analytics failed'
    
    return results
