#!/usr/bin/env python3
"""
Optimized ETL Pipeline for uploading audiences with MAIDs from Snowflake to Meta
Combines the best performance optimizations with correct naming format
"""

import csv
import json
import time
import logging
import argparse
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime

from meta_api_client_optimized import OptimizedMetaAPIClient
from snowflake_connector import SnowflakeAudienceConnector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'etl_pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def create_correct_audience_name(app_name: str, os: str) -> str:
    """
    Create audience name in correct format: US_{OS}_{AppName}_PIE
    Preserves spaces in app name, only removes problematic characters
    """
    # Only replace characters that aren't allowed in Meta audience names
    # Keep spaces as they are
    clean_name = app_name.replace(':', '').replace('-', '').replace('™', '').replace('®', '')
    clean_name = clean_name.replace('!', '').replace('&', 'and').replace('+', 'plus')
    clean_name = clean_name.replace('/', ' ').replace('\\', ' ')
    clean_name = clean_name.replace('–', ' ')  # em dash
    clean_name = clean_name.replace('—', ' ')  # em dash
    
    # Remove any double spaces that might have been created
    while '  ' in clean_name:
        clean_name = clean_name.replace('  ', ' ')
    
    # Trim spaces at the beginning and end
    clean_name = clean_name.strip()
    
    # Ensure it doesn't exceed Meta's character limit (200 chars)
    max_length = 200 - len('US__PIE') - len(os)
    if len(clean_name) > max_length:
        clean_name = clean_name[:max_length].rstrip()
    
    return f"US_{os}_{clean_name}_PIE"


def read_apps_from_csv(csv_path: str) -> List[Dict[str, str]]:
    """Read unique app+OS combinations from CSV file"""
    apps = []
    seen_combinations = set()
    
    if not Path(csv_path).exists():
        logger.error(f"CSV file not found: {csv_path}")
        return apps
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row_num, row in enumerate(reader, start=2):  # Start at 2 because row 1 is header
            if row.get('App name') and row.get('OS'):
                app_name = row['App name'].strip()
                os_type = row['OS'].strip()
                
                # Create unique key to avoid duplicates
                unique_key = f"{app_name}|{os_type}"
                
                if unique_key not in seen_combinations:
                    apps.append({
                        'name': app_name,
                        'os': os_type,
                        'row': row_num
                    })
                    seen_combinations.add(unique_key)
    
    logger.info(f"Found {len(apps)} unique app+OS combinations in CSV file")
    return apps


def upload_app_audience_optimized(
    app_data: Dict[str, str],
    meta_client: OptimizedMetaAPIClient,
    snowflake: SnowflakeAudienceConnector,
    batch_size: int = 100000,  # Conservative batch size for Meta API
    skip_existing: bool = True
) -> Dict:
    """
    Upload a single app's audience using optimized methods
    
    Args:
        app_data: Dictionary with 'name', 'os', and optionally 'row' keys
        meta_client: Optimized Meta API client
        snowflake: Snowflake connector
        batch_size: Batch size for Meta API uploads (100K is safe, 500K may cause errors)
        skip_existing: Whether to skip if audience already exists
    
    Returns:
        Result dictionary with upload statistics
    """
    app_name = app_data['name']
    os_type = app_data['os']
    row_num = app_data.get('row', 'unknown')
    
    # Use correct naming format (with spaces preserved)
    audience_name = create_correct_audience_name(app_name, os_type)
    
    result = {
        'app_name': app_name,
        'os': os_type,
        'audience_name': audience_name,
        'row': row_num,
        'timestamp': datetime.now().isoformat()
    }
    
    start_time = time.time()
    
    try:
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing row {row_num}: {app_name} ({os_type})")
        logger.info(f"Audience name: {audience_name}")
        
        # Check if audience already exists
        if skip_existing:
            existing = meta_client.list_custom_audiences(limit=500)
            existing_names = {aud.get('name'): aud.get('id') for aud in existing}
            
            if audience_name in existing_names:
                logger.info(f"Audience already exists (ID: {existing_names[audience_name]}), skipping...")
                result['status'] = 'skipped'
                result['reason'] = 'already_exists'
                result['audience_id'] = existing_names[audience_name]
                return result
        
        # Get MAID count from Snowflake
        logger.info("Checking MAIDs in Snowflake...")
        maid_count = snowflake.get_audience_count(app_name)
        result['maid_count'] = maid_count
        
        if maid_count == 0:
            logger.warning(f"No MAIDs found for {app_name}")
            result['status'] = 'skipped'
            result['reason'] = 'no_maids'
            return result
        
        logger.info(f"Found {maid_count:,} MAIDs")
        
        # Create custom audience in Meta
        logger.info("Creating custom audience...")
        audience = meta_client.create_custom_audience(
            name=audience_name,
            description=f"Audience for {app_name} on {os_type}"
        )
        audience_id = audience['id']
        result['audience_id'] = audience_id
        logger.info(f"✓ Created audience: {audience_id}")
        
        # Fetch MAIDs from Snowflake using optimized method (chunked for very large datasets)
        logger.info("Fetching MAIDs from Snowflake (optimized)...")
        fetch_start = time.time()
        
        # Use appropriate Snowflake batch size based on data volume
        if maid_count > 5_000_000:
            sf_batch_size = 2_000_000
        elif maid_count > 1_000_000:
            sf_batch_size = 1_000_000
        else:
            sf_batch_size = 500_000
        
        logger.info(f"Using Snowflake batch size: {sf_batch_size:,}")
        
        try:
            batches = snowflake.get_batch_audience_maids(app_name, batch_size=sf_batch_size)
        except Exception as e:
            logger.error(f"Failed to fetch MAIDs from Snowflake: {str(e)}")
            # For very large datasets that fail, try to skip and continue
            if maid_count > 20_000_000:
                logger.warning(f"Skipping {app_name} due to size ({maid_count:,} MAIDs) and fetch error")
                result['status'] = 'skipped'
                result['reason'] = f'Too large to fetch ({maid_count:,} MAIDs)'
                result['error'] = str(e)
                return result
            else:
                raise  # Re-raise for smaller datasets
        
        if not batches:
            logger.warning(f"No MAIDs retrieved for {app_name}")
            result['status'] = 'created_empty'
            return result
        
        total_maids_fetched = sum(len(batch) for batch in batches)
        fetch_time = time.time() - fetch_start
        logger.info(f"✓ Fetched {total_maids_fetched:,} MAIDs in {fetch_time:.2f}s")
        result['maids_fetched'] = total_maids_fetched
        result['fetch_time'] = round(fetch_time, 2)
        
        # Upload MAIDs to Meta using optimized batch method
        upload_start = time.time()
        total_uploaded = 0
        
        logger.info(f"Uploading MAIDs to Meta (batch size: {batch_size:,})...")
        
        for sf_batch_num, sf_batch in enumerate(batches, 1):
            logger.info(f"Processing Snowflake batch {sf_batch_num}/{len(batches)} ({len(sf_batch):,} MAIDs)")
            
            # Upload this Snowflake batch to Meta in smaller chunks
            batch_uploaded = 0
            for i in range(0, len(sf_batch), batch_size):
                meta_batch = sf_batch[i:i + batch_size]
                
                try:
                    # Use optimized batch upload
                    upload_response = meta_client.add_users_to_audience_batch(
                        audience_id=audience_id,
                        users=meta_batch,
                        schema=['MADID'],
                        is_hashed=False,
                        optimized_batch_size=batch_size
                    )
                    
                    if upload_response.get('users_uploaded'):
                        batch_uploaded += upload_response['users_uploaded']
                        total_uploaded += upload_response['users_uploaded']
                        
                        # Progress update every 500K records
                        if total_uploaded % 500000 == 0:
                            logger.info(f"  Progress: {total_uploaded:,}/{total_maids_fetched:,} MAIDs uploaded")
                
                except Exception as e:
                    logger.error(f"Error uploading batch: {str(e)}")
                    # Continue with next batch instead of failing completely
                    continue
        
        upload_time = time.time() - upload_start
        
        if total_uploaded > 0:
            result['maids_uploaded'] = total_uploaded
            result['upload_time'] = round(upload_time, 2)
            result['upload_speed'] = round(total_uploaded / upload_time, 0) if upload_time > 0 else 0
            logger.info(f"✓ Uploaded {total_uploaded:,} MAIDs in {upload_time:.2f}s")
            if result['upload_speed'] > 0:
                logger.info(f"  Speed: {result['upload_speed']:,.0f} MAIDs/second")
            result['status'] = 'success'
        else:
            logger.warning("No MAIDs were uploaded")
            result['status'] = 'upload_failed'
    
    except Exception as e:
        logger.error(f"Error processing {app_name}: {str(e)}")
        result['status'] = 'error'
        result['error'] = str(e)
    
    finally:
        result['total_time'] = round(time.time() - start_time, 2)
        logger.info(f"Total processing time: {result['total_time']:.2f}s")
    
    return result


def main():
    """Main ETL pipeline function"""
    
    parser = argparse.ArgumentParser(description='Optimized ETL Pipeline for Meta Audience Upload')
    parser.add_argument('--csv', default='Untitled spreadsheet - Sheet3.csv',
                        help='CSV file with app names and OS (default: Sheet3.csv)')
    parser.add_argument('--batch-size', type=int, default=100000,
                        help='Batch size for Meta API uploads (default: 100000)')
    parser.add_argument('--skip-existing', action='store_true', default=True,
                        help='Skip audiences that already exist (default: True)')
    parser.add_argument('--limit', type=int, default=0,
                        help='Limit number of apps to process (0 = no limit)')
    parser.add_argument('--start-row', type=int, default=0,
                        help='Start processing from specific row number')
    
    args = parser.parse_args()
    
    print("\n" + "="*80)
    print("OPTIMIZED ETL PIPELINE - SNOWFLAKE TO META")
    print("="*80)
    
    # Read apps from CSV
    logger.info(f"Reading apps from {args.csv}...")
    apps = read_apps_from_csv(args.csv)
    
    if not apps:
        logger.error("No apps found to process")
        return 1
    
    # Apply row filtering if specified
    if args.start_row > 0:
        apps = [app for app in apps if app.get('row', 0) >= args.start_row]
        logger.info(f"Filtered to {len(apps)} apps starting from row {args.start_row}")
    
    # Apply limit if specified
    if args.limit > 0:
        apps = apps[:args.limit]
        logger.info(f"Limited to {args.limit} apps")
    
    # Initialize connections
    try:
        logger.info("\nInitializing connections...")
        meta_client = OptimizedMetaAPIClient()
        snowflake = SnowflakeAudienceConnector()
        snowflake.connect()
        logger.info("✓ Connections established")
    except Exception as e:
        logger.error(f"Failed to initialize connections: {e}")
        return 1
    
    # Process apps
    results = []
    success_count = 0
    error_count = 0
    skipped_count = 0
    
    print("\n" + "="*80)
    print("PROCESSING APPS")
    print("="*80)
    
    for i, app_data in enumerate(apps, 1):
        logger.info(f"\n[{i}/{len(apps)}] Processing app...")
        
        result = upload_app_audience_optimized(
            app_data=app_data,
            meta_client=meta_client,
            snowflake=snowflake,
            batch_size=args.batch_size,
            skip_existing=args.skip_existing
        )
        
        results.append(result)
        
        # Track statistics
        if result['status'] == 'success':
            success_count += 1
        elif result['status'] in ['error', 'upload_failed']:
            error_count += 1
        else:
            skipped_count += 1
        
        # Save progress after each app
        progress_file = f'etl_progress_{datetime.now().strftime("%Y%m%d")}.json'
        with open(progress_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        # Small delay between apps to avoid overwhelming the API
        if i < len(apps):
            time.sleep(2)
    
    # Close connections
    snowflake.close()
    
    # Final summary
    print("\n" + "="*80)
    print("ETL PIPELINE SUMMARY")
    print("="*80)
    
    print(f"\nTotal apps processed: {len(apps)}")
    print(f"  ✓ Successful: {success_count}")
    print(f"  ⚠ Skipped: {skipped_count}")
    print(f"  ✗ Errors: {error_count}")
    
    # Performance statistics
    total_maids = sum(r.get('maids_uploaded', 0) for r in results if r.get('maids_uploaded'))
    total_time = sum(r.get('total_time', 0) for r in results if r.get('total_time'))
    
    if total_maids > 0 and total_time > 0:
        print(f"\nPerformance:")
        print(f"  Total MAIDs uploaded: {total_maids:,}")
        print(f"  Total processing time: {total_time:.2f}s")
        print(f"  Average speed: {total_maids/total_time:,.0f} MAIDs/second")
    
    # Save final results
    results_file = f'etl_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nResults saved to: {results_file}")
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
