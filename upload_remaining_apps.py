#!/usr/bin/env python3
"""
Upload remaining skipped apps (lines 64-74), skipping problematic ones
"""

import csv
import json
import time
import logging
from pathlib import Path
from typing import List, Dict, Tuple
from datetime import datetime
from meta_api_client_optimized import OptimizedMetaAPIClient
from snowflake_connector import SnowflakeAudienceConnector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'remaining_upload_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Apps to skip due to size or other issues
PROBLEMATIC_APPS = [
    "Dice Puzzle - Merge puzzle"  # 41M MAIDs - too large
]

# Maximum MAIDs to process (skip if larger)
MAX_MAIDS_LIMIT = 10_000_000  # 10 million


def read_skipped_apps(csv_file: str, start_line: int = 64, end_line: int = 74) -> List[Dict[str, str]]:
    """Read specific lines from skipped apps CSV file"""
    apps = []
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):  # Start at 2 because line 1 is header
            if i >= start_line and i <= end_line:
                if row.get('App name') and row.get('OS'):
                    app_name = row['App name'].strip()
                    # Skip problematic apps
                    if app_name not in PROBLEMATIC_APPS:
                        apps.append({
                            'name': app_name,
                            'os': row['OS'].strip(),
                            'line_number': i
                        })
                    else:
                        logger.warning(f"Skipping problematic app on line {i}: {app_name}")
            elif i > end_line:
                break
    return apps


def create_audience_name(app_name: str, os: str) -> str:
    """Create audience name in format: US_{OS}_{AppName}_PIE"""
    # Clean app name - replace special chars with underscores
    clean_name = app_name.replace(' ', '_').replace('-', '_').replace(':', '')
    clean_name = ''.join(c if c.isalnum() or c == '_' else '' for c in clean_name)
    
    # Ensure it doesn't exceed Meta's character limit (200 chars)
    max_length = 200 - len('US__PIE') - len(os)
    if len(clean_name) > max_length:
        clean_name = clean_name[:max_length]
    
    return f"US_{os}_{clean_name}_PIE"


def upload_app_safely(
    app_data: Dict[str, str],
    meta_client: OptimizedMetaAPIClient,
    snowflake: SnowflakeAudienceConnector
) -> Dict:
    """
    Upload a single app's audience with safety checks
    """
    app_name = app_data['name']
    os_type = app_data['os']
    line_number = app_data.get('line_number', 'unknown')
    audience_name = create_audience_name(app_name, os_type)
    
    result = {
        'app_name': app_name,
        'os': os_type,
        'audience_name': audience_name,
        'line_number': line_number,
        'timestamp': datetime.now().isoformat()
    }
    
    start_time = time.time()
    
    try:
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing line {line_number}: {app_name} ({os_type})")
        logger.info(f"Audience name: {audience_name}")
        
        # Get MAID count first
        maid_count = snowflake.get_audience_count(app_name)
        result['maid_count'] = maid_count
        
        if maid_count == 0:
            logger.warning(f"No MAIDs found for {app_name}")
            result['status'] = 'skipped'
            result['reason'] = 'No MAIDs found'
            return result
        
        # Check if audience is too large
        if maid_count > MAX_MAIDS_LIMIT:
            logger.warning(f"Audience too large: {maid_count:,} MAIDs (limit: {MAX_MAIDS_LIMIT:,})")
            result['status'] = 'skipped'
            result['reason'] = f'Too large ({maid_count:,} MAIDs)'
            return result
        
        logger.info(f"Found {maid_count:,} MAIDs")
        
        # Create custom audience
        logger.info("Creating custom audience...")
        audience = meta_client.create_custom_audience(
            name=audience_name,
            description=f"Audience for {app_name} on {os_type}"
        )
        audience_id = audience['id']
        result['audience_id'] = audience_id
        logger.info(f"✓ Created audience: {audience_id}")
        
        # Fetch MAIDs in appropriate batch size
        logger.info("Fetching MAIDs from Snowflake...")
        fetch_start = time.time()
        
        # Use smaller batch size for large audiences to avoid memory issues
        if maid_count > 5_000_000:
            batch_size = 2_000_000
        elif maid_count > 1_000_000:
            batch_size = 1_000_000
        else:
            batch_size = 500_000
        
        logger.info(f"Using Snowflake batch size: {batch_size:,}")
        batches = snowflake.get_batch_audience_maids(app_name, batch_size=batch_size)
        
        if not batches:
            logger.warning(f"No MAIDs retrieved for {app_name}")
            result['status'] = 'created_empty'
            return result
        
        # Count total MAIDs fetched
        total_maids_fetched = sum(len(batch) for batch in batches)
        fetch_time = time.time() - fetch_start
        logger.info(f"✓ Fetched {total_maids_fetched:,} MAIDs in {fetch_time:.2f}s")
        result['maids_fetched'] = total_maids_fetched
        result['fetch_time'] = round(fetch_time, 2)
        
        # Upload MAIDs to Meta in batches
        upload_start = time.time()
        total_uploaded = 0
        
        # Use smaller batch size for Meta API to avoid timeouts
        meta_batch_size = 50_000  # Conservative batch size for stability
        
        logger.info(f"Uploading MAIDs to Meta (batch size: {meta_batch_size:,})...")
        
        for sf_batch_num, sf_batch in enumerate(batches, 1):
            logger.info(f"Processing Snowflake batch {sf_batch_num}/{len(batches)} ({len(sf_batch):,} MAIDs)")
            
            # Upload this Snowflake batch to Meta in smaller chunks
            batch_uploaded = 0
            for i in range(0, len(sf_batch), meta_batch_size):
                meta_batch = sf_batch[i:i + meta_batch_size]
                
                try:
                    # Use the standard add_users_to_audience_batch method with smaller batch
                    upload_response = meta_client.add_users_to_audience_batch(
                        audience_id=audience_id,
                        users=meta_batch,
                        schema=['MADID'],
                        is_hashed=False,
                        optimized_batch_size=meta_batch_size
                    )
                    
                    if upload_response.get('users_uploaded'):
                        batch_uploaded += upload_response['users_uploaded']
                        total_uploaded += upload_response['users_uploaded']
                        logger.info(f"  Uploaded {batch_uploaded:,}/{len(sf_batch):,} from this batch "
                                  f"(Total: {total_uploaded:,}/{total_maids_fetched:,})")
                    
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
    """Main function to process remaining apps"""
    
    print("\n" + "="*80)
    print("UPLOAD REMAINING SKIPPED APPS (Lines 64-74)")
    print("="*80)
    
    # Configuration
    csv_file = 'skipped_apps.csv'
    results_file = f'remaining_upload_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    
    # Read apps from lines 64-74
    logger.info(f"Reading apps from {csv_file} (lines 64-74)...")
    apps = read_skipped_apps(csv_file, start_line=64, end_line=74)
    
    logger.info(f"Found {len(apps)} apps to process (after filtering problematic ones)")
    
    if not apps:
        logger.error("No apps found to process")
        return 1
    
    # Show which apps will be processed
    logger.info("\nApps to process:")
    for app in apps:
        logger.info(f"  Line {app['line_number']}: {app['name']} ({app['os']})")
    
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
        logger.info(f"\nProcessing app {i}/{len(apps)}")
        
        result = upload_app_safely(
            app_data=app_data,
            meta_client=meta_client,
            snowflake=snowflake
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
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        # Small delay between apps to avoid overwhelming the API
        if i < len(apps):
            time.sleep(2)
    
    # Close connections
    snowflake.close()
    
    # Print summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    
    print(f"\nTotal apps processed: {len(apps)}")
    print(f"  ✓ Successful: {success_count}")
    print(f"  ⚠ Skipped/Empty: {skipped_count}")
    print(f"  ✗ Errors: {error_count}")
    
    # Performance statistics
    total_maids = sum(r.get('maids_uploaded', 0) for r in results if r.get('maids_uploaded'))
    total_upload_time = sum(r.get('upload_time', 0) for r in results if r.get('upload_time'))
    
    if total_upload_time > 0:
        avg_speed = total_maids / total_upload_time
        print(f"\nPerformance:")
        print(f"  Total MAIDs uploaded: {total_maids:,}")
        print(f"  Total upload time: {total_upload_time:.2f}s")
        print(f"  Average speed: {avg_speed:,.0f} MAIDs/second")
    
    print(f"\nResults saved to: {results_file}")
    
    # Show any errors or skipped apps
    if error_count > 0 or skipped_count > 0:
        print("\n" + "="*80)
        print("DETAILS")
        print("="*80)
        
        for r in results:
            if r.get('status') == 'error':
                print(f"\nERROR - Line {r.get('line_number')}: {r['app_name']}:")
                print(f"  Error: {r.get('error', 'Unknown error')}")
            elif r.get('status') in ['skipped', 'created_empty', 'upload_failed']:
                print(f"\nSKIPPED - Line {r.get('line_number')}: {r['app_name']}:")
                print(f"  Reason: {r.get('reason', r.get('status'))}")
                if r.get('maid_count'):
                    print(f"  MAIDs: {r.get('maid_count'):,}")
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())

