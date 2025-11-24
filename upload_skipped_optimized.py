#!/usr/bin/env python3
"""
Upload skipped apps using the optimized file upload method
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
        logging.FileHandler(f'optimized_upload_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def read_skipped_apps(csv_file: str) -> List[Dict[str, str]]:
    """Read skipped apps from CSV file"""
    apps = []
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('App name') and row.get('OS'):
                apps.append({
                    'name': row['App name'].strip(),
                    'os': row['OS'].strip()
                })
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


def upload_app_with_optimized_method(
    app_data: Dict[str, str],
    meta_client: OptimizedMetaAPIClient,
    snowflake: SnowflakeAudienceConnector,
    use_file_upload: bool = True,
    batch_threshold: int = 100000
) -> Dict:
    """
    Upload a single app's audience using optimized methods
    
    Args:
        app_data: Dictionary with 'name' and 'os' keys
        meta_client: Optimized Meta API client
        snowflake: Snowflake connector
        use_file_upload: Whether to use file upload for large audiences
        batch_threshold: Threshold for switching to file upload
    
    Returns:
        Result dictionary
    """
    app_name = app_data['name']
    os_type = app_data['os']
    audience_name = create_audience_name(app_name, os_type)
    
    result = {
        'app_name': app_name,
        'os': os_type,
        'audience_name': audience_name,
        'timestamp': datetime.now().isoformat()
    }
    
    start_time = time.time()
    
    try:
        # Get MAID count first
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing: {app_name} ({os_type})")
        logger.info(f"Audience name: {audience_name}")
        
        maid_count = snowflake.get_audience_count(app_name)
        result['maid_count'] = maid_count
        
        if maid_count == 0:
            logger.warning(f"No MAIDs found for {app_name}")
            result['status'] = 'skipped'
            result['reason'] = 'No MAIDs found'
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
        
        # Fetch MAIDs
        logger.info("Fetching MAIDs from Snowflake...")
        fetch_start = time.time()
        
        # Determine batch size based on count
        if maid_count > 10_000_000:
            batch_size = 10_000_000  # 10M for very large audiences
        elif maid_count > 1_000_000:
            batch_size = 5_000_000   # 5M for large audiences
        else:
            batch_size = 1_000_000   # 1M for smaller audiences
        
        batches = snowflake.get_batch_audience_maids(app_name, batch_size=batch_size)
        
        if not batches:
            logger.warning(f"No MAIDs retrieved for {app_name}")
            result['status'] = 'created_empty'
            return result
        
        # Flatten all batches
        all_maids = []
        for batch in batches:
            all_maids.extend(batch)
        
        fetch_time = time.time() - fetch_start
        logger.info(f"✓ Fetched {len(all_maids):,} MAIDs in {fetch_time:.2f}s")
        result['maids_fetched'] = len(all_maids)
        result['fetch_time'] = round(fetch_time, 2)
        
        # Choose upload method based on size
        upload_start = time.time()
        
        # For now, always use optimized batch method since file upload has issues
        # Use 100K batch size which is 2x larger than old method but more stable
        logger.info(f"Using OPTIMIZED BATCH method (audience size: {len(all_maids):,})")
        logger.info(f"  Using 100K batch size (2x larger than old 50K method)")
        
        upload_response = meta_client.add_users_to_audience_batch(
            audience_id=audience_id,
            users=all_maids,
            schema=['MADID'],
            is_hashed=False,
            optimized_batch_size=100_000  # 2x larger than old 50K batches, more stable
        )
        
        result['upload_method'] = 'batch_optimized'
        
        upload_time = time.time() - upload_start
        
        if upload_response.get('users_uploaded'):
            result['maids_uploaded'] = upload_response['users_uploaded']
            result['upload_time'] = round(upload_time, 2)
            result['upload_speed'] = round(upload_response['users_uploaded'] / upload_time, 0)
            logger.info(f"✓ Uploaded {upload_response['users_uploaded']:,} MAIDs in {upload_time:.2f}s")
            logger.info(f"  Speed: {result['upload_speed']:,.0f} MAIDs/second")
            result['status'] = 'success'
        else:
            logger.warning("Upload may have failed - no confirmation received")
            result['status'] = 'uncertain'
        
    except Exception as e:
        logger.error(f"Error processing {app_name}: {str(e)}")
        result['status'] = 'error'
        result['error'] = str(e)
    
    finally:
        result['total_time'] = round(time.time() - start_time, 2)
        logger.info(f"Total processing time: {result['total_time']:.2f}s")
    
    return result


def main():
    """Main function to process skipped apps"""
    
    print("\n" + "="*80)
    print("OPTIMIZED UPLOAD FOR SKIPPED APPS")
    print("="*80)
    
    # Configuration
    csv_file = 'skipped_apps.csv'
    results_file = f'optimized_upload_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    
    # Read skipped apps
    logger.info(f"Reading apps from {csv_file}...")
    apps = read_skipped_apps(csv_file)
    
    # Filter to apps 53-74 (the ones user highlighted)
    # These are lines 53-74 in the file, which are indices 52-73 in 0-based list
    highlighted_apps = apps[52:74] if len(apps) > 73 else apps[52:]
    
    logger.info(f"Found {len(highlighted_apps)} apps to process (lines 53-74)")
    
    if not highlighted_apps:
        logger.error("No apps found to process")
        return 1
    
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
    
    for i, app_data in enumerate(highlighted_apps, 1):
        logger.info(f"\nProcessing app {i}/{len(highlighted_apps)}")
        
        result = upload_app_with_optimized_method(
            app_data=app_data,
            meta_client=meta_client,
            snowflake=snowflake,
            use_file_upload=False,  # Disable file upload for now (API issues)
            batch_threshold=100000  # Not used when file upload is disabled
        )
        
        results.append(result)
        
        # Track statistics
        if result['status'] == 'success':
            success_count += 1
        elif result['status'] in ['error']:
            error_count += 1
        else:
            skipped_count += 1
        
        # Save progress after each app
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        # Small delay between apps to avoid overwhelming the API
        if i < len(highlighted_apps):
            time.sleep(2)
    
    # Close connections
    snowflake.close()
    
    # Print summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    
    print(f"\nTotal apps processed: {len(highlighted_apps)}")
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
    
    # Method usage
    file_uploads = sum(1 for r in results if r.get('upload_method') == 'file_upload')
    batch_uploads = sum(1 for r in results if r.get('upload_method') == 'batch_optimized')
    
    print(f"\nUpload methods used:")
    print(f"  File upload: {file_uploads} audiences")
    print(f"  Batch upload: {batch_uploads} audiences")
    
    print(f"\nResults saved to: {results_file}")
    
    # Show any errors
    if error_count > 0:
        print("\n" + "="*80)
        print("ERRORS")
        print("="*80)
        for r in results:
            if r.get('status') == 'error':
                print(f"\n{r['app_name']}:")
                print(f"  Error: {r.get('error', 'Unknown error')}")
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
