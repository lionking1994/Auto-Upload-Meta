#!/usr/bin/env python3
"""
Special handler for very large apps (>20M MAIDs) that may timeout
Processes them with special handling and smaller chunks
"""

import sys
import time
import logging
from datetime import datetime
from meta_api_client_optimized import OptimizedMetaAPIClient
from snowflake_connector import SnowflakeAudienceConnector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'large_apps_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# List of known large apps that need special handling
LARGE_APPS = [
    {"name": "Dice Puzzle - Merge puzzle", "os": "Android", "row": 111},
    {"name": "Dice Puzzle - Merge puzzle", "os": "Android", "row": 174},  # Duplicate entry
    # Add more large apps here if identified
]

def create_correct_audience_name(app_name: str, os: str) -> str:
    """Create audience name in correct format preserving spaces"""
    clean_name = app_name.replace(':', '').replace('-', '').replace('™', '').replace('®', '')
    clean_name = clean_name.replace('!', '').replace('&', 'and').replace('+', 'plus')
    clean_name = clean_name.replace('/', ' ').replace('\\', ' ')
    
    while '  ' in clean_name:
        clean_name = clean_name.replace('  ', ' ')
    
    clean_name = clean_name.strip()
    
    max_length = 200 - len('US__PIE') - len(os)
    if len(clean_name) > max_length:
        clean_name = clean_name[:max_length].rstrip()
    
    return f"US_{os}_{clean_name}_PIE"


def process_large_app(app_data, meta_client, snowflake, skip_if_exists=True):
    """
    Process a single large app with special handling
    """
    app_name = app_data['name']
    os_type = app_data['os']
    row_num = app_data.get('row', 'unknown')
    audience_name = create_correct_audience_name(app_name, os_type)
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Processing LARGE APP (row {row_num}): {app_name} ({os_type})")
    logger.info(f"Audience name: {audience_name}")
    
    result = {
        'app_name': app_name,
        'os': os_type,
        'audience_name': audience_name,
        'row': row_num,
        'timestamp': datetime.now().isoformat()
    }
    
    start_time = time.time()
    
    try:
        # Check if audience already exists
        if skip_if_exists:
            existing = meta_client.list_custom_audiences(limit=500)
            existing_names = {aud.get('name'): aud.get('id') for aud in existing}
            
            if audience_name in existing_names:
                logger.info(f"Audience already exists (ID: {existing_names[audience_name]}), skipping...")
                result['status'] = 'skipped'
                result['reason'] = 'already_exists'
                result['audience_id'] = existing_names[audience_name]
                return result
        
        # Get MAID count
        logger.info("Checking MAIDs in Snowflake...")
        maid_count = snowflake.get_audience_count(app_name)
        result['maid_count'] = maid_count
        
        if maid_count == 0:
            logger.warning(f"No MAIDs found for {app_name}")
            result['status'] = 'skipped'
            result['reason'] = 'no_maids'
            return result
        
        logger.info(f"Found {maid_count:,} MAIDs")
        
        # Create audience
        logger.info("Creating custom audience...")
        audience = meta_client.create_custom_audience(
            name=audience_name,
            description=f"Large audience for {app_name} on {os_type}"
        )
        audience_id = audience['id']
        result['audience_id'] = audience_id
        logger.info(f"✓ Created audience: {audience_id}")
        
        # For very large apps, we'll just create the audience without uploading MAIDs
        # They can be uploaded separately or via Meta's UI
        if maid_count > 30_000_000:
            logger.warning(f"App has {maid_count:,} MAIDs - too large for API upload")
            logger.info("Audience created but MAIDs not uploaded. Use Meta UI for upload.")
            result['status'] = 'created_no_upload'
            result['reason'] = f'Too large for API ({maid_count:,} MAIDs)'
            return result
        
        # Try to fetch and upload with special handling
        logger.info("Attempting to fetch MAIDs with chunked approach...")
        fetch_start = time.time()
        
        try:
            # Use smaller batch size for very large datasets
            batches = snowflake.get_batch_audience_maids(app_name, batch_size=1_000_000)
            
            if not batches:
                logger.warning("No MAIDs retrieved")
                result['status'] = 'created_empty'
                return result
            
            total_maids_fetched = sum(len(batch) for batch in batches)
            fetch_time = time.time() - fetch_start
            logger.info(f"✓ Fetched {total_maids_fetched:,} MAIDs in {fetch_time:.2f}s")
            result['maids_fetched'] = total_maids_fetched
            result['fetch_time'] = round(fetch_time, 2)
            
            # Upload to Meta with very small batches to avoid timeouts
            upload_start = time.time()
            total_uploaded = 0
            meta_batch_size = 50_000  # Very conservative for large apps
            
            logger.info(f"Uploading MAIDs to Meta (batch size: {meta_batch_size:,})...")
            
            for sf_batch_num, sf_batch in enumerate(batches, 1):
                logger.info(f"Processing batch {sf_batch_num}/{len(batches)} ({len(sf_batch):,} MAIDs)")
                
                # Upload this batch to Meta in smaller chunks
                for i in range(0, len(sf_batch), meta_batch_size):
                    meta_batch = sf_batch[i:i + meta_batch_size]
                    
                    try:
                        upload_response = meta_client.add_users_to_audience_batch(
                            audience_id=audience_id,
                            users=meta_batch,
                            schema=['MADID'],
                            is_hashed=False,
                            optimized_batch_size=meta_batch_size
                        )
                        
                        if upload_response.get('users_uploaded'):
                            total_uploaded += upload_response['users_uploaded']
                            
                            if total_uploaded % 1_000_000 == 0:
                                logger.info(f"  Progress: {total_uploaded:,}/{total_maids_fetched:,} MAIDs uploaded")
                    
                    except Exception as e:
                        logger.error(f"Error uploading batch: {str(e)}")
                        continue
            
            upload_time = time.time() - upload_start
            
            if total_uploaded > 0:
                result['maids_uploaded'] = total_uploaded
                result['upload_time'] = round(upload_time, 2)
                logger.info(f"✓ Uploaded {total_uploaded:,} MAIDs in {upload_time:.2f}s")
                result['status'] = 'success'
            else:
                result['status'] = 'upload_failed'
                
        except Exception as e:
            logger.error(f"Failed to fetch/upload MAIDs: {str(e)}")
            result['status'] = 'created_fetch_failed'
            result['error'] = str(e)
            logger.info(f"Audience created but MAIDs could not be uploaded: {audience_id}")
    
    except Exception as e:
        logger.error(f"Error processing {app_name}: {str(e)}")
        result['status'] = 'error'
        result['error'] = str(e)
    
    finally:
        result['total_time'] = round(time.time() - start_time, 2)
        logger.info(f"Total processing time: {result['total_time']:.2f}s")
    
    return result


def main():
    """Process large apps with special handling"""
    
    print("\n" + "="*80)
    print("LARGE APPS SPECIAL HANDLER")
    print("="*80)
    
    if len(sys.argv) > 1 and sys.argv[1] == '--skip-dice-puzzle':
        logger.info("Skipping Dice Puzzle as requested")
        LARGE_APPS[:] = [app for app in LARGE_APPS if 'Dice Puzzle' not in app['name']]
    
    # Initialize connections
    try:
        logger.info("Initializing connections...")
        meta_client = OptimizedMetaAPIClient()
        snowflake = SnowflakeAudienceConnector()
        snowflake.connect()
        logger.info("✓ Connections established")
    except Exception as e:
        logger.error(f"Failed to initialize connections: {e}")
        return 1
    
    # Process each large app
    results = []
    for app_data in LARGE_APPS:
        result = process_large_app(app_data, meta_client, snowflake)
        results.append(result)
        
        # Save progress
        import json
        with open(f'large_apps_results_{datetime.now().strftime("%Y%m%d")}.json', 'w') as f:
            json.dump(results, f, indent=2)
        
        time.sleep(2)  # Delay between apps
    
    # Close connections
    snowflake.close()
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    
    for r in results:
        status = r.get('status', 'unknown')
        print(f"\n{r['app_name']} ({r['os']}): {status}")
        if status == 'success':
            print(f"  MAIDs uploaded: {r.get('maids_uploaded', 0):,}")
        elif status == 'created_no_upload':
            print(f"  Audience created but too large for API upload ({r.get('maid_count', 0):,} MAIDs)")
        elif status == 'error':
            print(f"  Error: {r.get('error', 'Unknown')}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
