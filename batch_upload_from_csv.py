#!/usr/bin/env python3
"""
Batch upload script to process multiple apps from CSV file to Meta
Reads app names from 'Untitled spreadsheet - Sheet2.csv' and uploads each as an audience
"""

import csv
import sys
import logging
import time
from pathlib import Path
from datetime import datetime
import argparse

from logger_config import setup_logging
from config import Config
from meta_api_client import MetaAPIClient
from snowflake_connector import SnowflakeAudienceConnector

# Set up logging
log_file_path = setup_logging()
logger = logging.getLogger(__name__)


def read_apps_from_csv(csv_path="Untitled spreadsheet - Sheet3.csv"):
    """Read app names and OS from CSV file"""
    apps = []
    
    if not Path(csv_path).exists():
        logger.error(f"CSV file not found: {csv_path}")
        return apps
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        # Skip header
        next(reader, None)
        
        for row in reader:
            if row and len(row) >= 2 and row[0].strip():
                app_data = {
                    'name': row[0].strip(),
                    'os': row[1].strip() if len(row) > 1 else 'Unknown'
                }
                apps.append(app_data)
    
    logger.info(f"Found {len(apps)} apps in CSV file")
    return apps


def create_audience_name(app_name, os_type="Unknown"):
    """Create a Meta-friendly audience name in format: US_OS_AppName_PIE"""
    # Clean the app name - remove special characters but keep spaces for readability
    clean_name = ''.join(c if c.isalnum() or c in ' -' else '' for c in app_name)
    # Replace spaces with underscores for the final format
    clean_name = clean_name.replace(' ', '_').replace('-', '_')
    
    # Clean the OS type
    clean_os = os_type.replace(' ', '').replace('-', '_')
    
    # Create the audience name in the required format
    audience_name = f"US_{clean_os}_{clean_name}_PIE"
    
    # Limit total length to Meta's requirements (typically 200 chars max)
    if len(audience_name) > 200:
        # Truncate the app name part if needed
        max_app_length = 200 - len(f"US_{clean_os}__PIE")
        clean_name = clean_name[:max_app_length]
        audience_name = f"US_{clean_os}_{clean_name}_PIE"
    
    return audience_name


def upload_single_app_audience(meta_client, snowflake_connector, app_data, dry_run=False):
    """
    Upload a single app's MAIDs as an audience to Meta
    
    Args:
        meta_client: Meta API client
        snowflake_connector: Snowflake connector
        app_data: Dictionary with 'name' and 'os' keys
        dry_run: Whether to run in dry-run mode
    
    Returns:
        dict: Result with status, audience_id, maid_count, error
    """
    # Handle both string (old format) and dict (new format) inputs
    if isinstance(app_data, str):
        app_name = app_data
        os_type = "Unknown"
    else:
        app_name = app_data.get('name', '')
        os_type = app_data.get('os', 'Unknown')
    
    result = {
        'app_name': app_name,
        'os': os_type,
        'audience_id': None,
        'audience_name': None,
        'maid_count': 0,
        'status': 'pending',
        'error': None,
        'processing_time': 0
    }
    
    start_time = time.time()
    
    try:
        # Create audience name with new format
        audience_name = create_audience_name(app_name, os_type)
        result['audience_name'] = audience_name
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing: {app_name}")
        logger.info(f"Audience name: {audience_name}")
        
        # Step 1: Check if MAIDs exist in Snowflake
        logger.info(f"Checking MAIDs in Snowflake for: {app_name}")
        maid_count = snowflake_connector.get_audience_count(app_name)
        
        if maid_count == 0:
            logger.warning(f"No MAIDs found for {app_name} in Snowflake")
            result['status'] = 'skipped'
            result['error'] = 'No MAIDs found'
            return result
        
        logger.info(f"Found {maid_count:,} MAIDs")
        result['maid_count'] = maid_count
        
        if dry_run:
            logger.info("DRY RUN: Would create audience and upload MAIDs")
            result['status'] = 'dry_run'
            return result
        
        # Step 2: Create audience in Meta
        logger.info("Creating audience in Meta...")
        create_response = meta_client.create_custom_audience(
            name=audience_name,
            description=f"Mobile users from {app_name}"
        )
        
        audience_id = create_response.get('id')
        if not audience_id:
            raise ValueError("Failed to create audience - no ID returned")
        
        result['audience_id'] = audience_id
        logger.info(f"Created audience with ID: {audience_id}")
        
        # Step 3: Fetch and upload MAIDs in batches
        logger.info("Fetching MAIDs from Snowflake...")
        
        # Use optimized batch fetching (500K per batch for balance of speed and memory)
        batches = snowflake_connector.get_batch_audience_maids(app_name, batch_size=500000)
        
        if not batches:
            logger.warning(f"No MAIDs retrieved for {app_name}")
            result['status'] = 'created_empty'
            return result
        
        # Upload MAIDs to Meta
        total_uploaded = 0
        for i, batch in enumerate(batches, 1):
            logger.info(f"Uploading batch {i}/{len(batches)} ({len(batch):,} MAIDs)...")
            
            upload_response = meta_client.add_users_to_audience(
                audience_id=audience_id,
                users=batch,
                schema=['MADID'],
                is_hashed=False  # MAIDs should not be hashed
            )
            
            if upload_response.get('users_uploaded'):
                total_uploaded += upload_response['users_uploaded']
                logger.info(f"Batch {i} uploaded successfully ({upload_response['users_uploaded']:,} MAIDs)")
            else:
                logger.warning(f"Batch {i} upload may have failed")
        
        logger.info(f"Total MAIDs uploaded: {total_uploaded:,}")
        result['status'] = 'success'
        
    except Exception as e:
        logger.error(f"Error processing {app_name}: {str(e)}")
        result['status'] = 'error'
        result['error'] = str(e)
    
    finally:
        result['processing_time'] = round(time.time() - start_time, 2)
    
    return result


def main():
    """Main function to process all apps from CSV"""
    parser = argparse.ArgumentParser(description='Batch upload apps to Meta from CSV')
    parser.add_argument(
        '--csv',
        default='Untitled spreadsheet - Sheet3.csv',
        help='Path to CSV file with app names and OS'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Test run without creating audiences'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of apps to process'
    )
    parser.add_argument(
        '--start-from',
        type=int,
        default=0,
        help='Start from app number (0-based index)'
    )
    
    args = parser.parse_args()
    
    logger.info("="*60)
    logger.info("BATCH AUDIENCE UPLOAD FROM CSV")
    logger.info("="*60)
    logger.info(f"CSV file: {args.csv}")
    logger.info(f"Dry run: {args.dry_run}")
    
    # Read apps from CSV
    apps = read_apps_from_csv(args.csv)
    
    if not apps:
        logger.error("No apps found in CSV file")
        return 1
    
    # Apply start and limit
    if args.start_from:
        apps = apps[args.start_from:]
        logger.info(f"Starting from app #{args.start_from}")
    
    if args.limit:
        apps = apps[:args.limit]
        logger.info(f"Limited to {args.limit} apps")
    
    logger.info(f"Will process {len(apps)} apps")
    
    # Initialize connections
    try:
        logger.info("\nInitializing connections...")
        meta_client = MetaAPIClient()
        snowflake_connector = SnowflakeAudienceConnector()
        snowflake_connector.connect()
        logger.info("✓ Connections established")
    except Exception as e:
        logger.error(f"Failed to initialize connections: {str(e)}")
        return 1
    
    # Process each app
    results = []
    success_count = 0
    error_count = 0
    skipped_count = 0
    
    for i, app_data in enumerate(apps, 1):
        app_name = app_data['name'] if isinstance(app_data, dict) else app_data
        logger.info(f"\n[{i}/{len(apps)}] Processing: {app_name}")
        
        result = upload_single_app_audience(
            meta_client=meta_client,
            snowflake_connector=snowflake_connector,
            app_data=app_data,
            dry_run=args.dry_run
        )
        
        results.append(result)
        
        # Update counters
        if result['status'] == 'success':
            success_count += 1
        elif result['status'] in ['error']:
            error_count += 1
        elif result['status'] in ['skipped', 'created_empty']:
            skipped_count += 1
        
        # Add delay to avoid rate limiting
        if i < len(apps):
            time.sleep(2)  # 2 second delay between apps
    
    # Disconnect
    snowflake_connector.disconnect()
    
    # Print summary
    logger.info("\n" + "="*60)
    logger.info("BATCH UPLOAD SUMMARY")
    logger.info("="*60)
    logger.info(f"Total apps processed: {len(results)}")
    logger.info(f"Successful: {success_count}")
    logger.info(f"Errors: {error_count}")
    logger.info(f"Skipped: {skipped_count}")
    
    # Save results to file
    results_file = f"batch_upload_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    import json
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    logger.info(f"\nResults saved to: {results_file}")
    
    # Print detailed results
    logger.info("\nDetailed Results:")
    for result in results:
        status_emoji = {
            'success': '✓',
            'error': '✗',
            'skipped': '○',
            'created_empty': '△',
            'dry_run': '◊'
        }.get(result['status'], '?')
        
        logger.info(
            f"{status_emoji} {result['app_name'][:40]:<40} | "
            f"Status: {result['status']:<12} | "
            f"MAIDs: {result['maid_count']:>10,} | "
            f"Time: {result['processing_time']:>6.1f}s"
        )
        
        if result['error']:
            logger.info(f"   Error: {result['error']}")
    
    return 0 if error_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
