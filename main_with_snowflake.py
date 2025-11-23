"""
Main orchestration script for Meta Audience Upload with Snowflake Integration
This version fetches actual MAIDs from Snowflake instead of generating sample data
"""
import argparse
import sys
import logging
from pathlib import Path
from datetime import datetime
import json

from logger_config import setup_logging
from config import Config
from meta_api_client import MetaAPIClient
from data_processor import AudienceDataProcessor
from audience_uploader import AudienceUploadManager
from snowflake_connector import SnowflakeAudienceConnector

# Set up logging first
log_file_path = setup_logging()
logger = logging.getLogger(__name__)


def validate_configuration():
    """Validate that all required configuration is present"""
    errors = []
    
    if not Config.META_ACCESS_TOKEN:
        errors.append("META_ACCESS_TOKEN is not set")
    
    if not Config.META_AD_ACCOUNT_ID:
        errors.append("META_AD_ACCOUNT_ID is not set")
    
    if not Path(Config.CSV_FILE_PATH).exists():
        errors.append(f"CSV file not found: {Config.CSV_FILE_PATH}")
    
    # Check Snowflake credentials
    import os
    if not os.getenv('SNOWFLAKE_ACCOUNT'):
        errors.append("SNOWFLAKE_ACCOUNT is not set")
    if not os.getenv('SNOWFLAKE_USER'):
        errors.append("SNOWFLAKE_USER is not set")
    # Check for either password or PAT token
    if not os.getenv('SNOWFLAKE_PASSWORD') and not os.getenv('SNOWFLAKE_PAT_TOKEN'):
        errors.append("Neither SNOWFLAKE_PASSWORD nor SNOWFLAKE_PAT_TOKEN is set")
    
    if errors:
        for error in errors:
            logger.error(error)
        return False
    
    return True


def upload_audience_with_maids(meta_client, snowflake_connector, audience_data):
    """
    Create audience and upload MAIDs from Snowflake
    
    Args:
        meta_client: Meta API client
        snowflake_connector: Snowflake connector
        audience_data: Audience metadata from CSV
        
    Returns:
        Tuple of (success, audience_id, users_uploaded)
    """
    try:
        # Step 1: Create the audience in Meta
        logger.info(f"Creating audience: {audience_data['name']}")
        create_response = meta_client.create_custom_audience(
            name=audience_data['name'],
            description=audience_data['description']
        )
        
        audience_id = create_response.get('id')
        if not audience_id:
            raise ValueError("No audience ID returned from Meta")
        
        logger.info(f"Created audience with ID: {audience_id}")
        
        # Step 2: Fetch MAIDs from Snowflake
        app_name = audience_data['original_name']  # Use original name for Snowflake query
        logger.info(f"Fetching MAIDs from Snowflake for: {app_name}")
        
        # Get batches of MAIDs (5M per batch for faster processing)
        batches = snowflake_connector.get_batch_audience_maids(app_name, batch_size=5000000)
        
        if not batches:
            logger.warning(f"No MAIDs found for {app_name}")
            return True, audience_id, 0
        
        # Step 3: Upload MAIDs to Meta in batches
        total_uploaded = 0
        for i, batch in enumerate(batches, 1):
            logger.info(f"Uploading batch {i}/{len(batches)} ({len(batch)} MAIDs) to audience {audience_id}")
            
            # Upload batch to Meta
            upload_response = meta_client.add_users_to_audience(
                audience_id=audience_id,
                users=batch,
                schema=['MADID'],  # Mobile Advertising ID schema
                is_hashed=False  # MAIDs don't need hashing
            )
            
            total_uploaded += len(batch)
            logger.info(f"Uploaded {total_uploaded} MAIDs so far")
        
        logger.info(f"Successfully uploaded {total_uploaded} MAIDs to audience {audience_data['name']}")
        return True, audience_id, total_uploaded
        
    except Exception as e:
        logger.error(f"Failed to upload audience {audience_data['name']}: {str(e)}")
        return False, None, 0


def main(args):
    """
    Main execution function with Snowflake integration
    
    Args:
        args: Command line arguments
    """
    logger.info("=" * 80)
    logger.info("Meta Audience Upload Script with Snowflake Integration")
    logger.info("=" * 80)
    
    # Validate configuration
    if not validate_configuration():
        logger.error("Configuration validation failed. Please check your settings.")
        if not args.force:
            sys.exit(1)
    
    try:
        # Initialize Snowflake connector
        logger.info("Initializing Snowflake connector...")
        snowflake_connector = SnowflakeAudienceConnector()
        
        # Test Snowflake connection
        if not args.skip_test:
            logger.info("Testing Snowflake connection...")
            if not snowflake_connector.test_connection():
                logger.error("Snowflake connection test failed")
                sys.exit(1)
        
        # Connect to Snowflake
        snowflake_connector.connect()
        
        # Initialize Meta API client
        logger.info("Initializing Meta API client...")
        meta_client = MetaAPIClient(
            access_token=Config.META_ACCESS_TOKEN,
            ad_account_id=args.ad_account_id or Config.META_AD_ACCOUNT_ID
        )
        logger.info(f"Meta API client initialized for account: {meta_client.ad_account_id}")
        
        # Load CSV data
        data_processor = AudienceDataProcessor(args.csv_file or Config.CSV_FILE_PATH)
        data_processor.load_data()
        
        # Process audiences from CSV
        logger.info(f"Processing top {args.top_n} apps from CSV...")
        audiences = data_processor.process_all_audiences(top_n=args.top_n)
        
        # Export audience mapping for reference
        mapping_file = f"audience_mapping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        data_processor.export_audience_mapping(mapping_file)
        logger.info(f"Audience mapping exported to: {mapping_file}")
        
        # Display summary if in dry run mode
        if args.dry_run:
            logger.info("DRY RUN MODE - Checking MAID counts in Snowflake")
            
            dry_run_data = []
            for audience in audiences[:10]:  # Check first 10 for dry run
                app_name = audience['original_name']
                count = snowflake_connector.get_audience_count(app_name)
                
                dry_run_data.append({
                    'app_name': app_name,
                    'audience_name': audience['name'],
                    'maid_count': count,
                    'os': audience['os'],
                    'category': audience['category']
                })
                
                logger.info(f"{app_name}: {count:,} MAIDs available")
            
            # Save dry run report
            dry_run_report = {
                'mode': 'dry_run_with_snowflake',
                'timestamp': datetime.now().isoformat(),
                'audiences_checked': len(dry_run_data),
                'data': dry_run_data
            }
            
            report_file = f"dry_run_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(report_file, 'w') as f:
                json.dump(dry_run_report, f, indent=2)
            logger.info(f"Dry run report saved to: {report_file}")
            
            snowflake_connector.disconnect()
            return
        
        # Perform actual upload
        logger.info("Starting audience upload to Meta with MAIDs from Snowflake...")
        
        upload_stats = {
            'total_audiences': len(audiences),
            'successful_uploads': 0,
            'failed_uploads': 0,
            'total_maids_uploaded': 0,
            'start_time': datetime.now(),
            'errors': []
        }
        
        created_audiences = []
        
        # Process each audience
        for i, audience in enumerate(audiences, 1):
            logger.info(f"\nProcessing audience {i}/{len(audiences)}: {audience['name']}")
            
            success, audience_id, maids_uploaded = upload_audience_with_maids(
                meta_client, 
                snowflake_connector, 
                audience
            )
            
            if success:
                upload_stats['successful_uploads'] += 1
                upload_stats['total_maids_uploaded'] += maids_uploaded
                
                created_audiences.append({
                    'id': audience_id,
                    'name': audience['name'],
                    'original_name': audience['original_name'],
                    'maids_uploaded': maids_uploaded,
                    'created_at': datetime.now().isoformat()
                })
            else:
                upload_stats['failed_uploads'] += 1
                upload_stats['errors'].append({
                    'audience': audience['name'],
                    'timestamp': datetime.now().isoformat()
                })
            
            # Rate limiting pause between audiences
            if i < len(audiences) and i % args.batch_size == 0:
                logger.info(f"Pausing {args.delay} seconds between batches...")
                import time
                time.sleep(args.delay)
        
        upload_stats['end_time'] = datetime.now()
        
        # Generate final report
        duration = (upload_stats['end_time'] - upload_stats['start_time']).total_seconds()
        
        report = {
            'summary': {
                'total_audiences': upload_stats['total_audiences'],
                'successful_uploads': upload_stats['successful_uploads'],
                'failed_uploads': upload_stats['failed_uploads'],
                'success_rate': (upload_stats['successful_uploads'] / upload_stats['total_audiences'] * 100) 
                               if upload_stats['total_audiences'] > 0 else 0,
                'total_maids_uploaded': upload_stats['total_maids_uploaded'],
                'duration_seconds': duration,
                'start_time': upload_stats['start_time'].isoformat(),
                'end_time': upload_stats['end_time'].isoformat()
            },
            'created_audiences': created_audiences,
            'errors': upload_stats['errors']
        }
        
        # Save report
        report_file = f"upload_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        # Display summary
        logger.info("\n" + "=" * 80)
        logger.info("UPLOAD COMPLETE - SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total Audiences: {report['summary']['total_audiences']}")
        logger.info(f"Successful: {report['summary']['successful_uploads']}")
        logger.info(f"Failed: {report['summary']['failed_uploads']}")
        logger.info(f"Success Rate: {report['summary']['success_rate']:.1f}%")
        logger.info(f"Total MAIDs Uploaded: {report['summary']['total_maids_uploaded']:,}")
        logger.info(f"Duration: {report['summary']['duration_seconds']:.1f} seconds")
        logger.info(f"Report saved to: {report_file}")
        logger.info(f"Log file: {log_file_path}")
        
        # Disconnect from Snowflake
        snowflake_connector.disconnect()
        
    except KeyboardInterrupt:
        logger.warning("Upload interrupted by user")
        if 'snowflake_connector' in locals():
            snowflake_connector.disconnect()
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        if 'snowflake_connector' in locals():
            snowflake_connector.disconnect()
        sys.exit(1)


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Upload audiences from CSV to Meta with MAIDs from Snowflake',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run to check MAID counts in Snowflake
  python main_with_snowflake.py --dry-run
  
  # Upload top 10 apps with MAIDs from Snowflake
  python main_with_snowflake.py --top-n 10
  
  # Full upload of 100 apps with MAIDs
  python main_with_snowflake.py --top-n 100 --batch-size 5
        """
    )
    
    parser.add_argument(
        '--ad-account-id',
        help='Meta Ad Account ID (format: act_XXXXX)',
        default=None
    )
    
    parser.add_argument(
        '--csv-file',
        help='Path to CSV file containing app data',
        default=None
    )
    
    parser.add_argument(
        '--top-n',
        type=int,
        default=100,
        help='Number of top apps to process (default: 100)'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10,
        help='Number of audiences to process before pausing (default: 10)'
    )
    
    parser.add_argument(
        '--delay',
        type=int,
        default=5,
        help='Delay in seconds between batches (default: 5)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Check MAID counts without uploading'
    )
    
    parser.add_argument(
        '--skip-test',
        action='store_true',
        help='Skip Snowflake connection test'
    )
    
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force execution even with configuration errors'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Set logging level (default: INFO)'
    )
    
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_arguments()
    
    # Update log level if specified
    if args.log_level != 'INFO':
        logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    main(args)
