"""
Main orchestration script for Meta Audience Upload
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
    
    if errors:
        for error in errors:
            logger.error(error)
        return False
    
    return True


def main(args):
    """
    Main execution function
    
    Args:
        args: Command line arguments
    """
    logger.info("=" * 80)
    logger.info("Meta Audience Upload Script Starting")
    logger.info("=" * 80)
    
    # Validate configuration
    if not validate_configuration():
        logger.error("Configuration validation failed. Please check your settings.")
        if not args.force:
            sys.exit(1)
    
    try:
        # Initialize components
        logger.info("Initializing components...")
        
        # Set up Meta API client
        meta_client = MetaAPIClient(
            access_token=Config.META_ACCESS_TOKEN,
            ad_account_id=args.ad_account_id or Config.META_AD_ACCOUNT_ID
        )
        logger.info(f"Meta API client initialized for account: {meta_client.ad_account_id}")
        
        # Set up data processor
        data_processor = AudienceDataProcessor(args.csv_file or Config.CSV_FILE_PATH)
        data_processor.load_data()
        
        # Process audiences
        logger.info(f"Processing top {args.top_n} apps from CSV...")
        audiences = data_processor.process_all_audiences(top_n=args.top_n)
        
        # Export audience mapping for reference
        mapping_file = f"audience_mapping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        data_processor.export_audience_mapping(mapping_file)
        logger.info(f"Audience mapping exported to: {mapping_file}")
        
        # Display summary if in dry run mode
        if args.dry_run:
            logger.info("DRY RUN MODE - No actual uploads will be performed")
            summary = data_processor.get_audience_summary()
            logger.info(f"\nAudiences to be created:\n{summary.to_string()}")
            
            # Save dry run report
            dry_run_report = {
                'mode': 'dry_run',
                'timestamp': datetime.now().isoformat(),
                'audiences_count': len(audiences),
                'audiences': [
                    {
                        'name': a['name'],
                        'original_name': a['original_name'],
                        'os': a['os'],
                        'category': a['category'],
                        'device_count': a['device_count']
                    }
                    for a in audiences
                ]
            }
            
            report_file = f"dry_run_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(report_file, 'w') as f:
                json.dump(dry_run_report, f, indent=2)
            logger.info(f"Dry run report saved to: {report_file}")
            
            return
        
        # Initialize upload manager
        upload_manager = AudienceUploadManager(
            meta_client=meta_client,
            data_processor=data_processor
        )
        
        # Perform upload
        logger.info("Starting audience upload to Meta...")
        logger.info(f"Upload mode: {'With sample users' if args.with_users else 'Audiences only (no users)'}")
        
        upload_stats = upload_manager.upload_audiences_batch(
            audiences=audiences,
            batch_size=args.batch_size,
            delay_between_batches=args.delay,
            with_users=args.with_users
        )
        
        # Verify uploads if requested
        if args.verify:
            logger.info("Verifying uploaded audiences...")
            verified = upload_manager.verify_uploads()
            logger.info(f"Verified {len(verified)} audiences")
        
        # Generate final report
        report = upload_manager.generate_report()
        
        # Display summary
        logger.info("\n" + "=" * 80)
        logger.info("UPLOAD COMPLETE - SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total Audiences: {report['summary']['total_audiences']}")
        logger.info(f"Successful: {report['summary']['successful_uploads']}")
        logger.info(f"Failed: {report['summary']['failed_uploads']}")
        logger.info(f"Success Rate: {report['summary']['success_rate']:.1f}%")
        logger.info(f"Total Users Uploaded: {report['summary']['total_users_uploaded']}")
        if report['summary']['duration_seconds']:
            logger.info(f"Duration: {report['summary']['duration_seconds']:.1f} seconds")
        logger.info(f"Report saved to: upload_report.json")
        logger.info(f"Log file: {log_file_path}")
        
        # Display any errors
        if report['errors']:
            logger.warning(f"\nErrors encountered: {len(report['errors'])}")
            for error in report['errors'][:5]:  # Show first 5 errors
                logger.warning(f"  - {error['audience']}: {error['error']}")
        
        # Cleanup if requested
        if args.cleanup:
            logger.info("Cleaning up progress tracking...")
            upload_manager.cleanup_progress()
        
        # Rollback if requested (useful for testing)
        if args.rollback:
            logger.warning("ROLLBACK requested - deleting created audiences...")
            deleted = upload_manager.rollback_uploads()
            logger.info(f"Deleted {deleted} audiences")
        
    except KeyboardInterrupt:
        logger.warning("Upload interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Upload audiences from CSV to Meta Audience Manager',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run to see what would be uploaded
  python main.py --dry-run
  
  # Upload top 10 apps for testing
  python main.py --top-n 10
  
  # Upload with custom batch size and delay
  python main.py --batch-size 5 --delay 10
  
  # Upload audiences only (no user data)
  python main.py --no-users
  
  # Full upload with verification
  python main.py --top-n 100 --verify --with-users
        """
    )
    
    # Required arguments (made optional with defaults from config)
    parser.add_argument(
        '--ad-account-id',
        help='Meta Ad Account ID (format: act_XXXXX)',
        default=None
    )
    
    # Data source
    parser.add_argument(
        '--csv-file',
        help='Path to CSV file containing app data',
        default=None
    )
    
    # Processing options
    parser.add_argument(
        '--top-n',
        type=int,
        default=100,
        help='Number of top apps to process (default: 100)'
    )
    
    # Upload options
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10,
        help='Number of audiences to upload per batch (default: 10)'
    )
    
    parser.add_argument(
        '--delay',
        type=int,
        default=5,
        help='Delay in seconds between batches (default: 5)'
    )
    
    parser.add_argument(
        '--with-users',
        action='store_true',
        default=False,
        help='Upload sample user data with audiences'
    )
    
    parser.add_argument(
        '--no-users',
        dest='with_users',
        action='store_false',
        help='Create audiences without uploading user data'
    )
    
    # Execution modes
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Perform dry run without actual uploads'
    )
    
    parser.add_argument(
        '--verify',
        action='store_true',
        help='Verify audiences after upload'
    )
    
    parser.add_argument(
        '--rollback',
        action='store_true',
        help='Delete all created audiences after upload (for testing)'
    )
    
    parser.add_argument(
        '--cleanup',
        action='store_true',
        help='Clean up progress tracking after completion'
    )
    
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force execution even with configuration errors'
    )
    
    # Logging
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
