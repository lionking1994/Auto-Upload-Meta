#!/usr/bin/env python3
"""
Script to check uploaded audiences and clean them up
- Verifies audience creation and MAID uploads
- Lists all custom audiences with details
- Provides option to delete test audiences
"""

import sys
import logging
import argparse
from datetime import datetime
import time

from logger_config import setup_logging
from meta_api_client import MetaAPIClient
from config import Config

# Set up logging
log_file_path = setup_logging()
logger = logging.getLogger(__name__)


def get_all_audiences(meta_client):
    """Fetch all custom audiences from Meta"""
    try:
        logger.info("Fetching all custom audiences...")
        
        # Get all custom audiences
        audiences = meta_client.list_custom_audiences(limit=500)
        
        logger.info(f"Found {len(audiences)} custom audiences")
        return audiences
    except Exception as e:
        logger.error(f"Error fetching audiences: {str(e)}")
        return []


def check_audience_details(meta_client, audience_id):
    """Get detailed information about a specific audience"""
    try:
        endpoint = f"{audience_id}"
        params = {
            'fields': 'id,name,description,approximate_count_lower_bound,approximate_count_upper_bound,time_created,time_updated,delivery_status,operation_status'
        }
        
        response = meta_client._make_request('GET', endpoint, params=params)
        return response
    except Exception as e:
        logger.error(f"Error getting details for audience {audience_id}: {str(e)}")
        # Try with fewer fields if the first attempt fails
        try:
            params = {
                'fields': 'id,name,description,time_created,delivery_status,operation_status'
            }
            response = meta_client._make_request('GET', endpoint, params=params)
            return response
        except:
            return None


def display_audience_info(audience_details):
    """Display formatted audience information"""
    if not audience_details:
        return
    
    print(f"\n  ID: {audience_details.get('id')}")
    print(f"  Name: {audience_details.get('name')}")
    print(f"  Description: {audience_details.get('description', 'N/A')}")
    
    # Parse and format time
    time_created = audience_details.get('time_created')
    if time_created:
        created_dt = datetime.fromtimestamp(int(time_created))
        print(f"  Created: {created_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Audience size (may take time to populate)
    lower = audience_details.get('approximate_count_lower_bound', 0)
    upper = audience_details.get('approximate_count_upper_bound', 0)
    if lower > 0 or upper > 0:
        print(f"  Approximate Size: {lower:,} - {upper:,} users")
    else:
        print(f"  Approximate Size: Processing... (may take up to 24 hours)")
    
    # Status
    delivery = audience_details.get('delivery_status', {})
    if isinstance(delivery, dict):
        print(f"  Delivery Status: {delivery.get('status', 'Unknown')}")
    
    operation = audience_details.get('operation_status', {})
    if isinstance(operation, dict):
        print(f"  Operation Status: {operation.get('status', 'Unknown')}")


def delete_audience(meta_client, audience_id, audience_name):
    """Delete a specific custom audience"""
    try:
        confirmation = input(f"\n⚠️  Are you sure you want to delete '{audience_name}'? (yes/no): ")
        if confirmation.lower() != 'yes':
            print("Skipped deletion")
            return False
        
        success = meta_client.delete_audience(audience_id)
        
        if success:
            logger.info(f"Successfully deleted audience: {audience_name} (ID: {audience_id})")
            return True
        else:
            logger.error(f"Failed to delete audience: {audience_name}")
            return False
    except Exception as e:
        logger.error(f"Error deleting audience {audience_id}: {str(e)}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Check and manage Meta Custom Audiences')
    parser.add_argument(
        '--check',
        action='store_true',
        help='Check all audiences and display details'
    )
    parser.add_argument(
        '--filter',
        type=str,
        help='Filter audiences by name pattern'
    )
    parser.add_argument(
        '--delete',
        action='store_true',
        help='Delete audiences (will ask for confirmation)'
    )
    parser.add_argument(
        '--delete-all-test',
        action='store_true',
        help='Delete all audiences created today (test audiences)'
    )
    parser.add_argument(
        '--audience-id',
        type=str,
        help='Specific audience ID to check or delete'
    )
    
    args = parser.parse_args()
    
    # Initialize Meta client
    try:
        logger.info("Initializing Meta API client...")
        meta_client = MetaAPIClient()
        logger.info("✓ Connected to Meta")
    except Exception as e:
        logger.error(f"Failed to initialize Meta client: {str(e)}")
        return 1
    
    # Get all audiences
    audiences = get_all_audiences(meta_client)
    
    if not audiences:
        print("No custom audiences found")
        return 0
    
    # Filter if requested
    if args.filter:
        filtered = [a for a in audiences if args.filter.lower() in a.get('name', '').lower()]
        print(f"\nFound {len(filtered)} audiences matching '{args.filter}':")
        audiences = filtered
    else:
        print(f"\nFound {len(audiences)} total custom audiences:")
    
    # Sort by creation time (newest first)
    audiences.sort(key=lambda x: x.get('time_created', ''), reverse=True)
    
    # Display audiences
    today = datetime.now().date()
    today_count = 0
    
    print("\n" + "="*60)
    for i, audience in enumerate(audiences, 1):
        # Check if created today
        time_created = audience.get('time_created')
        is_today = False
        if time_created:
            created_date = datetime.fromtimestamp(int(time_created)).date()
            is_today = created_date == today
            if is_today:
                today_count += 1
        
        # Display basic info
        marker = "📌 " if is_today else "   "
        print(f"\n{marker}[{i}] {audience.get('name')}")
        
        # Get detailed info if requested
        if args.check or args.audience_id == audience.get('id'):
            details = check_audience_details(meta_client, audience.get('id'))
            display_audience_info(details)
            time.sleep(0.5)  # Avoid rate limiting
    
    print("\n" + "="*60)
    print(f"\nSummary:")
    print(f"  Total audiences: {len(audiences)}")
    print(f"  Created today: {today_count}")
    
    # Delete operations
    if args.delete_all_test:
        print(f"\n⚠️  WARNING: This will delete all {today_count} audiences created today!")
        confirmation = input("Type 'DELETE ALL' to confirm: ")
        
        if confirmation == 'DELETE ALL':
            deleted_count = 0
            for audience in audiences:
                time_created = audience.get('time_created')
                if time_created:
                    created_date = datetime.fromtimestamp(int(time_created)).date()
                    if created_date == today:
                        if delete_audience(meta_client, audience.get('id'), audience.get('name')):
                            deleted_count += 1
                        time.sleep(1)  # Avoid rate limiting
            
            print(f"\n✅ Deleted {deleted_count} test audiences")
        else:
            print("Deletion cancelled")
    
    elif args.delete and args.audience_id:
        # Delete specific audience
        audience = next((a for a in audiences if a.get('id') == args.audience_id), None)
        if audience:
            delete_audience(meta_client, audience.get('id'), audience.get('name'))
        else:
            print(f"Audience ID {args.audience_id} not found")
    
    elif args.delete and args.filter:
        # Delete filtered audiences
        print(f"\n⚠️  This will delete {len(audiences)} audiences matching '{args.filter}'")
        confirmation = input("Type 'DELETE' to confirm: ")
        
        if confirmation == 'DELETE':
            deleted_count = 0
            for audience in audiences:
                if delete_audience(meta_client, audience.get('id'), audience.get('name')):
                    deleted_count += 1
                time.sleep(1)
            print(f"\n✅ Deleted {deleted_count} audiences")
        else:
            print("Deletion cancelled")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
