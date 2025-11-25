#!/usr/bin/env python3
"""
Check MAID counts for all audiences created from Sheet3.csv
"""

import csv
import logging
from pathlib import Path
from typing import List, Dict
from datetime import datetime

from meta_api_client_optimized import OptimizedMetaAPIClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_audience_name(app_name: str, os: str) -> str:
    """Create audience name in format: US_{OS}_{AppName}_PIE"""
    clean_name = app_name.replace(':', '').replace('-', '').replace('™', '').replace('®', '')
    clean_name = clean_name.replace('!', '').replace('&', 'and').replace('+', 'plus')
    clean_name = clean_name.replace('/', ' ').replace('\\', ' ')
    clean_name = clean_name.replace('–', ' ').replace('—', ' ')
    
    while '  ' in clean_name:
        clean_name = clean_name.replace('  ', ' ')
    
    clean_name = clean_name.strip()
    
    max_length = 200 - len(f"US_{os}__PIE")
    if len(clean_name) > max_length:
        clean_name = clean_name[:max_length].rstrip()
    
    return f"US_{os}_{clean_name}_PIE"


def read_apps_from_csv(csv_path: str) -> List[Dict[str, str]]:
    """Read unique app+OS combinations from CSV"""
    apps = []
    seen_combinations = set()
    
    if not Path(csv_path).exists():
        logger.error(f"CSV file not found: {csv_path}")
        return apps
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):
            if row.get('App name') and row.get('OS'):
                app_name = row['App name'].strip()
                os_type = row['OS'].strip()
                unique_key = f"{app_name}|{os_type}"
                
                if unique_key not in seen_combinations:
                    apps.append({
                        'name': app_name,
                        'os': os_type,
                        'row': i
                    })
                    seen_combinations.add(unique_key)
    
    return apps


def main():
    """Check MAID counts for all audiences"""
    
    # Read apps from CSV
    csv_path = "Untitled spreadsheet - Sheet3.csv"
    logger.info(f"Reading apps from {csv_path}...")
    apps = read_apps_from_csv(csv_path)
    logger.info(f"Found {len(apps)} unique app+OS combinations")
    
    # Initialize Meta client
    try:
        meta_client = OptimizedMetaAPIClient()
        logger.info("Connected to Meta API")
    except Exception as e:
        logger.error(f"Failed to connect to Meta API: {e}")
        return 1
    
    # Get all audiences
    logger.info("Fetching all custom audiences...")
    try:
        all_audiences = meta_client.list_custom_audiences(limit=500)
        logger.info(f"Found {len(all_audiences)} total audiences in Meta")
    except Exception as e:
        logger.error(f"Failed to fetch audiences: {e}")
        return 1
    
    # Create audience name mapping
    audience_map = {}
    for aud in all_audiences:
        audience_map[aud.get('name', '')] = {
            'id': aud.get('id'),
            'name': aud.get('name'),
            'approximate_count_lower_bound': aud.get('approximate_count_lower_bound', 0),
            'approximate_count_upper_bound': aud.get('approximate_count_upper_bound', 0)
        }
    
    # Check each app from CSV
    results = []
    found_count = 0
    total_maids = 0
    missing_audiences = []
    
    print("\n" + "="*100)
    print("AUDIENCE MAID COUNT REPORT")
    print("="*100)
    print(f"{'App Name':<50} {'OS':<10} {'Status':<15} {'MAID Count':<20}")
    print("-"*100)
    
    for app_data in apps:
        app_name = app_data['name']
        os_type = app_data['os']
        audience_name = create_audience_name(app_name, os_type)
        
        if audience_name in audience_map:
            aud_info = audience_map[audience_name]
            lower = aud_info['approximate_count_lower_bound']
            upper = aud_info['approximate_count_upper_bound']
            
            # Use the average of lower and upper bounds as estimate
            if upper > 0:
                estimated_count = (lower + upper) // 2
            else:
                estimated_count = lower
            
            found_count += 1
            total_maids += estimated_count
            
            status = "✓ Found"
            if estimated_count > 0:
                count_str = f"{estimated_count:,}"
            else:
                count_str = "No count available"
                status = "⚠ Empty"
            
            results.append({
                'app_name': app_name,
                'os': os_type,
                'audience_name': audience_name,
                'audience_id': aud_info['id'],
                'estimated_count': estimated_count,
                'status': status
            })
            
            print(f"{app_name[:50]:<50} {os_type:<10} {status:<15} {count_str:<20}")
        else:
            missing_audiences.append({
                'app_name': app_name,
                'os': os_type,
                'audience_name': audience_name
            })
            print(f"{app_name[:50]:<50} {os_type:<10} {'✗ Not Found':<15} {'N/A':<20}")
    
    # Summary
    print("\n" + "="*100)
    print("SUMMARY")
    print("="*100)
    print(f"Total unique apps in CSV: {len(apps)}")
    print(f"Audiences found in Meta: {found_count}")
    print(f"Audiences missing: {len(missing_audiences)}")
    print(f"\nTotal estimated MAIDs across all found audiences: {total_maids:,}")
    
    if found_count > 0:
        avg_maids = total_maids // found_count
        print(f"Average MAIDs per audience: {avg_maids:,}")
    
    # List missing audiences
    if missing_audiences:
        print("\n" + "-"*100)
        print("MISSING AUDIENCES:")
        print("-"*100)
        for miss in missing_audiences[:10]:  # Show first 10
            print(f"  - {miss['app_name']} ({miss['os']})")
        if len(missing_audiences) > 10:
            print(f"  ... and {len(missing_audiences) - 10} more")
    
    # Save detailed results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"audience_count_report_{timestamp}.csv"
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['app_name', 'os', 'audience_name', 'audience_id', 'estimated_count', 'status']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    print(f"\n✓ Detailed report saved to: {output_file}")
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
