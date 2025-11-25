#!/usr/bin/env python3
"""
Check MAID counts for all audiences created from Sheet3.csv
"""

import csv
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict

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
    
    csv_file = "Untitled spreadsheet - Sheet3.csv"
    
    print("\n" + "="*80)
    print("CHECKING MAID COUNTS FOR AUDIENCES FROM SHEET3.CSV")
    print("="*80)
    
    # Read apps from CSV
    logger.info(f"Reading apps from {csv_file}...")
    apps = read_apps_from_csv(csv_file)
    logger.info(f"Found {len(apps)} unique app+OS combinations")
    
    # Initialize Meta API client
    try:
        meta_client = OptimizedMetaAPIClient()
        logger.info("Connected to Meta API")
    except Exception as e:
        logger.error(f"Failed to initialize Meta API client: {e}")
        return 1
    
    # Get all audiences
    logger.info("Fetching all custom audiences...")
    all_audiences = meta_client.list_custom_audiences(limit=500)
    logger.info(f"Found {len(all_audiences)} total audiences in Meta")
    
    # Create mapping of expected audience names
    expected_audiences = {}
    for app in apps:
        audience_name = create_audience_name(app['name'], app['os'])
        expected_audiences[audience_name] = app
    
    # Check each audience
    results = []
    total_maids = 0
    audiences_with_maids = 0
    audiences_without_maids = 0
    missing_audiences = []
    
    print("\n" + "="*80)
    print("AUDIENCE ANALYSIS")
    print("="*80)
    
    # Check existing audiences
    for audience in all_audiences:
        aud_name = audience.get('name', '')
        if aud_name in expected_audiences:
            app_data = expected_audiences[aud_name]
            
            # Get audience size (approximate count)
            lower_bound = audience.get('approximate_count_lower_bound', 0)
            upper_bound = audience.get('approximate_count_upper_bound', 0)
            
            # Use average of bounds as estimate
            if upper_bound > 0:
                estimated_count = (lower_bound + upper_bound) // 2
            else:
                estimated_count = lower_bound
            
            result = {
                'audience_name': aud_name,
                'audience_id': audience.get('id'),
                'app_name': app_data['name'],
                'os': app_data['os'],
                'estimated_maids': estimated_count,
                'lower_bound': lower_bound,
                'upper_bound': upper_bound
            }
            
            results.append(result)
            
            if estimated_count > 0:
                audiences_with_maids += 1
                total_maids += estimated_count
                logger.info(f"✓ {aud_name}: ~{estimated_count:,} MAIDs")
            else:
                audiences_without_maids += 1
                logger.warning(f"⚠ {aud_name}: No MAIDs uploaded yet")
            
            # Remove from expected list
            del expected_audiences[aud_name]
    
    # Check for missing audiences
    for missing_name, app_data in expected_audiences.items():
        missing_audiences.append({
            'audience_name': missing_name,
            'app_name': app_data['name'],
            'os': app_data['os']
        })
        logger.warning(f"✗ Missing audience: {missing_name}")
    
    # Save detailed results
    results_file = f'audience_maid_counts_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    with open(results_file, 'w') as f:
        json.dump({
            'summary': {
                'total_expected_audiences': len(apps),
                'audiences_found': len(results),
                'audiences_missing': len(missing_audiences),
                'audiences_with_maids': audiences_with_maids,
                'audiences_without_maids': audiences_without_maids,
                'total_estimated_maids': total_maids
            },
            'audiences': results,
            'missing_audiences': missing_audiences
        }, f, indent=2)
    
    # Print summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"\nTotal unique apps in CSV: {len(apps)}")
    print(f"Audiences found in Meta: {len(results)}")
    print(f"Missing audiences: {len(missing_audiences)}")
    print(f"\nAudiences with MAIDs: {audiences_with_maids}")
    print(f"Audiences without MAIDs: {audiences_without_maids}")
    print(f"\nTotal estimated MAIDs uploaded: {total_maids:,}")
    
    if audiences_with_maids > 0:
        avg_maids = total_maids // audiences_with_maids
        print(f"Average MAIDs per audience (with data): {avg_maids:,}")
    
    print(f"\nDetailed results saved to: {results_file}")
    
    # Show top 10 audiences by MAID count
    if results:
        print("\n" + "="*80)
        print("TOP 10 AUDIENCES BY MAID COUNT")
        print("="*80)
        sorted_results = sorted(results, key=lambda x: x['estimated_maids'], reverse=True)
        for i, aud in enumerate(sorted_results[:10], 1):
            print(f"{i}. {aud['app_name']} ({aud['os']}): ~{aud['estimated_maids']:,} MAIDs")
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
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
    
    print("\n" + "="*80)
    print("CHECKING MAID COUNTS FOR SHEET3.CSV AUDIENCES")
    print("="*80)
    
    # Read apps from CSV
    csv_path = "Untitled spreadsheet - Sheet3.csv"
    logger.info(f"Reading apps from {csv_path}...")
    apps = read_apps_from_csv(csv_path)
    logger.info(f"Found {len(apps)} unique app+OS combinations")
    
    # Initialize Meta API client
    try:
        meta_client = OptimizedMetaAPIClient()
        logger.info("Connected to Meta API")
    except Exception as e:
        logger.error(f"Failed to connect to Meta API: {e}")
        return 1
    
    # Get all audiences
    logger.info("Fetching all custom audiences...")
    all_audiences = meta_client.list_custom_audiences(limit=500)
    logger.info(f"Found {len(all_audiences)} total audiences in account")
    
    # Create a map of expected audience names
    expected_audiences = {}
    for app in apps:
        audience_name = create_audience_name(app['name'], app['os'])
        expected_audiences[audience_name] = app
    
    # Check each expected audience
    results = []
    total_maids = 0
    found_count = 0
    missing_count = 0
    
    print("\n" + "-"*80)
    print("AUDIENCE STATUS:")
    print("-"*80)
    
    for expected_name, app_info in expected_audiences.items():
        found = False
        for audience in all_audiences:
            if audience.get('name') == expected_name:
                found = True
                found_count += 1
                
                # Get audience details
                audience_id = audience.get('id')
                lower_bound = audience.get('approximate_count_lower_bound', 0)
                upper_bound = audience.get('approximate_count_upper_bound', 0)
                
                # Calculate average
                if lower_bound and upper_bound:
                    avg_count = (int(lower_bound) + int(upper_bound)) // 2
                elif lower_bound:
                    avg_count = int(lower_bound)
                elif upper_bound:
                    avg_count = int(upper_bound)
                else:
                    avg_count = 0
                
                total_maids += avg_count
                
                result = {
                    'app_name': app_info['name'],
                    'os': app_info['os'],
                    'audience_name': expected_name,
                    'audience_id': audience_id,
                    'maid_count': avg_count,
                    'lower_bound': lower_bound,
                    'upper_bound': upper_bound,
                    'status': 'found'
                }
                results.append(result)
                
                if avg_count > 0:
                    print(f"✓ {app_info['name']} ({app_info['os']}): {avg_count:,} MAIDs")
                else:
                    print(f"⚠ {app_info['name']} ({app_info['os']}): No MAIDs uploaded yet")
                break
        
        if not found:
            missing_count += 1
            result = {
                'app_name': app_info['name'],
                'os': app_info['os'],
                'audience_name': expected_name,
                'status': 'missing'
            }
            results.append(result)
            print(f"✗ {app_info['name']} ({app_info['os']}): Audience not found")
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY:")
    print("="*80)
    
    print(f"\nTotal unique apps in CSV: {len(apps)}")
    print(f"Audiences found: {found_count}")
    print(f"Audiences missing: {missing_count}")
    print(f"\nTotal MAIDs uploaded: {total_maids:,}")
    
    if found_count > 0:
        avg_per_audience = total_maids // found_count
        print(f"Average MAIDs per audience: {avg_per_audience:,}")
    
    # Find top audiences by MAID count
    sorted_results = sorted([r for r in results if r.get('maid_count', 0) > 0], 
                           key=lambda x: x.get('maid_count', 0), 
                           reverse=True)
    
    if sorted_results:
        print("\n" + "-"*80)
        print("TOP 10 AUDIENCES BY MAID COUNT:")
        print("-"*80)
        for i, result in enumerate(sorted_results[:10], 1):
            print(f"{i}. {result['app_name']} ({result['os']}): {result['maid_count']:,} MAIDs")
    
    # Save detailed results
    output_file = f"audience_maid_counts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['app_name', 'os', 'audience_name', 'audience_id', 'maid_count', 
                     'lower_bound', 'upper_bound', 'status']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    print(f"\nDetailed results saved to: {output_file}")
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
