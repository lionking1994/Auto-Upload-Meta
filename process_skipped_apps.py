#!/usr/bin/env python3
"""
Process only the apps that were skipped in the previous run
Reads the results JSON file and processes only apps with 'skipped' status
"""

import json
import sys
import argparse
from pathlib import Path

def extract_skipped_apps(results_file):
    """Extract apps that were skipped from the results JSON file"""
    if not Path(results_file).exists():
        print(f"Results file not found: {results_file}")
        return []
    
    with open(results_file, 'r') as f:
        results = json.load(f)
    
    skipped_apps = []
    for result in results:
        if result.get('status') == 'skipped':
            # Find the OS from the audience name or default to Unknown
            audience_name = result.get('audience_name', '')
            # Extract OS from audience name format: US_OS_AppName_PIE
            parts = audience_name.split('_')
            os_type = parts[1] if len(parts) > 1 else 'Unknown'
            
            skipped_apps.append({
                'name': result.get('app_name'),
                'os': os_type
            })
    
    return skipped_apps

def create_temp_csv(skipped_apps, output_file='skipped_apps.csv'):
    """Create a temporary CSV file with skipped apps"""
    import csv
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['App name', 'OS'])
        for app in skipped_apps:
            writer.writerow([app['name'], app['os']])
    
    return output_file

def main():
    parser = argparse.ArgumentParser(description='Process skipped apps from previous run')
    parser.add_argument(
        '--results-file',
        default='batch_upload_results_20251122_030717.json',
        help='Path to the results JSON file from previous run'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Just show which apps would be processed'
    )
    
    args = parser.parse_args()
    
    # Extract skipped apps
    skipped_apps = extract_skipped_apps(args.results_file)
    
    if not skipped_apps:
        print("No skipped apps found in the results file")
        return 0
    
    print(f"Found {len(skipped_apps)} skipped apps:")
    for i, app in enumerate(skipped_apps[:10], 1):
        print(f"  {i}. {app['name']} ({app['os']})")
    if len(skipped_apps) > 10:
        print(f"  ... and {len(skipped_apps) - 10} more")
    
    if args.dry_run:
        print("\nDry run - no processing will be done")
        return 0
    
    # Create temporary CSV file
    temp_csv = create_temp_csv(skipped_apps)
    print(f"\nCreated temporary CSV file: {temp_csv}")
    
    # Run the batch upload script with the temporary CSV
    import subprocess
    cmd = [
        'python', 'batch_upload_from_csv.py',
        '--csv', temp_csv
    ]
    
    print(f"\nRunning: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    
    return result.returncode

if __name__ == "__main__":
    sys.exit(main())
