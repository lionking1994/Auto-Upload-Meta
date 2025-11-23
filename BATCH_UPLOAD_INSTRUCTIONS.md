# Batch Upload Instructions

## Overview
The batch upload script processes apps from a CSV file and creates Meta Custom Audiences with their corresponding MAIDs from Snowflake.

## Setup Complete ✅
- Fixed MAID deduplication issue (now using DISTINCT query)
- Optimized Snowflake data fetching (single query instead of OFFSET pagination)
- Created batch upload script that handles multiple apps
- Successfully tested with 3 apps

## CSV File
- **File**: `Untitled spreadsheet - Sheet2.csv`
- **Total Apps**: 183
- **Format**: Simple list of app names (one per row)

## Usage

### Test Run (Dry Run)
```bash
cd /home/ubuntu/Auto-Upload-Meta
source venv/bin/activate
python batch_upload_from_csv.py --dry-run --limit 5
```

### Upload First N Apps
```bash
python batch_upload_from_csv.py --limit 10
```

### Upload All Apps
```bash
python batch_upload_from_csv.py
```

### Resume from Specific App
```bash
# Start from app #50 (0-based index)
python batch_upload_from_csv.py --start-from 50
```

### Upload Range of Apps
```bash
# Upload apps 50-70
python batch_upload_from_csv.py --start-from 50 --limit 20
```

## Performance Metrics
Based on testing:
- **Snowflake Fetch**: ~30-60 seconds per app (depending on MAID count)
- **Meta Upload**: ~2-3 seconds per 50K MAIDs
- **Total Time per App**: ~1-2 minutes for typical apps

## Results
The script creates:
1. **Meta Custom Audiences**: Named as `{App Name}_{timestamp}`
2. **Log File**: In `logs/` directory with detailed progress
3. **Results JSON**: `batch_upload_results_{timestamp}.json` with summary

## Successfully Uploaded (Test Run)
1. ✅ Bingo Quest - Summer Garden Adventure (721,176 MAIDs)
2. ✅ Word Bingo - Fun Word Game (308,175 MAIDs)  
3. ✅ BINGO! (182,008 MAIDs)

## Notes
- The script adds a 2-second delay between apps to avoid rate limiting
- MAIDs are uploaded in batches of 50K to Meta (within 500K Snowflake batches)
- Duplicate MAIDs are automatically removed (DISTINCT query)
- Empty audiences (no MAIDs found) are skipped

## Monitoring
Check upload progress in real-time:
```bash
tail -f logs/$(ls -t logs/ | head -1)
```

## Error Handling
- If the script fails, it can be safely rerun
- Use `--start-from` to skip already processed apps
- Check the results JSON file for detailed status of each app
