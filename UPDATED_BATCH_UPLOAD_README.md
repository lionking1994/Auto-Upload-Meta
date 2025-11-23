# Updated Batch Upload System

## Changes Made

### New Audience Naming Format
Audiences are now created with the format: **`US_{OS}_{AppName}_PIE`**

Examples:
- `US_Android_Bingo_Quest___Summer_Garden_Adventure_PIE`
- `US_iOS_BINGO_PIE`
- `US_Android_Word_Bingo___Fun_Word_Game_PIE`

### New CSV Format
The script now reads from **`Untitled spreadsheet - Sheet3.csv`** which includes:
- Column 1: App name
- Column 2: Operating System (OS)

Sample CSV format:
```
App name,OS
Bingo Quest - Summer Garden Adventure,Android
Word Bingo - Fun Word Game,Android
BINGO!,iOS
```

### Supported OS Types
The script handles various OS types from the CSV:
- Android
- iOS
- iPadOS
- GoogleTV
- And any other OS specified in the CSV

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

### Use Different CSV File
```bash
python batch_upload_from_csv.py --csv "your_file.csv"
```

## CSV File Requirements
- Must have header row: `App name,OS`
- App name in first column
- OS in second column
- File should be UTF-8 encoded

## Naming Logic
1. **US_** prefix for all audiences (hardcoded)
2. **OS** from the CSV (spaces/special chars removed)
3. **App Name** with:
   - Special characters removed
   - Spaces converted to underscores
   - Hyphens converted to underscores
4. **_PIE** suffix for all audiences (hardcoded)

## Character Limits
- Maximum audience name length: 200 characters
- If name exceeds limit, app name portion is truncated

## Results
The script generates:
1. **Log file** in `logs/` directory
2. **JSON results file** with timestamp
3. Each result includes:
   - app_name
   - os
   - audience_id (when created)
   - audience_name
   - maid_count
   - status
   - processing_time
