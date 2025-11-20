# Meta Audience Upload Automation

An automated pipeline for uploading custom audiences from CSV data to Meta Audience Manager via the Meta Marketing API.

## Features

- ✅ **Bulk Upload**: Process and upload hundreds of audiences automatically
- ✅ **Rate Limiting**: Built-in rate limiting to respect Meta API limits (200 calls/hour)
- ✅ **Batch Processing**: Upload audiences in configurable batches with delays
- ✅ **Progress Tracking**: Resume interrupted uploads from where they left off
- ✅ **Error Handling**: Comprehensive error handling with detailed logging
- ✅ **Dry Run Mode**: Test your configuration without making actual uploads
- ✅ **Verification**: Verify uploaded audiences after creation
- ✅ **Reporting**: Generate detailed reports of upload operations
- ✅ **Sample Data Generation**: Generate test user data for development

## Prerequisites

- Python 3.8 or higher
- Meta Business Account with API access
- Ad Account ID
- Valid Meta Access Token

## Installation

1. Clone or download this project to your local machine

2. Install required dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

### Method 1: Environment Variables (Recommended)

Create a `.env` file in the project root:

```env
# Meta API Configuration
META_ACCESS_TOKEN=your_access_token_here
META_AD_ACCOUNT_ID=act_your_ad_account_id
META_APP_ID=your_app_id  # Optional
META_APP_SECRET=your_app_secret  # Optional

# Processing Configuration
CSV_FILE_PATH=Untitled spreadsheet - Sheet1.csv
API_CALLS_PER_HOUR=200
BATCH_SIZE=50000
```

### Method 2: Direct Configuration

Edit `config.py` directly with your credentials (not recommended for production).

## Usage

### Basic Usage

1. **Dry Run** (recommended first step):
```bash
python main.py --dry-run
```

2. **Upload Top 10 Apps** (for testing):
```bash
python main.py --top-n 10 --ad-account-id act_YOUR_ACCOUNT_ID
```

3. **Full Upload of Top 100 Apps**:
```bash
python main.py --top-n 100 --with-users --verify
```

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--ad-account-id` | Meta Ad Account ID (format: act_XXXXX) | From config |
| `--csv-file` | Path to CSV file with app data | From config |
| `--top-n` | Number of top apps to process | 100 |
| `--batch-size` | Audiences to upload per batch | 10 |
| `--delay` | Seconds between batches | 5 |
| `--with-users` | Upload sample user data | False |
| `--dry-run` | Simulate without uploading | False |
| `--verify` | Verify audiences after upload | False |
| `--rollback` | Delete created audiences (testing) | False |
| `--cleanup` | Clean progress tracking | False |
| `--log-level` | Logging level (DEBUG/INFO/WARNING/ERROR) | INFO |

### Examples

#### Test with Small Batch
```bash
python main.py --top-n 5 --dry-run
```

#### Upload with Custom Settings
```bash
python main.py --top-n 50 --batch-size 5 --delay 10 --verify
```

#### Upload with User Data
```bash
python main.py --top-n 100 --with-users --batch-size 20
```

#### Debug Mode
```bash
python main.py --log-level DEBUG --top-n 10
```

## CSV File Format

The script expects a CSV file with the following columns:
- `App Name`: Name of the application
- `OS`: Operating system (Android/iOS)
- `Category`: App category
- `Device Count`: Number of devices/users
- `Table Name`: Data source table

Example:
```csv
Table Name,Category,App Name,OS,Device Count
Table 2,Games > Puzzle,Hit it Rich! Free Casino Slots,Android,41458390
Table 2,Games > Casual,Free Slots: Hot Vegas Slot Machines,Android,1855256
```

## Output Files

The script generates several output files:

1. **`audience_mapping_[timestamp].csv`**: Maps original app names to Meta audience names
2. **`upload_report.json`**: Detailed report of the upload operation
3. **`upload_progress.json`**: Progress tracking for resume capability
4. **`logs/[timestamp]_meta_audience_upload.log`**: Detailed execution logs

## API Rate Limits

Meta API has the following limits:
- **200 API calls per hour** per user/app combination
- **10,000-100,000 users** per batch upload
- The script automatically handles rate limiting and will slow down if approaching limits

## Troubleshooting

### Common Issues

1. **"Ad Account ID is required"**
   - Ensure META_AD_ACCOUNT_ID is set in your .env file
   - Format should be: `act_123456789`

2. **"401 Unauthorized"**
   - Check that your access token is valid
   - Tokens may expire - generate a new one if needed

3. **"Rate limit exceeded"**
   - The script will automatically wait and retry
   - Consider reducing batch-size or increasing delay

4. **"CSV file not found"**
   - Ensure the CSV file path is correct in your configuration
   - Use absolute path if needed

### Resume Interrupted Uploads

If an upload is interrupted, simply run the same command again. The script will:
1. Load progress from `upload_progress.json`
2. Skip already completed audiences
3. Continue from where it left off

To start fresh, use the `--cleanup` flag or delete `upload_progress.json`.

## Production Deployment

For production use with real Snowflake data:

1. Modify `data_processor.py` to fetch real user data from Snowflake:
```python
def get_users_from_snowflake(app_name, limit=None):
    # Your Snowflake query logic here
    pass
```

2. Update the schema in `meta_api_client.py` based on your available data:
```python
schema = ['EMAIL', 'PHONE', 'FN', 'LN', 'COUNTRY', 'STATE', 'CITY', 'ZIP']
```

3. Consider implementing:
   - Database connection pooling
   - Async processing for large datasets
   - Distributed processing for massive uploads
   - Monitoring and alerting

## Security Notes

- **Never commit credentials** to version control
- Use environment variables or secure vaults for production
- Rotate access tokens regularly
- Implement proper access controls for production deployment
- Consider using Meta's App Secret Proof for enhanced security

## Support

For issues or questions:
1. Check the logs in `logs/` directory
2. Review the dry run output first
3. Verify your Meta API credentials and permissions
4. Ensure your Ad Account has the necessary permissions

## License

This project is provided as-is for internal use.

## Changelog

### Version 1.0.0 (November 2024)
- Initial release with core functionality
- Support for CSV-based audience upload
- Rate limiting and batch processing
- Progress tracking and resume capability
- Comprehensive error handling and logging
