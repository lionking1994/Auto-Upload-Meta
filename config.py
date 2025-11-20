"""
Configuration module for Meta Audience Upload
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    # Meta API Configuration
    META_ACCESS_TOKEN = os.getenv('META_ACCESS_TOKEN', 'EAAbKhZCuEdKEBPYKv41bQOnsYHEjLXt25zofbZA37iGl2ZCi86Br2S7WwtGdeuyXzuwc14uXHMzgs25lP16keZCamXZA5RZCRjDDX5SZAfztbsH4a6zELZAqFwmZAAoZBu4U8HDjTJuZCJ7I9FXgIwoO3PmSGZAaDvAx6Vvaq1w6yNcBWM1ti7WTRGBZA94LxZCC5FCb78u70ZD')
    META_AD_ACCOUNT_ID = os.getenv('META_AD_ACCOUNT_ID', '290877649')  # Default from env_template
    META_APP_ID = os.getenv('META_APP_ID', '')
    META_APP_SECRET = os.getenv('META_APP_SECRET', '')
    
    # API Rate Limiting
    API_CALLS_PER_HOUR = int(os.getenv('API_CALLS_PER_HOUR', '200'))
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '50000'))  # Users per batch
    
    # File Configuration
    CSV_FILE_PATH = os.getenv('CSV_FILE_PATH', 'Untitled spreadsheet - Sheet1.csv')
    
    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = 'meta_audience_upload.log'
    
    # Meta API Endpoints
    META_API_VERSION = 'v18.0'
    META_BASE_URL = f'https://graph.facebook.com/{META_API_VERSION}'
    
    # Retry Configuration
    MAX_RETRIES = 3
    RETRY_DELAY = 5  # seconds
