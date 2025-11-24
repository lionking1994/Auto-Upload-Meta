"""
Configuration module for Meta and Snowflake connections
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
env_path = Path(__file__).parent / '.env'
load_dotenv(env_path)


class Config:
    """Configuration class for API credentials and settings"""
    
    # Meta API Configuration
    META_ACCESS_TOKEN = os.getenv('META_ACCESS_TOKEN')
    META_AD_ACCOUNT_ID = os.getenv('META_AD_ACCOUNT_ID')
    META_API_VERSION = 'v21.0'
    META_API_BASE_URL = f'https://graph.facebook.com/{META_API_VERSION}'
    
    # Snowflake Configuration
    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE', 'GAMING')
    SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
    SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE')
    
    # Optional: Snowflake PAT Token for MFA bypass
    SNOWFLAKE_PAT_TOKEN = os.getenv('SNOWFLAKE_PAT_TOKEN')
    
    # Batch processing settings
    DEFAULT_BATCH_SIZE = 50000
    MAX_BATCH_SIZE = 500000
    
    # API Rate limiting
    API_CALLS_PER_HOUR = 200
    MAX_RETRIES = 3
    RETRY_DELAY = 5
    
    # File paths
    LOG_DIR = Path(__file__).parent / 'logs'
    LOG_DIR.mkdir(exist_ok=True)
    
    @classmethod
    def validate(cls):
        """Validate that required configuration is present"""
        errors = []
        
        if not cls.META_ACCESS_TOKEN:
            errors.append("META_ACCESS_TOKEN is not set")
        if not cls.META_AD_ACCOUNT_ID:
            errors.append("META_AD_ACCOUNT_ID is not set")
        if not cls.SNOWFLAKE_ACCOUNT:
            errors.append("SNOWFLAKE_ACCOUNT is not set")
        if not cls.SNOWFLAKE_USER:
            errors.append("SNOWFLAKE_USER is not set")
        if not cls.SNOWFLAKE_PASSWORD and not cls.SNOWFLAKE_PAT_TOKEN:
            errors.append("Either SNOWFLAKE_PASSWORD or SNOWFLAKE_PAT_TOKEN must be set")
        
        if errors:
            raise ValueError(f"Configuration errors: {', '.join(errors)}")
        
        return True
