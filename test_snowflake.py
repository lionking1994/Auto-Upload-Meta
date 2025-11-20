"""
Test script to verify Snowflake connection and MAID queries
"""
import sys
import logging
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_snowflake_connection():
    """Test Snowflake connection and query for MAIDs"""
    
    # Check if credentials are set
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    user = os.getenv('SNOWFLAKE_USER')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    pat_token = os.getenv('SNOWFLAKE_PAT_TOKEN')
    
    if not account or not user:
        logger.error("Missing required: SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER")
        return False
    
    if not password and not pat_token:
        logger.error("Missing authentication: Need either SNOWFLAKE_PASSWORD or SNOWFLAKE_PAT_TOKEN")
        logger.info("For MFA-enabled accounts, use SNOWFLAKE_PAT_TOKEN with your programmatic access token")
        return False
    
    if pat_token:
        logger.info("Using Programmatic Access Token (PAT) authentication (MFA bypass)")
    else:
        logger.info("Using password authentication")
    
    try:
        from snowflake_connector import SnowflakeAudienceConnector
        
        logger.info("=" * 60)
        logger.info("Testing Snowflake Connection")
        logger.info("=" * 60)
        
        # Initialize connector
        connector = SnowflakeAudienceConnector()
        
        # Test connection
        logger.info("\n1. Testing basic connection...")
        if not connector.test_connection():
            logger.error("Connection test failed")
            return False
        
        logger.info("✓ Connection successful!")
        
        # Connect for queries
        connector.connect()
        
        # Test the exact query format
        logger.info("\n2. Testing MAID query with 'Hit it Rich! Free Casino Slots'...")
        
        # Use warehouse
        connector.cursor.execute("USE WAREHOUSE COMPUTE_WH")
        logger.info("✓ Using WAREHOUSE COMPUTE_WH")
        
        # Run the exact query
        test_query = """
        SELECT DEVICE_ID_VALUE as MAID
        FROM GAMING.PUBLIC.KOCHAVA_GAMINGAUDIENCES_TBL
        WHERE APP_NAME_PROPER = 'Hit it Rich! Free Casino Slots'
        LIMIT 10
        """
        
        connector.cursor.execute(test_query)
        results = connector.cursor.fetchall()
        
        if results:
            logger.info(f"✓ Found {len(results)} MAIDs (showing first 10):")
            for i, row in enumerate(results[:5], 1):
                logger.info(f"   {i}. {row[0]}")
            if len(results) > 5:
                logger.info(f"   ... and {len(results) - 5} more")
        else:
            logger.warning("No MAIDs found for 'Hit it Rich! Free Casino Slots'")
        
        # Get total count
        logger.info("\n3. Getting total MAID count...")
        count = connector.get_audience_count('Hit it Rich! Free Casino Slots')
        logger.info(f"✓ Total MAIDs for 'Hit it Rich! Free Casino Slots': {count:,}")
        
        # Test fetching a batch
        logger.info("\n4. Testing batch fetch (first 100 MAIDs)...")
        maids = connector.get_audience_maids('Hit it Rich! Free Casino Slots', limit=100)
        logger.info(f"✓ Successfully fetched {len(maids)} MAIDs in correct format")
        
        # Show sample of formatted data
        if maids:
            logger.info("Sample formatted MAID (ready for Meta API):")
            logger.info(f"   {maids[0]}")
        
        # List some other available apps
        logger.info("\n5. Checking for other apps in the table...")
        query = """
        SELECT DISTINCT APP_NAME_PROPER
        FROM GAMING.PUBLIC.KOCHAVA_GAMINGAUDIENCES_TBL
        WHERE APP_NAME_PROPER IS NOT NULL
        LIMIT 10
        """
        connector.cursor.execute("USE WAREHOUSE COMPUTE_WH")
        connector.cursor.execute(query)
        apps = connector.cursor.fetchall()
        
        if apps:
            logger.info(f"✓ Found {len(apps)} apps (showing first 10):")
            for i, app in enumerate(apps, 1):
                logger.info(f"   {i}. {app[0]}")
        
        # Disconnect
        connector.disconnect()
        
        logger.info("\n" + "=" * 60)
        logger.info("✓ All tests passed successfully!")
        logger.info("=" * 60)
        logger.info("\nYour Snowflake connection is working correctly.")
        logger.info("You can now run the main script to upload audiences to Meta.")
        
        return True
        
    except Exception as e:
        logger.error(f"Test failed with error: {str(e)}", exc_info=True)
        return False


if __name__ == "__main__":
    success = test_snowflake_connection()
    sys.exit(0 if success else 1)
