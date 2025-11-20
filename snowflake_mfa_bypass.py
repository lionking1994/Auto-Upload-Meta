"""
Snowflake connection with MFA bypass options
"""
import os
import snowflake.connector
from dotenv import load_dotenv
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def connect_with_external_browser():
    """
    Connect using external browser authentication (SSO/MFA friendly)
    This will open your browser for authentication
    """
    try:
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            authenticator='externalbrowser',
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
            database=os.getenv('SNOWFLAKE_DATABASE', 'GAMING'),
            schema=os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
            role=os.getenv('SNOWFLAKE_ROLE')
        )
        logger.info("Connected successfully with external browser authentication!")
        return conn
    except Exception as e:
        logger.error(f"External browser auth failed: {e}")
        return None

def connect_with_session_token():
    """
    Connect using a session token from Snowflake
    The token you provided looks like a session/JWT token
    """
    try:
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            token=os.getenv('SNOWFLAKE_TOKEN'),
            authenticator='oauth',
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
            database=os.getenv('SNOWFLAKE_DATABASE', 'GAMING'),
            schema=os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
            role=os.getenv('SNOWFLAKE_ROLE')
        )
        logger.info("Connected successfully with session token!")
        return conn
    except Exception as e:
        logger.error(f"Session token auth failed: {e}")
        return None

def connect_with_password_and_mfa_cache():
    """
    Connect using password with MFA token caching
    """
    try:
        # This will prompt for MFA code once and cache it
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
            database=os.getenv('SNOWFLAKE_DATABASE', 'GAMING'),
            schema=os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
            role=os.getenv('SNOWFLAKE_ROLE'),
            client_session_keep_alive=True  # Keeps session alive
        )
        logger.info("Connected successfully with password!")
        return conn
    except Exception as e:
        logger.error(f"Password auth failed: {e}")
        return None

def test_connection():
    """Test different authentication methods"""
    
    print("=" * 60)
    print("Testing Snowflake Authentication Methods")
    print("=" * 60)
    
    # Method 1: Try session token
    print("\n1. Trying session token authentication...")
    conn = connect_with_session_token()
    
    if conn:
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
        result = cursor.fetchone()
        print(f"✓ Success! User: {result[0]}, Role: {result[1]}, Warehouse: {result[2]}")
        
        # Test the query
        cursor.execute("USE WAREHOUSE COMPUTE_WH")
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM GAMING.PUBLIC.KOCHAVA_GAMINGAUDIENCES_TBL
            WHERE APP_NAME_PROPER = 'Hit it Rich! Free Casino Slots'
        """)
        count = cursor.fetchone()[0]
        print(f"✓ Found {count:,} MAIDs for 'Hit it Rich! Free Casino Slots'")
        
        cursor.close()
        conn.close()
        return True
    
    # Method 2: Try external browser
    print("\n2. Trying external browser authentication...")
    print("   (This will open your browser for authentication)")
    response = input("   Do you want to try browser authentication? (y/n): ")
    
    if response.lower() == 'y':
        conn = connect_with_external_browser()
        if conn:
            print("✓ Browser authentication successful!")
            conn.close()
            return True
    
    # Method 3: Try password with MFA
    print("\n3. Trying password authentication...")
    conn = connect_with_password_and_mfa_cache()
    if conn:
        print("✓ Password authentication successful!")
        conn.close()
        return True
    
    print("\n✗ All authentication methods failed")
    print("\nFor programmatic access with MFA, you need to:")
    print("1. Generate a key pair for authentication, OR")
    print("2. Use external browser authentication (semi-automated), OR")
    print("3. Create a service account without MFA")
    
    return False

if __name__ == "__main__":
    test_connection()
