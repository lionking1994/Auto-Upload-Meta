"""
Snowflake connector for fetching audience data (MAIDs)
"""
import snowflake.connector
import pandas as pd
import logging
from typing import List, Dict, Optional
import os
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


class SnowflakeAudienceConnector:
    """Connect to Snowflake and fetch audience data (MAIDs) for each app"""
    
    def __init__(self):
        """Initialize Snowflake connection parameters"""
        # Check authentication method
        pat_token = os.getenv('SNOWFLAKE_PAT_TOKEN')
        password = os.getenv('SNOWFLAKE_PASSWORD')
        
        if pat_token:
            # Use Programmatic Access Token (PAT) - bypasses MFA
            self.connection_params = {
                'user': os.getenv('SNOWFLAKE_USER'),
                'password': pat_token,  # PAT goes in password field
                'account': os.getenv('SNOWFLAKE_ACCOUNT'),
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
                'database': os.getenv('SNOWFLAKE_DATABASE', 'GAMING'),
                'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
            }
            
            # Add role if specified
            role = os.getenv('SNOWFLAKE_ROLE')
            if role:
                self.connection_params['role'] = role
                
            logger.info("Using Programmatic Access Token (PAT) authentication")
        elif password:
            # Use regular password authentication
            self.connection_params = {
                'account': os.getenv('SNOWFLAKE_ACCOUNT'),
                'user': os.getenv('SNOWFLAKE_USER'),
                'password': password,
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
                'database': os.getenv('SNOWFLAKE_DATABASE', 'GAMING'),
                'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
            }
            
            # Add role if specified
            role = os.getenv('SNOWFLAKE_ROLE')
            if role:
                self.connection_params['role'] = role
                
            logger.info("Using password authentication")
        else:
            raise ValueError("No authentication method configured. Set either SNOWFLAKE_PASSWORD or SNOWFLAKE_PAT_TOKEN")
        
        self.connection = None
        self.cursor = None
    
    def connect(self):
        """Establish connection to Snowflake"""
        try:
            logger.info("Connecting to Snowflake...")
            self.connection = snowflake.connector.connect(**self.connection_params)
            self.cursor = self.connection.cursor()
            logger.info("Successfully connected to Snowflake")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            raise
    
    def disconnect(self):
        """Close Snowflake connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Disconnected from Snowflake")
    
    def get_audience_maids(self, app_name: str, limit: Optional[int] = None) -> List[Dict]:
        """
        Fetch MAIDs (Mobile Advertising IDs) for a specific app
        
        Args:
            app_name: The app name to query for
            limit: Optional limit on number of MAIDs to fetch
            
        Returns:
            List of dictionaries containing MAIDs
        """
        if not self.connection:
            self.connect()
        
        try:
            # Use the warehouse
            self.cursor.execute("USE WAREHOUSE COMPUTE_WH")
            
            # Build the query - exact format as provided
            query = """
            SELECT DEVICE_ID_VALUE as MAID
            FROM GAMING.PUBLIC.KOCHAVA_GAMINGAUDIENCES_TBL
            WHERE APP_NAME_PROPER = %s
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            logger.info(f"Fetching MAIDs for app: {app_name}")
            
            # Execute query
            self.cursor.execute(query, (app_name,))
            
            # Fetch results
            results = self.cursor.fetchall()
            
            # Convert to list of dictionaries for Meta API
            maids = []
            for row in results:
                if row[0]:  # Check if MAID is not null
                    maids.append({
                        'madid': row[0]  # Meta's field name for mobile advertising ID
                    })
            
            logger.info(f"Fetched {len(maids)} MAIDs for {app_name}")
            return maids
            
        except Exception as e:
            logger.error(f"Error fetching MAIDs for {app_name}: {str(e)}")
            raise
    
    def get_audience_count(self, app_name: str) -> int:
        """
        Get count of MAIDs for a specific app
        
        Args:
            app_name: The app name to query for
            
        Returns:
            Count of MAIDs
        """
        if not self.connection:
            self.connect()
        
        try:
            # Use the warehouse
            self.cursor.execute("USE WAREHOUSE COMPUTE_WH")
            
            query = """
            SELECT COUNT(DISTINCT DEVICE_ID_VALUE) as count
            FROM GAMING.PUBLIC.KOCHAVA_GAMINGAUDIENCES_TBL
            WHERE APP_NAME_PROPER = %s
            """
            
            self.cursor.execute(query, (app_name,))
            result = self.cursor.fetchone()
            
            count = result[0] if result else 0
            logger.info(f"App '{app_name}' has {count:,} MAIDs")
            return count
            
        except Exception as e:
            logger.error(f"Error getting count for {app_name}: {str(e)}")
            return 0
    
    def get_batch_audience_maids(self, app_name: str, batch_size: int = 50000) -> List[List[Dict]]:
        """
        Fetch MAIDs in batches for large audiences
        
        Args:
            app_name: The app name to query for
            batch_size: Size of each batch (Meta's limit is typically 10k-100k)
            
        Yields:
            Batches of MAID dictionaries
        """
        if not self.connection:
            self.connect()
        
        try:
            # Use the warehouse
            self.cursor.execute("USE WAREHOUSE COMPUTE_WH")
            
            # First get total count
            total_count = self.get_audience_count(app_name)
            
            if total_count == 0:
                logger.warning(f"No MAIDs found for {app_name}")
                return []
            
            logger.info(f"Fetching {total_count:,} MAIDs for {app_name} in batches of {batch_size:,}")
            
            batches = []
            offset = 0
            
            while offset < total_count:
                # Use warehouse for each batch query
                self.cursor.execute("USE WAREHOUSE COMPUTE_WH")
                
                query = f"""
                SELECT DEVICE_ID_VALUE as MAID
                FROM GAMING.PUBLIC.KOCHAVA_GAMINGAUDIENCES_TBL
                WHERE APP_NAME_PROPER = %s
                ORDER BY DEVICE_ID_VALUE
                LIMIT {batch_size}
                OFFSET {offset}
                """
                
                self.cursor.execute(query, (app_name,))
                results = self.cursor.fetchall()
                
                if not results:
                    break
                
                # Convert batch to list of dictionaries
                batch = []
                for row in results:
                    if row[0]:  # Check if MAID is not null
                        batch.append({
                            'madid': row[0]
                        })
                
                if batch:
                    batches.append(batch)
                    logger.info(f"Fetched batch {len(batches)} ({len(batch)} MAIDs) for {app_name}")
                
                offset += batch_size
            
            return batches
            
        except Exception as e:
            logger.error(f"Error fetching batches for {app_name}: {str(e)}")
            raise
    
    def test_connection(self) -> bool:
        """Test Snowflake connection and permissions"""
        try:
            self.connect()
            
            # Test query
            self.cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE()")
            result = self.cursor.fetchone()
            
            logger.info(f"Connected as user: {result[0]}, role: {result[1]}, database: {result[2]}")
            
            # Test access to the gaming audiences table
            self.cursor.execute("""
                SELECT COUNT(*) 
                FROM GAMING.PUBLIC.KOCHAVA_GAMINGAUDIENCES_TBL 
                LIMIT 1
            """)
            
            logger.info("Successfully accessed KOCHAVA_GAMINGAUDIENCES_TBL")
            return True
            
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False
        finally:
            self.disconnect()
    
    def get_all_app_names(self) -> List[str]:
        """Get list of all unique app names in the table"""
        if not self.connection:
            self.connect()
        
        try:
            query = """
            SELECT DISTINCT APP_NAME_PROPER
            FROM GAMING.PUBLIC.KOCHAVA_GAMINGAUDIENCES_TBL
            WHERE APP_NAME_PROPER IS NOT NULL
            ORDER BY APP_NAME_PROPER
            """
            
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            
            app_names = [row[0] for row in results if row[0]]
            logger.info(f"Found {len(app_names)} unique apps in Snowflake")
            
            return app_names
            
        except Exception as e:
            logger.error(f"Error fetching app names: {str(e)}")
            return []
