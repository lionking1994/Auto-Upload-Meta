"""
Optimized Snowflake Connector for Audience MAIDs
Uses cursor-based fetching instead of OFFSET for massive performance improvement
"""
import snowflake.connector
import pandas as pd
import logging
from typing import List, Dict, Optional, Generator
import os
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


class OptimizedSnowflakeConnector:
    """Optimized connector for fetching millions of MAIDs efficiently"""
    
    def __init__(self):
        """Initialize with credentials from environment"""
        self.account = os.getenv('SNOWFLAKE_ACCOUNT')
        self.user = os.getenv('SNOWFLAKE_USER')
        self.warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
        self.database = os.getenv('SNOWFLAKE_DATABASE', 'GAMING')
        self.schema = os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
        self.role = os.getenv('SNOWFLAKE_ROLE')
        
        # Use PAT token if available
        self.pat_token = os.getenv('SNOWFLAKE_PAT_TOKEN')
        self.password = os.getenv('SNOWFLAKE_PASSWORD') if not self.pat_token else None
        
        self.connection = None
        self.cursor = None
        
        if self.pat_token:
            logger.info("Using Programmatic Access Token (PAT) authentication")
        else:
            logger.info("Using password authentication")
    
    def connect(self):
        """Establish connection to Snowflake"""
        try:
            logger.info("Connecting to Snowflake...")
            
            conn_params = {
                'user': self.user,
                'account': self.account,
                'warehouse': self.warehouse,
                'database': self.database,
                'schema': self.schema
            }
            
            if self.role:
                conn_params['role'] = self.role
            
            if self.pat_token:
                conn_params['token'] = self.pat_token
                conn_params['authenticator'] = 'oauth'
            else:
                conn_params['password'] = self.password
            
            self.connection = snowflake.connector.connect(**conn_params)
            self.cursor = self.connection.cursor()
            
            logger.info("Successfully connected to Snowflake")
            
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
    
    def get_maids_stream(self, app_name: str, batch_size: int = 50000) -> Generator[List[Dict], None, None]:
        """
        Stream MAIDs using efficient cursor-based fetching
        This is MUCH faster than OFFSET/LIMIT for large datasets
        
        Args:
            app_name: The app name to query for
            batch_size: Size of each batch to yield
            
        Yields:
            Batches of MAID dictionaries
        """
        if not self.connection:
            self.connect()
        
        try:
            # Use warehouse
            self.cursor.execute(f"USE WAREHOUSE {self.warehouse}")
            
            # Execute the query ONCE - no OFFSET needed!
            query = """
            SELECT DEVICE_ID_VALUE as MAID
            FROM GAMING.PUBLIC.KOCHAVA_GAMINGAUDIENCES_TBL
            WHERE APP_NAME_PROPER = %s
            """
            
            logger.info(f"Executing query for {app_name} (streaming results)...")
            self.cursor.execute(query, (app_name,))
            
            batch = []
            total_fetched = 0
            
            # Stream results efficiently
            while True:
                # Fetch batch_size rows at a time
                rows = self.cursor.fetchmany(batch_size)
                
                if not rows:
                    # Yield any remaining items in the last batch
                    if batch:
                        logger.info(f"Yielding final batch of {len(batch)} MAIDs")
                        yield batch
                    break
                
                # Process this batch
                batch = []
                for row in rows:
                    if row[0]:  # Check if MAID is not null
                        batch.append({'madid': row[0]})
                
                total_fetched += len(batch)
                logger.info(f"Fetched {total_fetched:,} MAIDs so far...")
                
                if batch:
                    yield batch
            
            logger.info(f"Completed fetching {total_fetched:,} MAIDs for {app_name}")
            
        except Exception as e:
            logger.error(f"Error streaming MAIDs for {app_name}: {str(e)}")
            raise
    
    def export_maids_to_csv(self, app_name: str, output_file: str):
        """
        Export MAIDs directly to CSV file - fastest method
        
        Args:
            app_name: The app name to query for
            output_file: Path to output CSV file
        """
        if not self.connection:
            self.connect()
        
        try:
            # Use warehouse
            self.cursor.execute(f"USE WAREHOUSE {self.warehouse}")
            
            logger.info(f"Exporting MAIDs for {app_name} to {output_file}...")
            
            # Use COPY INTO for maximum performance
            # This exports directly to a local file
            query = f"""
            COPY INTO 'file://{output_file}'
            FROM (
                SELECT DEVICE_ID_VALUE as MAID
                FROM GAMING.PUBLIC.KOCHAVA_GAMINGAUDIENCES_TBL
                WHERE APP_NAME_PROPER = '{app_name.replace("'", "''")}'
            )
            FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 0)
            SINGLE = TRUE
            OVERWRITE = TRUE
            """
            
            # For local file export, we need to use pandas
            query = """
            SELECT DEVICE_ID_VALUE as MAID
            FROM GAMING.PUBLIC.KOCHAVA_GAMINGAUDIENCES_TBL
            WHERE APP_NAME_PROPER = %s
            """
            
            # Fetch all results into a DataFrame
            logger.info("Fetching all MAIDs...")
            df = pd.read_sql(query, self.connection, params=(app_name,))
            
            # Save to CSV
            logger.info(f"Saving {len(df):,} MAIDs to {output_file}...")
            df.to_csv(output_file, index=False)
            
            logger.info(f"Successfully exported {len(df):,} MAIDs to {output_file}")
            return len(df)
            
        except Exception as e:
            logger.error(f"Error exporting MAIDs: {str(e)}")
            raise
    
    def get_all_maids_at_once(self, app_name: str) -> List[Dict]:
        """
        Fetch ALL MAIDs in one query - use with caution for large datasets
        This matches your Snowflake UI experience (40 seconds for 1.7M records)
        
        Args:
            app_name: The app name to query for
            
        Returns:
            List of all MAID dictionaries
        """
        if not self.connection:
            self.connect()
        
        try:
            # Use warehouse
            self.cursor.execute(f"USE WAREHOUSE {self.warehouse}")
            
            query = """
            SELECT DEVICE_ID_VALUE as MAID
            FROM GAMING.PUBLIC.KOCHAVA_GAMINGAUDIENCES_TBL
            WHERE APP_NAME_PROPER = %s
            """
            
            logger.info(f"Fetching ALL MAIDs for {app_name} in one query...")
            self.cursor.execute(query, (app_name,))
            
            # Fetch everything at once
            results = self.cursor.fetchall()
            
            # Convert to list of dictionaries
            maids = []
            for row in results:
                if row[0]:  # Check if MAID is not null
                    maids.append({'madid': row[0]})
            
            logger.info(f"Fetched {len(maids):,} MAIDs for {app_name}")
            return maids
            
        except Exception as e:
            logger.error(f"Error fetching MAIDs for {app_name}: {str(e)}")
            raise


# Example usage
if __name__ == "__main__":
    import time
    
    connector = OptimizedSnowflakeConnector()
    connector.connect()
    
    app_name = "Free Slots: Hot Vegas Slot Machines"
    
    # Method 1: Stream in batches (memory efficient)
    print("\n=== Method 1: Streaming Batches ===")
    start = time.time()
    total = 0
    for batch in connector.get_maids_stream(app_name, batch_size=100000):
        total += len(batch)
        print(f"Received batch of {len(batch):,} MAIDs (total: {total:,})")
    print(f"Time: {time.time() - start:.2f} seconds")
    
    # Method 2: Export to CSV (fastest for saving to disk)
    print("\n=== Method 2: Export to CSV ===")
    start = time.time()
    count = connector.export_maids_to_csv(app_name, f"{app_name.replace(':', '_')}_maids.csv")
    print(f"Exported {count:,} MAIDs in {time.time() - start:.2f} seconds")
    
    # Method 3: Get all at once (matches your UI experience)
    print("\n=== Method 3: Fetch All at Once ===")
    start = time.time()
    all_maids = connector.get_all_maids_at_once(app_name)
    print(f"Fetched {len(all_maids):,} MAIDs in {time.time() - start:.2f} seconds")
    
    connector.disconnect()

