#!/usr/bin/env python3
"""
Test the optimized workflow for fetching and uploading MAIDs
This demonstrates the performance improvement over the original implementation
"""
import time
import logging
import sys
from datetime import datetime
import snowflake.connector
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_optimized_fetch():
    """
    Test the optimized single-query streaming approach
    """
    try:
        # Connection parameters
        conn_params = {
            'user': os.getenv('SNOWFLAKE_USER'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
            'database': os.getenv('SNOWFLAKE_DATABASE', 'GAMING'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
            'role': os.getenv('SNOWFLAKE_ROLE')
        }
        
        # Use PAT token if available
        pat_token = os.getenv('SNOWFLAKE_PAT_TOKEN')
        if pat_token:
            conn_params['token'] = pat_token
            conn_params['authenticator'] = 'oauth'
            logger.info("Using PAT token authentication")
        else:
            conn_params['password'] = os.getenv('SNOWFLAKE_PASSWORD')
            logger.info("Using password authentication")
        
        logger.info("=" * 80)
        logger.info("OPTIMIZED WORKFLOW TEST")
        logger.info("=" * 80)
        
        # Connect to Snowflake
        logger.info("Connecting to Snowflake...")
        connection = snowflake.connector.connect(**conn_params)
        cursor = connection.cursor()
        logger.info("✓ Connected successfully")
        
        # Use warehouse
        cursor.execute("USE WAREHOUSE COMPUTE_WH")
        
        # Test with a smaller app first
        app_name = "Free Slots: Hot Vegas Slot Machines"
        logger.info(f"\nTesting with: {app_name}")
        logger.info("-" * 60)
        
        # Method 1: Original OFFSET approach (simulate for comparison)
        logger.info("\n📊 METHOD 1: Original OFFSET Approach (First 100K only)")
        start_time = time.time()
        
        # Simulate the old approach with just 2 batches to show the problem
        batch_size = 50000
        total_old = 0
        
        for offset in [0, 50000]:
            query = f"""
            SELECT DEVICE_ID_VALUE as MAID
            FROM GAMING.PUBLIC.KOCHAVA_GAMINGAUDIENCES_TBL
            WHERE APP_NAME_PROPER = %s
            ORDER BY DEVICE_ID_VALUE
            LIMIT {batch_size}
            OFFSET {offset}
            """
            batch_start = time.time()
            cursor.execute(query, (app_name,))
            results = cursor.fetchall()
            batch_time = time.time() - batch_start
            total_old += len(results)
            logger.info(f"  Batch {offset//batch_size + 1}: {len(results):,} MAIDs in {batch_time:.2f}s (OFFSET {offset:,})")
        
        old_time = time.time() - start_time
        logger.info(f"  Total: {total_old:,} MAIDs in {old_time:.2f} seconds")
        logger.info(f"  ⚠️ Note: Each OFFSET query gets progressively slower!")
        
        # Method 2: Optimized streaming approach
        logger.info("\n🚀 METHOD 2: Optimized Streaming (All MAIDs)")
        start_time = time.time()
        
        # Single query, no OFFSET!
        query = """
        SELECT DEVICE_ID_VALUE as MAID
        FROM GAMING.PUBLIC.KOCHAVA_GAMINGAUDIENCES_TBL
        WHERE APP_NAME_PROPER = %s
        """
        
        logger.info(f"  Executing single query (no OFFSET)...")
        query_start = time.time()
        cursor.execute(query, (app_name,))
        query_time = time.time() - query_start
        logger.info(f"  ✓ Query executed in {query_time:.2f} seconds")
        
        # Stream results in batches
        total_streamed = 0
        batch_count = 0
        stream_start = time.time()
        
        while True:
            batch = cursor.fetchmany(100000)  # Fetch 100K at a time
            if not batch:
                break
            batch_count += 1
            total_streamed += len(batch)
            if batch_count <= 5 or batch_count % 5 == 0:  # Log first 5 and every 5th
                logger.info(f"  Batch {batch_count}: Streamed {len(batch):,} MAIDs (total: {total_streamed:,})")
        
        stream_time = time.time() - stream_start
        total_time = time.time() - start_time
        
        logger.info(f"\n  ✓ Streamed {total_streamed:,} MAIDs")
        logger.info(f"  Query time: {query_time:.2f}s")
        logger.info(f"  Stream time: {stream_time:.2f}s")
        logger.info(f"  Total time: {total_time:.2f}s")
        
        # Method 3: Get count for comparison
        logger.info("\n📈 METHOD 3: Quick Count Check")
        count_start = time.time()
        cursor.execute("""
            SELECT COUNT(DISTINCT DEVICE_ID_VALUE) as count
            FROM GAMING.PUBLIC.KOCHAVA_GAMINGAUDIENCES_TBL
            WHERE APP_NAME_PROPER = %s
        """, (app_name,))
        count_result = cursor.fetchone()
        count_time = time.time() - count_start
        total_count = count_result[0] if count_result else 0
        
        logger.info(f"  Total MAIDs in database: {total_count:,}")
        logger.info(f"  Count query time: {count_time:.2f}s")
        
        # Performance comparison
        logger.info("\n" + "=" * 80)
        logger.info("PERFORMANCE COMPARISON")
        logger.info("=" * 80)
        
        if total_count > 0:
            # Extrapolate old method time for full dataset
            old_estimated_time = (old_time / 100000) * total_count
            speedup = old_estimated_time / total_time if total_time > 0 else 0
            
            logger.info(f"Dataset: {app_name}")
            logger.info(f"Total MAIDs: {total_count:,}")
            logger.info(f"\nOptimized Method:")
            logger.info(f"  - Time: {total_time:.2f} seconds")
            logger.info(f"  - Speed: {total_streamed/total_time:.0f} MAIDs/second")
            logger.info(f"\nOld Method (estimated for full dataset):")
            logger.info(f"  - Estimated time: {old_estimated_time:.0f} seconds ({old_estimated_time/60:.1f} minutes)")
            logger.info(f"  - Speed degradation with OFFSET")
            logger.info(f"\n🎯 Speedup: {speedup:.1f}x faster!")
        
        # Test with Hit it Rich (40M MAIDs) - just get count
        logger.info("\n" + "=" * 80)
        logger.info("LARGE DATASET PROJECTION")
        logger.info("=" * 80)
        
        large_app = "Hit it Rich! Free Casino Slots"
        cursor.execute("""
            SELECT COUNT(DISTINCT DEVICE_ID_VALUE) as count
            FROM GAMING.PUBLIC.KOCHAVA_GAMINGAUDIENCES_TBL
            WHERE APP_NAME_PROPER = %s
        """, (large_app,))
        large_count = cursor.fetchone()[0]
        
        if large_count > 0 and total_time > 0:
            # Project time for large dataset
            projected_time = (total_time / total_count) * large_count
            old_projected = projected_time * speedup if speedup > 0 else projected_time * 10
            
            logger.info(f"App: {large_app}")
            logger.info(f"MAIDs: {large_count:,}")
            logger.info(f"\nProjected times:")
            logger.info(f"  Optimized method: {projected_time:.0f}s ({projected_time/60:.1f} minutes)")
            logger.info(f"  Old method: {old_projected:.0f}s ({old_projected/60:.1f} minutes / {old_projected/3600:.1f} hours)")
            logger.info(f"  Speedup: {(old_projected/projected_time):.1f}x faster!")
        
        # Close connection
        cursor.close()
        connection.close()
        logger.info("\n✓ Test completed successfully!")
        
        return True
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        return False


if __name__ == "__main__":
    success = test_optimized_fetch()
    sys.exit(0 if success else 1)

