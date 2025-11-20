"""
Data Processor for CSV audience data
"""
import pandas as pd
import logging
from typing import List, Dict, Tuple, Optional
from pathlib import Path
import re

logger = logging.getLogger(__name__)


class AudienceDataProcessor:
    """Process and prepare audience data from CSV for Meta upload"""
    
    def __init__(self, csv_file_path: str):
        """
        Initialize data processor
        
        Args:
            csv_file_path: Path to CSV file containing audience data
        """
        self.csv_file_path = Path(csv_file_path)
        if not self.csv_file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_file_path}")
        
        self.df = None
        self.processed_audiences = []
    
    def load_data(self) -> pd.DataFrame:
        """
        Load data from CSV file
        
        Returns:
            Loaded DataFrame
        """
        logger.info(f"Loading data from {self.csv_file_path}")
        self.df = pd.read_csv(self.csv_file_path)
        logger.info(f"Loaded {len(self.df)} rows from CSV")
        
        # Log column names for debugging
        logger.debug(f"Columns found: {list(self.df.columns)}")
        
        return self.df
    
    def get_top_apps(self, n: int = 100, sort_by: str = 'Device Count') -> pd.DataFrame:
        """
        Get top N apps sorted by specified column
        
        Args:
            n: Number of top apps to retrieve
            sort_by: Column to sort by
            
        Returns:
            DataFrame with top N apps
        """
        if self.df is None:
            self.load_data()
        
        # Ensure the sort column exists
        if sort_by not in self.df.columns:
            logger.warning(f"Column '{sort_by}' not found. Available columns: {list(self.df.columns)}")
            # Try to find a similar column
            for col in self.df.columns:
                if 'device' in col.lower() and 'count' in col.lower():
                    sort_by = col
                    logger.info(f"Using column '{sort_by}' for sorting")
                    break
        
        # Convert to numeric if needed
        if sort_by in self.df.columns:
            self.df[sort_by] = pd.to_numeric(self.df[sort_by], errors='coerce')
        
        # Sort and get top N
        top_apps = self.df.nlargest(n, sort_by)
        logger.info(f"Selected top {len(top_apps)} apps by {sort_by}")
        
        return top_apps
    
    def prepare_audience_data(self, app_row: pd.Series) -> Dict:
        """
        Prepare audience data for a single app
        
        Args:
            app_row: Row from DataFrame containing app data
            
        Returns:
            Dictionary with audience information
        """
        # Clean app name for use as audience name
        app_name = str(app_row.get('App Name', 'Unknown App'))
        clean_name = self._clean_audience_name(app_name)
        
        # Prepare audience metadata
        audience_data = {
            'name': clean_name,
            'original_name': app_name,
            'description': f"Custom audience for {app_name} - {app_row.get('OS', 'Unknown OS')} - {app_row.get('Category', 'Unknown Category')}",
            'category': app_row.get('Category', 'Unknown'),
            'os': app_row.get('OS', 'Unknown'),
            'device_count': int(app_row.get('Device Count', 0)),
            'table_name': app_row.get('Table Name', 'Unknown'),
            # This will be populated with actual user data from Snowflake in production
            'users': []
        }
        
        return audience_data
    
    def _clean_audience_name(self, name: str) -> str:
        """
        Clean and format audience name for Meta
        
        Args:
            name: Original app name
            
        Returns:
            Cleaned name suitable for Meta audience
        """
        # Remove special characters that might cause issues
        clean = re.sub(r'[^\w\s\-\.]', '', name)
        # Replace multiple spaces with single space
        clean = re.sub(r'\s+', ' ', clean)
        # Trim to Meta's character limit (typically 500 chars for audience names)
        clean = clean[:200].strip()
        
        # Add timestamp suffix to ensure uniqueness
        # from datetime import datetime
        # timestamp = datetime.now().strftime("%Y%m%d")
        # clean = f"{clean} - {timestamp}"
        
        return clean
    
    def process_all_audiences(self, top_n: int = 100) -> List[Dict]:
        """
        Process all audiences from the CSV
        
        Args:
            top_n: Number of top apps to process
            
        Returns:
            List of processed audience data
        """
        top_apps = self.get_top_apps(n=top_n)
        
        self.processed_audiences = []
        for idx, row in top_apps.iterrows():
            audience_data = self.prepare_audience_data(row)
            audience_data['index'] = idx
            self.processed_audiences.append(audience_data)
        
        logger.info(f"Processed {len(self.processed_audiences)} audiences")
        return self.processed_audiences
    
    def get_audience_summary(self) -> pd.DataFrame:
        """
        Get summary of processed audiences
        
        Returns:
            DataFrame with audience summary
        """
        if not self.processed_audiences:
            logger.warning("No audiences processed yet")
            return pd.DataFrame()
        
        summary_data = []
        for audience in self.processed_audiences:
            summary_data.append({
                'Audience Name': audience['name'],
                'Original App Name': audience['original_name'],
                'OS': audience['os'],
                'Category': audience['category'],
                'Device Count': audience['device_count']
            })
        
        return pd.DataFrame(summary_data)
    
    def export_audience_mapping(self, output_file: str = 'audience_mapping.csv'):
        """
        Export mapping of original app names to audience names
        
        Args:
            output_file: Path to output CSV file
        """
        if not self.processed_audiences:
            logger.warning("No audiences to export")
            return
        
        mapping_data = []
        for audience in self.processed_audiences:
            mapping_data.append({
                'Original_App_Name': audience['original_name'],
                'Meta_Audience_Name': audience['name'],
                'OS': audience['os'],
                'Category': audience['category'],
                'Device_Count': audience['device_count'],
                'Description': audience['description']
            })
        
        mapping_df = pd.DataFrame(mapping_data)
        mapping_df.to_csv(output_file, index=False)
        logger.info(f"Exported audience mapping to {output_file}")
    
    def generate_sample_user_data(self, audience: Dict, sample_size: int = 1000) -> List[Dict]:
        """
        Generate sample user data for testing
        NOTE: In production, this would fetch real data from Snowflake
        
        Args:
            audience: Audience dictionary
            sample_size: Number of sample users to generate
            
        Returns:
            List of user dictionaries
        """
        import random
        import string
        
        users = []
        for i in range(min(sample_size, audience.get('device_count', 100))):
            # Generate sample email
            random_string = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
            email = f"user_{random_string}@example.com"
            
            users.append({
                'email': email,
                # In production, you might have additional fields like:
                # 'phone': generate_phone(),
                # 'fn': first_name,
                # 'ln': last_name,
                # 'country': country_code,
                # 'state': state_code,
                # 'city': city_name,
                # 'zip': zip_code,
                # 'madid': mobile_advertiser_id,
                # 'extern_id': external_id
            })
        
        return users
    
    def get_audience_users_from_snowflake(self, app_name: str, snowflake_connector=None) -> List[Dict]:
        """
        Fetch actual MAIDs from Snowflake for a specific app
        
        Args:
            app_name: Original app name from CSV
            snowflake_connector: SnowflakeAudienceConnector instance
            
        Returns:
            List of user dictionaries with MAIDs
        """
        if not snowflake_connector:
            logger.warning("No Snowflake connector provided, returning empty list")
            return []
        
        try:
            # Fetch MAIDs from Snowflake
            users = snowflake_connector.get_audience_maids(app_name)
            logger.info(f"Fetched {len(users)} MAIDs for {app_name} from Snowflake")
            return users
        except Exception as e:
            logger.error(f"Error fetching MAIDs from Snowflake: {str(e)}")
            return []
