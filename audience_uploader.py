"""
Audience Uploader with batch processing and rate limiting
"""
import time
import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
from tqdm import tqdm
import json
from pathlib import Path

from meta_api_client import MetaAPIClient
from data_processor import AudienceDataProcessor
from config import Config

logger = logging.getLogger(__name__)


class AudienceUploadManager:
    """Manages the upload of audiences to Meta with rate limiting and error handling"""
    
    def __init__(self, meta_client: MetaAPIClient = None, data_processor: AudienceDataProcessor = None):
        """
        Initialize upload manager
        
        Args:
            meta_client: Meta API client instance
            data_processor: Data processor instance
        """
        self.meta_client = meta_client or MetaAPIClient()
        self.data_processor = data_processor
        
        # Track upload statistics
        self.upload_stats = {
            'total_audiences': 0,
            'successful_uploads': 0,
            'failed_uploads': 0,
            'total_users_uploaded': 0,
            'start_time': None,
            'end_time': None,
            'errors': []
        }
        
        # Track created audiences for rollback if needed
        self.created_audiences = []
        
        # Progress tracking
        self.progress_file = Path('upload_progress.json')
        self.completed_audiences = self.load_progress()
    
    def load_progress(self) -> set:
        """
        Load progress from file to resume interrupted uploads
        
        Returns:
            Set of completed audience names
        """
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    data = json.load(f)
                    return set(data.get('completed', []))
            except Exception as e:
                logger.warning(f"Could not load progress file: {e}")
        return set()
    
    def save_progress(self):
        """Save current progress to file"""
        try:
            with open(self.progress_file, 'w') as f:
                json.dump({
                    'completed': list(self.completed_audiences),
                    'stats': self.upload_stats,
                    'created_audiences': self.created_audiences
                }, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save progress: {e}")
    
    def upload_single_audience(self, audience_data: Dict, with_users: bool = True) -> Tuple[bool, Optional[str]]:
        """
        Upload a single audience to Meta
        
        Args:
            audience_data: Audience information dictionary
            with_users: Whether to upload users (False for testing audience creation only)
            
        Returns:
            Tuple of (success, audience_id)
        """
        audience_name = audience_data['name']
        
        # Check if already completed
        if audience_name in self.completed_audiences:
            logger.info(f"Skipping {audience_name} - already uploaded")
            return True, None
        
        try:
            # Step 1: Create the custom audience
            logger.info(f"Creating audience: {audience_name}")
            create_response = self.meta_client.create_custom_audience(
                name=audience_name,
                description=audience_data.get('description', '')
            )
            
            audience_id = create_response.get('id')
            if not audience_id:
                raise ValueError("No audience ID returned from Meta")
            
            logger.info(f"Created audience with ID: {audience_id}")
            
            # Track created audience
            self.created_audiences.append({
                'id': audience_id,
                'name': audience_name,
                'created_at': datetime.now().isoformat()
            })
            
            # Step 2: Upload users if requested
            if with_users:
                users = audience_data.get('users', [])
                
                # If no users provided, generate sample data for testing
                if not users and self.data_processor:
                    logger.info(f"Generating sample user data for {audience_name}")
                    users = self.data_processor.generate_sample_user_data(
                        audience_data, 
                        sample_size=min(1000, audience_data.get('device_count', 100))
                    )
                
                if users:
                    logger.info(f"Uploading {len(users)} users to {audience_name}")
                    upload_response = self.meta_client.add_users_to_audience(
                        audience_id=audience_id,
                        users=users,
                        schema=['EMAIL']  # Adjust based on your data
                    )
                    logger.info(f"Successfully uploaded users to {audience_name}")
                else:
                    logger.warning(f"No users to upload for {audience_name}")
            
            # Mark as completed
            self.completed_audiences.add(audience_name)
            self.save_progress()
            
            return True, audience_id
            
        except Exception as e:
            logger.error(f"Failed to upload audience {audience_name}: {str(e)}")
            self.upload_stats['errors'].append({
                'audience': audience_name,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
            return False, None
    
    def upload_audiences_batch(self, audiences: List[Dict], 
                             batch_size: int = 10,
                             delay_between_batches: int = 5,
                             with_users: bool = True) -> Dict:
        """
        Upload multiple audiences in batches with rate limiting
        
        Args:
            audiences: List of audience data dictionaries
            batch_size: Number of audiences to upload per batch
            delay_between_batches: Seconds to wait between batches
            with_users: Whether to upload users
            
        Returns:
            Upload statistics
        """
        self.upload_stats['total_audiences'] = len(audiences)
        self.upload_stats['start_time'] = datetime.now()
        
        logger.info(f"Starting upload of {len(audiences)} audiences in batches of {batch_size}")
        
        # Filter out already completed audiences
        remaining_audiences = [a for a in audiences if a['name'] not in self.completed_audiences]
        logger.info(f"{len(remaining_audiences)} audiences remaining after checking progress")
        
        # Process in batches
        with tqdm(total=len(remaining_audiences), desc="Uploading audiences") as pbar:
            for i in range(0, len(remaining_audiences), batch_size):
                batch = remaining_audiences[i:i + batch_size]
                batch_num = i // batch_size + 1
                total_batches = (len(remaining_audiences) + batch_size - 1) // batch_size
                
                logger.info(f"Processing batch {batch_num}/{total_batches}")
                
                for audience in batch:
                    success, audience_id = self.upload_single_audience(audience, with_users=with_users)
                    
                    if success:
                        self.upload_stats['successful_uploads'] += 1
                        if with_users and 'users' in audience:
                            self.upload_stats['total_users_uploaded'] += len(audience.get('users', []))
                    else:
                        self.upload_stats['failed_uploads'] += 1
                    
                    pbar.update(1)
                    
                    # Check API usage and potentially slow down
                    api_stats = self.meta_client.get_api_usage_stats()
                    if api_stats['calls_per_hour_rate'] > Config.API_CALLS_PER_HOUR * 0.8:
                        wait_time = 30
                        logger.warning(f"Approaching rate limit, waiting {wait_time} seconds")
                        time.sleep(wait_time)
                
                # Delay between batches
                if i + batch_size < len(remaining_audiences):
                    logger.info(f"Waiting {delay_between_batches} seconds before next batch...")
                    time.sleep(delay_between_batches)
        
        self.upload_stats['end_time'] = datetime.now()
        self.save_progress()
        
        return self.upload_stats
    
    def verify_uploads(self, audience_ids: List[str] = None) -> List[Dict]:
        """
        Verify that audiences were created successfully
        
        Args:
            audience_ids: List of audience IDs to verify (or use created_audiences)
            
        Returns:
            List of audience details
        """
        if audience_ids is None:
            audience_ids = [a['id'] for a in self.created_audiences]
        
        verified_audiences = []
        for audience_id in audience_ids:
            try:
                details = self.meta_client.get_audience_details(audience_id)
                verified_audiences.append(details)
                logger.info(f"Verified audience {details.get('name')} - Count: {details.get('approximate_count', 0)}")
            except Exception as e:
                logger.error(f"Could not verify audience {audience_id}: {e}")
        
        return verified_audiences
    
    def rollback_uploads(self, audience_ids: List[str] = None) -> int:
        """
        Delete uploaded audiences (useful for testing or error recovery)
        
        Args:
            audience_ids: List of audience IDs to delete (or use created_audiences)
            
        Returns:
            Number of audiences deleted
        """
        if audience_ids is None:
            audience_ids = [a['id'] for a in self.created_audiences]
        
        deleted = 0
        for audience_id in audience_ids:
            try:
                if self.meta_client.delete_audience(audience_id):
                    deleted += 1
                    logger.info(f"Deleted audience {audience_id}")
            except Exception as e:
                logger.error(f"Could not delete audience {audience_id}: {e}")
        
        return deleted
    
    def generate_report(self, output_file: str = 'upload_report.json') -> Dict:
        """
        Generate detailed upload report
        
        Args:
            output_file: Path to save report
            
        Returns:
            Report dictionary
        """
        duration = None
        if self.upload_stats['start_time'] and self.upload_stats['end_time']:
            duration = (self.upload_stats['end_time'] - self.upload_stats['start_time']).total_seconds()
        
        report = {
            'summary': {
                'total_audiences': self.upload_stats['total_audiences'],
                'successful_uploads': self.upload_stats['successful_uploads'],
                'failed_uploads': self.upload_stats['failed_uploads'],
                'success_rate': (self.upload_stats['successful_uploads'] / self.upload_stats['total_audiences'] * 100) 
                               if self.upload_stats['total_audiences'] > 0 else 0,
                'total_users_uploaded': self.upload_stats['total_users_uploaded'],
                'duration_seconds': duration,
                'start_time': self.upload_stats['start_time'].isoformat() if self.upload_stats['start_time'] else None,
                'end_time': self.upload_stats['end_time'].isoformat() if self.upload_stats['end_time'] else None
            },
            'created_audiences': self.created_audiences,
            'errors': self.upload_stats['errors'],
            'api_usage': self.meta_client.get_api_usage_stats()
        }
        
        # Save report
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"Report saved to {output_file}")
        
        return report
    
    def cleanup_progress(self):
        """Clean up progress tracking file"""
        if self.progress_file.exists():
            self.progress_file.unlink()
            logger.info("Cleaned up progress file")
