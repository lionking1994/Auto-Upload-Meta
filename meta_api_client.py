"""
Meta API Client for Custom Audience Management
"""
import requests
import json
import time
import hashlib
from typing import List, Dict, Optional, Any
from datetime import datetime
import logging
from ratelimit import limits, sleep_and_retry
from config import Config

logger = logging.getLogger(__name__)


class MetaAPIClient:
    """Client for interacting with Meta Marketing API"""
    
    def __init__(self, access_token: str = None, ad_account_id: str = None):
        """
        Initialize Meta API Client
        
        Args:
            access_token: Meta API access token
            ad_account_id: Meta Ad Account ID (format: act_XXXXX)
        """
        self.access_token = access_token or Config.META_ACCESS_TOKEN
        self.ad_account_id = ad_account_id or Config.META_AD_ACCOUNT_ID
        
        if not self.ad_account_id:
            raise ValueError("Ad Account ID is required. Please set META_AD_ACCOUNT_ID in your environment.")
        
        # Ensure ad_account_id has correct format
        if not self.ad_account_id.startswith('act_'):
            self.ad_account_id = f'act_{self.ad_account_id}'
        
        self.base_url = Config.META_BASE_URL
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json'
        })
        
        # Track API calls for rate limiting
        self.api_calls_made = 0
        self.last_reset_time = datetime.now()
    
    @sleep_and_retry
    @limits(calls=Config.API_CALLS_PER_HOUR, period=3600)
    def _make_request(self, method: str, endpoint: str, data: Dict = None, params: Dict = None) -> Dict:
        """
        Make API request with rate limiting and retry logic
        
        Args:
            method: HTTP method (GET, POST, DELETE, etc.)
            endpoint: API endpoint
            data: Request body data
            params: Query parameters
            
        Returns:
            API response as dictionary
        """
        url = f"{self.base_url}/{endpoint}"
        
        # Add access token to params
        if params is None:
            params = {}
        params['access_token'] = self.access_token
        
        # Retry logic
        for attempt in range(Config.MAX_RETRIES):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    json=data,
                    params=params
                )
                
                # Log the request
                logger.debug(f"API Request: {method} {url}")
                self.api_calls_made += 1
                
                # Check for rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', Config.RETRY_DELAY))
                    logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.error(f"API request failed (attempt {attempt + 1}/{Config.MAX_RETRIES}): {str(e)}")
                if attempt < Config.MAX_RETRIES - 1:
                    time.sleep(Config.RETRY_DELAY * (attempt + 1))
                else:
                    raise
    
    def create_custom_audience(self, name: str, description: str = "", subtype: str = "CUSTOM") -> Dict:
        """
        Create a new custom audience
        
        Args:
            name: Audience name
            description: Audience description
            subtype: Audience subtype (CUSTOM, LOOKALIKE, etc.)
            
        Returns:
            Created audience details
        """
        endpoint = f"{self.ad_account_id}/customaudiences"
        
        data = {
            "name": name,
            "description": description,
            "subtype": subtype,
            "customer_file_source": "USER_PROVIDED_ONLY"
        }
        
        logger.info(f"Creating custom audience: {name}")
        response = self._make_request("POST", endpoint, data=data)
        logger.info(f"Successfully created audience: {name} (ID: {response.get('id')})")
        
        return response
    
    def hash_user_data(self, data: str) -> str:
        """
        Hash user data according to Meta's requirements
        
        Args:
            data: Raw user data (email, phone, etc.)
            
        Returns:
            SHA256 hashed and normalized data
        """
        # Normalize: lowercase and strip whitespace
        normalized = data.lower().strip()
        # Hash with SHA256
        return hashlib.sha256(normalized.encode()).hexdigest()
    
    def add_users_to_audience(self, audience_id: str, users: List[Dict], 
                            schema: List[str] = None, is_hashed: bool = False) -> Dict:
        """
        Add users to an existing custom audience
        
        Args:
            audience_id: Custom audience ID
            users: List of user data dictionaries
            schema: Data schema (e.g., ['EMAIL', 'PHONE', 'FN', 'LN', 'MADID'])
            is_hashed: Whether the data is already hashed
            
        Returns:
            Upload session details
        """
        endpoint = f"{audience_id}/users"
        
        # Default schema if not provided
        if schema is None:
            # Check if users contain MAIDs
            if users and 'madid' in users[0]:
                schema = ['MADID']
            else:
                schema = ['EMAIL']
        
        # Process users in batches
        batch_size = Config.BATCH_SIZE
        total_users = len(users)
        uploaded = 0
        
        logger.info(f"Starting upload of {total_users} users to audience {audience_id}")
        
        for i in range(0, total_users, batch_size):
            batch = users[i:i + batch_size]
            
            # Prepare data array
            data_array = []
            for user in batch:
                user_data = []
                for field in schema:
                    value = user.get(field.lower(), '')
                    # MAIDs should NOT be hashed - they're already device identifiers
                    if value and not is_hashed and field != 'MADID':
                        value = self.hash_user_data(str(value))
                    user_data.append(value)
                data_array.append(user_data)
            
            # Create payload
            payload = {
                "payload": {
                    "schema": schema,
                    "data": data_array
                }
            }
            
            # For initial batch, create session
            if i == 0:
                payload["payload"]["is_raw"] = True if not is_hashed else False
            
            # Upload batch
            logger.info(f"Uploading batch {i//batch_size + 1}/{(total_users + batch_size - 1)//batch_size}")
            response = self._make_request("POST", endpoint, data=payload)
            
            uploaded += len(batch)
            logger.info(f"Uploaded {uploaded}/{total_users} users")
            
            # Small delay between batches to avoid overwhelming the API
            if i + batch_size < total_users:
                time.sleep(1)
        
        logger.info(f"Successfully uploaded {total_users} users to audience {audience_id}")
        return {"users_uploaded": total_users, "audience_id": audience_id}
    
    def get_audience_details(self, audience_id: str) -> Dict:
        """
        Get details of a custom audience
        
        Args:
            audience_id: Custom audience ID
            
        Returns:
            Audience details
        """
        endpoint = audience_id
        params = {
            "fields": "id,name,description,approximate_count,delivery_status,operation_status"
        }
        
        return self._make_request("GET", endpoint, params=params)
    
    def list_custom_audiences(self, limit: int = 100) -> List[Dict]:
        """
        List all custom audiences for the ad account
        
        Args:
            limit: Maximum number of audiences to retrieve
            
        Returns:
            List of audience details
        """
        endpoint = f"{self.ad_account_id}/customaudiences"
        params = {
            "fields": "id,name,description,approximate_count,delivery_status",
            "limit": limit
        }
        
        response = self._make_request("GET", endpoint, params=params)
        return response.get("data", [])
    
    def delete_audience(self, audience_id: str) -> bool:
        """
        Delete a custom audience
        
        Args:
            audience_id: Custom audience ID
            
        Returns:
            True if successful
        """
        endpoint = audience_id
        
        logger.info(f"Deleting audience {audience_id}")
        response = self._make_request("DELETE", endpoint)
        logger.info(f"Successfully deleted audience {audience_id}")
        
        return response.get("success", False)
    
    def get_api_usage_stats(self) -> Dict:
        """
        Get current API usage statistics
        
        Returns:
            Dictionary with API usage stats
        """
        elapsed = (datetime.now() - self.last_reset_time).total_seconds()
        
        return {
            "api_calls_made": self.api_calls_made,
            "time_elapsed_seconds": elapsed,
            "calls_per_hour_rate": (self.api_calls_made / elapsed) * 3600 if elapsed > 0 else 0,
            "limit_per_hour": Config.API_CALLS_PER_HOUR
        }
