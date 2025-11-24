"""
Optimized Meta API Client with File-based Upload Support
"""
import requests
import json
import time
import hashlib
import gzip
import io
from typing import List, Dict, Optional, Any
from datetime import datetime
import logging
from ratelimit import limits, sleep_and_retry
from config import Config

logger = logging.getLogger(__name__)


class OptimizedMetaAPIClient:
    """Optimized client for Meta Marketing API with file-based upload support"""
    
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
        
        self.base_url = Config.META_API_BASE_URL
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json'
        })
        
        # Track API calls for rate limiting
        self.api_calls_made = 0
        self.last_reset_time = datetime.now()
    
    @sleep_and_retry
    @limits(calls=Config.API_CALLS_PER_HOUR, period=3600)
    def _make_request(self, method: str, endpoint: str, data: Any = None, 
                     params: Dict = None, files: Dict = None, json_data: Dict = None) -> Dict:
        """
        Make API request with rate limiting and retry logic
        
        Args:
            method: HTTP method (GET, POST, DELETE, etc.)
            endpoint: API endpoint
            data: Form data or raw data
            params: Query parameters
            files: Files to upload
            json_data: JSON data (use this instead of data for JSON payloads)
            
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
                # Prepare request kwargs
                request_kwargs = {
                    'method': method,
                    'url': url,
                    'params': params
                }
                
                if json_data is not None:
                    request_kwargs['json'] = json_data
                elif data is not None:
                    request_kwargs['data'] = data
                    
                if files is not None:
                    request_kwargs['files'] = files
                    # Don't set Content-Type for multipart/form-data
                    if 'Content-Type' in self.session.headers:
                        del self.session.headers['Content-Type']
                
                response = self.session.request(**request_kwargs)
                
                # Restore Content-Type header if it was removed
                if files is not None and 'Content-Type' not in self.session.headers:
                    self.session.headers['Content-Type'] = 'application/json'
                
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
        response = self._make_request("POST", endpoint, json_data=data)
        logger.info(f"Successfully created audience: {name} (ID: {response.get('id')})")
        
        return response
    
    def upload_maids_via_file(self, audience_id: str, maids: List[Dict], 
                              compress: bool = True, batch_size: int = 10_000_000) -> Dict:
        """
        Upload MAIDs using file-based approach (like Meta UI)
        
        Args:
            audience_id: Custom audience ID
            maids: List of MAID dictionaries [{'madid': 'xxx'}, ...]
            compress: Whether to compress the file with gzip
            batch_size: Maximum MAIDs per file (Meta limit is ~50M per file)
            
        Returns:
            Upload result
        """
        total_maids = len(maids)
        logger.info(f"Starting file-based upload of {total_maids:,} MAIDs to audience {audience_id}")
        
        # Process in batches if necessary (Meta has file size limits)
        uploaded_count = 0
        session_ids = []
        
        for batch_start in range(0, total_maids, batch_size):
            batch_end = min(batch_start + batch_size, total_maids)
            batch = maids[batch_start:batch_end]
            batch_num = (batch_start // batch_size) + 1
            total_batches = (total_maids + batch_size - 1) // batch_size
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch):,} MAIDs)")
            
            # Step 1: Create CSV content
            csv_lines = ["MADID"]  # Header
            csv_lines.extend([maid.get('madid', '') for maid in batch if maid.get('madid')])
            csv_content = "\n".join(csv_lines)
            
            # Compress if requested
            if compress:
                logger.info(f"Compressing CSV data...")
                original_size = len(csv_content.encode())
                
                # Create gzip buffer
                gz_buffer = io.BytesIO()
                with gzip.GzipFile(fileobj=gz_buffer, mode='wb') as gz_file:
                    gz_file.write(csv_content.encode())
                
                file_data = gz_buffer.getvalue()
                compressed_size = len(file_data)
                logger.info(f"Compressed {original_size:,} bytes to {compressed_size:,} bytes "
                          f"({100 * (1 - compressed_size/original_size):.1f}% reduction)")
            else:
                file_data = csv_content.encode()
            
            # Step 2: Create upload session
            logger.info(f"Creating upload session...")
            session_endpoint = f"{audience_id}/sessions"
            session_data = {
                "payload": {
                    "schema": ["MADID"],
                    "is_raw": True,
                    "data_source": {
                        "type": "FILE_UPLOAD"
                    }
                }
            }
            
            session_response = self._make_request("POST", session_endpoint, json_data=session_data)
            session_id = session_response.get('id')
            session_ids.append(session_id)
            logger.info(f"Created session: {session_id}")
            
            # Step 3: Upload file in chunks (Meta recommends chunks for large files)
            chunk_size = 5 * 1024 * 1024  # 5MB chunks
            total_chunks = (len(file_data) + chunk_size - 1) // chunk_size
            
            logger.info(f"Uploading file ({len(file_data):,} bytes) in {total_chunks} chunks...")
            
            for chunk_num in range(total_chunks):
                chunk_start = chunk_num * chunk_size
                chunk_end = min(chunk_start + chunk_size, len(file_data))
                chunk_data = file_data[chunk_start:chunk_end]
                
                upload_endpoint = f"{audience_id}/usersofacustomaudience"
                
                # Prepare multipart form data
                form_data = {
                    'session': json.dumps({"session_id": session_id}),
                    'payload': (
                        f'batch_{batch_num}_chunk_{chunk_num + 1}.{"csv.gz" if compress else "csv"}',
                        chunk_data,
                        'application/octet-stream' if compress else 'text/csv'
                    )
                }
                
                # Upload chunk
                logger.info(f"  Uploading chunk {chunk_num + 1}/{total_chunks} "
                          f"({len(chunk_data):,} bytes)")
                
                upload_response = self._make_request(
                    "POST", 
                    upload_endpoint,
                    files=form_data
                )
                
                if upload_response.get('success'):
                    logger.info(f"  ✓ Chunk {chunk_num + 1} uploaded successfully")
                else:
                    logger.warning(f"  ⚠ Chunk {chunk_num + 1} upload may have issues")
            
            # Step 4: Close session to trigger processing
            logger.info(f"Closing session to trigger processing...")
            close_endpoint = f"{audience_id}/sessions"
            close_data = {
                "session": {"session_id": session_id},
                "last_batch_flag": batch_end >= total_maids
            }
            
            close_response = self._make_request("POST", close_endpoint, json_data=close_data)
            
            if close_response.get('success'):
                uploaded_count += len(batch)
                logger.info(f"✓ Batch {batch_num} uploaded successfully ({uploaded_count:,}/{total_maids:,} total)")
            else:
                logger.warning(f"⚠ Batch {batch_num} close session response: {close_response}")
        
        logger.info(f"✓ File-based upload complete: {uploaded_count:,} MAIDs uploaded in {len(session_ids)} session(s)")
        
        return {
            "users_uploaded": uploaded_count,
            "audience_id": audience_id,
            "session_ids": session_ids,
            "method": "file_upload"
        }
    
    def add_users_to_audience_batch(self, audience_id: str, users: List[Dict], 
                                   schema: List[str] = None, is_hashed: bool = False,
                                   optimized_batch_size: int = 500_000) -> Dict:
        """
        Optimized batch upload with larger batch sizes and no delays
        
        Args:
            audience_id: Custom audience ID
            users: List of user data dictionaries
            schema: Data schema (e.g., ['EMAIL', 'PHONE', 'FN', 'LN', 'MADID'])
            is_hashed: Whether the data is already hashed
            optimized_batch_size: Larger batch size for better performance
            
        Returns:
            Upload session details
        """
        endpoint = f"{audience_id}/users"
        
        # Default schema if not provided
        if schema is None:
            if users and 'madid' in users[0]:
                schema = ['MADID']
            else:
                schema = ['EMAIL']
        
        # Use larger batch size for better performance
        batch_size = optimized_batch_size
        total_users = len(users)
        uploaded = 0
        
        logger.info(f"Starting optimized batch upload of {total_users:,} users to audience {audience_id}")
        logger.info(f"Using batch size: {batch_size:,} (vs old: 50,000)")
        
        num_batches = (total_users + batch_size - 1) // batch_size
        logger.info(f"Will make {num_batches} API calls (vs {(total_users + 50000 - 1) // 50000} with old batch size)")
        
        for i in range(0, total_users, batch_size):
            batch = users[i:i + batch_size]
            
            # Prepare data array
            data_array = []
            for user in batch:
                user_data = []
                for field in schema:
                    value = user.get(field.lower(), '')
                    # MAIDs should NOT be hashed
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
            
            # Upload batch (NO DELAY!)
            logger.info(f"Uploading batch {i//batch_size + 1}/{num_batches} ({len(batch):,} records)...")
            response = self._make_request("POST", endpoint, json_data=payload)
            
            uploaded += len(batch)
            logger.info(f"Uploaded {uploaded:,}/{total_users:,} users")
        
        logger.info(f"✓ Successfully uploaded {total_users:,} users to audience {audience_id}")
        return {"users_uploaded": total_users, "audience_id": audience_id, "method": "batch_optimized"}
    
    def hash_user_data(self, data: str) -> str:
        """Hash user data according to Meta's requirements"""
        normalized = data.lower().strip()
        return hashlib.sha256(normalized.encode()).hexdigest()
    
    def get_audience_details(self, audience_id: str) -> Dict:
        """Get details of a custom audience"""
        endpoint = audience_id
        params = {
            "fields": "id,name,description,delivery_status,operation_status"
        }
        return self._make_request("GET", endpoint, params=params)
    
    def list_custom_audiences(self, limit: int = 100) -> List[Dict]:
        """List all custom audiences for the ad account"""
        endpoint = f"{self.ad_account_id}/customaudiences"
        params = {
            "fields": "id,name,description,time_created,time_updated",
            "limit": limit
        }
        response = self._make_request("GET", endpoint, params=params)
        return response.get("data", [])
    
    def delete_audience(self, audience_id: str) -> bool:
        """Delete a custom audience"""
        endpoint = audience_id
        logger.info(f"Deleting audience {audience_id}")
        response = self._make_request("DELETE", endpoint)
        logger.info(f"Successfully deleted audience {audience_id}")
        return response.get("success", False)

