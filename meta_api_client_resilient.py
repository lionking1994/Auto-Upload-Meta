"""
Resilient Meta API Client with timeout handling and connection management
"""

import os
import json
import time
import requests
import logging
from typing import List, Dict, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
import signal
from contextlib import contextmanager

load_dotenv()
logger = logging.getLogger(__name__)


class TimeoutException(Exception):
    """Custom timeout exception"""
    pass


@contextmanager
def timeout(seconds):
    """Context manager for timeout"""
    def timeout_handler(signum, frame):
        raise TimeoutException(f"Operation timed out after {seconds} seconds")
    
    # Set the signal handler and alarm
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)  # Disable alarm


class ResilientMetaAPIClient:
    """Meta API client with resilience features"""
    
    def __init__(self):
        self.access_token = os.getenv("META_ACCESS_TOKEN")
        self.ad_account_id = os.getenv("META_AD_ACCOUNT_ID")
        self.api_version = "v21.0"
        self.base_url = f"https://graph.facebook.com/{self.api_version}"
        
        if not self.access_token or not self.ad_account_id:
            raise ValueError("Meta credentials not found in environment variables")
        
        # Create session with retry strategy
        self.session = self._create_session()
        self.request_count = 0
        self.last_session_refresh = time.time()
    
    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry strategy"""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[408, 429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"]
        )
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=10
        )
        
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set default headers
        session.headers.update({
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        })
        
        return session
    
    def _refresh_session_if_needed(self):
        """Refresh session every 30 minutes to avoid connection issues"""
        if time.time() - self.last_session_refresh > 1800:  # 30 minutes
            logger.info("Refreshing HTTP session...")
            self.session.close()
            self.session = self._create_session()
            self.last_session_refresh = time.time()
            self.request_count = 0
    
    def health_check(self) -> bool:
        """Check if Meta API is accessible"""
        try:
            url = f"{self.base_url}/{self.ad_account_id}"
            params = {"fields": "id,name"}
            response = self.session.get(url, params=params, timeout=10)
            if response.status_code == 200:
                return True
            else:
                logger.error(f"Health check returned status {response.status_code}: {response.text}")
                return False
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
    
    def create_custom_audience(
        self,
        name: str,
        description: str = "",
        timeout_seconds: int = 30
    ) -> Dict:
        """Create custom audience with timeout"""
        try:
            with timeout(timeout_seconds):
                self._refresh_session_if_needed()
                
                url = f"{self.base_url}/{self.ad_account_id}/customaudiences"
                payload = {
                    "name": name,
                    "description": description,
                    "subtype": "CUSTOM",
                    "customer_file_source": "USER_PROVIDED_ONLY"
                }
                
                response = self.session.post(url, json=payload, timeout=timeout_seconds)
                response.raise_for_status()
                return response.json()
                
        except TimeoutException:
            logger.error(f"Timeout creating audience: {name}")
            raise
        except Exception as e:
            logger.error(f"Error creating audience: {e}")
            raise
    
    def delete_audience(self, audience_id: str, timeout_seconds: int = 30) -> bool:
        """Delete custom audience with timeout"""
        try:
            with timeout(timeout_seconds):
                self._refresh_session_if_needed()
                
                url = f"{self.base_url}/{audience_id}"
                response = self.session.delete(url, timeout=timeout_seconds)
                return response.status_code == 200
                
        except TimeoutException:
            logger.error(f"Timeout deleting audience: {audience_id}")
            return False
        except Exception as e:
            logger.error(f"Error deleting audience: {e}")
            return False
    
    def list_custom_audiences(
        self,
        limit: int = 100,
        timeout_seconds: int = 30
    ) -> List[Dict]:
        """List custom audiences with timeout"""
        try:
            with timeout(timeout_seconds):
                self._refresh_session_if_needed()
                
                url = f"{self.base_url}/{self.ad_account_id}/customaudiences"
                params = {
                    "fields": "id,name,description,approximate_count_lower_bound,approximate_count_upper_bound",
                    "limit": limit
                }
                
                response = self.session.get(url, params=params, timeout=timeout_seconds)
                response.raise_for_status()
                return response.json().get('data', [])
                
        except TimeoutException:
            logger.error("Timeout listing audiences")
            return []
        except Exception as e:
            logger.error(f"Error listing audiences: {e}")
            return []
    
    def add_users_batch_with_retry(
        self,
        audience_id: str,
        users: List[Dict],
        schema: List[str],
        is_hashed: bool = False,
        max_retries: int = 3,
        batch_timeout: int = 60
    ) -> Dict:
        """Add users to audience with retry and timeout handling"""
        
        for attempt in range(max_retries):
            try:
                with timeout(batch_timeout):
                    self._refresh_session_if_needed()
                    
                    url = f"{self.base_url}/{audience_id}/users"
                    
                    # Prepare payload
                    payload = {
                        "payload": {
                            "schema": schema,
                            "is_raw": not is_hashed,
                            "data": [[user.get(field.lower(), '') for field in schema] for user in users]
                        }
                    }
                    
                    # Make request with timeout
                    response = self.session.post(
                        url,
                        json=payload,
                        timeout=batch_timeout - 5  # Leave 5 seconds buffer
                    )
                    
                    if response.status_code == 200:
                        result = response.json()
                        return {
                            'success': True,
                            'users_uploaded': result.get('num_received', len(users)),
                            'num_invalid': result.get('num_invalid_entries', 0)
                        }
                    elif response.status_code == 408:
                        # Request timeout - retry
                        logger.warning(f"Request timeout (attempt {attempt + 1}/{max_retries})")
                        time.sleep(5 * (attempt + 1))  # Exponential backoff
                        continue
                    else:
                        logger.error(f"API error {response.status_code}: {response.text}")
                        if attempt < max_retries - 1:
                            time.sleep(5 * (attempt + 1))
                            continue
                        return {'success': False, 'error': response.text}
                        
            except TimeoutException:
                logger.warning(f"Batch upload timeout (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(5 * (attempt + 1))
                    continue
                return {'success': False, 'error': 'Timeout'}
                
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"Connection error (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    # Refresh session on connection error
                    self.session.close()
                    self.session = self._create_session()
                    time.sleep(5 * (attempt + 1))
                    continue
                return {'success': False, 'error': 'Connection error'}
                
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5 * (attempt + 1))
                    continue
                return {'success': False, 'error': str(e)}
        
        return {'success': False, 'error': f'Failed after {max_retries} attempts'}
    
    def close(self):
        """Close the session"""
        try:
            self.session.close()
        except:
            pass

"""

import os
import json
import time
import requests
import logging
from typing import List, Dict, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
import signal
from contextlib import contextmanager

load_dotenv()
logger = logging.getLogger(__name__)


class TimeoutException(Exception):
    """Custom timeout exception"""
    pass


@contextmanager
def timeout(seconds):
    """Context manager for timeout"""
    def timeout_handler(signum, frame):
        raise TimeoutException(f"Operation timed out after {seconds} seconds")
    
    # Set the signal handler and alarm
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)  # Disable alarm


class ResilientMetaAPIClient:
    """Meta API client with resilience features"""
    
    def __init__(self):
        self.access_token = os.getenv("META_ACCESS_TOKEN")
        self.ad_account_id = os.getenv("META_AD_ACCOUNT_ID")
        self.api_version = "v21.0"
        self.base_url = f"https://graph.facebook.com/{self.api_version}"
        
        if not self.access_token or not self.ad_account_id:
            raise ValueError("Meta credentials not found in environment variables")
        
        # Create session with retry strategy
        self.session = self._create_session()
        self.request_count = 0
        self.last_session_refresh = time.time()
    
    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry strategy"""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[408, 429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"]
        )
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=10
        )
        
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set default headers
        session.headers.update({
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        })
        
        return session
    
    def _refresh_session_if_needed(self):
        """Refresh session every 30 minutes to avoid connection issues"""
        if time.time() - self.last_session_refresh > 1800:  # 30 minutes
            logger.info("Refreshing HTTP session...")
            self.session.close()
            self.session = self._create_session()
            self.last_session_refresh = time.time()
            self.request_count = 0
    
    def health_check(self) -> bool:
        """Check if Meta API is accessible"""
        try:
            url = f"{self.base_url}/{self.ad_account_id}"
            params = {"fields": "id,name"}
            response = self.session.get(url, params=params, timeout=10)
            if response.status_code == 200:
                return True
            else:
                logger.error(f"Health check returned status {response.status_code}: {response.text}")
                return False
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
    
    def create_custom_audience(
        self,
        name: str,
        description: str = "",
        timeout_seconds: int = 30
    ) -> Dict:
        """Create custom audience with timeout"""
        try:
            with timeout(timeout_seconds):
                self._refresh_session_if_needed()
                
                url = f"{self.base_url}/{self.ad_account_id}/customaudiences"
                payload = {
                    "name": name,
                    "description": description,
                    "subtype": "CUSTOM",
                    "customer_file_source": "USER_PROVIDED_ONLY"
                }
                
                response = self.session.post(url, json=payload, timeout=timeout_seconds)
                response.raise_for_status()
                return response.json()
                
        except TimeoutException:
            logger.error(f"Timeout creating audience: {name}")
            raise
        except Exception as e:
            logger.error(f"Error creating audience: {e}")
            raise
    
    def delete_audience(self, audience_id: str, timeout_seconds: int = 30) -> bool:
        """Delete custom audience with timeout"""
        try:
            with timeout(timeout_seconds):
                self._refresh_session_if_needed()
                
                url = f"{self.base_url}/{audience_id}"
                response = self.session.delete(url, timeout=timeout_seconds)
                return response.status_code == 200
                
        except TimeoutException:
            logger.error(f"Timeout deleting audience: {audience_id}")
            return False
        except Exception as e:
            logger.error(f"Error deleting audience: {e}")
            return False
    
    def list_custom_audiences(
        self,
        limit: int = 100,
        timeout_seconds: int = 30
    ) -> List[Dict]:
        """List custom audiences with timeout"""
        try:
            with timeout(timeout_seconds):
                self._refresh_session_if_needed()
                
                url = f"{self.base_url}/{self.ad_account_id}/customaudiences"
                params = {
                    "fields": "id,name,description,approximate_count_lower_bound,approximate_count_upper_bound",
                    "limit": limit
                }
                
                response = self.session.get(url, params=params, timeout=timeout_seconds)
                response.raise_for_status()
                return response.json().get('data', [])
                
        except TimeoutException:
            logger.error("Timeout listing audiences")
            return []
        except Exception as e:
            logger.error(f"Error listing audiences: {e}")
            return []
    
    def add_users_batch_with_retry(
        self,
        audience_id: str,
        users: List[Dict],
        schema: List[str],
        is_hashed: bool = False,
        max_retries: int = 3,
        batch_timeout: int = 60
    ) -> Dict:
        """Add users to audience with retry and timeout handling"""
        
        for attempt in range(max_retries):
            try:
                with timeout(batch_timeout):
                    self._refresh_session_if_needed()
                    
                    url = f"{self.base_url}/{audience_id}/users"
                    
                    # Prepare payload
                    payload = {
                        "payload": {
                            "schema": schema,
                            "is_raw": not is_hashed,
                            "data": [[user.get(field.lower(), '') for field in schema] for user in users]
                        }
                    }
                    
                    # Make request with timeout
                    response = self.session.post(
                        url,
                        json=payload,
                        timeout=batch_timeout - 5  # Leave 5 seconds buffer
                    )
                    
                    if response.status_code == 200:
                        result = response.json()
                        return {
                            'success': True,
                            'users_uploaded': result.get('num_received', len(users)),
                            'num_invalid': result.get('num_invalid_entries', 0)
                        }
                    elif response.status_code == 408:
                        # Request timeout - retry
                        logger.warning(f"Request timeout (attempt {attempt + 1}/{max_retries})")
                        time.sleep(5 * (attempt + 1))  # Exponential backoff
                        continue
                    else:
                        logger.error(f"API error {response.status_code}: {response.text}")
                        if attempt < max_retries - 1:
                            time.sleep(5 * (attempt + 1))
                            continue
                        return {'success': False, 'error': response.text}
                        
            except TimeoutException:
                logger.warning(f"Batch upload timeout (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(5 * (attempt + 1))
                    continue
                return {'success': False, 'error': 'Timeout'}
                
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"Connection error (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    # Refresh session on connection error
                    self.session.close()
                    self.session = self._create_session()
                    time.sleep(5 * (attempt + 1))
                    continue
                return {'success': False, 'error': 'Connection error'}
                
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5 * (attempt + 1))
                    continue
                return {'success': False, 'error': str(e)}
        
        return {'success': False, 'error': f'Failed after {max_retries} attempts'}
    
    def close(self):
        """Close the session"""
        try:
            self.session.close()
        except:
            pass
