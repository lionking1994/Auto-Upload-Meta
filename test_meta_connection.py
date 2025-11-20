"""
Test Meta API connection
"""
import requests
import os
from dotenv import load_dotenv

load_dotenv()

def test_meta_connection():
    """Test if we can connect to Meta API"""
    
    token = os.getenv('META_ACCESS_TOKEN')
    ad_account_id = os.getenv('META_AD_ACCOUNT_ID', '290877649')
    
    if not ad_account_id.startswith('act_'):
        ad_account_id = f'act_{ad_account_id}'
    
    print("=" * 60)
    print("Testing Meta API Connection")
    print("=" * 60)
    
    # Test 1: Basic connection test
    print("\n1. Testing basic API connection...")
    url = "https://graph.facebook.com/v18.0/me"
    params = {"access_token": token}
    
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            print(f"✓ Connected! User ID: {data.get('id', 'Unknown')}")
        else:
            print(f"✗ Failed with status: {response.status_code}")
            print(f"Response: {response.text}")
    except requests.exceptions.Timeout:
        print("✗ Connection timeout - check your internet/firewall")
    except requests.exceptions.ConnectionError as e:
        print(f"✗ Connection error: {e}")
    except Exception as e:
        print(f"✗ Error: {e}")
    
    # Test 2: Test ad account access
    print("\n2. Testing ad account access...")
    url = f"https://graph.facebook.com/v18.0/{ad_account_id}"
    params = {
        "access_token": token,
        "fields": "id,name,account_status"
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            print(f"✓ Ad Account accessible!")
            print(f"   ID: {data.get('id')}")
            print(f"   Name: {data.get('name', 'N/A')}")
            print(f"   Status: {data.get('account_status', 'N/A')}")
        else:
            print(f"✗ Failed with status: {response.status_code}")
            print(f"Response: {response.text}")
    except requests.exceptions.Timeout:
        print("✗ Connection timeout - check your internet/firewall")
    except requests.exceptions.ConnectionError as e:
        print(f"✗ Connection error: {e}")
    except Exception as e:
        print(f"✗ Error: {e}")
    
    # Test 3: List existing custom audiences
    print("\n3. Checking existing custom audiences...")
    url = f"https://graph.facebook.com/v18.0/{ad_account_id}/customaudiences"
    params = {
        "access_token": token,
        "fields": "id,name,approximate_count",
        "limit": 5
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            audiences = data.get('data', [])
            print(f"✓ Found {len(audiences)} existing audiences")
            for aud in audiences[:3]:
                print(f"   - {aud.get('name', 'Unknown')} (ID: {aud.get('id')})")
        else:
            print(f"✗ Failed with status: {response.status_code}")
            print(f"Response: {response.text}")
    except requests.exceptions.Timeout:
        print("✗ Connection timeout - check your internet/firewall")
    except requests.exceptions.ConnectionError as e:
        print(f"✗ Connection error: {e}")
    except Exception as e:
        print(f"✗ Error: {e}")
    
    print("\n" + "=" * 60)
    print("If you see connection timeouts:")
    print("1. Check if you're behind a corporate firewall/proxy")
    print("2. Try using a different network")
    print("3. Check if the access token is still valid")
    print("4. Try regenerating the Meta access token")

if __name__ == "__main__":
    test_meta_connection()
