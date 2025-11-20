"""
Quick test to verify Snowflake credentials
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

print("=" * 60)
print("Checking Snowflake Credentials from .env")
print("=" * 60)

# Check what's loaded (without showing password)
account = os.getenv('SNOWFLAKE_ACCOUNT')
user = os.getenv('SNOWFLAKE_USER')
password = os.getenv('SNOWFLAKE_PASSWORD')
warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')

print(f"Account: {account}")
print(f"User: {user}")
print(f"Password: {'*' * len(password) if password else 'NOT SET'}")
print(f"Warehouse: {warehouse}")

if not account or account == 'your_account_here':
    print("\n❌ SNOWFLAKE_ACCOUNT needs to be updated in .env")
if not user or user == 'your_username_here':
    print("\n❌ SNOWFLAKE_USER needs to be updated in .env")
if not password or password == 'your_password_here':
    print("\n❌ SNOWFLAKE_PASSWORD needs to be updated in .env")

if account and user and password and \
   account != 'your_account_here' and \
   user != 'your_username_here' and \
   password != 'your_password_here':
    print("\n✓ Credentials appear to be set. Run test_snowflake.py to verify connection.")
else:
    print("\n⚠️ Please update the credentials in your .env file first!")
    print("\nEdit the .env file and replace:")
    print("  - your_username_here → with your actual Snowflake username")
    print("  - your_password_here → with your actual Snowflake password")
    print("  - your_account_here → with your Snowflake account (if needed)")
