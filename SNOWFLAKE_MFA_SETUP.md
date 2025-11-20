# Snowflake MFA Bypass Setup Guide

Since your account has MFA enabled, you need to set up **Key Pair Authentication** for programmatic access.

## Option 1: Key Pair Authentication (Recommended)

### Step 1: Generate a Key Pair

Run these commands in PowerShell:

```powershell
# Generate private key
openssl genrsa -out snowflake_key.pem 2048

# Generate public key
openssl rsa -in snowflake_key.pem -pubout -out snowflake_key.pub
```

If you don't have OpenSSL, you can:
- Install it via: `choco install openssl` (if you have Chocolatey)
- Or download from: https://slproweb.com/products/Win32OpenSSL.html

### Step 2: Add Public Key to Snowflake

1. Log into Snowflake web interface
2. Run this SQL (replace with your public key content):

```sql
ALTER USER FARID_ALTUN SET RSA_PUBLIC_KEY='MIIBIjANBgkqh...your-public-key-content...';
```

To get your public key content:
```powershell
# View public key (copy everything between BEGIN and END lines)
Get-Content snowflake_key.pub
```

### Step 3: Update Your .env File

```env
# Remove or comment out password and token
# SNOWFLAKE_PASSWORD=...
# SNOWFLAKE_TOKEN=...

# Add private key path
SNOWFLAKE_PRIVATE_KEY_PATH=snowflake_key.pem
```

## Option 2: Use External Browser Authentication

This opens a browser for each connection (not fully automated):

```python
conn = snowflake.connector.connect(
    account='PUB27113',
    user='FARID_ALTUN',
    authenticator='externalbrowser',
    warehouse='COMPUTE_WH',
    database='GAMING',
    schema='PUBLIC',
    role='FARID_API_ROLE'
)
```

## Option 3: Create a Service Account

Ask your Snowflake admin to:
1. Create a service account (e.g., `FARID_ALTUN_SERVICE`)
2. Disable MFA for this account
3. Grant same permissions as your main account

## Option 4: Session Token (What You Have)

The token you provided (`eyJraWQ...`) looks like a JWT session token. These are typically:
- Short-lived (expire quickly)
- Generated per session
- Not meant for long-term programmatic access

To use it properly, you might need to:
1. Check how it was generated in the Snowflake web portal
2. Verify if it's a session token or an OAuth token
3. Check its expiration time

## Testing Your Setup

After setting up key pair authentication:

```bash
python test_snowflake.py
```

## Current Status

Your token appears to be invalid or expired. The error message:
`Invalid OAuth access token`

This suggests the token format isn't what Snowflake expects for OAuth authentication.

## Recommendation

**Use Key Pair Authentication** - it's the standard for automated scripts with MFA-enabled accounts.
