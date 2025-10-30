# src/qb_auth.py
import os
import json
from requests_oauthlib import OAuth2Session
from dotenv import load_dotenv

# Load environment variables early
load_dotenv()

# Optional Vercel Blob support (for production token persistence)
try:
    from vercel_blob import put, get
except ImportError:
    put = get = None

BLOB_KEY = "qb_tokens.json"

def load_tokens():
    """
    Try to load tokens in this order:
    1. From .env (preferred for local dev)
    2. From Vercel Blob (fallback for cloud)
    """
    # 1️⃣ Try from environment variables
    env_tokens = {
        "access_token": os.getenv("QB_ACCESS_TOKEN"),
        "refresh_token": os.getenv("QB_REFRESH_TOKEN"),
        "realmId": os.getenv("QB_REALM_ID"),
    }

    if all(env_tokens.values()):
        return env_tokens

    # 2️⃣ Fallback to Vercel Blob
    if get:
        try:
            data = get(BLOB_KEY)
            return json.loads(data.decode())
        except Exception as e:
            print(f"Warning: Could not read blob tokens ({e})")

    # 3️⃣ No tokens found
    return {}

def save_tokens(tokens):
    """
    Optionally save tokens to Vercel Blob for cloud persistence.
    For local dev, just keep them in .env or print for manual update.
    """
    if put:
        put(BLOB_KEY, json.dumps(tokens).encode())
    else:
        print("⚠️ Running locally — tokens not saved automatically. "
              "Please update your .env file manually if needed.")

class QuickBooksAuth:
    def __init__(self):
        self.client_id = os.getenv("QB_CLIENT_ID")
        self.client_secret = os.getenv("QB_CLIENT_SECRET")
        self.redirect_uri = os.getenv("QB_REDIRECT_URI")
        self.environment = os.getenv("QB_ENVIRONMENT", "sandbox")

        self.authorization_base_url = "https://appcenter.intuit.com/connect/oauth2"
        self.token_url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"

        # Load tokens (from .env or blob)
        self.tokens = load_tokens()

    def get_authorization_url(self):
        oauth = OAuth2Session(
            self.client_id,
            redirect_uri=self.redirect_uri,
            scope=["com.intuit.quickbooks.accounting"]
        )
        url, state = oauth.authorization_url(self.authorization_base_url)
        return url, state

    def fetch_tokens(self, auth_response):
        oauth = OAuth2Session(
            self.client_id,
            redirect_uri=self.redirect_uri,
            scope=["com.intuit.quickbooks.accounting"]
        )
        tokens = oauth.fetch_token(
            self.token_url,
            authorization_response=auth_response,
            client_secret=self.client_secret
        )
        save_tokens(tokens)
        self.tokens = tokens
        return tokens

    def get_access_token(self):
        return self.tokens.get("access_token")

    def get_realm_id(self):
        return self.tokens.get("realmId")
