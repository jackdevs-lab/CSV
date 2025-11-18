# src/qb_auth.py
import os
import json
import time
import requests
from dotenv import load_dotenv

load_dotenv()

# Optional Vercel Blob support
try:
    from vercel_blob import put, get
except ImportError:
    put = get = None

BLOB_KEY = "qb_tokens.json"

def load_tokens():
    """Load tokens: .env first (dev), then Vercel Blob (prod)"""
    # 1. Try environment variables (set manually or injected)
    env_tokens = {
        "access_token": os.getenv("QB_ACCESS_TOKEN"),
        "refresh_token": os.getenv("QB_REFRESH_TOKEN"),
        "realmId": os.getenv("QB_REALM_ID"),
        "expires_at": os.getenv("QB_TOKEN_EXPIRES_AT"),  # Unix timestamp
    }

    if all(v for k, v in env_tokens.items() if k != "expires_at" or v):
        # Convert expires_at to float if exists
        if env_tokens["expires_at"]:
            env_tokens["expires_at"] = float(env_tokens["expires_at"])
        return env_tokens

    # 2. Fallback: Vercel Blob
    if get:
        try:
            data = get(BLOB_KEY)
            tokens = json.loads(data.decode())
            tokens["expires_at"] = tokens.get("expires_at", 0)
            return tokens
        except Exception as e:
            print(f"Warning: Failed to load tokens from blob: {e}")

    return {}

def save_tokens(tokens: dict):
    """Save tokens to Vercel Blob (production) — local dev just logs"""
    # Always save expires_at
    if "expires_in" in tokens:
        tokens["expires_at"] = time.time() + tokens["expires_in"] - 60  # 1 min safety
        del tokens["expires_in"]  # clean up

    if put:
        try:
            put(BLOB_KEY, json.dumps(tokens).encode())
            print("Tokens saved to Vercel Blob")
        except Exception as e:
            print(f"Failed to save tokens to blob: {e}")
    else:
        print("LOCAL DEV: New tokens generated. Update .env manually:")
        print(f"QB_ACCESS_TOKEN={tokens.get('access_token')}")
        print(f"QB_REFRESH_TOKEN={tokens.get('refresh_token')}")
        print(f"QB_TOKEN_EXPIRES_AT={tokens.get('expires_at', 0)}")

class QuickBooksAuth:
    def __init__(self):
        self.client_id = os.getenv("QB_CLIENT_ID")
        self.client_secret = os.getenv("QB_CLIENT_SECRET")
        self.redirect_uri = os.getenv("QB_REDIRECT_URI")
        self.environment = os.getenv("QB_ENVIRONMENT", "production").lower()

        # Production URLs only
        self.authorization_base_url = "https://appcenter.intuit.com/connect/oauth2"
        self.token_url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"

        self._tokens = load_tokens()
        self._lock = False  # Simple lock to prevent parallel refresh

    def _refresh_token_if_needed(self):
        """Refresh token if expired or near expiry (5 min buffer)"""
        now = time.time()
        expires_at = self._tokens.get("expires_at", 0)

        if expires_at > now + 300:  # More than 5 min left
            return

        if self._lock:
            while self._lock:
                time.sleep(0.1)  # Wait for other refresh to finish
            return

        self._lock = True
        try:
            refresh_token = self._tokens.get("refresh_token")
            if not refresh_token:
                raise ValueError("No refresh token available — re-authenticate at /login")

            print("Refreshing QuickBooks access token...")
            response = requests.post(
                self.token_url,
                headers={
                    "Accept": "application/json",
                    "Authorization": "Basic " + requests.auth._basic_auth_str(
                        self.client_id, self.client_secret
                    ),
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                },
            )
            response.raise_for_status()
            new_tokens = response.json()

            # Update tokens
            self._tokens.update({
                "access_token": new_tokens["access_token"],
                "refresh_token": new_tokens.get("refresh_token", refresh_token),
                "expires_at": time.time() + new_tokens["expires_in"] - 60,
            })

            save_tokens(self._tokens.copy())
            print("Token refreshed successfully")

        except Exception as e:
            print(f"Failed to refresh token: {e}")
            raise
        finally:
            self._lock = False

    def get_valid_access_token(self):
        """Main method — always returns a valid access token"""
        self._refresh_token_if_needed()
        token = self._tokens.get("access_token")
        if not token:
            raise ValueError("No access token — visit /login to authenticate")
        return token

    def get_realm_id(self):
        return self._tokens.get("realmId")

    # Keep existing OAuth flow methods
    def get_authorization_url(self):
        from requests_oauthlib import OAuth2Session
        oauth = OAuth2Session(
            self.client_id,
            redirect_uri=self.redirect_uri,
            scope=["com.intuit.quickbooks.accounting"]
        )
        url, state = oauth.authorization_url(self.authorization_base_url)
        return url, state

    def fetch_tokens(self, auth_response_url):
        from requests_oauthlib import OAuth2Session
        oauth = OAuth2Session(
            self.client_id,
            redirect_uri=self.redirect_uri,
            scope=["com.intuit.quickbooks.accounting"]
        )
        tokens = oauth.fetch_token(
            self.token_url,
            authorization_response=auth_response_url,
            client_secret=self.client_secret
        )

        # Add realmId from query params
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(auth_response_url)
        realm_id = parse_qs(parsed.query).get("realmId", [None])[0]

        full_tokens = {
            "access_token": tokens["access_token"],
            "refresh_token": tokens["refresh_token"],
            "expires_at": time.time() + tokens["expires_in"] - 60,
            "realmId": realm_id,
        }
        self._tokens = full_tokens
        save_tokens(full_tokens)
        return full_tokens