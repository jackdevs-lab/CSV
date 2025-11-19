# src/qb_auth.py
import os
import time
import requests
from urllib.parse import urlparse, parse_qs
from src.logger import setup_logger

logger = setup_logger(__name__)

class QuickBooksAuth:
    """
    Bulletproof QuickBooks OAuth2 handler
    - Stores ONLY the refresh token in environment
    - Everything else is generated on-the-fly
    - 100% works on Vercel, local, Docker, etc.
    """

    def __init__(self):
        self.client_id = os.getenv("QB_CLIENT_ID")
        self.client_secret = os.getenv("QB_CLIENT_SECRET")
        self.redirect_uri = os.getenv("QB_REDIRECT_URI")
        self.environment = os.getenv("QB_ENVIRONMENT", "production").lower()

        if not all([self.client_id, self.client_secret, self.redirect_uri]):
            raise ValueError("Missing QB_CLIENT_ID, QB_CLIENT_SECRET, or QB_REDIRECT_URI")

        self.token_url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
        self.authorization_base_url = "https://appcenter.intuit.com/connect/oauth2"

        # Permanent refresh token from env (this is the ONLY thing you ever store)
        self.permanent_refresh_token = os.getenv("QB_REFRESH_TOKEN")
        if not self.permanent_refresh_token:
            raise ValueError("QB_REFRESH_TOKEN not set in environment. Re-authenticate at /login")

        # Runtime tokens (generated from permanent refresh token)
        self._tokens = {
            "access_token": None,
            "refresh_token": self.permanent_refresh_token,
            "expires_at": 0,  # Force first refresh
            "realmId": os.getenv("QB_REALM_ID")  # Optional: can be in env or from auth
        }
        self._lock = False

    def _refresh_token_if_needed(self):
        now = time.time()
        expires_at = self._tokens.get("expires_at", 0)

        # Force refresh if expired or within 5 minutes
        if expires_at > now + 300:
            return

        if self._lock:
            while self._lock:
                time.sleep(0.1)
            return

        self._lock = True
        try:
            logger.info("Refreshing QuickBooks access token...")
            response = requests.post(
                self.token_url,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                auth=(self.client_id, self.client_secret),  # ← THIS IS THE CORRECT WAY
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self._tokens["refresh_token"],
                },
                timeout=30,
            )
            response.raise_for_status()
            new_tokens = response.json()

            expires_in = int(new_tokens.get("expires_in", 3600))

            self._tokens.update({
                "access_token": new_tokens["access_token"],
                "refresh_token": new_tokens.get("refresh_token", self._tokens["refresh_token"]),
                "expires_at": now + expires_in - 60,
            })

            logger.info("QuickBooks token refreshed successfully")

        except Exception as e:
            logger.error(f"Failed to refresh QuickBooks token: {e}", exc_info=True)
            raise
        finally:
            self._lock = False

    def get_valid_access_token(self):
        self._refresh_token_if_needed()
        return self._tokens["access_token"]

    def get_realm_id(self):
        return self._tokens.get("realmId")

    # OAuth flow — only used once every 100 days or on new setup
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

        realm_id = parse_qs(urlparse(auth_response_url).query).get("realmId", [None])[0]

        # Update permanent refresh token in environment (you'll do this manually once)
        new_refresh_token = tokens["refresh_token"]
        logger.info("NEW REFRESH TOKEN GENERATED!")
        logger.info(f"QB_REFRESH_TOKEN={new_refresh_token}")
        logger.info("↑↑↑ COPY THIS AND UPDATE IN VERCEL ENVIRONMENT VARIABLES ↑↑↑")

        self._tokens.update({
            "access_token": tokens["access_token"],
            "refresh_token": new_refresh_token,
            "expires_at": time.time() + tokens["expires_in"] - 60,
            "realmId": realm_id,
        })

        if realm_id and not os.getenv("QB_REALM_ID"):
            logger.info(f"QB_REALM_ID={realm_id}")

        return self._tokens