# src/qb_auth.py
import os
import time
import requests
import json
from urllib.parse import urlparse, parse_qs
from src.logger import setup_logger

logger = setup_logger(__name__)


class QuickBooksAuthError(Exception):
    """
    Raised when the OAuth2 token exchange fails.

    Attributes:
        error_code:              Intuit error code (e.g. 'invalid_grant', 'invalid_client')
        status_code:             HTTP status code from the token endpoint
        refresh_token_invalid:   True when the refresh token itself is expired/revoked
                                 and the user must re-authenticate at /login
    """
    def __init__(self, message, error_code=None, status_code=None, refresh_token_invalid=False):
        super().__init__(message)
        self.error_code = error_code
        self.status_code = status_code
        self.refresh_token_invalid = refresh_token_invalid


class QuickBooksAuth:
    """
    Bulletproof QuickBooks OAuth2 handler
    - Stores ONLY the refresh token in a persistent file (or env as fallback)
    - Everything else is generated on-the-fly
    - Works on Render with attached disk for persistence
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

        # Persistent token file (mount a disk on Render at /data and set TOKEN_FILE=/data/tokens.json)
        self.token_file = os.getenv("TOKEN_FILE", "tokens.json")

        # Create directory if needed (skip if dirname is empty, e.g., file in cwd)
        dir_path = os.path.dirname(self.token_file)
        if dir_path and not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)

        # Load refresh token from file if available, else from env
        self.permanent_refresh_token = self._get_stored_value("qb_refresh_token") or os.getenv("QB_REFRESH_TOKEN")
        if not self.permanent_refresh_token:
            raise ValueError("QB_REFRESH_TOKEN not set in file or environment. Re-authenticate at /login")

        # Runtime tokens (generated from permanent refresh token)
        self._tokens = {
            "access_token": None,
            "refresh_token": self.permanent_refresh_token,
            "expires_at": 0,  # Force first refresh
            "realmId": self._get_stored_value("qb_realm_id") or os.getenv("QB_REALM_ID")  # Optional: can be in env or from auth
        }
        self._lock = False

    def _get_stored_value(self, key):
        if os.path.exists(self.token_file):
            try:
                with open(self.token_file, 'r') as f:
                    data = json.load(f)
                    return data.get(key)
            except Exception as e:
                logger.error(f"Failed to read token file: {e}")
        return None

    def _set_stored_value(self, key, value):
        data = {}
        if os.path.exists(self.token_file):
            try:
                with open(self.token_file, 'r') as f:
                    data = json.load(f)
            except Exception as e:
                logger.error(f"Failed to read token file for update: {e}")
        data[key] = value
        try:
            with open(self.token_file, 'w') as f:
                json.dump(data, f)
            logger.info(f"Updated {key} in token file")
        except Exception as e:
            logger.error(f"Failed to write token file: {e}")

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
                    # Also send credentials in the body — Intuit's bearer endpoint
                    # accepts them here and this resolves spurious 400 requests.
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
                timeout=30,
            )

            # Capture the real Intuit error payload instead of a bare 400.
            if response.status_code >= 400:
                error_code = None
                error_desc = response.text
                try:
                    payload = response.json()
                    error_code = payload.get("error")
                    error_desc = payload.get("error_description") or error_desc
                except Exception:
                    pass

                # An expired/revoked refresh token cannot be recovered without re-auth.
                refresh_token_invalid = error_code in (
                    "invalid_grant",
                    "invalid_token",
                    "token_expired",
                    "unauthorized_client",
                )

                logger.error(
                    "QuickBooks token refresh failed | HTTP %s | error=%s | description=%s",
                    response.status_code,
                    error_code,
                    error_desc,
                )

                message = (
                    "QuickBooks OAuth token refresh failed (HTTP {status}). "
                    "Intuit error: {code} - {desc}".format(
                        status=response.status_code,
                        code=error_code or "unknown",
                        desc=error_desc,
                    )
                )
                if refresh_token_invalid:
                    message += (
                        "\nThe stored refresh token is expired or revoked. "
                        "Re-authenticate at /login to mint a fresh refresh token."
                    )

                raise QuickBooksAuthError(
                    message,
                    error_code=error_code,
                    status_code=response.status_code,
                    refresh_token_invalid=refresh_token_invalid,
                )

            new_tokens = response.json()

            expires_in = int(new_tokens.get("expires_in", 3600))

            new_refresh = new_tokens.get("refresh_token")
            if new_refresh and new_refresh != self._tokens["refresh_token"]:
                logger.info("New refresh token received - updating storage")
                self._set_stored_value("qb_refresh_token", new_refresh)

            self._tokens.update({
                "access_token": new_tokens["access_token"],
                "refresh_token": new_refresh or self._tokens["refresh_token"],
                "expires_at": now + expires_in - 60,
            })

            logger.info("QuickBooks token refreshed successfully")
            logger.info(f"Current QuickBooks access token: {self._tokens['access_token']}")

        except Exception as e:
            logger.error(f"Failed to refresh QuickBooks token: {e}", exc_info=True)
            raise
        finally:
            self._lock = False

    def get_valid_access_token(self):
        self._refresh_token_if_needed()
        return self._tokens["access_token"]

    def log_access_token(self):
        access_token = self._tokens.get("access_token")
        if access_token:
            logger.info(f"Current QuickBooks access token: {access_token}")
        else:
            logger.info("No QuickBooks access token has been generated yet.")

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

        # Update permanent refresh token in file (and log for manual verification)
        new_refresh_token = tokens["refresh_token"]
        self._set_stored_value("qb_refresh_token", new_refresh_token)
        if realm_id:
            self._set_stored_value("qb_realm_id", realm_id)

        logger.info("NEW REFRESH TOKEN GENERATED!")
        logger.info(f"QB_REFRESH_TOKEN={new_refresh_token}")
        logger.info("↑↑↑ COPY THIS AND UPDATE IN RENDER ENVIRONMENT VARIABLES AS BACKUP ↑↑↑")

        self._tokens.update({
            "access_token": tokens["access_token"],
            "refresh_token": new_refresh_token,
            "expires_at": time.time() + tokens["expires_in"] - 60,
            "realmId": realm_id,
        })

        logger.info(f"Current QuickBooks access token: {self._tokens['access_token']}")

        if realm_id and not os.getenv("QB_REALM_ID"):
            logger.info(f"QB_REALM_ID={realm_id}")

        return self._tokens