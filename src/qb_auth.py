# src/qb_auth.py
import os
import time
from datetime import datetime, timezone
import requests
from urllib.parse import urlparse, parse_qs
from src.logger import setup_logger
import db

logger = setup_logger(__name__)


class QuickBooksAuthError(Exception):
    def __init__(self, message, error_code=None, status_code=None,
                 refresh_token_invalid=False):
        super().__init__(message)
        self.error_code = error_code
        self.status_code = status_code
        self.refresh_token_invalid = refresh_token_invalid


class QuickBooksAuth:
    TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    AUTH_URL = "https://appcenter.intuit.com/connect/oauth2"

    def __init__(self):
        self.client_id = os.getenv("QB_CLIENT_ID")
        self.client_secret = os.getenv("QB_CLIENT_SECRET")
        self.redirect_uri = os.getenv("QB_REDIRECT_URI")
        if not all([self.client_id, self.client_secret, self.redirect_uri]):
            raise ValueError("Missing QB_CLIENT_ID/SECRET/REDIRECT_URI")
        self._tokens = None

    def _load(self):
        row = db.get_token_row()
        if not row:
            raise QuickBooksAuthError(
                "No QuickBooks tokens in DB. Visit /login to authenticate.",
                refresh_token_invalid=True)
        self._tokens = {
            "access_token": row["access_token"],
            "refresh_token": row["refresh_token"],
            "expires_at": row["access_token_expires_at"].timestamp()
                          if row["access_token_expires_at"] else 0,
            "realmId": row["realm_id"],
            "refresh_token_issued_at": row["refresh_token_issued_at"].timestamp(),
        }
        return self._tokens

    def _persist(self, *, refresh_token, refresh_token_issued_at,
                 access_token=None, access_token_expires_at=None, realm_id=None):
        db.upsert_token_row(
            realm_id=realm_id or self._tokens["realmId"],
            refresh_token=refresh_token,
            refresh_token_issued_at=datetime.fromtimestamp(
                refresh_token_issued_at, tz=timezone.utc),
            access_token=access_token,
            access_token_expires_at=(datetime.fromtimestamp(
                access_token_expires_at, tz=timezone.utc)
                if access_token_expires_at else None),
        )

    def _refresh_token_if_needed(self):
        t = self._load()
        if t["expires_at"] > time.time() + 300:
            return
        logger.info("Refreshing QuickBooks access token...")
        health = self.refresh_token_health()
        logger.info("Refresh token health: %s (age_days=%s)",
                    health.get("reason"), health.get("age_days"))

        r = requests.post(self.TOKEN_URL,
            headers={"Accept": "application/json",
                     "Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "refresh_token",
                "refresh_token": t["refresh_token"],
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }, timeout=30)

        if r.status_code >= 400:
            error_code, desc = None, r.text
            try:
                p = r.json()
                error_code = p.get("error")
                desc = p.get("error_description") or desc
            except Exception:
                pass
            invalid = error_code in ("invalid_grant", "invalid_token",
                                     "token_expired", "unauthorized_client")
            msg = f"Token refresh failed (HTTP {r.status_code}): {error_code} - {desc}"
            if invalid:
                msg += "\nRefresh token expired/revoked. Re-authenticate at /login."
            raise QuickBooksAuthError(msg, error_code=error_code,
                                      status_code=r.status_code,
                                      refresh_token_invalid=invalid)

        new = r.json()
        expires_in = int(new.get("expires_in", 3600))
        new_refresh = new.get("refresh_token")
        rotated = bool(new_refresh and new_refresh != t["refresh_token"])

        self._persist(
            refresh_token=new_refresh or t["refresh_token"],
            refresh_token_issued_at=(time.time() if rotated
                                     else t["refresh_token_issued_at"]),
            access_token=new["access_token"],
            access_token_expires_at=time.time() + expires_in - 60,
        )
        self._tokens = None
        logger.info("Token refreshed successfully")

    def get_valid_access_token(self):
        t = self._load()
        if t["expires_at"] <= time.time() + 300:
            self._refresh_token_if_needed()
            t = self._load()
        return t["access_token"]

    def get_realm_id(self):
        return self._load()["realmId"]

    def refresh_token_health(self):
        try:
            t = self._load()
        except QuickBooksAuthError:
            return {"status": "no_token", "reason": "no_token", "age_days": None}
        age_days = (time.time() - t["refresh_token_issued_at"]) / 86400.0
        if age_days >= 100.0:
            return {"status": "invalid", "reason": "aged_out",
                    "age_days": round(age_days, 1)}
        return {"status": "ok", "reason": "ok", "age_days": round(age_days, 1)}

    def get_authorization_url(self):
        from requests_oauthlib import OAuth2Session
        oauth = OAuth2Session(self.client_id, redirect_uri=self.redirect_uri,
                              scope=["com.intuit.quickbooks.accounting"])
        return oauth.authorization_url(self.AUTH_URL)

    def fetch_tokens(self, auth_response_url):
        from requests_oauthlib import OAuth2Session
        oauth = OAuth2Session(self.client_id, redirect_uri=self.redirect_uri,
                              scope=["com.intuit.quickbooks.accounting"])
        tokens = oauth.fetch_token(self.TOKEN_URL,
            authorization_response=auth_response_url,
            client_secret=self.client_secret)
        realm_id = parse_qs(urlparse(auth_response_url).query).get("realmId", [None])[0]
        if not realm_id:
            raise QuickBooksAuthError("No realmId in callback URL")
        now = time.time()
        self._persist(realm_id=realm_id,
                      refresh_token=tokens["refresh_token"],
                      refresh_token_issued_at=now,
                      access_token=tokens["access_token"],
                      access_token_expires_at=now + int(tokens["expires_in"]) - 60)
        self._tokens = None
        logger.info("OAuth complete. Tokens stored in DB (realm_id=%s).", realm_id)
        return {"realmId": realm_id,
                "access_token": tokens["access_token"],
                "refresh_token": tokens["refresh_token"],
                "expires_in": tokens["expires_in"]}