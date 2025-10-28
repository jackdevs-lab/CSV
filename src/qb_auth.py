# src/qb_auth.py
import os
import json
from requests_oauthlib import OAuth2Session

# Vercel Blob (safe fallback for local dev)
try:
    from vercel_blob import put, get
except:
    put = get = None

BLOB_KEY = "qb_tokens.json"

def load_tokens():
    if get:
        try:
            data = get(BLOB_KEY)
            return json.loads(data.decode())
        except:
            pass
    return {}

def save_tokens(tokens):
    if put:
        put(BLOB_KEY, json.dumps(tokens).encode())

class QuickBooksAuth:
    def __init__(self):
        self.client_id = os.getenv("QB_CLIENT_ID")
        self.client_secret = os.getenv("QB_CLIENT_SECRET")
        self.redirect_uri = os.getenv("QB_REDIRECT_URI")
        self.environment = os.getenv("QB_ENVIRONMENT", "sandbox")

        self.authorization_base_url = "https://appcenter.intuit.com/connect/oauth2"
        self.token_url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"

        self.tokens = load_tokens()

    def get_authorization_url(self):
        oauth = OAuth2Session(
            self.client_id,
            redirect_uri=self.redirect_uri,
            scope=["com.intuit.quickbooks.accounting"]  # THIS LINE WAS MISSING
        )
        url, state = oauth.authorization_url(self.authorization_base_url)
        return url, state

    def fetch_tokens(self, auth_response):
        oauth = OAuth2Session(
            self.client_id,
            redirect_uri=self.redirect_uri,
            scope=["com.intuit.quickbooks.accounting"]  # Also add here (optional but safe)
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