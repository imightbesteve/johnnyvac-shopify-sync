#!/usr/bin/env python3
"""Shared Shopify Admin API authentication.

Resolves an Admin API access token for the GraphQL Admin API.

Preferred path is the OAuth **client_credentials grant** (set SHOPIFY_CLIENT_ID
+ SHOPIFY_CLIENT_SECRET): it mints a fresh token on demand that always reflects
the app's *current* granted scopes and is valid for 24h. This avoids the
stale-token trap where a long-lived token keeps its original scopes forever —
which is exactly what silently broke the archive 301 redirects after
write_online_store_navigation was added to the app.

Falls back to a static SHOPIFY_ACCESS_TOKEN when client credentials aren't
provided (e.g. local one-off runs).
"""

import os
import time
from datetime import datetime

import requests

SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', 'kingsway-janitorial.myshopify.com')

# Cache the minted token for the life of the process so repeated calls don't
# hit the token endpoint every time.
_cache = {'token': None, 'expires_at': 0.0}


def _log(msg, level='INFO'):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [{level}] {msg}", flush=True)


def _mint_with_client_credentials(client_id: str, client_secret: str):
    """Exchange client credentials for a 24h offline Admin API access token."""
    url = f"https://{SHOPIFY_STORE}/admin/oauth/access_token"
    resp = requests.post(url, data={
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
    }, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    token = data.get('access_token')
    expires_in = int(data.get('expires_in', 86399))
    # Refresh a minute early to avoid using a token that expires mid-run.
    _cache['token'] = token
    _cache['expires_at'] = time.time() + expires_in - 60
    _log(f"Minted Admin API token via client_credentials (scopes: {data.get('scope', '?')})")
    return token


def get_access_token():
    """Return an Admin API access token, or None if no credentials are set.

    Uses client_credentials when SHOPIFY_CLIENT_ID + SHOPIFY_CLIENT_SECRET are
    present; otherwise returns the static SHOPIFY_ACCESS_TOKEN.
    """
    client_id = os.environ.get('SHOPIFY_CLIENT_ID')
    client_secret = os.environ.get('SHOPIFY_CLIENT_SECRET')
    if client_id and client_secret:
        if _cache['token'] and time.time() < _cache['expires_at']:
            return _cache['token']
        try:
            return _mint_with_client_credentials(client_id, client_secret)
        except Exception as e:
            _log(f"client_credentials token request failed ({e}); "
                 f"falling back to SHOPIFY_ACCESS_TOKEN if set", 'ERROR')
    return os.environ.get('SHOPIFY_ACCESS_TOKEN') or None
