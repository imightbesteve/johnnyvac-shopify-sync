#!/usr/bin/env python3
"""
One-Time 301 Redirect Backfill - Kingsway Janitorial

Creates 301 URL redirects for products that were archived (set to DRAFT)
BEFORE the app had the write_online_store_navigation scope, so their dead
/products/<handle> URLs currently 404 in Google instead of redirecting.

The daily sync only creates a redirect at the moment it archives a product,
and it early-returns on products that are already DRAFT — so these ~1,082
previously-archived products would never get a redirect without this backfill.

Target selection mirrors the sync's redirect_target_for():
  - redirect to /collections/<handle> when one of the product's tags matches
    a real collection handle on the store
  - otherwise fall back to the homepage "/"

Existing redirects are skipped (idempotent — safe to re-run).

Usage:
    python backfill_redirects.py --dry-run    # Preview only (default)
    python backfill_redirects.py --live       # Actually create redirects

Environment Variables:
    SHOPIFY_STORE: Store URL
    SHOPIFY_CLIENT_ID + SHOPIFY_CLIENT_SECRET: preferred — mints a fresh token
        via client_credentials that carries the app's current scopes (needs
        write_online_store_navigation for urlRedirectCreate).
    SHOPIFY_ACCESS_TOKEN: fallback static token when client creds aren't set.
"""

import os
import sys
import time
import requests
import argparse
from datetime import datetime

from shopify_auth import get_access_token

# Configuration
SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', 'kingsway-janitorial.myshopify.com')
SHOPIFY_ACCESS_TOKEN = get_access_token() or ''
API_VERSION = '2026-01'
GRAPHQL_URL = f'https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/graphql.json'
HEADERS = {
    'Content-Type': 'application/json',
    'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN
}

RATE_LIMIT_DELAY = 0.5
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3


def log(msg, level='INFO'):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [{level}] {msg}", flush=True)


def graphql(query, variables=None):
    payload = {'query': query}
    if variables:
        payload['variables'] = variables

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(GRAPHQL_URL, json=payload, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if resp.status_code in (429, 503):
                wait = min(int(resp.headers.get('Retry-After', (attempt + 1) * 10)), 60)
                log(f"HTTP {resp.status_code}, retry in {wait}s...", 'WARNING')
                time.sleep(wait)
                continue
            resp.raise_for_status()
            result = resp.json()
            if 'errors' in result:
                log(f"GraphQL errors: {result['errors']}", 'WARNING')
            return result
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            if attempt < MAX_RETRIES - 1:
                wait = (attempt + 1) * 10
                log(f"Connection error, retry {attempt+1}/{MAX_RETRIES} in {wait}s...", 'WARNING')
                time.sleep(wait)
            else:
                raise
    return {}


def require_redirect_scope():
    """Fail fast with a clear message if the token can't read/write redirects.

    urlRedirect access needs read/write_online_store_navigation. Without it the
    API returns data:null + an ACCESS_DENIED error, so check up front instead of
    crashing mid-run."""
    query = """
    query {
      currentAppInstallation {
        accessScopes { handle }
      }
    }
    """
    result = graphql(query)
    scopes = {
        s.get('handle')
        for s in ((result.get('data') or {})
                  .get('currentAppInstallation', {})
                  .get('accessScopes') or [])
    }
    missing = {'read_online_store_navigation', 'write_online_store_navigation'} - scopes
    if missing:
        log("=" * 60, 'ERROR')
        log(f"MISSING SCOPE(S): {', '.join(sorted(missing))}", 'ERROR')
        log("The access token cannot read/write URL redirects. Add", 'ERROR')
        log("read_online_store_navigation + write_online_store_navigation to the", 'ERROR')
        log("app, Release the version, reinstall so the token picks up the new", 'ERROR')
        log("scopes, and update the SHOPIFY_ACCESS_TOKEN secret if it rotated.", 'ERROR')
        log("=" * 60, 'ERROR')
        sys.exit(1)


def fetch_collection_handles():
    """All collection handles on the store — valid redirect targets."""
    log("Fetching collection handles...")
    query = """
    query ($cursor: String) {
      collections(first: 250, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes { handle }
      }
    }
    """
    handles = set()
    cursor = None
    while True:
        result = graphql(query, {'cursor': cursor} if cursor else None)
        data = (result.get('data') or {}).get('collections') or {}
        for node in data.get('nodes', []):
            if node.get('handle'):
                handles.add(node['handle'])
        page_info = data.get('pageInfo', {})
        if not page_info.get('hasNextPage'):
            break
        cursor = page_info['endCursor']
        time.sleep(RATE_LIMIT_DELAY)
    log(f"✓ Found {len(handles)} collections")
    return handles


def fetch_existing_redirect_paths():
    """Paths that already redirect — skip these (idempotent re-runs)."""
    log("Fetching existing redirects...")
    query = """
    query ($cursor: String) {
      urlRedirects(first: 250, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes { path }
      }
    }
    """
    paths = set()
    cursor = None
    while True:
        result = graphql(query, {'cursor': cursor} if cursor else None)
        data = (result.get('data') or {}).get('urlRedirects') or {}
        for node in data.get('nodes', []):
            if node.get('path'):
                paths.add(node['path'])
        page_info = data.get('pageInfo', {})
        if not page_info.get('hasNextPage'):
            break
        cursor = page_info['endCursor']
        time.sleep(RATE_LIMIT_DELAY)
    log(f"✓ Found {len(paths)} existing redirects")
    return paths


def fetch_draft_products():
    """All DRAFT (archived) products with their handle and tags."""
    log("Fetching DRAFT products...")
    query = """
    query ($cursor: String) {
      products(first: 250, after: $cursor, query: "status:draft") {
        pageInfo { hasNextPage endCursor }
        nodes { id handle tags }
      }
    }
    """
    products = []
    cursor = None
    while True:
        result = graphql(query, {'cursor': cursor} if cursor else None)
        data = (result.get('data') or {}).get('products') or {}
        for node in data.get('nodes', []):
            products.append({
                'id': node['id'],
                'handle': node.get('handle', ''),
                'tags': node.get('tags', []) or [],
            })
        page_info = data.get('pageInfo', {})
        if not page_info.get('hasNextPage'):
            break
        cursor = page_info['endCursor']
        if len(products) % 1000 == 0:
            log(f"  Fetched {len(products)} draft products...")
        time.sleep(RATE_LIMIT_DELAY)
    log(f"✓ Fetched {len(products)} draft products")
    return products


def redirect_target_for(tags, collection_handles):
    """Pick the collection page for the product's category; homepage fallback.
    Mirrors sync_shopify_bulk_v3.redirect_target_for()."""
    for tag in tags:
        if tag in collection_handles:
            return f"/collections/{tag}"
    return "/"


def create_url_redirect(path, target):
    mutation = """
    mutation urlRedirectCreate($urlRedirect: UrlRedirectInput!) {
      urlRedirectCreate(urlRedirect: $urlRedirect) {
        urlRedirect { id }
        userErrors { field message }
      }
    }
    """
    result = graphql(mutation, {'urlRedirect': {'path': path, 'target': target}})
    # Access-denied surfaces as a top-level error, not userErrors — flag it
    # clearly so a missing write_online_store_navigation scope is obvious.
    top_errors = result.get('errors') or []
    if top_errors:
        if any('access denied' in (e.get('message') or '').lower() for e in top_errors):
            log(f"  {path} DENIED — token missing write_online_store_navigation scope", 'ERROR')
        else:
            log(f"  Redirect {path} failed: {top_errors}", 'WARNING')
        return False
    errors = result.get('data', {}).get('urlRedirectCreate', {}).get('userErrors', [])
    if errors:
        # "already exists" is fine — the redirect is in place
        if any('exists' in (e.get('message') or '').lower() for e in errors):
            return True
        log(f"  Redirect {path} failed: {errors}", 'WARNING')
        return False
    return True


def main():
    parser = argparse.ArgumentParser(description='Backfill 301 redirects for archived products')
    parser.add_argument('--dry-run', action='store_true', default=True)
    parser.add_argument('--live', action='store_true')
    args = parser.parse_args()
    dry_run = not args.live

    log("=" * 60)
    log("301 REDIRECT BACKFILL (archived products)")
    log("=" * 60)
    log(f"Store: {SHOPIFY_STORE}")
    log(f"Mode:  {'DRY RUN' if dry_run else 'LIVE'}")

    if not SHOPIFY_ACCESS_TOKEN:
        log("No Shopify credentials — set SHOPIFY_CLIENT_ID + SHOPIFY_CLIENT_SECRET "
            "(preferred) or SHOPIFY_ACCESS_TOKEN", 'ERROR')
        sys.exit(1)

    require_redirect_scope()
    collection_handles = fetch_collection_handles()
    existing_paths = fetch_existing_redirect_paths()
    drafts = fetch_draft_products()

    # Build the work list
    to_create = []        # (path, target)
    skipped_no_handle = 0
    skipped_existing = 0
    to_collection = 0
    to_homepage = 0

    for p in drafts:
        if not p['handle']:
            skipped_no_handle += 1
            continue
        path = f"/products/{p['handle']}"
        if path in existing_paths:
            skipped_existing += 1
            continue
        target = redirect_target_for(p['tags'], collection_handles)
        if target == "/":
            to_homepage += 1
        else:
            to_collection += 1
        to_create.append((path, target))

    log(f"\nBREAKDOWN:")
    log(f"  Draft products:            {len(drafts)}")
    log(f"  Already redirected (skip): {skipped_existing}")
    log(f"  No handle (skip):          {skipped_no_handle}")
    log(f"  To create:                 {len(to_create)}")
    log(f"    → to a collection:       {to_collection}")
    log(f"    → to homepage (no tag):  {to_homepage}")

    if not to_create:
        log("\nNothing to do — every archived product already redirects.")
        return

    log(f"\n{'DRY RUN - ' if dry_run else ''}Creating {len(to_create)} redirects...")

    created = 0
    errors = 0
    for i, (path, target) in enumerate(to_create, 1):
        if dry_run:
            created += 1
            if i <= 20:
                log(f"  [dry-run] {path} -> {target}")
        else:
            if create_url_redirect(path, target):
                created += 1
            else:
                errors += 1
            time.sleep(RATE_LIMIT_DELAY)

        if i % 200 == 0 or i == len(to_create):
            log(f"  Progress: {i}/{len(to_create)} ({created} ok, {errors} errors)")

    log(f"\n{'=' * 60}")
    log("COMPLETE")
    log(f"{'=' * 60}")
    log(f"  Created:  {created}")
    log(f"  Errors:   {errors}")
    log(f"  Skipped:  {skipped_existing} (already redirected) + {skipped_no_handle} (no handle)")

    if dry_run:
        log("\n  DRY RUN — no changes made. Run with --live to apply.")


if __name__ == '__main__':
    main()
