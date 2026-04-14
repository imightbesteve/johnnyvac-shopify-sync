#!/usr/bin/env python3
"""
One-Time MPN Metafield Backfill - Kingsway Janitorial

Sets custom.mpn metafield (from variant SKU) on all products that don't have it yet.
Run once to catch the ~4,895 products that were "unchanged" during the v3.4 sync.

Usage:
    python backfill_mpn.py --dry-run          # Preview only
    python backfill_mpn.py --live             # Actually update

Environment Variables:
    SHOPIFY_STORE: Store URL
    SHOPIFY_ACCESS_TOKEN: Admin API access token
"""

import os
import sys
import json
import time
import requests
import argparse
from datetime import datetime

# Configuration
SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', 'kingsway-janitorial.myshopify.com')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN', '')
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
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < MAX_RETRIES - 1:
                wait = (attempt + 1) * 10
                log(f"Connection error, retry {attempt+1}/{MAX_RETRIES} in {wait}s...", 'WARNING')
                time.sleep(wait)
            else:
                raise
    return {}


def fetch_all_products():
    """Fetch all products with their SKU and existing custom.mpn metafield."""
    log("Fetching all products...")

    query = """
    query ($cursor: String) {
      products(first: 250, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        edges {
          node {
            id
            variants(first: 1) {
              nodes { sku }
            }
            mpn: metafield(namespace: "custom", key: "mpn") {
              id
              value
            }
          }
        }
      }
    }
    """

    products = []
    cursor = None

    while True:
        result = graphql(query, {'cursor': cursor} if cursor else None)
        data = result.get('data', {}).get('products', {})
        edges = data.get('edges', [])
        page_info = data.get('pageInfo', {})

        for edge in edges:
            node = edge['node']
            sku = ''
            variants = node.get('variants', {}).get('nodes', [])
            if variants:
                sku = variants[0].get('sku', '') or ''

            mpn_field = node.get('mpn')
            existing_mpn = mpn_field.get('value', '') if mpn_field else ''

            products.append({
                'id': node['id'],
                'sku': sku,
                'existing_mpn': existing_mpn
            })

        if not page_info.get('hasNextPage'):
            break
        cursor = page_info['endCursor']

        if len(products) % 1000 == 0:
            log(f"  Fetched {len(products)} products...")

        time.sleep(RATE_LIMIT_DELAY)

    log(f"✓ Fetched {len(products)} total products")
    return products


def set_mpn_metafield(product_id, sku):
    """Set the custom.mpn metafield on a single product."""
    mutation = """
    mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields { id }
        userErrors { field message }
      }
    }
    """

    variables = {
        "metafields": [{
            "ownerId": product_id,
            "namespace": "custom",
            "key": "mpn",
            "value": sku,
            "type": "single_line_text_field"
        }]
    }

    result = graphql(mutation, variables)
    errors = result.get('data', {}).get('metafieldsSet', {}).get('userErrors', [])
    if errors:
        log(f"  Error on {product_id}: {errors}", 'WARNING')
        return False
    return bool(result.get('data', {}).get('metafieldsSet', {}).get('metafields'))


def main():
    parser = argparse.ArgumentParser(description='Backfill MPN metafield from variant SKU')
    parser.add_argument('--dry-run', action='store_true', default=True)
    parser.add_argument('--live', action='store_true')
    args = parser.parse_args()
    dry_run = not args.live

    log("=" * 60)
    log("MPN METAFIELD BACKFILL")
    log("=" * 60)
    log(f"Store: {SHOPIFY_STORE}")
    log(f"Mode:  {'DRY RUN' if dry_run else 'LIVE'}")

    if not SHOPIFY_ACCESS_TOKEN:
        log("SHOPIFY_ACCESS_TOKEN not set", 'ERROR')
        sys.exit(1)

    # Fetch all products
    products = fetch_all_products()

    # Categorize
    already_set = []
    needs_backfill = []
    no_sku = []

    for p in products:
        if not p['sku']:
            no_sku.append(p)
        elif p['existing_mpn'] == p['sku']:
            already_set.append(p)
        else:
            needs_backfill.append(p)

    log(f"\nBREAKDOWN:")
    log(f"  Already has correct MPN: {len(already_set)}")
    log(f"  Needs backfill:          {len(needs_backfill)}")
    log(f"  No SKU (skipping):       {len(no_sku)}")

    if not needs_backfill:
        log("\nNothing to do — all products already have MPN set!")
        return

    # Process backfill
    log(f"\n{'DRY RUN - ' if dry_run else ''}Setting MPN on {len(needs_backfill)} products...")

    updated = 0
    errors = 0

    for i, p in enumerate(needs_backfill, 1):
        if dry_run:
            updated += 1
        else:
            if set_mpn_metafield(p['id'], p['sku']):
                updated += 1
            else:
                errors += 1
            time.sleep(RATE_LIMIT_DELAY)

        if i % 200 == 0 or i == len(needs_backfill):
            log(f"  Progress: {i}/{len(needs_backfill)} ({updated} ok, {errors} errors)")

    # Summary
    log(f"\n{'=' * 60}")
    log(f"COMPLETE")
    log(f"{'=' * 60}")
    log(f"  Updated:  {updated}")
    log(f"  Errors:   {errors}")
    log(f"  Skipped:  {len(already_set)} (already set) + {len(no_sku)} (no SKU)")

    if dry_run:
        log(f"\n  DRY RUN — no changes made. Run with --live to apply.")


if __name__ == '__main__':
    main()
