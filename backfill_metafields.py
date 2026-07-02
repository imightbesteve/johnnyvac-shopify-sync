#!/usr/bin/env python3
"""
Product Metadata Backfill - Kingsway Janitorial

Computes the structured `custom.*` metafields (mpn, brand, pack_quantity,
material, compatible_models, size_inches, voltage) from each product's title,
type and SKU, and writes any that are missing or different. The daily sync
writes the same set for products it touches; this backfill covers the
thousands of products the delta never picks up.

Idempotent: products whose metafields already match are skipped.

Usage:
    python backfill_metafields.py --dry-run     # Preview only (default)
    python backfill_metafields.py --live        # Actually update

Environment Variables:
    SHOPIFY_STORE: Store URL
    SHOPIFY_CLIENT_ID / SHOPIFY_CLIENT_SECRET: preferred (runtime-minted token)
    SHOPIFY_ACCESS_TOKEN: static token fallback
"""

import os
import sys
import time
import argparse
import requests
from datetime import datetime
from typing import Dict, List

from product_content import (
    extract_brand, extract_compatible_models, extract_dimensions,
    extract_material, extract_pack_quantity,
)
from shopify_auth import get_access_token

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
METAFIELDS_PER_CALL = 25  # metafieldsSet hard limit

MANAGED_KEYS = ('mpn', 'brand', 'pack_quantity', 'material',
                'compatible_models', 'size_inches', 'voltage')


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
            if result.get('data') is None:
                result['data'] = {}
            return result
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            if attempt < MAX_RETRIES - 1:
                wait = (attempt + 1) * 10
                log(f"Connection error, retry {attempt+1}/{MAX_RETRIES} in {wait}s...", 'WARNING')
                time.sleep(wait)
            else:
                raise
    return {}


def desired_metafields(product_id: str, title: str, product_type: str, sku: str) -> List[Dict]:
    """Same metadata the daily sync writes (keep in step with build_metafields)."""
    fields = []

    def add(key, value, mtype='single_line_text_field'):
        if value not in (None, '', []):
            fields.append({
                "ownerId": product_id,
                "namespace": "custom",
                "key": key,
                "value": str(value),
                "type": mtype
            })

    add('mpn', sku)
    add('brand', extract_brand(title))
    add('pack_quantity', extract_pack_quantity(title), 'number_integer')
    add('material', extract_material(title))
    add('compatible_models', ', '.join(extract_compatible_models(title, product_type)))
    specs = extract_dimensions(title)
    add('size_inches', specs.get('size_inches'))
    add('voltage', specs.get('voltage'))
    return fields


def fetch_all_products() -> List[Dict]:
    """All products with title/type/SKU and current custom.* metafield values."""
    log("Fetching all products...")

    query = """
    query ($cursor: String) {
      products(first: 250, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        edges {
          node {
            id
            title
            productType
            variants(first: 1) { nodes { sku } }
            metafields(first: 20, namespace: "custom") {
              nodes { key value }
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
            variants = node.get('variants', {}).get('nodes', [])
            existing = {
                m['key']: m.get('value', '')
                for m in node.get('metafields', {}).get('nodes', [])
            }
            products.append({
                'id': node['id'],
                'title': node.get('title', '') or '',
                'product_type': node.get('productType', '') or '',
                'sku': (variants[0].get('sku') or '') if variants else '',
                'existing': existing,
            })

        if not page_info.get('hasNextPage'):
            break
        cursor = page_info['endCursor']

        if len(products) % 2500 < 250:
            log(f"  Fetched {len(products)} products...")
        time.sleep(RATE_LIMIT_DELAY)

    log(f"✓ Fetched {len(products)} total products")
    return products


def metafields_set(batch: List[Dict]) -> int:
    """Write up to 25 metafields (mixed owners). Returns count written."""
    mutation = """
    mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields { id }
        userErrors { field message }
      }
    }
    """
    result = graphql(mutation, {"metafields": batch})
    payload = result.get('data', {}).get('metafieldsSet', {}) or {}
    errors = payload.get('userErrors', [])
    if errors:
        log(f"  Batch errors: {errors[:3]}{'...' if len(errors) > 3 else ''}", 'WARNING')
    return len(payload.get('metafields') or [])


def main():
    parser = argparse.ArgumentParser(description='Backfill structured product metafields')
    parser.add_argument('--dry-run', action='store_true', default=True)
    parser.add_argument('--live', action='store_true')
    parser.add_argument('--limit', type=int, help='Only process the first N products')
    args = parser.parse_args()
    dry_run = not args.live

    log("=" * 60)
    log("PRODUCT METADATA BACKFILL")
    log("=" * 60)
    log(f"Store: {SHOPIFY_STORE}")
    log(f"Mode:  {'DRY RUN' if dry_run else 'LIVE'}")

    if not SHOPIFY_ACCESS_TOKEN:
        log("No Shopify credentials (set SHOPIFY_CLIENT_ID/SECRET or SHOPIFY_ACCESS_TOKEN)", 'ERROR')
        sys.exit(1)

    products = fetch_all_products()
    if args.limit:
        products = products[:args.limit]

    # Work out which metafields actually need writing
    pending: List[Dict] = []
    products_touched = 0
    up_to_date = 0

    for p in products:
        wanted = desired_metafields(p['id'], p['title'], p['product_type'], p['sku'])
        missing = [
            m for m in wanted
            if p['existing'].get(m['key'], None) != m['value']
        ]
        if missing:
            products_touched += 1
            pending.extend(missing)
        else:
            up_to_date += 1

    log(f"\nBREAKDOWN:")
    log(f"  Products already up to date: {up_to_date}")
    log(f"  Products needing metadata:   {products_touched}")
    log(f"  Metafields to write:         {len(pending)}")

    if not pending:
        log("\nNothing to do — all products already have their metadata!")
        return

    if dry_run:
        # Show a sample of what would be written
        log(f"\nDRY RUN — sample of what would be written:")
        for m in pending[:15]:
            log(f"  {m['ownerId'].split('/')[-1]}  custom.{m['key']} = {m['value']}")
        log(f"\nDRY RUN — no changes made. Run with --live to apply.")
        return

    log(f"\nWriting {len(pending)} metafields in batches of {METAFIELDS_PER_CALL}...")
    written = 0
    for start in range(0, len(pending), METAFIELDS_PER_CALL):
        batch = [
            {k: v for k, v in m.items()}
            for m in pending[start:start + METAFIELDS_PER_CALL]
        ]
        written += metafields_set(batch)
        done = min(start + METAFIELDS_PER_CALL, len(pending))
        if done % 500 < METAFIELDS_PER_CALL or done == len(pending):
            log(f"  Progress: {done}/{len(pending)} ({written} written)")
        time.sleep(RATE_LIMIT_DELAY)

    log(f"\n{'=' * 60}")
    log("COMPLETE")
    log(f"{'=' * 60}")
    log(f"  Metafields written: {written}")
    log(f"  Products touched:   {products_touched}")


if __name__ == '__main__':
    main()
