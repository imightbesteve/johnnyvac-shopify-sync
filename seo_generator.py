#!/usr/bin/env python3
"""
SEO Metadata Generator v2 for Kingsway Janitorial Shopify Store
Generates optimized SEO meta titles and meta descriptions for products.

v2 CHANGES:
  - Migrated from the deprecated REST Admin API to GraphQL (API 2026-01)
  - Generation logic moved to product_content.py (shared with the daily
    sync, which now sets SEO metadata on newly created products)
  - --only-missing flag: only fill products that have no SEO title yet
    (the daily sync keeps new products covered, so this is the safe default
    for re-runs)

Environment Variables:
    SHOPIFY_STORE: Store URL (e.g., kingsway-janitorial.myshopify.com)
    SHOPIFY_ACCESS_TOKEN: Admin API access token (shpat_...)

Usage:
    python seo_generator.py --dry-run --export seo_results.csv
    python seo_generator.py --only-missing --export seo_results.csv
"""

import os
import csv
import time
import argparse
import requests
from datetime import datetime

from product_content import generate_seo_title, generate_seo_description

SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', 'kingsway-janitorial.myshopify.com')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN', '')
API_VERSION = '2026-01'
RATE_LIMIT_DELAY = 0.5
REQUEST_TIMEOUT = 30
MAX_RETRIES = 5


def log(msg, level='INFO'):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [{level}] {msg}", flush=True)


class SEOGenerator:
    def __init__(self, store, token):
        self.store = store
        self.token = token
        self.graphql_url = f"https://{store}/admin/api/{API_VERSION}/graphql.json"
        self.headers = {
            "X-Shopify-Access-Token": token,
            "Content-Type": "application/json"
        }
        self.stats = {"processed": 0, "updated": 0, "skipped": 0, "errors": 0}
        self.results = []

    def _graphql(self, query, variables=None):
        payload = {'query': query}
        if variables:
            payload['variables'] = variables
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.post(self.graphql_url, json=payload, headers=self.headers, timeout=REQUEST_TIMEOUT)
                if resp.status_code in (429, 503):
                    wait = min(int(resp.headers.get('Retry-After', (attempt + 1) * 10)), 60)
                    log(f"HTTP {resp.status_code}, retry {attempt+1}/{MAX_RETRIES} in {wait}s...", 'WARNING')
                    if attempt < MAX_RETRIES - 1:
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

    def get_all_products(self):
        """Fetch all products with their current SEO fields."""
        query = """query ($cursor: String) { products(first: 250, after: $cursor) {
            edges { node { id title handle
                seo { title description }
                variants(first: 1) { nodes { sku } } } cursor }
            pageInfo { hasNextPage } } }"""
        products = []
        cursor = None
        while True:
            result = self._graphql(query, {'cursor': cursor} if cursor else None)
            edges = result.get('data', {}).get('products', {}).get('edges', [])
            pi = result.get('data', {}).get('products', {}).get('pageInfo', {})
            for edge in edges:
                products.append(edge['node'])
            if len(products) % 1000 < 250 and products:
                print(f"  Fetched {len(products)} products...")
            if not pi.get('hasNextPage'):
                break
            cursor = edges[-1]['cursor']
            time.sleep(RATE_LIMIT_DELAY)
        return products

    def update_product_seo(self, product_id, seo_title, seo_description):
        mutation = """mutation productUpdate($product: ProductUpdateInput!) {
            productUpdate(product: $product) { product { id } userErrors { field message } } }"""
        result = self._graphql(mutation, {'product': {
            'id': product_id,
            'seo': {'title': seo_title, 'description': seo_description}
        }})
        errors = result.get('data', {}).get('productUpdate', {}).get('userErrors', [])
        if errors:
            log(f"Update error {product_id}: {errors}", 'WARNING')
            return False
        return bool(result.get('data', {}).get('productUpdate', {}).get('product'))

    def process_products(self, dry_run=False, limit=None, only_missing=False):
        print("\n" + "=" * 60)
        print("SEO METADATA GENERATOR v2 - Kingsway Janitorial")
        print("=" * 60)

        print("\n📥 Fetching products from Shopify...")
        products = self.get_all_products()

        if only_missing:
            before = len(products)
            products = [p for p in products if not (p.get('seo') or {}).get('title')]
            print(f"  {before - len(products)} products already have SEO titles (skipped)")

        if limit:
            products = products[:limit]

        print(f"\n✅ Found {len(products)} products to process")
        if dry_run:
            print("\n🔍 DRY RUN MODE - No changes will be made")

        print("\n" + "-" * 60)
        print("Processing products...")
        print("-" * 60 + "\n")

        for i, product in enumerate(products, 1):
            product_id = product.get("id")
            title = product.get("title", "Unknown")
            handle = product.get("handle", "")
            variants = product.get('variants', {}).get('nodes', [])
            sku = variants[0].get('sku', '') if variants else ''

            seo_title = generate_seo_title(title, sku)
            seo_description = generate_seo_description(title, sku)

            result = {
                "id": product_id,
                "handle": handle,
                "original_title": title,
                "seo_title": seo_title,
                "seo_description": seo_description,
                "status": "pending"
            }

            if not dry_run:
                if self.update_product_seo(product_id, seo_title, seo_description):
                    result["status"] = "updated"
                    self.stats["updated"] += 1
                else:
                    result["status"] = "error"
                    self.stats["errors"] += 1
                time.sleep(RATE_LIMIT_DELAY)
            else:
                result["status"] = "dry_run"

            self.results.append(result)
            self.stats["processed"] += 1

            if i % 50 == 0 or i == len(products):
                print(f"  Processed {i}/{len(products)} products...")

        return self.results

    def export_results(self, filename=None):
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"seo_results_{timestamp}.csv"
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                "handle", "original_title", "seo_title", "seo_description", "status"
            ])
            writer.writeheader()
            for result in self.results:
                writer.writerow({
                    "handle": result["handle"],
                    "original_title": result["original_title"],
                    "seo_title": result["seo_title"],
                    "seo_description": result["seo_description"],
                    "status": result["status"]
                })
        return filename

    def print_summary(self):
        print("\n" + "=" * 60)
        print("PROCESSING SUMMARY")
        print("=" * 60)
        print(f"  Total processed: {self.stats['processed']}")
        print(f"  Updated:         {self.stats['updated']}")
        print(f"  Errors:          {self.stats['errors']}")
        print("=" * 60)


def main():
    store = os.environ.get("SHOPIFY_STORE", SHOPIFY_STORE)
    token = os.environ.get("SHOPIFY_ACCESS_TOKEN", SHOPIFY_ACCESS_TOKEN)

    if not token:
        print("❌ Error: SHOPIFY_ACCESS_TOKEN not set")
        return

    parser = argparse.ArgumentParser(description="Generate SEO metadata for Shopify products")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without updating")
    parser.add_argument("--limit", type=int, help="Limit number of products to process")
    parser.add_argument("--export", type=str, help="Export results to CSV file")
    parser.add_argument("--only-missing", action="store_true",
                        help="Only process products without an existing SEO title")
    args = parser.parse_args()

    generator = SEOGenerator(store, token)
    results = generator.process_products(dry_run=args.dry_run, limit=args.limit,
                                         only_missing=args.only_missing)

    if args.export or args.dry_run:
        filename = generator.export_results(args.export)
        print(f"\n📄 Results exported to: {filename}")

    generator.print_summary()

    print("\n📋 SAMPLE RESULTS (first 5):")
    print("-" * 60)
    for result in results[:5]:
        print(f"\nOriginal: {result['original_title'][:50]}...")
        print(f"SEO Title: {result['seo_title']}")
        print(f"SEO Desc:  {result['seo_description']}")


if __name__ == "__main__":
    main()
