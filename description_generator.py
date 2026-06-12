#!/usr/bin/env python3
"""
Bulk Product Description Generator v3 for Kingsway Janitorial
Generates rich, unique product descriptions for products with thin/empty body content.

v3 CHANGES:
  - Content logic moved to product_content.py (shared with the daily sync,
    so the sync no longer overwrites these descriptions)
  - AI-written descriptions via the Claude API when ANTHROPIC_API_KEY is set
    (falls back to templates per-product on any failure)
  - --template flag forces template generation even when an API key exists

Environment Variables:
    SHOPIFY_STORE: Store URL (e.g., kingsway-janitorial.myshopify.com)
    SHOPIFY_ACCESS_TOKEN: Admin API access token (shpat_...)
    ANTHROPIC_API_KEY: (optional) enables AI-written descriptions
    ANTHROPIC_MODEL: (optional) Claude model id, default claude-opus-4-8

Usage:
    python description_generator.py --dry-run --limit 50 --export results.csv
    python description_generator.py --live --export results.csv
"""

import os, re, csv, time, argparse, requests
from datetime import datetime

from product_content import (
    ai_available, build_description, generate_descriptions_ai,
    get_template_key, strip_html,
)

SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', 'kingsway-janitorial.myshopify.com')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN', '')
API_VERSION = '2026-01'
MIN_DESCRIPTION_LENGTH = 80
RATE_LIMIT_DELAY = 0.75
REQUEST_TIMEOUT = 30
MAX_RETRIES = 5


def log(msg, level='INFO'):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [{level}] {msg}", flush=True)


class DescriptionGenerator:
    def __init__(self, store, token):
        self.store = store
        self.token = token
        self.graphql_url = f'https://{store}/admin/api/{API_VERSION}/graphql.json'
        self.headers = {'Content-Type': 'application/json', 'X-Shopify-Access-Token': token}
        self.stats = {'total_fetched': 0, 'thin_descriptions': 0, 'generated': 0,
                      'ai_generated': 0, 'updated': 0, 'skipped': 0, 'errors': 0}
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

    def fetch_thin_products(self, min_desc_length=MIN_DESCRIPTION_LENGTH):
        log("Fetching products from Shopify...")
        query = """query ($cursor: String) { products(first: 250, after: $cursor) {
            edges { node { id title descriptionHtml productType vendor handle status
                variants(first: 1) { nodes { sku } } } cursor }
            pageInfo { hasNextPage } } }"""
        all_count, thin = 0, []
        cursor = None
        while True:
            result = self._graphql(query, {'cursor': cursor} if cursor else None)
            edges = result.get('data', {}).get('products', {}).get('edges', [])
            pi = result.get('data', {}).get('products', {}).get('pageInfo', {})
            for edge in edges:
                node = edge['node']
                all_count += 1
                if len(strip_html(node.get('descriptionHtml') or '')) < min_desc_length:
                    thin.append(node)
            if not pi.get('hasNextPage'):
                break
            cursor = edges[-1]['cursor']
            if all_count % 1000 == 0:
                log(f"  Fetched {all_count} products...")
            time.sleep(RATE_LIMIT_DELAY)
        self.stats['total_fetched'] = all_count
        self.stats['thin_descriptions'] = len(thin)
        log(f"✓ {all_count} total, {len(thin)} with thin descriptions (< {min_desc_length} chars)")
        return thin

    def update_product_description(self, product_id, html):
        mutation = """mutation productUpdate($product: ProductUpdateInput!) {
            productUpdate(product: $product) { product { id } userErrors { field message } } }"""
        result = self._graphql(mutation, {'product': {'id': product_id, 'descriptionHtml': html}})
        errors = result.get('data', {}).get('productUpdate', {}).get('userErrors', [])
        if errors:
            log(f"Update error {product_id}: {errors}", 'WARNING')
            return False
        return bool(result.get('data', {}).get('productUpdate', {}).get('product'))

    def process(self, dry_run=True, limit=None, min_length=MIN_DESCRIPTION_LENGTH, use_ai=True):
        log("=" * 70)
        log("BULK DESCRIPTION GENERATOR v3 — Kingsway Janitorial")
        log("=" * 70)
        engine = 'Claude AI' if (use_ai and ai_available()) else 'templates'
        log(f"Store: {self.store} | Min length: {min_length} | Mode: {'DRY RUN' if dry_run else 'LIVE'} | Engine: {engine}")

        thin = self.fetch_thin_products(min_length)
        if limit:
            thin = thin[:limit]
            log(f"Limited to {limit}")
        if not thin:
            log("No products need enrichment!")
            return []

        # Generate AI descriptions up front (batched), then fill gaps with templates
        ai_descriptions = {}
        if use_ai and ai_available():
            items = []
            for prod in thin:
                sku = prod.get('variants', {}).get('nodes', [{}])[0].get('sku', '') or prod['id']
                items.append({'sku': sku, 'title': prod.get('title', ''),
                              'product_type': prod.get('productType', '')})
            log(f"Generating AI descriptions for {len(items)} products via Claude API...")
            ai_descriptions = generate_descriptions_ai(items)
            log(f"✓ AI generated {len(ai_descriptions)} descriptions ({len(items) - len(ai_descriptions)} will use templates)")

        log(f"\nWriting descriptions for {len(thin)} products...")
        for i, prod in enumerate(thin, 1):
            pid = prod['id']
            title = prod.get('title', '')
            pt = prod.get('productType', '')
            old_text = strip_html(prod.get('descriptionHtml') or '')
            sku = prod.get('variants', {}).get('nodes', [{}])[0].get('sku', '') or pid
            tk = get_template_key(pt, title)

            ai_desc = ai_descriptions.get(sku)
            new_html = build_description(title, pt, sku, jv_desc_html='', ai_desc=ai_desc)
            new_text = strip_html(new_html)
            if ai_desc:
                self.stats['ai_generated'] += 1

            r = {'product_id': pid, 'sku': sku, 'title': title, 'product_type': pt,
                 'template_key': tk, 'engine': 'ai' if ai_desc else 'template',
                 'old_length': len(old_text), 'new_length': len(new_text),
                 'new_description': new_html, 'new_description_text': new_text, 'status': 'pending'}

            if not dry_run:
                try:
                    if self.update_product_description(pid, new_html):
                        r['status'] = 'updated'; self.stats['updated'] += 1
                    else:
                        r['status'] = 'error'; self.stats['errors'] += 1
                except Exception as e:
                    log(f"Error updating {sku}: {e}", 'WARNING')
                    r['status'] = 'error'; self.stats['errors'] += 1
                time.sleep(RATE_LIMIT_DELAY)
            else:
                r['status'] = 'dry_run'

            self.stats['generated'] += 1
            self.results.append(r)
            if i % 100 == 0 or i == len(thin):
                log(f"  Progress: {i}/{len(thin)}")
        return self.results

    def export_results(self, filename='description_results.csv'):
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=['sku', 'title', 'product_type', 'template_key',
                                              'engine', 'old_length', 'new_length',
                                              'new_description_text', 'status'])
            w.writeheader()
            for r in self.results:
                w.writerow({k: r[k] for k in w.fieldnames})
        log(f"📄 Exported to {filename}")

    def print_summary(self):
        log("\n" + "=" * 70)
        log("SUMMARY")
        log("=" * 70)
        for k in ['total_fetched', 'thin_descriptions', 'generated', 'ai_generated', 'updated', 'errors']:
            log(f"  {k:25s} {self.stats[k]}")
        if self.results:
            tc = {}
            for r in self.results:
                tc[r['template_key']] = tc.get(r['template_key'], 0) + 1
            log("\nBy template:")
            for k, c in sorted(tc.items(), key=lambda x: -x[1]):
                log(f"  {c:5d}  {k}")

    def print_samples(self, count=5):
        log(f"\n📋 SAMPLES ({count}):")
        log("-" * 70)
        for r in self.results[:count]:
            log(f"\nSKU: {r['sku']} | Type: {r['product_type']} → {r['template_key']} ({r['engine']})")
            log(f"Title: {r['title']}")
            log(f"Desc: {r['new_description_text']}")
            log("-" * 40)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true', default=True)
    parser.add_argument('--live', action='store_true')
    parser.add_argument('--template', action='store_true',
                        help='Force template engine even if ANTHROPIC_API_KEY is set')
    parser.add_argument('--limit', type=int)
    parser.add_argument('--min-length', type=int, default=MIN_DESCRIPTION_LENGTH)
    parser.add_argument('--export', type=str, default='description_results.csv')
    parser.add_argument('--samples', type=int, default=10)
    args = parser.parse_args()

    token = os.environ.get('SHOPIFY_ACCESS_TOKEN', SHOPIFY_ACCESS_TOKEN)
    if not token:
        log("❌ SHOPIFY_ACCESS_TOKEN not set", 'ERROR')
        return
    store = os.environ.get('SHOPIFY_STORE', SHOPIFY_STORE)

    gen = DescriptionGenerator(store, token)
    gen.process(dry_run=not args.live, limit=args.limit,
                min_length=args.min_length, use_ai=not args.template)
    gen.export_results(args.export)
    gen.print_summary()
    gen.print_samples(args.samples)


if __name__ == '__main__':
    main()
