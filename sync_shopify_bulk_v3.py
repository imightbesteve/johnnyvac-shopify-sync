#!/usr/bin/env python3
# sync_shopify_bulk_v3.py
"""
JohnnyVac to Shopify Sync v4.0 - CONTENT, SEO & INVENTORY OVERHAUL

CHANGES from v3.4:
- FIXED: inventory quantities are now actually written to Shopify
  (inventorySetQuantities, batched 250/call). Previously the delta flagged
  inventory changes but never applied them, so the same products were
  re-updated every single day and stock levels went stale.
- FIXED: price changes are now applied in the bulk path too (second bulk
  mutation with productVariantsBulkUpdate). Previously bulk updates silently
  skipped price.
- FIXED: rich descriptions are PROTECTED. The sync only writes a description
  when the existing one is thin (< 80 chars of text); enriched descriptions
  from description_generator.py / the AI engine are never overwritten.
- NEW: new/thin products get a rich description at sync time (Claude AI via
  ANTHROPIC_API_KEY when available, template fallback otherwise).
- NEW: SEO meta title/description set on create, and backfilled on products
  that have none.
- NEW: vendor = real detected brand (Hoover, Miele, ...) instead of
  hardcoding "JohnnyVac" on everything. Brand + MPN is the GTIN substitute
  Google uses, so brand accuracy matters.
- NEW: Shopify Standard Product Taxonomy category set per product (powers
  Google Merchant Center categorization / structured data).
- NEW: manual tags are preserved — only managed tags (category handle,
  confidence:*, source:*) are replaced; everything else is kept.
- NEW: archiving a product now creates a 301 URL redirect to its collection
  page (fixes the GSC "Not found (404)" errors).
- productUpdate migrated from the deprecated `input` argument to `product`
  (ProductUpdateInput) for API 2026-01.
"""

import os
import re
import csv
import json
import time
import requests
import threading
from typing import Dict, List, Optional, Tuple, Set
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from categorizer_v4 import ProductCategorizer
from product_content import (
    ai_available, build_description, compute_vendor,
    generate_descriptions_ai, generate_seo_description, generate_seo_title,
    strip_html, taxonomy_for_handle,
)

# =============================================================================
# CONFIGURATION
# =============================================================================

SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', 'kingsway-janitorial.myshopify.com')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')
CSV_URL = 'https://www.johnnyvacstock.com/sigm_all_jv_products/JVWebProducts.csv'
IMAGE_BASE_URL = 'https://www.johnnyvacstock.com/photos/web/'

LANGUAGE = 'en'
DRY_RUN = os.environ.get('DRY_RUN', 'false').lower() == 'true'
ARCHIVE_MISSING = os.environ.get('ARCHIVE_MISSING', 'true').lower() == 'true'

# Out-of-stock products stay ACTIVE and show as "Sold out" instead of being
# unpublished to DRAFT (which 404s the URL and churns Google's index every
# time stock flips). Set KEEP_OOS_ACTIVE=false to restore the old behavior.
KEEP_OOS_ACTIVE = os.environ.get('KEEP_OOS_ACTIVE', 'true').lower() == 'true'

# Create 301 redirects (archived product URL -> its collection) when archiving.
# Requires the write_online_store_navigation scope on the access token; if the
# scope is missing, redirects are skipped automatically (archiving still runs).
CREATE_REDIRECTS = os.environ.get('CREATE_REDIRECTS', 'true').lower() == 'true'

# Description shorter than this (text chars) counts as "thin" and gets enriched
MIN_DESCRIPTION_LENGTH = 80

# API settings
API_VERSION = '2026-01'
GRAPHQL_URL = f'https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/graphql.json'
HEADERS = {
    'Content-Type': 'application/json',
    'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN
}

# Rate limiting - Shopify allows 2/sec for standard, 4/sec for Plus
RATE_LIMIT_PER_SECOND = 2
MAX_CONCURRENT = 4
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3

# Bulk operation settings
POLL_INTERVAL = 10
MAX_POLL_TIME = 3600  # 1 hour max per bulk operation

# Tags the sync owns (everything else on a product is preserved)
MANAGED_TAG_PREFIXES = ('confidence:', 'source:')

# =============================================================================
# LOGGING
# =============================================================================

def log(message: str, level: str = 'INFO'):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] [{level}] {message}", flush=True)

# =============================================================================
# THREAD-SAFE RATE LIMITER
# =============================================================================

class RateLimiter:
    def __init__(self, requests_per_second: float = 2.0):
        self.requests_per_second = requests_per_second
        self.requests: List[float] = []
        self._lock = threading.Lock()

    def throttle(self):
        with self._lock:
            now = time.time()
            self.requests = [t for t in self.requests if now - t < 1.0]

            if len(self.requests) >= self.requests_per_second:
                oldest = min(self.requests)
                wait_time = 1.0 - (now - oldest) + 0.05
                if wait_time > 0:
                    self._lock.release()
                    try:
                        time.sleep(wait_time)
                    finally:
                        self._lock.acquire()
                    return self.throttle()

            self.requests.append(time.time())

rate_limiter = RateLimiter(RATE_LIMIT_PER_SECOND)

# =============================================================================
# GRAPHQL HELPERS
# =============================================================================

def graphql_request(query: str, variables: Optional[Dict] = None, use_rate_limit: bool = True) -> Dict:
    """Make a GraphQL request to Shopify"""
    if use_rate_limit:
        rate_limiter.throttle()

    payload = {'query': query}
    if variables:
        payload['variables'] = variables

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(GRAPHQL_URL, json=payload, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            result = response.json()

            if 'errors' in result:
                log(f"GraphQL errors: {result['errors']}", 'WARNING')

            return result

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < MAX_RETRIES - 1:
                wait = (attempt + 1) * 5
                log(f"Connection error, retry {attempt + 1}/{MAX_RETRIES} in {wait}s...", 'WARNING')
                time.sleep(wait)
            else:
                raise

    return {}

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def clean_html(html_str: str) -> str:
    if not html_str:
        return ''
    html_str = re.sub(r'<meta[^>]*>', '', html_str, flags=re.IGNORECASE)
    html_str = re.sub(r'<p>\s*(<i></i>)?\s*(&nbsp;)?\s*</p>', '', html_str, flags=re.IGNORECASE)
    html_str = re.sub(r'<i>\s*</i>', '', html_str, flags=re.IGNORECASE)
    html_str = re.sub(r'generatedBy="[^"]*"', '', html_str)
    html_str = re.sub(r'\s+', ' ', html_str).strip()
    return html_str if html_str not in ['', '<p></p>', ' '] else ''

def normalize_price(price_str: str) -> str:
    try:
        return f"{float(price_str):.2f}"
    except (ValueError, TypeError):
        return "0.00"

def build_metafields(sku: str) -> List[Dict]:
    """custom.mpn = JohnnyVac SKU. Google accepts Brand + MPN instead of GTIN."""
    metafields = []
    if sku:
        metafields.append({
            "namespace": "custom",
            "key": "mpn",
            "value": sku,
            "type": "single_line_text_field"
        })
    return metafields

def merge_tags(existing_tags: List[str], managed_tags: List[str], known_handles: Set[str]) -> List[str]:
    """Replace only the tags this sync owns; preserve everything added manually."""
    preserved = [
        t for t in (existing_tags or [])
        if not t.startswith(MANAGED_TAG_PREFIXES) and t not in known_handles
    ]
    return sorted(set(preserved) | set(managed_tags))

# =============================================================================
# DESIRED STATE (per CSV product)
# =============================================================================

def build_desired_state(product: Dict) -> Dict:
    """Compute everything we want Shopify to hold for this CSV row."""
    sku = product.get('SKU', '')
    category_info = product.get('category', {})
    title = product.get('ProductTitleEN' if LANGUAGE == 'en' else 'ProductTitleFR', '') or sku
    jv_desc = clean_html(product.get('ProductDescriptionEN' if LANGUAGE == 'en' else 'ProductDescriptionFR', ''))
    inventory = int(float(product.get('Inventory', '0') or 0))
    handle_tag = category_info.get('handle', 'uncategorized')

    return {
        'sku': sku,
        'title': title,
        'jv_desc': jv_desc,
        'jv_desc_is_rich': len(strip_html(jv_desc)) >= MIN_DESCRIPTION_LENGTH,
        'price': normalize_price(product.get('RegularPrice', '0.00')),
        'inventory': inventory,
        # inventoryPolicy DENY already prevents overselling; keeping the page
        # ACTIVE preserves its Google indexing while it shows "Sold out"
        'status': 'ACTIVE' if (KEEP_OOS_ACTIVE or inventory > 0) else 'DRAFT',
        'product_type': category_info.get('product_type', 'Other > Needs Review'),
        'vendor': compute_vendor(title),
        'category_gid': taxonomy_for_handle(handle_tag),
        'handle_tag': handle_tag,
        'managed_tags': [
            handle_tag,
            f"confidence:{category_info.get('confidence', 'low')}",
            f"source:{category_info.get('source', 'unknown')}"
        ],
        'upc': product.get('upc', '') or None,
    }

# =============================================================================
# CSV FETCHING
# =============================================================================

def fetch_csv_data() -> Tuple[List[Dict], List[str]]:
    log(f"Fetching CSV from: {CSV_URL}")

    response = requests.get(CSV_URL, timeout=60)
    response.raise_for_status()
    lines = response.text.splitlines()

    reader = csv.DictReader(lines, delimiter=';')

    products = []
    seen_skus: Set[str] = set()
    duplicate_skus: List[str] = []

    for row in reader:
        sku = (row.get('SKU') or '').strip()
        if not sku:
            continue
        if sku in seen_skus:
            duplicate_skus.append(sku)
            continue
        seen_skus.add(sku)
        cleaned_row = {k: (v.strip() if v else '') for k, v in row.items()}
        products.append(cleaned_row)

    log(f"✓ Parsed {len(products)} products from CSV")
    if duplicate_skus:
        log(f"  Found {len(duplicate_skus)} duplicate SKUs", 'WARNING')

    return products, duplicate_skus

# =============================================================================
# LOCATION ID FETCH (needed for inventory)
# =============================================================================

_location_id_cache = None

def get_default_location_id() -> Optional[str]:
    """Get the default location ID for inventory operations"""
    global _location_id_cache
    if _location_id_cache:
        return _location_id_cache

    query = """
    query {
        locations(first: 1) {
            edges {
                node {
                    id
                    name
                }
            }
        }
    }
    """

    result = graphql_request(query, use_rate_limit=False)
    edges = result.get('data', {}).get('locations', {}).get('edges', [])
    if edges:
        _location_id_cache = edges[0]['node']['id']
        log(f"Using location: {edges[0]['node']['name']} ({_location_id_cache})")
        return _location_id_cache
    return None

# =============================================================================
# BULK QUERY - Fetch existing products (FAST!)
# =============================================================================

def get_existing_products_bulk() -> Dict[str, Dict]:
    """Use bulk operation to fetch all existing products - this is the fast part!"""
    log("Starting bulk query for existing products...")

    mutation = """
    mutation {
      bulkOperationRunQuery(
        query: \"\"\"
        {
          products {
            edges {
              node {
                id
                title
                handle
                vendor
                productType
                status
                tags
                description(truncateAt: 200)
                category {
                  id
                }
                seo {
                  title
                }
                variants(first: 5) {
                  edges {
                    node {
                      id
                      sku
                      price
                      inventoryQuantity
                      inventoryItem {
                        id
                      }
                    }
                  }
                }
              }
            }
          }
        }
        \"\"\"
      ) {
        bulkOperation {
          id
          status
        }
        userErrors {
          field
          message
        }
      }
    }
    """

    result = graphql_request(mutation, use_rate_limit=False)

    errors = result.get('data', {}).get('bulkOperationRunQuery', {}).get('userErrors', [])
    if errors:
        log(f"Bulk query errors: {errors}", 'ERROR')
        raise Exception(f"Bulk query failed: {errors}")

    return poll_and_download_bulk_results()

def poll_and_download_bulk_results() -> Dict[str, Dict]:
    """Poll bulk operation and download results"""
    log("Polling for bulk query completion...")

    query = """
    query {
      currentBulkOperation {
        id
        status
        errorCode
        objectCount
        url
      }
    }
    """

    start_time = time.time()

    while time.time() - start_time < MAX_POLL_TIME:
        result = graphql_request(query, use_rate_limit=False)
        operation = result.get('data', {}).get('currentBulkOperation')

        if not operation:
            time.sleep(POLL_INTERVAL)
            continue

        status = operation.get('status')
        count = operation.get('objectCount', 0)
        log(f"  Bulk operation status: {status}, objects: {count}")

        if status == 'COMPLETED':
            url = operation.get('url')
            if url:
                return download_bulk_results(url)
            return {}
        elif status in ['FAILED', 'CANCELED']:
            raise Exception(f"Bulk operation failed: {operation.get('errorCode')}")

        time.sleep(POLL_INTERVAL)

    raise Exception("Bulk operation timed out")

def _existing_from_product_node(obj: Dict) -> Dict:
    return {
        'product_id': obj['id'],
        'title': obj.get('title', ''),
        'handle': obj.get('handle', ''),
        'vendor': obj.get('vendor', ''),
        'product_type': obj.get('productType', ''),
        'status': obj.get('status', 'ACTIVE'),
        'tags': obj.get('tags') or [],
        'description_text': strip_html(obj.get('description', '') or ''),
        'category_id': (obj.get('category') or {}).get('id', '') or '',
        'seo_title': (obj.get('seo') or {}).get('title', '') or '',
    }

def download_bulk_results(url: str) -> Dict[str, Dict]:
    """Download and parse bulk query results"""
    log("Downloading bulk results...")

    response = requests.get(url, timeout=120)
    response.raise_for_status()

    products = {}
    current_product = None

    for line in response.text.strip().split('\n'):
        if not line:
            continue
        obj = json.loads(line)

        # Product line (has id but not sku)
        if 'id' in obj and 'sku' not in obj and '__parentId' not in obj:
            current_product = _existing_from_product_node(obj)
        # Variant line (has sku and __parentId)
        elif 'sku' in obj:
            sku = obj.get('sku')
            if sku and current_product:
                products[sku] = {
                    **current_product,
                    'variant_id': obj['id'],
                    'inventory_item_id': (obj.get('inventoryItem') or {}).get('id', ''),
                    'price': obj.get('price', '0'),
                    'inventory': obj.get('inventoryQuantity', 0)
                }

    log(f"✓ Parsed {len(products)} existing products from Shopify")
    return products

def get_existing_products_paginated() -> Dict[str, Dict]:
    """Fallback: fetch products with pagination if bulk fails"""
    log("Using paginated fetch (fallback)...")
    products = {}
    cursor = None
    page = 0

    query = """
    query getProducts($cursor: String) {
        products(first: 100, after: $cursor) {
            edges {
                node {
                    id
                    title
                    handle
                    vendor
                    productType
                    status
                    tags
                    description(truncateAt: 200)
                    category { id }
                    seo { title }
                    variants(first: 5) {
                        edges {
                            node {
                                id
                                sku
                                price
                                inventoryQuantity
                                inventoryItem { id }
                            }
                        }
                    }
                }
                cursor
            }
            pageInfo { hasNextPage }
        }
    }
    """

    while True:
        page += 1
        result = graphql_request(query, {'cursor': cursor} if cursor else None)

        edges = result.get('data', {}).get('products', {}).get('edges', [])
        page_info = result.get('data', {}).get('products', {}).get('pageInfo', {})

        for edge in edges:
            node = edge['node']
            base = _existing_from_product_node(node)
            for var_edge in node.get('variants', {}).get('edges', []):
                variant = var_edge['node']
                sku = variant.get('sku')
                if sku:
                    products[sku] = {
                        **base,
                        'variant_id': variant['id'],
                        'inventory_item_id': (variant.get('inventoryItem') or {}).get('id', ''),
                        'price': variant.get('price', '0'),
                        'inventory': variant.get('inventoryQuantity', 0)
                    }

        if page % 20 == 0:
            log(f"  Page {page}, products: {len(products)}")

        if not page_info.get('hasNextPage'):
            break
        cursor = edges[-1]['cursor']

    log(f"✓ Fetched {len(products)} existing products")
    return products

# =============================================================================
# DELTA CALCULATION
# =============================================================================

def calculate_delta(
    csv_products: List[Dict],
    existing_products: Dict[str, Dict],
    known_handles: Set[str]
) -> Tuple[List[Dict], List[Dict], List[Dict], List[str]]:
    """Calculate what needs to be created, updated, or archived"""

    to_create = []
    to_update = []
    unchanged = []
    csv_skus = set()

    counts = {'core': 0, 'price': 0, 'inventory': 0, 'vendor': 0, 'tags': 0,
              'seo': 0, 'description': 0, 'category': 0}

    for product in csv_products:
        sku = product.get('SKU', '')
        csv_skus.add(sku)
        desired = build_desired_state(product)
        product['_desired'] = desired

        if sku not in existing_products:
            to_create.append(product)
            continue

        existing = existing_products[sku]
        final_tags = merge_tags(existing.get('tags', []), desired['managed_tags'], known_handles)
        product['_final_tags'] = final_tags

        flags = {
            'core': (
                existing['title'] != desired['title'] or
                existing['product_type'] != desired['product_type'] or
                existing['status'] != desired['status']
            ),
            'price': normalize_price(existing['price']) != desired['price'],
            'inventory': existing.get('inventory', 0) != desired['inventory'],
            'vendor': existing.get('vendor', '') != desired['vendor'],
            'tags': set(existing.get('tags', [])) != set(final_tags),
            'seo': not existing.get('seo_title'),
            'description': len(existing.get('description_text', '')) < MIN_DESCRIPTION_LENGTH,
            'category': bool(desired['category_gid']) and existing.get('category_id', '') != desired['category_gid'],
        }

        if any(flags.values()):
            for k, v in flags.items():
                if v:
                    counts[k] += 1
            product['_existing'] = existing
            product['_flags'] = flags
            to_update.append(product)
        else:
            unchanged.append(product)

    missing_skus = [sku for sku in existing_products if sku not in csv_skus]

    log(f"\nDELTA SUMMARY:")
    log(f"  To CREATE: {len(to_create)}")
    log(f"  To UPDATE: {len(to_update)}")
    for k, v in counts.items():
        if v:
            log(f"    - {k} changes: {v}")
    log(f"  UNCHANGED: {len(unchanged)} (skipping)")
    log(f"  MISSING (will archive): {len(missing_skus)}")

    return to_create, to_update, unchanged, missing_skus

# =============================================================================
# DESCRIPTION GENERATION PASS (AI with template fallback)
# =============================================================================

def needs_generated_description(product: Dict) -> bool:
    """True when neither Shopify nor the JV feed has a rich description."""
    desired = product['_desired']
    if desired['jv_desc_is_rich']:
        return False
    if '_existing' in product:
        return product.get('_flags', {}).get('description', False)
    return True  # new product with thin JV description

def generate_missing_descriptions(products: List[Dict]):
    """One batched AI pass for every product that needs a generated description.
    Results land in product['_ai_desc']; build_description() falls back to
    templates for anything the AI pass didn't return."""
    needing = [p for p in products if needs_generated_description(p)]
    if not needing:
        return

    if ai_available() and not DRY_RUN:
        log(f"Generating AI descriptions for {len(needing)} products (Claude API)...")
        items = [{'sku': p['_desired']['sku'],
                  'title': p['_desired']['title'],
                  'product_type': p['_desired']['product_type']} for p in needing]
        ai_results = generate_descriptions_ai(items)
        for p in needing:
            p['_ai_desc'] = ai_results.get(p['_desired']['sku'])
        log(f"✓ AI generated {len(ai_results)} descriptions "
            f"({len(needing) - len(ai_results)} will use templates)")
    else:
        engine = 'DRY RUN' if DRY_RUN else 'no ANTHROPIC_API_KEY'
        log(f"{len(needing)} products need generated descriptions (templates — {engine})")

def description_for(product: Dict) -> str:
    desired = product['_desired']
    return build_description(
        desired['title'], desired['product_type'], desired['sku'],
        jv_desc_html=desired['jv_desc'], ai_desc=product.get('_ai_desc'),
        min_length=MIN_DESCRIPTION_LENGTH,
    )

# =============================================================================
# INPUT BUILDERS
# =============================================================================

def build_create_input(product: Dict, location_id: Optional[str]) -> Dict:
    """ProductSetInput for a brand-new product."""
    d = product['_desired']

    variant_input = {
        "sku": d['sku'],
        "price": d['price'],
        "barcode": d['upc'],
        "inventoryPolicy": "DENY",
        "inventoryItem": {"tracked": True},
        "optionValues": [
            {"optionName": "Title", "name": "Default Title"}
        ]
    }
    if location_id and d['inventory'] > 0:
        variant_input["inventoryQuantities"] = [{
            "locationId": location_id,
            "name": "available",
            "quantity": d['inventory']
        }]

    product_input = {
        "title": d['title'],
        "descriptionHtml": description_for(product),
        "productType": d['product_type'],
        "vendor": d['vendor'],
        "status": d['status'],
        "tags": sorted(set(d['managed_tags'])),
        "metafields": build_metafields(d['sku']),
        "seo": {
            "title": generate_seo_title(d['title'], d['sku']),
            "description": generate_seo_description(d['title'], d['sku']),
        },
        "productOptions": [
            {"name": "Title", "values": [{"name": "Default Title"}]}
        ],
        "variants": [variant_input],
        "files": [{
            "originalSource": f"{IMAGE_BASE_URL}{d['sku']}.jpg",
            "contentType": "IMAGE"
        }]
    }
    if d['category_gid']:
        product_input["category"] = d['category_gid']
    return product_input

def build_update_input(product: Dict) -> Dict:
    """ProductUpdateInput for an existing product (product-level fields only;
    price goes through productVariantsBulkUpdate, inventory through
    inventorySetQuantities)."""
    d = product['_desired']
    existing = product['_existing']
    flags = product.get('_flags', {})

    update_input = {
        "id": existing['product_id'],
        "title": d['title'],
        "productType": d['product_type'],
        "vendor": d['vendor'],
        "tags": product.get('_final_tags', sorted(set(d['managed_tags']))),
        "status": d['status'],
        "metafields": build_metafields(d['sku']),
    }
    # Only write a description when the existing one is thin — never
    # clobber enriched content.
    if flags.get('description'):
        update_input["descriptionHtml"] = description_for(product)
    # Only set SEO when the product has none (manual SEO edits are kept).
    if flags.get('seo'):
        update_input["seo"] = {
            "title": generate_seo_title(d['title'], d['sku']),
            "description": generate_seo_description(d['title'], d['sku']),
        }
    if flags.get('category') and d['category_gid']:
        update_input["category"] = d['category_gid']
    return update_input

# =============================================================================
# PRODUCT CREATE - Using productSet (API 2024-01+)
# =============================================================================

PRODUCT_SET_MUTATION = """
mutation productSet($input: ProductSetInput!, $synchronous: Boolean!) {
    productSet(input: $input, synchronous: $synchronous) {
        product {
            id
        }
        userErrors {
            field
            message
            code
        }
    }
}
"""

def create_product(product_data: Dict) -> Optional[str]:
    """Create a single product using productSet mutation."""
    sku = product_data['_desired']['sku']

    if DRY_RUN:
        return f"dry-run-{sku}"

    location_id = get_default_location_id()
    variables = {
        "synchronous": True,
        "input": build_create_input(product_data, location_id)
    }

    result = graphql_request(PRODUCT_SET_MUTATION, variables)

    user_errors = result.get('data', {}).get('productSet', {}).get('userErrors', [])
    if user_errors:
        # Filter out non-critical errors (like image not found)
        critical_errors = [e for e in user_errors if e.get('code') not in ['MEDIA_ERROR', 'INVALID_URL']]
        if critical_errors:
            log(f"Create {sku} failed: {critical_errors}", 'WARNING')
            return None

    prod = result.get('data', {}).get('productSet', {}).get('product')
    if prod:
        return prod['id']

    return None

# =============================================================================
# PRODUCT UPDATE - productUpdate + productVariantsBulkUpdate
# =============================================================================

PRODUCT_UPDATE_MUTATION = """
mutation productUpdate($product: ProductUpdateInput!) {
    productUpdate(product: $product) {
        product {
            id
        }
        userErrors {
            field
            message
        }
    }
}
"""

VARIANT_PRICE_MUTATION = """
mutation productVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
    productVariantsBulkUpdate(productId: $productId, variants: $variants) {
        productVariants {
            id
        }
        userErrors {
            field
            message
        }
    }
}
"""

def update_product(product_data: Dict) -> bool:
    """Update an existing product (individual fallback path)."""
    existing = product_data['_existing']
    d = product_data['_desired']

    if DRY_RUN:
        return True

    result = graphql_request(PRODUCT_UPDATE_MUTATION, {"product": build_update_input(product_data)})

    errors = result.get('data', {}).get('productUpdate', {}).get('userErrors', [])
    if errors:
        log(f"Update product {d['sku']} failed: {errors}", 'WARNING')
        return False

    if product_data.get('_flags', {}).get('price'):
        result = graphql_request(VARIANT_PRICE_MUTATION, {
            "productId": existing['product_id'],
            "variants": [{"id": existing['variant_id'], "price": d['price']}]
        })
        errors = result.get('data', {}).get('productVariantsBulkUpdate', {}).get('userErrors', [])
        if errors:
            log(f"Update variant {d['sku']} failed: {errors}", 'WARNING')
            # Don't return False - product was updated, just variant failed

    return True

# =============================================================================
# INVENTORY SYNC — inventorySetQuantities, batched
# =============================================================================

INVENTORY_SET_MUTATION = """
mutation inventorySetQuantities($input: InventorySetQuantitiesInput!) {
    inventorySetQuantities(input: $input) {
        inventoryAdjustmentGroup {
            createdAt
        }
        userErrors {
            field
            message
            code
        }
    }
}
"""

def sync_inventory_quantities(products: List[Dict]) -> int:
    """Write absolute 'available' quantities for every product whose inventory
    changed. Batched 250 per call — this is the fix for inventory never
    actually being synced (and the same products re-updating every day)."""
    changed = [
        p for p in products
        if p.get('_flags', {}).get('inventory') and p.get('_existing', {}).get('inventory_item_id')
    ]
    if not changed:
        return 0

    location_id = get_default_location_id()
    if not location_id:
        log("No location available — skipping inventory sync", 'WARNING')
        return 0

    log(f"\nSyncing inventory for {len(changed)} products...")
    if DRY_RUN:
        log(f"[DRY RUN] Would set inventory on {len(changed)} products")
        return len(changed)

    updated = 0
    CHUNK = 250
    for start in range(0, len(changed), CHUNK):
        chunk = changed[start:start + CHUNK]
        quantities = [{
            "inventoryItemId": p['_existing']['inventory_item_id'],
            "locationId": location_id,
            "quantity": p['_desired']['inventory'],
        } for p in chunk]

        result = graphql_request(INVENTORY_SET_MUTATION, {"input": {
            "name": "available",
            "reason": "correction",
            "ignoreCompareQuantity": True,
            "quantities": quantities,
        }})
        errors = result.get('data', {}).get('inventorySetQuantities', {}).get('userErrors', [])
        if errors:
            log(f"  Inventory batch errors: {errors[:3]}{'...' if len(errors) > 3 else ''}", 'WARNING')
        else:
            updated += len(chunk)
        log(f"  Inventory progress: {min(start + CHUNK, len(changed))}/{len(changed)}")

    log(f"✓ Inventory synced for {updated} products")
    return updated

# =============================================================================
# BULK OPERATIONS - Try bulk first, fallback to individual
# =============================================================================

def run_bulk_mutation(jsonl_lines: List[Dict], mutation: str, expected_count: int,
                      label: str) -> Tuple[bool, int]:
    """Stage a JSONL file and run a bulkOperationRunMutation with it."""
    jsonl_file = 'bulk_input.jsonl'
    with open(jsonl_file, 'w', encoding='utf-8') as f:
        for line in jsonl_lines:
            f.write(json.dumps(line) + '\n')

    staged_mutation = """
    mutation {
      stagedUploadsCreate(input: [{
        resource: BULK_MUTATION_VARIABLES,
        filename: "bulk_input.jsonl",
        mimeType: "text/jsonl",
        httpMethod: POST
      }]) {
        stagedTargets {
          url
          resourceUrl
          parameters { name value }
        }
        userErrors { field message }
      }
    }
    """

    try:
        result = graphql_request(staged_mutation, use_rate_limit=False)

        errors = result.get('data', {}).get('stagedUploadsCreate', {}).get('userErrors', [])
        if errors:
            log(f"Staged upload error: {errors}", 'WARNING')
            return False, 0

        target = result['data']['stagedUploadsCreate']['stagedTargets'][0]
        upload_url = target['url']
        params = {p['name']: p['value'] for p in target['parameters']}
        # The 'key' parameter is what stagedUploadPath expects, not the full URL
        staged_path = params.get('key', target['resourceUrl'])

        with open(jsonl_file, 'rb') as f:
            files = {'file': ('bulk_input.jsonl', f, 'text/jsonl')}
            upload_response = requests.post(upload_url, data=params, files=files, timeout=300)
            upload_response.raise_for_status()

        log(f"✓ JSONL uploaded, starting bulk {label}...")

        bulk_mutation = f'''
        mutation {{
          bulkOperationRunMutation(
            mutation: {json.dumps(mutation)},
            stagedUploadPath: {json.dumps(staged_path)}
          ) {{
            bulkOperation {{ id status }}
            userErrors {{ field message }}
          }}
        }}
        '''

        result = graphql_request(bulk_mutation, use_rate_limit=False)

        errors = result.get('data', {}).get('bulkOperationRunMutation', {}).get('userErrors', [])
        if errors:
            log(f"Bulk mutation error: {errors}", 'WARNING')
            return False, 0

        return poll_bulk_mutation(expected_count)

    except Exception as e:
        log(f"Bulk {label} failed: {e}", 'WARNING')
        return False, 0


def try_bulk_create(products: List[Dict]) -> Tuple[bool, int]:
    """Create products via bulk productSet. Returns (success, count)."""
    if not products or DRY_RUN:
        return False, 0

    log("Attempting bulk create (fast method)...")
    location_id = get_default_location_id()

    lines = [{
        "input": build_create_input(p, location_id),
        "synchronous": True
    } for p in products]

    mutation = ("mutation call($input: ProductSetInput!, $synchronous: Boolean!) "
                "{ productSet(input: $input, synchronous: $synchronous) "
                "{ product { id } userErrors { field message } } }")
    return run_bulk_mutation(lines, mutation, len(products), 'create')


def try_bulk_update(products: List[Dict]) -> Tuple[bool, int]:
    """Update products via bulk productUpdate. Returns (success, count)."""
    if not products or DRY_RUN:
        return False, 0

    log("Attempting bulk update (fast method)...")
    lines = [{"product": build_update_input(p)} for p in products]

    mutation = ("mutation call($product: ProductUpdateInput!) "
                "{ productUpdate(product: $product) "
                "{ product { id } userErrors { field message } } }")
    return run_bulk_mutation(lines, mutation, len(products), 'update')


def try_bulk_price_update(products: List[Dict]) -> Tuple[bool, int]:
    """Apply price changes via bulk productVariantsBulkUpdate.
    (v3.x never updated prices in the bulk path at all.)"""
    priced = [p for p in products if p.get('_flags', {}).get('price')]
    if not priced or DRY_RUN:
        return True, 0

    log(f"Applying {len(priced)} price changes (bulk)...")
    lines = [{
        "productId": p['_existing']['product_id'],
        "variants": [{"id": p['_existing']['variant_id'], "price": p['_desired']['price']}]
    } for p in priced]

    mutation = ("mutation call($productId: ID!, $variants: [ProductVariantsBulkInput!]!) "
                "{ productVariantsBulkUpdate(productId: $productId, variants: $variants) "
                "{ productVariants { id } userErrors { field message } } }")
    return run_bulk_mutation(lines, mutation, len(priced), 'price update')


def poll_bulk_mutation(expected_count: int) -> Tuple[bool, int]:
    """Poll bulk mutation until complete"""
    log("Polling for bulk mutation completion...")

    query = """
    query {
      currentBulkOperation {
        id
        status
        errorCode
        objectCount
        rootObjectCount
        url
      }
    }
    """

    start_time = time.time()

    while time.time() - start_time < MAX_POLL_TIME:
        result = graphql_request(query, use_rate_limit=False)
        operation = result.get('data', {}).get('currentBulkOperation')

        if not operation:
            time.sleep(POLL_INTERVAL)
            continue

        status = operation.get('status')
        root_count = operation.get('rootObjectCount', 0)

        log(f"  Status: {status}, processed: {root_count}/{expected_count}")

        if status == 'COMPLETED':
            log(f"✓ Bulk operation completed! Processed {root_count} products")
            return True, root_count
        elif status in ['FAILED', 'CANCELED']:
            log(f"Bulk operation failed: {operation.get('errorCode')}", 'WARNING')
            return False, 0

        time.sleep(POLL_INTERVAL)

    log("Bulk operation timed out", 'WARNING')
    return False, 0


def batch_process(products: List[Dict], operation: str, func) -> int:
    """Process products in batches with concurrent requests"""
    if not products:
        return 0

    log(f"\n{operation} {len(products)} products...")

    successful = 0
    failed = 0
    CHUNK_SIZE = 50  # Report progress every 50 products

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as executor:
        futures = {executor.submit(func, p): p for p in products}

        for i, future in enumerate(as_completed(futures), 1):
            try:
                result = future.result()
                if result:
                    successful += 1
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                log(f"Error: {e}", 'WARNING')

            if i % CHUNK_SIZE == 0 or i == len(products):
                elapsed = time.time() - start_time
                rate = i / elapsed if elapsed > 0 else 0
                remaining = (len(products) - i) / rate if rate > 0 else 0
                log(f"  Progress: {i}/{len(products)} ({successful} ok, {failed} failed) - {rate:.1f}/sec, ~{remaining:.0f}s remaining")

    log(f"✓ {operation} complete: {successful} successful, {failed} failed")
    return successful

# =============================================================================
# ARCHIVE MISSING PRODUCTS (+ 301 redirects for the dead URLs)
# =============================================================================

_collection_handle_cache: Dict[str, bool] = {}
# Flips to True if the token lacks write_online_store_navigation, so we stop
# attempting redirects (and log once) instead of failing on every archive.
_redirects_disabled = False

def collection_exists(handle: str) -> bool:
    if handle in _collection_handle_cache:
        return _collection_handle_cache[handle]
    query = """
    query ($handle: String!) {
        collectionByHandle(handle: $handle) { id }
    }
    """
    result = graphql_request(query, {'handle': handle})
    exists = bool(result.get('data', {}).get('collectionByHandle'))
    _collection_handle_cache[handle] = exists
    return exists

def redirect_target_for(existing: Dict, known_handles: Set[str]) -> str:
    """Pick the collection page for the product's category; homepage fallback."""
    for tag in existing.get('tags', []):
        if tag in known_handles and collection_exists(tag):
            return f"/collections/{tag}"
    return "/"

def create_url_redirect(path: str, target: str) -> bool:
    """Create a 301 redirect. Returns False (and disables further attempts) if
    the token lacks the write_online_store_navigation scope, rather than
    crashing the archive pass."""
    global _redirects_disabled
    if _redirects_disabled:
        return False

    mutation = """
    mutation urlRedirectCreate($urlRedirect: UrlRedirectInput!) {
        urlRedirectCreate(urlRedirect: $urlRedirect) {
            urlRedirect { id }
            userErrors { field message }
        }
    }
    """
    result = graphql_request(mutation, {'urlRedirect': {'path': path, 'target': target}})

    # Access-denied (missing scope) comes back as top-level errors with data: null.
    top_errors = result.get('errors') or []
    if top_errors:
        if 'write_online_store_navigation' in json.dumps(top_errors):
            _redirects_disabled = True
            log("urlRedirectCreate denied — token is missing the "
                "'write_online_store_navigation' scope. Skipping all redirects "
                "this run; products are still archived. Add the scope to enable "
                "301 redirects.", 'WARNING')
        else:
            log(f"Redirect {path} error: {top_errors}", 'WARNING')
        return False

    payload = (result.get('data') or {}).get('urlRedirectCreate') or {}
    errors = payload.get('userErrors', [])
    if errors:
        # "already exists" is fine — the redirect is in place
        if any('exists' in (e.get('message') or '').lower() for e in errors):
            return True
        log(f"Redirect {path} failed: {errors}", 'WARNING')
        return False
    return True

def archive_product(sku: str, existing: Dict, known_handles: Set[str]) -> bool:
    """Archive a product (set to DRAFT) and 301-redirect its URL to the
    matching collection so Google doesn't accumulate 404s. A redirect failure
    never blocks the archive."""
    if existing.get('status') == 'DRAFT':
        return True  # Already archived

    if DRY_RUN:
        return True

    result = graphql_request(PRODUCT_UPDATE_MUTATION, {
        'product': {
            'id': existing['product_id'],
            'status': 'DRAFT'
        }
    })

    ok = bool((result.get('data') or {}).get('productUpdate', {}).get('product'))
    if ok and CREATE_REDIRECTS and not _redirects_disabled and existing.get('handle'):
        try:
            target = redirect_target_for(existing, known_handles)
            create_url_redirect(f"/products/{existing['handle']}", target)
        except Exception as e:
            log(f"Redirect for {sku} skipped: {e}", 'WARNING')
    return ok


def archive_missing_products(missing_skus: List[str], existing_products: Dict[str, Dict],
                             known_handles: Set[str]) -> int:
    """Archive products no longer in CSV"""
    if not missing_skus or not ARCHIVE_MISSING:
        return 0

    log(f"\nArchiving {len(missing_skus)} missing products (with 301 redirects)...")

    archived = 0
    for sku in missing_skus:
        existing = existing_products.get(sku)
        if existing and archive_product(sku, existing, known_handles):
            archived += 1

    log(f"✓ Archived {archived} products")
    return archived

# =============================================================================
# MAIN
# =============================================================================

def main():
    start_time = time.time()

    log("=" * 70)
    log("JohnnyVac to Shopify Sync v4.0 - CONTENT, SEO & INVENTORY OVERHAUL")
    log("=" * 70)
    log("Inventory + price sync fixed | descriptions protected | SEO + taxonomy + brand set")
    log(f"AI descriptions: {'ENABLED (Claude API)' if ai_available() else 'disabled (no ANTHROPIC_API_KEY — using templates)'}")

    if DRY_RUN:
        log("🔸 DRY RUN MODE - No changes will be made", 'WARNING')

    if not SHOPIFY_ACCESS_TOKEN:
        log("Error: SHOPIFY_ACCESS_TOKEN not set", 'ERROR')
        return

    # Step 1: Initialize categorizer
    log("\n[1/8] Initializing categorization system...")
    categorizer = ProductCategorizer('category_map_v4.json')
    known_handles = set(categorizer.category_by_handle.keys())

    # Step 2: Fetch CSV
    log("\n[2/8] Fetching CSV data...")
    csv_products, duplicate_skus = fetch_csv_data()

    # Step 3: Categorize
    log("\n[3/8] Categorizing products...")
    categorized_products, skipped_products = categorizer.batch_categorize(
        csv_products, language=LANGUAGE, skip_placeholders=True
    )
    log(f"✓ Categorized {len(categorized_products)} products")
    log(f"  Skipped {len(skipped_products)} placeholder products")

    # Export reports
    categorizer.export_needs_review(categorized_products, 'needs_review.csv')
    categorizer.export_skipped(skipped_products, 'skipped_products.csv')

    # Step 4: Fetch existing products (using bulk query - fast!)
    log("\n[4/8] Fetching existing Shopify products...")
    fetch_start = time.time()
    try:
        existing_products = get_existing_products_bulk()
    except Exception as e:
        log(f"Bulk query failed ({e}), using paginated fallback...", 'WARNING')
        existing_products = get_existing_products_paginated()
    fetch_time = time.time() - fetch_start
    log(f"  Fetch completed in {fetch_time:.1f}s")

    # Step 5: Calculate delta
    log("\n[5/8] Calculating delta...")
    to_create, to_update, unchanged, missing_skus = calculate_delta(
        categorized_products, existing_products, known_handles
    )

    # Step 6: Generate descriptions for new/thin products (AI or templates)
    log("\n[6/8] Generating descriptions for new/thin products...")
    generate_missing_descriptions(to_create + to_update)

    # Step 7: Execute sync
    log("\n[7/8] Syncing to Shopify...")
    sync_start = time.time()

    created = 0
    updated = 0
    inventoried = 0

    if DRY_RUN:
        log(f"[DRY RUN] Would create {len(to_create)} products")
        log(f"[DRY RUN] Would update {len(to_update)} products")
        created = len(to_create)
        updated = len(to_update)
        inventoried = sync_inventory_quantities(to_update)
    else:
        # Try bulk operations first, fall back to individual if they fail

        if to_create:
            success, count = try_bulk_create(to_create)
            if success:
                created = count
            else:
                log("Falling back to individual creates...")
                created = batch_process(to_create, "Creating", create_product)

        if to_update:
            success, count = try_bulk_update(to_update)
            if success:
                updated = count
                price_ok, _ = try_bulk_price_update(to_update)
                if not price_ok:
                    log("Bulk price update failed — applying prices individually...")
                    priced = [p for p in to_update if p.get('_flags', {}).get('price')]
                    def apply_price(p):
                        result = graphql_request(VARIANT_PRICE_MUTATION, {
                            "productId": p['_existing']['product_id'],
                            "variants": [{"id": p['_existing']['variant_id'],
                                          "price": p['_desired']['price']}]
                        })
                        return not result.get('data', {}).get('productVariantsBulkUpdate', {}).get('userErrors', [])
                    batch_process(priced, "Pricing", apply_price)
            else:
                log("Falling back to individual updates...")
                updated = batch_process(to_update, "Updating", update_product)

        # Inventory quantities (creates already get theirs via productSet)
        inventoried = sync_inventory_quantities(to_update)

    sync_time = time.time() - sync_start

    # Step 8: Archive missing
    log("\n[8/8] Archiving missing products...")
    archived = archive_missing_products(missing_skus, existing_products, known_handles)

    # Summary
    total_time = time.time() - start_time

    log("\n" + "=" * 70)
    log("SYNC COMPLETE")
    log("=" * 70)
    log(f"Total time: {total_time/60:.1f} minutes ({total_time:.0f} seconds)")
    log(f"\nResults:")
    log(f"  ✅ Created: {created}")
    log(f"  ✏️  Updated: {updated}")
    log(f"  📦 Inventory synced: {inventoried}")
    log(f"  ⏭️  Unchanged: {len(unchanged)} (skipping)")
    log(f"  🗑️  Archived: {archived} (with 301 redirects)")
    log(f"  ⛔ Skipped: {len(skipped_products)}")
    log(f"\nPerformance:")
    log(f"  Fetch existing: {fetch_time:.1f}s (bulk query)")
    log(f"  Sync operations: {sync_time:.1f}s")

    if DRY_RUN:
        log("\n🔸 DRY RUN - No actual changes were made")


if __name__ == '__main__':
    main()
