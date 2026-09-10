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
    adapt_metafields_to_definitions, ai_available, build_description,
    compute_vendor, extract_brand, extract_compatible_models,
    extract_dimensions, extract_material, extract_pack_quantity,
    generate_descriptions_ai, generate_seo_description, generate_seo_title,
    strip_html, taxonomy_for_handle,
)
from shopify_auth import get_access_token

# =============================================================================
# CONFIGURATION
# =============================================================================

SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', 'kingsway-janitorial.myshopify.com')
# Prefer client_credentials (fresh token w/ current scopes); fall back to a
# static SHOPIFY_ACCESS_TOKEN. See shopify_auth.py.
SHOPIFY_ACCESS_TOKEN = get_access_token()
CSV_URL = 'https://www.johnnyvacstock.com/sigm_all_jv_products/JVWebProducts.csv'
IMAGE_BASE_URL = 'https://www.johnnyvacstock.com/photos/web/'

LANGUAGE = 'en'
DRY_RUN = os.environ.get('DRY_RUN', 'false').lower() == 'true'
ARCHIVE_MISSING = os.environ.get('ARCHIVE_MISSING', 'true').lower() == 'true'

# Out-of-stock products stay ACTIVE and show as "Sold out" instead of being
# unpublished to DRAFT (which 404s the URL and churns Google's index every
# time stock flips). Set KEEP_OOS_ACTIVE=false to restore the old behavior.
KEEP_OOS_ACTIVE = os.environ.get('KEEP_OOS_ACTIVE', 'true').lower() == 'true'

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
        # Loop instead of recursing: the old version re-acquired the
        # non-reentrant lock it already held before recursing, deadlocking
        # the whole sync the first time two requests landed in the same
        # second (this is what silently hung the 2026-07-04 archive pass
        # for 3 hours until the job timeout).
        while True:
            with self._lock:
                now = time.time()
                self.requests = [t for t in self.requests if now - t < 1.0]
                if len(self.requests) < self.requests_per_second:
                    self.requests.append(now)
                    return
                wait_time = 1.0 - (now - min(self.requests)) + 0.05
            time.sleep(max(wait_time, 0.01))

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

            # Shopify nulls the whole `data` payload when a non-nullable field
            # errors (e.g. ACCESS_DENIED on a missing scope). Normalize so
            # callers' result.get('data', {}) chains don't crash on None.
            if result.get('data') is None:
                result['data'] = {}

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

_metafield_def_types: Optional[Dict[str, str]] = None

def get_metafield_definition_types() -> Dict[str, str]:
    """key -> type name for the store's pinned custom.* product metafield
    definitions. Writes must match these types or Shopify rejects them."""
    global _metafield_def_types
    if _metafield_def_types is None:
        query = """
        query {
          metafieldDefinitions(first: 100, ownerType: PRODUCT, namespace: "custom") {
            nodes { key type { name } }
          }
        }
        """
        result = graphql_request(query)
        nodes = (result.get('data', {})
                       .get('metafieldDefinitions', {}) or {}).get('nodes', []) or []
        _metafield_def_types = {n['key']: n['type']['name'] for n in nodes}
        if _metafield_def_types:
            log(f"Store metafield definitions (custom.*): {_metafield_def_types}")
    return _metafield_def_types

def build_metafields(desired: Dict) -> List[Dict]:
    """Structured metadata extracted from the title/type, namespace `custom`.
    custom.mpn = JohnnyVac SKU. Google accepts Brand + MPN instead of GTIN."""
    title = desired.get('title', '')
    metafields = []

    def add(key: str, value, mtype: str = 'single_line_text_field'):
        if value not in (None, '', []):
            metafields.append({
                "namespace": "custom",
                "key": key,
                "value": str(value),
                "type": mtype
            })

    add('mpn', desired.get('sku', ''))
    add('brand', extract_brand(title))
    add('pack_quantity', extract_pack_quantity(title), 'number_integer')
    add('material', extract_material(title))
    add('compatible_models',
        ', '.join(extract_compatible_models(title, desired.get('product_type', ''))))
    specs = extract_dimensions(title)
    add('size_inches', specs.get('size_inches'))
    add('voltage', specs.get('voltage'))
    return adapt_metafields_to_definitions(metafields, get_metafield_definition_types())

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

    # Only ask for the id: `Location.name` requires the read_locations scope,
    # which the client_credentials token (write_inventory,write_products)
    # doesn't have — requesting it nulls the whole response and broke every
    # scheduled sync from 2026-06-24 on.
    query = """
    query {
        locations(first: 1) {
            edges {
                node {
                    id
                }
            }
        }
    }
    """

    result = graphql_request(query, use_rate_limit=False)
    edges = result.get('data', {}).get('locations', {}).get('edges', [])
    if edges:
        _location_id_cache = edges[0]['node']['id']
        log(f"Using location: {_location_id_cache}")
        return _location_id_cache
    log("Could not resolve a location — inventory quantities cannot be "
        "written this run (check the app's access scopes)", 'ERROR')
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

    payload = result.get('data', {}).get('bulkOperationRunQuery', {}) or {}
    errors = payload.get('userErrors', [])
    if errors:
        log(f"Bulk query errors: {errors}", 'ERROR')
        raise Exception(f"Bulk query failed: {errors}")

    operation_id = (payload.get('bulkOperation') or {}).get('id')
    if not operation_id:
        raise Exception("Bulk query returned no operation id")

    return poll_and_download_bulk_results(operation_id)

def poll_and_download_bulk_results(operation_id: Optional[str] = None) -> Dict[str, Dict]:
    """Poll bulk operation and download results.

    Raises rather than returning an empty catalog. An empty `existing_products`
    makes every SKU in the feed look new, which is how the store accumulated
    11,008 duplicate products across Nov-Dec 2025.
    """
    log("Polling for bulk query completion...")

    query = """
    query {
      currentBulkOperation(type: QUERY) {
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

        # currentBulkOperation returns the most recent QUERY operation, which is
        # not necessarily the one just started. Waiting for ours avoids reading a
        # previous run's result - whose URL may have expired, yielding no URL at all.
        if operation_id and operation.get('id') != operation_id:
            log(f"  Waiting for {operation_id}, saw {operation.get('id')}")
            time.sleep(POLL_INTERVAL)
            continue

        status = operation.get('status')
        count = operation.get('objectCount', 0)
        log(f"  Bulk operation status: {status}, objects: {count}")

        if status == 'COMPLETED':
            url = operation.get('url')
            if not url:
                # Previously returned {} here, which the caller could not
                # distinguish from a genuinely empty store.
                raise Exception(
                    f"Bulk operation COMPLETED with no result URL (objectCount={count}); "
                    "refusing to treat the catalog as empty"
                )
            return download_bulk_results(url)
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

# SKUs seen ONLY on archived products. The catalog cleanup archives redundant
# duplicate copies, and a SKU whose every copy is archived must not be treated
# as absent from Shopify -- creating it again would rebuild the duplicate this
# store just spent a cleanup removing.
ARCHIVED_ONLY_SKUS: Set[str] = set()


def _register_existing(products: Dict[str, Dict], sku: str, record: Dict) -> None:
    """Collapse duplicate SKUs onto a single record.

    Two rules the plain `products[sku] = record` did not have:

    ARCHIVED copies never enter the lookup. They used to, and because the last
    copy parsed wins and archived duplicates tend to be the newest ids, an
    archived copy could become *the* record for its SKU -- after which the sync
    would update it, force it back to ACTIVE, or "archive" it to DRAFT, undoing
    the cleanup. Excluding them also keeps them out of `missing_skus`.

    Among the copies that remain, ACTIVE beats DRAFT, so the record matches the
    copy the cleanup keeps rather than whichever happened to be parsed last.
    """
    if record.get('status') == 'ARCHIVED':
        if sku not in products:
            ARCHIVED_ONLY_SKUS.add(sku)
        return
    ARCHIVED_ONLY_SKUS.discard(sku)
    prev = products.get(sku)
    if prev is not None and prev.get('status') == 'ACTIVE' and record.get('status') != 'ACTIVE':
        return
    products[sku] = record


def download_bulk_results(url: str) -> Dict[str, Dict]:
    """Download and parse bulk query results"""
    log("Downloading bulk results...")
    ARCHIVED_ONLY_SKUS.clear()

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
                _register_existing(products, sku, {
                    **current_product,
                    'variant_id': obj['id'],
                    'inventory_item_id': (obj.get('inventoryItem') or {}).get('id', ''),
                    'price': obj.get('price', '0'),
                    'inventory': obj.get('inventoryQuantity', 0)
                })

    log(f"✓ Parsed {len(products)} existing products from Shopify")
    return products

# Loose on purpose: duplicate SKUs collapse in the lookup, so the SKU count sits
# legitimately below the product count until the catalog cleanup completes.
MIN_EXISTING_RATIO = 0.5


def get_product_count() -> int:
    """Cheap authoritative product count, used to sanity-check the bulk fetch."""
    query = """
    query {
      productsCount {
        count
      }
    }
    """
    result = graphql_request(query, use_rate_limit=False)
    return int(((result.get('data') or {}).get('productsCount') or {}).get('count') or 0)


def verify_existing_products(existing_products: Dict[str, Dict], reported_count: int) -> None:
    """Abort before the delta if the fetched catalog looks implausibly small.

    A catalog that appears to have vanished is a fetch failure, never a reason to
    recreate every product in the feed.
    """
    fetched = len(existing_products)

    if reported_count <= 0:
        if fetched == 0:
            raise Exception(
                "Existing-product fetch returned nothing and the product count is "
                "unavailable; refusing to run the delta"
            )
        log("Could not read productsCount; skipping catalog size check", 'WARNING')
        return

    if fetched == 0:
        raise Exception(
            f"Existing-product fetch returned 0 SKUs but the store reports "
            f"{reported_count} products; refusing to run the delta"
        )

    # productsCount saturates at 10000, so only compare below that ceiling.
    expected = min(reported_count, 10000)
    if fetched < expected * MIN_EXISTING_RATIO:
        raise Exception(
            f"Existing-product fetch returned only {fetched} SKUs against "
            f"{reported_count} products in the store; refusing to run the delta. "
            "Creating from an incomplete catalog is what produced the Nov-Dec 2025 duplicates."
        )

    log(f"  Catalog check OK: {fetched} SKUs against {reported_count} products")


def get_existing_products_paginated() -> Dict[str, Dict]:
    """Fallback: fetch products with pagination if bulk fails"""
    log("Using paginated fetch (fallback)...")
    ARCHIVED_ONLY_SKUS.clear()
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
                    _register_existing(products, sku, {
                        **base,
                        'variant_id': variant['id'],
                        'inventory_item_id': (variant.get('inventoryItem') or {}).get('id', ''),
                        'price': variant.get('price', '0'),
                        'inventory': variant.get('inventoryQuantity', 0)
                    })

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
    skipped_archived = 0

    counts = {'core': 0, 'price': 0, 'inventory': 0, 'vendor': 0, 'tags': 0,
              'seo': 0, 'description': 0, 'category': 0}

    for product in csv_products:
        sku = product.get('SKU', '')
        csv_skus.add(sku)
        desired = build_desired_state(product)
        product['_desired'] = desired

        if sku not in existing_products:
            if sku in ARCHIVED_ONLY_SKUS:
                # every copy of this SKU is archived: the cleanup retired it
                # deliberately. Recreating it would rebuild the duplicate.
                skipped_archived += 1
                continue
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
    if skipped_archived:
        log(f"  SKIPPED (archived duplicates, not recreated): {skipped_archived}")

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
        "metafields": build_metafields(d),
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
        "metafields": build_metafields(d),
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
    mutation = """
    mutation urlRedirectCreate($urlRedirect: UrlRedirectInput!) {
        urlRedirectCreate(urlRedirect: $urlRedirect) {
            urlRedirect { id }
            userErrors { field message }
        }
    }
    """
    result = graphql_request(mutation, {'urlRedirect': {'path': path, 'target': target}})

    # Top-level errors (e.g. access denied) never surface in userErrors, so an
    # archive-redirect would otherwise fail silently. Flag the scope problem
    # loudly — this is what let the GSC 404 backlog build up.
    top_errors = result.get('errors') or []
    if top_errors:
        if any('access denied' in (e.get('message') or '').lower() for e in top_errors):
            log(f"Redirect {path} DENIED — token is missing the "
                f"write_online_store_navigation scope; the URL will 404 in Google", 'ERROR')
        else:
            log(f"Redirect {path} failed: {top_errors}", 'WARNING')
        return False

    errors = result.get('data', {}).get('urlRedirectCreate', {}).get('userErrors', [])
    if errors:
        # "already exists" is fine — the redirect is in place
        if any('exists' in (e.get('message') or '').lower() for e in errors):
            return True
        log(f"Redirect {path} failed: {errors}", 'WARNING')
        return False
    return True


def check_redirect_scope() -> bool:
    """Verify the access token can write URL redirects before we rely on it.

    Without write_online_store_navigation, every archive 301 fails silently and
    discontinued product URLs pile up as 404s in Google. Warn loudly at startup
    so a missing scope can't recur unnoticed."""
    query = """
    query {
      currentAppInstallation {
        accessScopes { handle }
      }
    }
    """
    result = graphql_request(query)
    scopes = {
        s.get('handle')
        for s in (result.get('data', {})
                        .get('currentAppInstallation', {})
                        .get('accessScopes', []) or [])
    }
    if 'write_online_store_navigation' not in scopes:
        log("=" * 70, 'WARNING')
        log("MISSING SCOPE: write_online_store_navigation is NOT granted.", 'WARNING')
        log("Archive 301 redirects will fail silently and discontinued product", 'WARNING')
        log("URLs will 404 in Google Search Console. Add the scope to the app and", 'WARNING')
        log("re-release / reinstall to update the access token.", 'WARNING')
        log("=" * 70, 'WARNING')
        return False
    return True

def archive_product(sku: str, existing: Dict, known_handles: Set[str]) -> bool:
    """Archive a product (set to DRAFT) and 301-redirect its URL to the
    matching collection so Google doesn't accumulate 404s."""
    if existing.get('status') in ('DRAFT', 'ARCHIVED'):
        # ARCHIVED matters as much as DRAFT here: this function "archives" by
        # setting DRAFT, so without the ARCHIVED case it would take a properly
        # archived product and *un*-archive it back to DRAFT.
        return True  # Already archived

    if DRY_RUN:
        return True

    result = graphql_request(PRODUCT_UPDATE_MUTATION, {
        'product': {
            'id': existing['product_id'],
            'status': 'DRAFT'
        }
    })

    ok = bool(result.get('data', {}).get('productUpdate', {}).get('product'))
    if ok and existing.get('handle'):
        target = redirect_target_for(existing, known_handles)
        create_url_redirect(f"/products/{existing['handle']}", target)
    return ok


def archive_missing_products(missing_skus: List[str], existing_products: Dict[str, Dict],
                             known_handles: Set[str]) -> int:
    """Archive products no longer in CSV"""
    if not missing_skus or not ARCHIVE_MISSING:
        return 0

    log(f"\nArchiving {len(missing_skus)} missing products (with 301 redirects)...")

    archived = 0
    for i, sku in enumerate(missing_skus, 1):
        existing = existing_products.get(sku)
        if existing and archive_product(sku, existing, known_handles):
            archived += 1
        if i % 100 == 0:
            log(f"  Archive progress: {i}/{len(missing_skus)} ({archived} archived)")

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

    # Verify the token can write redirects; warn loudly if not (archive 301s
    # fail silently without write_online_store_navigation).
    if not DRY_RUN:
        check_redirect_scope()

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

    reported_count = 0
    try:
        reported_count = get_product_count()
    except Exception as e:
        log(f"Could not read productsCount ({e})", 'WARNING')

    try:
        existing_products = get_existing_products_bulk()
    except Exception as e:
        log(f"Bulk query failed ({e}), using paginated fallback...", 'WARNING')
        existing_products = get_existing_products_paginated()
    fetch_time = time.time() - fetch_start
    log(f"  Fetch completed in {fetch_time:.1f}s")

    verify_existing_products(existing_products, reported_count)

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
