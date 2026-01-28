#!/usr/bin/env python3
# sync_shopify_v3.py
"""
JohnnyVac to Shopify Sync v3.2 - SMART HYBRID

Uses Shopify API 2026-01 with smart fallback:
1. Bulk Query for fetching existing products (fast!)
2. Tries Bulk Mutation first for creates/updates (fastest)
3. Falls back to batched individual mutations if bulk fails

Expected time:
- If bulk works: ~15-20 minutes
- If fallback needed: ~45-60 minutes
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
MAX_POLL_TIME = 3600  # 1 hour max for bulk query

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

def sanitize_tag(s: str) -> str:
    return re.sub(r'[^a-z0-9\-_]+', '-', s.lower()).strip('-')[:80]

def normalize_price(price_str: str) -> str:
    try:
        return f"{float(price_str):.2f}"
    except (ValueError, TypeError):
        return "0.00"

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
                productType
                status
                variants(first: 5) {
                  edges {
                    node {
                      id
                      sku
                      price
                      inventoryQuantity
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
            current_product = {
                'product_id': obj['id'],
                'title': obj.get('title', ''),
                'product_type': obj.get('productType', ''),
                'status': obj.get('status', 'ACTIVE')
            }
        # Variant line (has sku and __parentId)
        elif 'sku' in obj:
            sku = obj.get('sku')
            if sku and current_product:
                products[sku] = {
                    **current_product,
                    'variant_id': obj['id'],
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
        products(first: 250, after: $cursor) {
            edges {
                node {
                    id
                    title
                    productType
                    status
                    variants(first: 5) {
                        edges {
                            node {
                                id
                                sku
                                price
                                inventoryQuantity
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
            product = edge['node']
            for var_edge in product.get('variants', {}).get('edges', []):
                variant = var_edge['node']
                sku = variant.get('sku')
                if sku:
                    products[sku] = {
                        'product_id': product['id'],
                        'variant_id': variant['id'],
                        'title': product.get('title', ''),
                        'product_type': product.get('productType', ''),
                        'status': product.get('status', 'ACTIVE'),
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
    existing_products: Dict[str, Dict]
) -> Tuple[List[Dict], List[Dict], List[Dict], List[str]]:
    """Calculate what needs to be created, updated, or archived"""
    
    to_create = []
    to_update = []
    unchanged = []
    csv_skus = set()
    
    for product in csv_products:
        sku = product.get('SKU', '')
        csv_skus.add(sku)
        category_info = product.get('category', {})
        
        if sku not in existing_products:
            to_create.append(product)
        else:
            existing = existing_products[sku]
            
            new_title = product.get('ProductTitleEN' if LANGUAGE == 'en' else 'ProductTitleFR', '')
            new_price = normalize_price(product.get('RegularPrice', '0'))
            new_inventory = int(float(product.get('Inventory', '0') or 0))
            new_product_type = category_info.get('product_type', '')
            new_status = 'ACTIVE' if new_inventory > 0 else 'DRAFT'
            
            has_changes = (
                existing['title'] != new_title or
                normalize_price(existing['price']) != new_price or
                existing.get('inventory', 0) != new_inventory or
                existing['product_type'] != new_product_type or
                existing['status'] != new_status
            )
            
            if has_changes:
                product['_existing'] = existing
                to_update.append(product)
            else:
                unchanged.append(product)
    
    missing_skus = [sku for sku in existing_products if sku not in csv_skus]
    
    log(f"\nDELTA SUMMARY:")
    log(f"  To CREATE: {len(to_create)}")
    log(f"  To UPDATE: {len(to_update)}")
    log(f"  UNCHANGED: {len(unchanged)} (skipping)")
    log(f"  MISSING (will archive): {len(missing_skus)}")
    
    return to_create, to_update, unchanged, missing_skus

# =============================================================================
# BULK MUTATION - Try this first (fast), fall back to individual if it fails
# =============================================================================

def try_bulk_create(products: List[Dict]) -> Tuple[bool, int]:
    """
    Try to create products using bulk mutation (much faster).
    Returns (success, count).
    If it fails, caller should fall back to individual creates.
    """
    if not products or DRY_RUN:
        return False, 0
    
    log("Attempting bulk create (fast method)...")
    
    # Generate JSONL
    jsonl_file = 'bulk_creates.jsonl'
    with open(jsonl_file, 'w', encoding='utf-8') as f:
        for product in products:
            sku = product.get('SKU', '')
            category_info = product.get('category', {})
            
            title = product.get('ProductTitleEN' if LANGUAGE == 'en' else 'ProductTitleFR', sku)
            description = clean_html(product.get('ProductDescriptionEN' if LANGUAGE == 'en' else 'ProductDescriptionFR', ''))
            price = product.get('RegularPrice', '0.00')
            inventory = int(float(product.get('Inventory', '0') or 0))
            weight = float(product.get('weight', '0') or 0)
            upc = product.get('upc', '')
            product_type = category_info.get('product_type', 'Other > Needs Review')
            status = 'ACTIVE' if inventory > 0 else 'DRAFT'
            
            tags = [
                category_info.get('handle', 'uncategorized'),
                f"confidence:{category_info.get('confidence', 'low')}",
                f"source:{category_info.get('source', 'unknown')}"
            ]
            
            mutation_input = {
                "input": {
                    "title": title,
                    "descriptionHtml": description,
                    "productType": product_type,
                    "vendor": "JohnnyVac",
                    "status": status,
                    "tags": tags,
                    "variants": [{
                        "sku": sku,
                        "price": str(price),
                        "inventoryPolicy": "DENY",
                        "barcode": upc if upc else None,
                        "weight": weight,
                        "weightUnit": "KILOGRAMS"
                    }],
                    "images": [{"src": f"{IMAGE_BASE_URL}{sku}.jpg"}]
                }
            }
            f.write(json.dumps(mutation_input) + '\n')
    
    # Get staged upload URL
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
        resource_url = target['resourceUrl']
        params = {p['name']: p['value'] for p in target['parameters']}
        
        # Upload file
        with open(jsonl_file, 'rb') as f:
            files = {'file': ('bulk_input.jsonl', f, 'text/jsonl')}
            upload_response = requests.post(upload_url, data=params, files=files, timeout=300)
            upload_response.raise_for_status()
        
        log("✓ JSONL uploaded, starting bulk mutation...")
        
        # Run bulk mutation
        bulk_mutation = f'''
        mutation {{
          bulkOperationRunMutation(
            mutation: "mutation call($input: ProductInput!) {{ productCreate(input: $input) {{ product {{ id }} userErrors {{ field message }} }} }}",
            stagedUploadPath: "{resource_url}"
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
        
        # Poll for completion
        return poll_bulk_mutation(len(products))
        
    except Exception as e:
        log(f"Bulk create failed: {e}", 'WARNING')
        return False, 0

def try_bulk_update(products: List[Dict]) -> Tuple[bool, int]:
    """
    Try to update products using bulk mutation (much faster).
    Returns (success, count).
    """
    if not products or DRY_RUN:
        return False, 0
    
    log("Attempting bulk update (fast method)...")
    
    # Generate JSONL
    jsonl_file = 'bulk_updates.jsonl'
    with open(jsonl_file, 'w', encoding='utf-8') as f:
        for product in products:
            existing = product.get('_existing', {})
            category_info = product.get('category', {})
            
            title = product.get('ProductTitleEN' if LANGUAGE == 'en' else 'ProductTitleFR', '')
            description = clean_html(product.get('ProductDescriptionEN' if LANGUAGE == 'en' else 'ProductDescriptionFR', ''))
            product_type = category_info.get('product_type', existing.get('product_type', ''))
            inventory = int(float(product.get('Inventory', '0') or 0))
            status = 'ACTIVE' if inventory > 0 else 'DRAFT'
            
            tags = [
                category_info.get('handle', 'uncategorized'),
                f"confidence:{category_info.get('confidence', 'low')}",
                f"source:{category_info.get('source', 'unknown')}"
            ]
            
            mutation_input = {
                "input": {
                    "id": existing['product_id'],
                    "title": title,
                    "descriptionHtml": description,
                    "productType": product_type,
                    "tags": tags,
                    "status": status
                }
            }
            f.write(json.dumps(mutation_input) + '\n')
    
    # Get staged upload URL
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
        resource_url = target['resourceUrl']
        params = {p['name']: p['value'] for p in target['parameters']}
        
        # Upload file
        with open(jsonl_file, 'rb') as f:
            files = {'file': ('bulk_input.jsonl', f, 'text/jsonl')}
            upload_response = requests.post(upload_url, data=params, files=files, timeout=300)
            upload_response.raise_for_status()
        
        log("✓ JSONL uploaded, starting bulk mutation...")
        
        # Run bulk mutation
        bulk_mutation = f'''
        mutation {{
          bulkOperationRunMutation(
            mutation: "mutation call($input: ProductInput!) {{ productUpdate(input: $input) {{ product {{ id }} userErrors {{ field message }} }} }}",
            stagedUploadPath: "{resource_url}"
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
        
        # Poll for completion
        return poll_bulk_mutation(len(products))
        
    except Exception as e:
        log(f"Bulk update failed: {e}", 'WARNING')
        return False, 0

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
        count = operation.get('objectCount', 0)
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

def create_product(product_data: Dict) -> Optional[str]:
    """Create a single product"""
    sku = product_data.get('SKU', '')
    category_info = product_data.get('category', {})
    
    if DRY_RUN:
        return f"dry-run-{sku}"
    
    title = product_data.get('ProductTitleEN' if LANGUAGE == 'en' else 'ProductTitleFR', sku)
    description = clean_html(product_data.get('ProductDescriptionEN' if LANGUAGE == 'en' else 'ProductDescriptionFR', ''))
    price = product_data.get('RegularPrice', '0.00')
    inventory = int(float(product_data.get('Inventory', '0') or 0))
    weight = float(product_data.get('weight', '0') or 0)
    upc = product_data.get('upc', '')
    product_type = category_info.get('product_type', 'Other > Needs Review')
    status = 'ACTIVE' if inventory > 0 else 'DRAFT'
    
    tags = [
        category_info.get('handle', 'uncategorized'),
        f"confidence:{category_info.get('confidence', 'low')}",
        f"source:{category_info.get('source', 'unknown')}"
    ]
    
    mutation = """
    mutation createProduct($input: ProductInput!) {
        productCreate(input: $input) {
            product { id }
            userErrors { field message }
        }
    }
    """
    
    variables = {
        'input': {
            'title': title,
            'descriptionHtml': description,
            'productType': product_type,
            'tags': tags,
            'vendor': 'JohnnyVac',
            'status': status,
            'variants': [{
                'sku': sku,
                'price': str(price),
                'inventoryPolicy': 'DENY',
                'barcode': upc if upc else None,
                'weight': weight,
                'weightUnit': 'KILOGRAMS'
            }],
            'images': [{'src': f"{IMAGE_BASE_URL}{sku}.jpg"}]
        }
    }
    
    result = graphql_request(mutation, variables)
    
    if result.get('data', {}).get('productCreate', {}).get('product'):
        return result['data']['productCreate']['product']['id']
    else:
        errors = result.get('data', {}).get('productCreate', {}).get('userErrors', [])
        if errors:
            log(f"Create {sku} failed: {errors}", 'WARNING')
        return None

def update_product(product_data: Dict) -> bool:
    """Update a single product"""
    existing = product_data.get('_existing', {})
    category_info = product_data.get('category', {})
    sku = product_data.get('SKU', '')
    
    if DRY_RUN:
        return True
    
    title = product_data.get('ProductTitleEN' if LANGUAGE == 'en' else 'ProductTitleFR', '')
    description = clean_html(product_data.get('ProductDescriptionEN' if LANGUAGE == 'en' else 'ProductDescriptionFR', ''))
    product_type = category_info.get('product_type', existing.get('product_type', ''))
    inventory = int(float(product_data.get('Inventory', '0') or 0))
    status = 'ACTIVE' if inventory > 0 else 'DRAFT'
    
    tags = [
        category_info.get('handle', 'uncategorized'),
        f"confidence:{category_info.get('confidence', 'low')}",
        f"source:{category_info.get('source', 'unknown')}"
    ]
    
    mutation = """
    mutation updateProduct($input: ProductInput!) {
        productUpdate(input: $input) {
            product { id }
            userErrors { field message }
        }
    }
    """
    
    variables = {
        'input': {
            'id': existing['product_id'],
            'title': title,
            'descriptionHtml': description,
            'productType': product_type,
            'tags': tags,
            'status': status
        }
    }
    
    result = graphql_request(mutation, variables)
    
    if result.get('data', {}).get('productUpdate', {}).get('product'):
        return True
    else:
        errors = result.get('data', {}).get('productUpdate', {}).get('userErrors', [])
        if errors:
            log(f"Update {sku} failed: {errors}", 'WARNING')
        return False

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
            
            # Progress update every CHUNK_SIZE products
            if i % CHUNK_SIZE == 0 or i == len(products):
                elapsed = time.time() - start_time
                rate = i / elapsed if elapsed > 0 else 0
                remaining = (len(products) - i) / rate if rate > 0 else 0
                log(f"  Progress: {i}/{len(products)} ({successful} ok, {failed} failed) - {rate:.1f}/sec, ~{remaining:.0f}s remaining")
    
    log(f"✓ {operation} complete: {successful} successful, {failed} failed")
    return successful

# =============================================================================
# ARCHIVE MISSING PRODUCTS
# =============================================================================

def archive_product(sku: str, existing: Dict) -> bool:
    """Archive a single product (set to DRAFT)"""
    if existing.get('status') == 'DRAFT':
        return True  # Already archived
    
    if DRY_RUN:
        return True
    
    mutation = """
    mutation updateProduct($input: ProductInput!) {
        productUpdate(input: $input) {
            product { id }
            userErrors { field message }
        }
    }
    """
    
    result = graphql_request(mutation, {
        'input': {
            'id': existing['product_id'],
            'status': 'DRAFT'
        }
    })
    
    return bool(result.get('data', {}).get('productUpdate', {}).get('product'))

def archive_missing_products(missing_skus: List[str], existing_products: Dict[str, Dict]) -> int:
    """Archive products no longer in CSV"""
    if not missing_skus or not ARCHIVE_MISSING:
        return 0
    
    log(f"\nArchiving {len(missing_skus)} missing products...")
    
    archived = 0
    for sku in missing_skus:
        existing = existing_products.get(sku)
        if existing and archive_product(sku, existing):
            archived += 1
    
    log(f"✓ Archived {archived} products")
    return archived

# =============================================================================
# MAIN
# =============================================================================

def main():
    start_time = time.time()
    
    log("=" * 70)
    log("JohnnyVac to Shopify Sync v3.2 - SMART HYBRID (API 2026-01)")
    log("=" * 70)
    
    if DRY_RUN:
        log("🔸 DRY RUN MODE - No changes will be made", 'WARNING')
    
    if not SHOPIFY_ACCESS_TOKEN:
        log("Error: SHOPIFY_ACCESS_TOKEN not set", 'ERROR')
        return
    
    # Step 1: Initialize categorizer
    log("\n[1/7] Initializing categorization system...")
    categorizer = ProductCategorizer('category_map_v4.json')
    
    # Step 2: Fetch CSV
    log("\n[2/7] Fetching CSV data...")
    csv_products, duplicate_skus = fetch_csv_data()
    
    # Step 3: Categorize
    log("\n[3/7] Categorizing products...")
    categorized_products, skipped_products = categorizer.batch_categorize(
        csv_products, language=LANGUAGE, skip_placeholders=True
    )
    log(f"✓ Categorized {len(categorized_products)} products")
    log(f"  Skipped {len(skipped_products)} placeholder products")
    
    # Export reports
    categorizer.export_needs_review(categorized_products, 'needs_review.csv')
    categorizer.export_skipped(skipped_products, 'skipped_products.csv')
    
    # Step 4: Fetch existing products (using bulk query - fast!)
    log("\n[4/7] Fetching existing Shopify products...")
    fetch_start = time.time()
    try:
        existing_products = get_existing_products_bulk()
    except Exception as e:
        log(f"Bulk query failed ({e}), using paginated fallback...", 'WARNING')
        existing_products = get_existing_products_paginated()
    fetch_time = time.time() - fetch_start
    log(f"  Fetch completed in {fetch_time:.1f}s")
    
    # Step 5: Calculate delta
    log("\n[5/7] Calculating delta...")
    to_create, to_update, unchanged, missing_skus = calculate_delta(
        categorized_products, existing_products
    )
    
    # Step 6: Execute sync
    log("\n[6/7] Syncing to Shopify...")
    sync_start = time.time()
    
    created = 0
    updated = 0
    
    if DRY_RUN:
        log(f"[DRY RUN] Would create {len(to_create)} products")
        log(f"[DRY RUN] Would update {len(to_update)} products")
        created = len(to_create)
        updated = len(to_update)
    else:
        # Try bulk operations first (much faster with 2026-01 API)
        # Fall back to individual mutations if bulk fails
        
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
            else:
                log("Falling back to individual updates...")
                updated = batch_process(to_update, "Updating", update_product)
    
    sync_time = time.time() - sync_start
    
    # Step 7: Archive missing
    log("\n[7/7] Archiving missing products...")
    archived = archive_missing_products(missing_skus, existing_products)
    
    # Summary
    total_time = time.time() - start_time
    
    log("\n" + "=" * 70)
    log("SYNC COMPLETE")
    log("=" * 70)
    log(f"Total time: {total_time/60:.1f} minutes ({total_time:.0f} seconds)")
    log(f"\nResults:")
    log(f"  ✅ Created: {created}")
    log(f"  ✏️  Updated: {updated}")
    log(f"  ⏭️  Unchanged: {len(unchanged)}")
    log(f"  🗑️  Archived: {archived}")
    log(f"  ⛔ Skipped: {len(skipped_products)}")
    log(f"\nPerformance:")
    log(f"  Fetch existing: {fetch_time:.1f}s (bulk query)")
    log(f"  Sync operations: {sync_time:.1f}s")
    
    if DRY_RUN:
        log("\n🔸 DRY RUN - No actual changes were made")

if __name__ == '__main__':
    main()
