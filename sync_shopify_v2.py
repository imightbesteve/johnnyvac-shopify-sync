#!/usr/bin/env python3
# sync_shopify_v2.py
"""
JohnnyVac to Shopify Sync Script v2.0 - FIXED & IMPROVED

Key Fixes:
- Thread-safe rate limiter
- Uses JohnnyVac categories as primary signal
- Skips placeholder products (rarely ordered, demo, discontinued)
- Handles duplicate SKUs with logging
- Normalizes price comparisons
- Cleans HTML descriptions
- Sets missing products to DRAFT (products that disappear from CSV)
- Dry-run mode for testing

Author: Fixed version based on code review
"""

import re
import os
import csv
import json
import time
import hashlib
import threading
import requests
from typing import Dict, List, Optional, Tuple, Set
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import the fixed categorizer
from categorizer_v4 import ProductCategorizer

# =============================================================================
# CONFIGURATION
# =============================================================================

SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', 'kingsway-janitorial.myshopify.com')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')
CSV_URL = 'https://www.johnnyvacstock.com/sigm_all_jv_products/JVWebProducts.csv'
IMAGE_BASE_URL = 'https://www.johnnyvacstock.com/photos/web/'

# Processing settings
BATCH_SIZE = 100
RATE_LIMIT_PER_SECOND = 2  # Shopify standard = 2, Plus = 4
MAX_CONCURRENT_REQUESTS = 4
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
LANGUAGE = 'en'

# Feature flags
DRY_RUN = os.environ.get('DRY_RUN', 'false').lower() == 'true'
ARCHIVE_MISSING = os.environ.get('ARCHIVE_MISSING', 'true').lower() == 'true'

# API URL
API_URL = f'https://{SHOPIFY_STORE}/admin/api/2024-01/graphql.json'

# =============================================================================
# THREAD-SAFE RATE LIMITER
# =============================================================================

class RateLimiter:
    """Thread-safe rate limiter using token bucket algorithm"""
    
    def __init__(self, requests_per_second: float = 2.0):
        self.requests_per_second = requests_per_second
        self.requests: List[float] = []
        self._lock = threading.Lock()
    
    def throttle(self):
        """Wait if necessary to stay under rate limit (thread-safe)"""
        with self._lock:
            now = time.time()
            # Remove requests older than 1 second
            self.requests = [t for t in self.requests if now - t < 1.0]
            
            if len(self.requests) >= self.requests_per_second:
                # Need to wait
                oldest = min(self.requests)
                wait_time = 1.0 - (now - oldest) + 0.05  # 50ms buffer
                if wait_time > 0:
                    # Release lock while sleeping
                    self._lock.release()
                    try:
                        time.sleep(wait_time)
                    finally:
                        self._lock.acquire()
                    return self.throttle()  # Retry after waiting
            
            self.requests.append(time.time())


rate_limiter = RateLimiter(RATE_LIMIT_PER_SECOND)

# =============================================================================
# LOGGING
# =============================================================================

def log(message: str, level: str = 'INFO'):
    """Simple timestamped logging"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] [{level}] {message}")

# =============================================================================
# HTML CLEANING
# =============================================================================

def clean_html(html_str: str) -> str:
    """Clean HTML description, removing garbage and normalizing"""
    if not html_str:
        return ''
    
    # Remove meta tags
    html_str = re.sub(r'<meta[^>]*>', '', html_str, flags=re.IGNORECASE)
    
    # Remove empty paragraphs with just &nbsp;
    html_str = re.sub(r'<p>\s*(<i></i>)?\s*(&nbsp;)?\s*</p>', '', html_str, flags=re.IGNORECASE)
    html_str = re.sub(r'<P>\s*(<I></I>)?\s*(&nbsp;)?\s*</P>', '', html_str, flags=re.IGNORECASE)
    
    # Remove empty italic tags
    html_str = re.sub(r'<i>\s*</i>', '', html_str, flags=re.IGNORECASE)
    
    # Remove generatedBy comments
    html_str = re.sub(r'generatedBy="[^"]*"', '', html_str)
    
    # Normalize whitespace
    html_str = re.sub(r'\s+', ' ', html_str).strip()
    
    # If result is basically empty, return empty string
    if html_str in ['', '<p></p>', '<P></P>', ' ']:
        return ''
    
    return html_str

# =============================================================================
# PRICE NORMALIZATION
# =============================================================================

def normalize_price(price_str: str) -> str:
    """Normalize price to 2 decimal places for comparison"""
    try:
        return f"{float(price_str):.2f}"
    except (ValueError, TypeError):
        return "0.00"

# =============================================================================
# API REQUEST FUNCTIONS
# =============================================================================

def make_request(query: str, variables: Optional[Dict] = None, retry_count: int = 0) -> Dict:
    """Make GraphQL request with thread-safe rate limiting"""
    rate_limiter.throttle()
    
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN
    }
    
    payload = {'query': query}
    if variables:
        payload['variables'] = variables
    
    try:
        response = requests.post(
            API_URL, 
            json=payload, 
            headers=headers, 
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        return response.json()
        
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
        if retry_count < MAX_RETRIES:
            wait_time = (retry_count + 1) * 5
            log(f"Connection error, retrying in {wait_time}s... (attempt {retry_count + 1}/{MAX_RETRIES})", 'WARNING')
            time.sleep(wait_time)
            return make_request(query, variables, retry_count + 1)
        else:
            log(f"Max retries reached. Error: {str(e)}", 'ERROR')
            raise
    except requests.exceptions.RequestException as e:
        log(f"Request failed: {str(e)}", 'ERROR')
        raise

# =============================================================================
# CSV FETCHING & VALIDATION
# =============================================================================

def fetch_csv_data() -> Tuple[List[Dict], List[str]]:
    """
    Fetch and parse CSV data with validation.
    Returns: (products, duplicate_skus)
    """
    log(f"Fetching CSV from: {CSV_URL}")
    
    try:
        response = requests.get(CSV_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        lines = response.text.splitlines()
        
        # Use semicolon delimiter
        reader = csv.DictReader(lines, delimiter=';')
        
        products = []
        seen_skus: Set[str] = set()
        duplicate_skus: List[str] = []
        empty_rows = 0
        
        for row in reader:
            sku = (row.get('SKU') or '').strip()
            
            # Skip empty rows
            if not sku:
                empty_rows += 1
                continue
            
            # Track duplicates
            if sku in seen_skus:
                duplicate_skus.append(sku)
                continue  # Skip duplicate, keep first occurrence
            
            seen_skus.add(sku)
            
            # Strip whitespace from all fields
            cleaned_row = {k: (v.strip() if v else '') for k, v in row.items()}
            products.append(cleaned_row)
        
        log(f"✓ Parsed {len(products)} products from CSV")
        if empty_rows:
            log(f"  Skipped {empty_rows} empty rows", 'WARNING')
        if duplicate_skus:
            log(f"  Found {len(duplicate_skus)} duplicate SKUs (kept first occurrence)", 'WARNING')
            log(f"  Duplicate SKUs: {duplicate_skus[:10]}{'...' if len(duplicate_skus) > 10 else ''}", 'WARNING')
        
        return products, duplicate_skus
        
    except requests.exceptions.RequestException as e:
        log(f"Failed to fetch CSV: {str(e)}", 'ERROR')
        raise

# =============================================================================
# SHOPIFY PRODUCT FETCHING
# =============================================================================

def get_all_shopify_products() -> Dict[str, Dict]:
    """Fetch all existing Shopify products indexed by SKU"""
    log("Fetching existing Shopify products...")
    products = {}
    has_next_page = True
    cursor = None
    page = 0
    
    query = """
    query getProducts($cursor: String) {
        products(first: 250, after: $cursor) {
            edges {
                node {
                    id
                    title
                    descriptionHtml
                    productType
                    status
                    tags
                    variants(first: 10) {
                        edges {
                            node {
                                id
                                sku
                                price
                                inventoryQuantity
                                barcode
                            }
                        }
                    }
                }
                cursor
            }
            pageInfo {
                hasNextPage
            }
        }
    }
    """
    
    while has_next_page:
        page += 1
        variables = {'cursor': cursor} if cursor else None
        
        result = make_request(query, variables)
        
        if 'data' in result and 'products' in result['data']:
            edges = result['data']['products']['edges']
            page_info = result['data']['products']['pageInfo']
            
            for edge in edges:
                product = edge['node']
                for variant_edge in product['variants']['edges']:
                    sku = variant_edge['node']['sku']
                    if sku:
                        products[sku] = {
                            'product_id': product['id'],
                            'variant_id': variant_edge['node']['id'],
                            'title': product['title'],
                            'description': product.get('descriptionHtml', ''),
                            'product_type': product.get('productType', ''),
                            'price': variant_edge['node']['price'],
                            'inventory': variant_edge['node'].get('inventoryQuantity', 0),
                            'tags': product.get('tags', []),
                            'status': product.get('status', 'ACTIVE')
                        }
            
            has_next_page = page_info['hasNextPage']
            if edges:
                cursor = edges[-1]['cursor']
            
            if page % 10 == 0:
                log(f"  Fetched page {page}, total products: {len(products)}")
        else:
            log(f"Unexpected response: {result}", 'WARNING')
            break
    
    log(f"✓ Fetched {len(products)} existing products from Shopify")
    return products

# =============================================================================
# DELTA CALCULATION
# =============================================================================

def calculate_delta(
    csv_products: List[Dict], 
    existing_products: Dict[str, Dict]
) -> Tuple[List[Dict], List[Dict], List[Dict], List[str]]:
    """
    Compare CSV products with existing Shopify products.
    Returns: (to_create, to_update, unchanged, missing_skus)
    """
    log("\nCalculating delta (what needs syncing)...")
    
    to_create = []
    to_update = []
    unchanged = []
    
    csv_skus = set()
    
    for product in csv_products:
        sku = product.get('SKU', '')
        csv_skus.add(sku)
        category_info = product.get('category', {})
        
        if sku not in existing_products:
            # New product
            to_create.append(product)
        else:
            # Existing product - check if changed
            existing = existing_products[sku]
            
            # Get new values
            new_title = product.get('ProductTitleEN', '') if LANGUAGE == 'en' else product.get('ProductTitleFR', '')
            new_price = normalize_price(product.get('RegularPrice', '0'))
            new_inventory = int(float(product.get('Inventory', '0') or 0))
            new_product_type = category_info.get('product_type', '')
            
            # Determine expected status
            new_status = 'ACTIVE' if new_inventory > 0 else 'DRAFT'
            
            # Compare (using normalized prices)
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
    
    # Find products in Shopify that are no longer in CSV
    missing_skus = [
        sku for sku in existing_products.keys() 
        if sku not in csv_skus
    ]
    
    log(f"\n{'=' * 40}")
    log(f"DELTA SYNC PLAN")
    log(f"{'=' * 40}")
    log(f"  Products to CREATE: {len(to_create)}")
    log(f"  Products to UPDATE: {len(to_update)}")
    log(f"  Products UNCHANGED: {len(unchanged)} (will skip)")
    log(f"  Products MISSING from CSV: {len(missing_skus)} (will set to DRAFT)")
    log(f"  Total in CSV: {len(csv_products)}")
    
    if len(unchanged) > 0:
        skip_pct = (len(unchanged) / len(csv_products)) * 100
        log(f"  Work saved by delta sync: {skip_pct:.1f}%")
    
    return to_create, to_update, unchanged, missing_skus

# =============================================================================
# PRODUCT CREATION
# =============================================================================

def sanitize_tag(s: str) -> str:
    """Sanitize string for use as Shopify tag"""
    return re.sub(r'[^a-z0-9\-_]+', '-', s.lower()).strip('-')[:80]


def create_product(product_data: Dict, category_info: Dict) -> Optional[str]:
    """Create a single product in Shopify"""
    sku = product_data.get('SKU', '')
    
    if DRY_RUN:
        log(f"[DRY RUN] Would create: {sku}")
        return f"dry-run-{sku}"
    
    # Get product details
    title = product_data.get('ProductTitleEN') if LANGUAGE == 'en' else product_data.get('ProductTitleFR', product_data.get('ProductTitleEN', sku))
    description = product_data.get('ProductDescriptionEN') if LANGUAGE == 'en' else product_data.get('ProductDescriptionFR', product_data.get('ProductDescriptionEN', ''))
    description = clean_html(description)
    
    price = product_data.get('RegularPrice', '0.00')
    inventory = int(float(product_data.get('Inventory', '0') or 0))
    weight = float(product_data.get('weight', '0') or 0)
    upc = product_data.get('upc', '')
    image_url = f"{IMAGE_BASE_URL}{sku}.jpg"
    product_type = category_info.get('product_type', 'Other > Needs Review')
    
    # Build tags
    tags = [
        category_info.get('handle', 'uncategorized'),
        f"confidence:{category_info.get('confidence', 'low')}",
        f"source:{category_info.get('source', 'unknown')}"
    ]
    if category_info.get('matched_keyword'):
        tags.append(sanitize_tag(f"matched-{category_info['matched_keyword']}"))
    if category_info.get('jv_category'):
        tags.append(sanitize_tag(f"jv-{category_info['jv_category'][:40]}"))
    
    # Determine status based on inventory
    status = 'ACTIVE' if inventory > 0 else 'DRAFT'
    
    mutation = """
    mutation createProduct($input: ProductInput!) {
        productCreate(input: $input) {
            product { id title }
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
            'images': [{'src': image_url}]
        }
    }
    
    result = make_request(mutation, variables)
    
    if 'data' in result and result['data'].get('productCreate', {}).get('product'):
        return result['data']['productCreate']['product']['id']
    else:
        errors = result.get('data', {}).get('productCreate', {}).get('userErrors', [])
        log(f"Failed to create {sku}: {errors}", 'ERROR')
        return None


def batch_create_products(products: List[Dict]) -> int:
    """Create products in parallel batches"""
    if not products:
        return 0
    
    log(f"\nCreating {len(products)} new products...")
    created = 0
    
    CHUNK_SIZE = 20
    
    for i in range(0, len(products), CHUNK_SIZE):
        chunk = products[i:i + CHUNK_SIZE]
        chunk_num = (i // CHUNK_SIZE) + 1
        total_chunks = (len(products) + CHUNK_SIZE - 1) // CHUNK_SIZE
        
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS) as executor:
            futures = {
                executor.submit(create_product, p, p.get('category', {})): p
                for p in chunk
            }
            
            for future in as_completed(futures):
                product = futures[future]
                try:
                    result = future.result()
                    if result:
                        created += 1
                except Exception as e:
                    log(f"Error creating {product.get('SKU', 'unknown')}: {e}", 'ERROR')
        
        log(f"  Chunk {chunk_num}/{total_chunks}: {created}/{len(products)} created")
        
        if i + CHUNK_SIZE < len(products):
            time.sleep(2)
    
    log(f"✓ Created {created} products")
    return created

# =============================================================================
# PRODUCT UPDATE
# =============================================================================

def update_product(product_data: Dict, category_info: Dict, existing: Dict) -> bool:
    """Update a single product in Shopify"""
    sku = product_data.get('SKU', '')
    
    if DRY_RUN:
        log(f"[DRY RUN] Would update: {sku}")
        return True
    
    title = product_data.get('ProductTitleEN') if LANGUAGE == 'en' else product_data.get('ProductTitleFR', product_data.get('ProductTitleEN', sku))
    description = product_data.get('ProductDescriptionEN') if LANGUAGE == 'en' else product_data.get('ProductDescriptionFR', product_data.get('ProductDescriptionEN', ''))
    description = clean_html(description)
    
    product_type = category_info.get('product_type', existing.get('product_type', ''))
    inventory = int(float(product_data.get('Inventory', '0') or 0))
    status = 'ACTIVE' if inventory > 0 else 'DRAFT'
    
    # Build tags
    tags = [
        category_info.get('handle', 'uncategorized'),
        f"confidence:{category_info.get('confidence', 'low')}",
        f"source:{category_info.get('source', 'unknown')}"
    ]
    if category_info.get('matched_keyword'):
        tags.append(sanitize_tag(f"matched-{category_info['matched_keyword']}"))
    
    mutation = """
    mutation updateProduct($input: ProductInput!) {
        productUpdate(input: $input) {
            product { id title }
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
    
    result = make_request(mutation, variables)
    
    if 'data' in result and result['data'].get('productUpdate', {}).get('product'):
        return True
    else:
        errors = result.get('data', {}).get('productUpdate', {}).get('userErrors', [])
        log(f"Failed to update {sku}: {errors}", 'ERROR')
        return False


def batch_update_products(products: List[Dict]) -> int:
    """Update products in parallel batches"""
    if not products:
        return 0
    
    log(f"\nUpdating {len(products)} changed products...")
    updated = 0
    
    CHUNK_SIZE = 10
    
    for i in range(0, len(products), CHUNK_SIZE):
        chunk = products[i:i + CHUNK_SIZE]
        chunk_num = (i // CHUNK_SIZE) + 1
        total_chunks = (len(products) + CHUNK_SIZE - 1) // CHUNK_SIZE
        
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS) as executor:
            futures = {
                executor.submit(
                    update_product, 
                    p, 
                    p.get('category', {}), 
                    p['_existing']
                ): p
                for p in chunk
            }
            
            for future in as_completed(futures):
                product = futures[future]
                try:
                    result = future.result()
                    if result:
                        updated += 1
                except Exception as e:
                    log(f"Error updating {product.get('SKU', 'unknown')}: {e}", 'ERROR')
        
        log(f"  Chunk {chunk_num}/{total_chunks}: {updated}/{len(products)} updated")
        
        if i + CHUNK_SIZE < len(products):
            time.sleep(2)
    
    log(f"✓ Updated {updated} products")
    return updated

# =============================================================================
# ARCHIVE MISSING PRODUCTS
# =============================================================================

def archive_missing_products(missing_skus: List[str], existing_products: Dict[str, Dict]) -> int:
    """Set products that are no longer in CSV to DRAFT status"""
    if not missing_skus or not ARCHIVE_MISSING:
        return 0
    
    log(f"\nSetting {len(missing_skus)} missing products to DRAFT...")
    archived = 0
    
    mutation = """
    mutation updateProductStatus($input: ProductInput!) {
        productUpdate(input: $input) {
            product { id status }
            userErrors { field message }
        }
    }
    """
    
    for sku in missing_skus:
        if DRY_RUN:
            log(f"[DRY RUN] Would archive: {sku}")
            archived += 1
            continue
        
        existing = existing_products.get(sku)
        if not existing:
            continue
        
        # Skip if already DRAFT
        if existing.get('status') == 'DRAFT':
            continue
        
        variables = {
            'input': {
                'id': existing['product_id'],
                'status': 'DRAFT'
            }
        }
        
        try:
            result = make_request(mutation, variables)
            if 'data' in result and result['data'].get('productUpdate', {}).get('product'):
                archived += 1
            else:
                errors = result.get('data', {}).get('productUpdate', {}).get('userErrors', [])
                log(f"Failed to archive {sku}: {errors}", 'ERROR')
        except Exception as e:
            log(f"Error archiving {sku}: {e}", 'ERROR')
    
    log(f"✓ Archived {archived} products (set to DRAFT)")
    return archived

# =============================================================================
# MAIN
# =============================================================================

def main():
    start_time = time.time()
    
    log("=" * 70)
    log("JohnnyVac to Shopify Sync v2.0 - FIXED & IMPROVED")
    log("=" * 70)
    
    if DRY_RUN:
        log("🔸 DRY RUN MODE - No changes will be made", 'WARNING')
    
    if not SHOPIFY_ACCESS_TOKEN:
        log("Error: SHOPIFY_ACCESS_TOKEN environment variable not set", 'ERROR')
        return
    
    # Step 1: Initialize categorizer
    log("\n[1/8] Initializing categorization system...")
    categorizer = ProductCategorizer('category_map_v4.json')
    
    # Step 2: Fetch CSV data
    log("\n[2/8] Fetching CSV data...")
    csv_products, duplicate_skus = fetch_csv_data()
    
    # Step 3: Categorize products (with skip detection)
    log("\n[3/8] Categorizing products...")
    categorize_start = time.time()
    
    categorized_products, skipped_products = categorizer.batch_categorize(
        csv_products,
        language=LANGUAGE,
        enforce_min_products=True,
        skip_placeholders=True
    )
    
    categorize_time = time.time() - categorize_start
    log(f"✓ Categorized {len(categorized_products)} products in {categorize_time:.1f}s")
    log(f"  Skipped {len(skipped_products)} placeholder products")
    
    # Generate and log statistics
    stats = categorizer.get_category_stats(categorized_products)
    
    log("\n" + "=" * 40)
    log("CATEGORIZATION STATISTICS")
    log("=" * 40)
    log(f"Total products: {stats['total_products']}")
    log(f"Needs review: {stats['needs_review_count']} ({stats['needs_review_percentage']:.1f}%)")
    log(f"\nBy source:")
    for source, count in sorted(stats['by_source'].items(), key=lambda x: -x[1]):
        pct = (count / stats['total_products']) * 100
        log(f"  {source}: {count} ({pct:.1f}%)")
    log(f"\nBy confidence:")
    for conf, count in stats['by_confidence'].items():
        pct = (count / stats['total_products']) * 100
        log(f"  {conf}: {count} ({pct:.1f}%)")
    log(f"\nTop 10 categories:")
    sorted_cats = sorted(stats['by_category'].items(), key=lambda x: -x[1])
    for category, count in sorted_cats[:10]:
        pct = (count / stats['total_products']) * 100
        log(f"  {category}: {count} ({pct:.1f}%)")
    
    # Export reports
    categorizer.export_needs_review(categorized_products, 'needs_review.csv')
    categorizer.export_skipped(skipped_products, 'skipped_products.csv')
    
    # Step 4: Get existing products from Shopify
    log("\n[4/8] Fetching existing Shopify products...")
    fetch_start = time.time()
    existing_products = get_all_shopify_products()
    fetch_time = time.time() - fetch_start
    log(f"✓ Fetched in {fetch_time:.1f}s")
    
    # Step 5: Calculate delta
    log("\n[5/8] Calculating delta...")
    to_create, to_update, unchanged, missing_skus = calculate_delta(
        categorized_products, 
        existing_products
    )
    
    # Step 6: Execute sync
    log("\n[6/8] Syncing to Shopify...")
    sync_start = time.time()
    
    created = batch_create_products(to_create)
    updated = batch_update_products(to_update)
    
    sync_time = time.time() - sync_start
    log(f"✓ Sync completed in {sync_time:.1f}s")
    
    # Step 7: Archive missing products
    log("\n[7/8] Archiving missing products...")
    archived = archive_missing_products(missing_skus, existing_products)
    
    # Step 8: Final summary
    log("\n[8/8] Generating summary...")
    total_time = time.time() - start_time
    
    log("\n" + "=" * 70)
    log("SYNC COMPLETE")
    log("=" * 70)
    log(f"Total time: {total_time/60:.1f} minutes ({total_time:.1f} seconds)")
    if categorized_products:
        log(f"Products/second: {len(categorized_products)/total_time:.1f}")
    
    log(f"\nResults:")
    log(f"  ✅ Created: {created}")
    log(f"  ✏️  Updated: {updated}")
    log(f"  ⏭️  Unchanged (skipped): {len(unchanged)}")
    log(f"  🗑️  Archived (set to DRAFT): {archived}")
    log(f"  ⛔ Skipped (placeholders): {len(skipped_products)}")
    log(f"  📊 Total processed: {len(categorized_products)}")
    
    log(f"\nPerformance breakdown:")
    log(f"  Categorization: {categorize_time:.1f}s")
    log(f"  Fetch existing: {fetch_time:.1f}s")
    log(f"  Sync to Shopify: {sync_time:.1f}s")
    
    if len(unchanged) > 0 and categorized_products:
        log(f"  Work saved by delta sync: {len(unchanged)/len(categorized_products)*100:.1f}%")
    
    log(f"\nCategorization accuracy:")
    log(f"  JV category matches: {stats['by_source'].get('jv_category', 0)}")
    log(f"  Keyword matches: {stats['by_source'].get('keyword_match', 0)}")
    log(f"  Fallback matches: {stats['by_source'].get('global_part_fallback', 0)}")
    log(f"  Needs review: {stats['needs_review_count']} (see needs_review.csv)")
    log(f"  Skipped products: {len(skipped_products)} (see skipped_products.csv)")
    
    if duplicate_skus:
        log(f"\n⚠️  Warning: {len(duplicate_skus)} duplicate SKUs found in CSV (kept first occurrence)")
    
    if DRY_RUN:
        log("\n🔸 DRY RUN - No actual changes were made to Shopify")


if __name__ == '__main__':
    main()
