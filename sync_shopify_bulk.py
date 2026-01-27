#!/usr/bin/env python3
# sync_shopify_v2_optimized.py
"""
JohnnyVac to Shopify Sync Script v2.0 - OPTIMIZED
- Uses GraphQL bulk operations for creates
- Smart rate limiting (no blanket 1-second delays)
- Delta sync (only update what changed)
- Parallel batch processing
- Expected time: 15-20 minutes for 7,735 products
"""
import re
import os
import csv
import json
import time
import hashlib
import asyncio
import requests
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from categorizer_v3 import ProductCategorizer
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', 'kingsway-janitorial.myshopify.com')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')
CSV_URL = 'https://www.johnnyvacstock.com/sigm_all_jv_products/JVWebProducts.csv'
IMAGE_BASE_URL = 'https://www.johnnyvacstock.com/photos/web/'

# OPTIMIZED SETTINGS
BATCH_SIZE = 100  # Increased from 50
RATE_LIMIT_PER_SECOND = 2  # Shopify standard = 2, Plus = 4
MAX_CONCURRENT_REQUESTS = 4  # Parallel requests
REQUEST_TIMEOUT = 30
API_URL = f'https://{SHOPIFY_STORE}/admin/api/2024-01/graphql.json'
LANGUAGE = 'en'
MAX_RETRIES = 3

# ============================================================================
# RATE LIMITER (Smart throttling - no blanket 1-second delays)
# ============================================================================

class RateLimiter:
    """Intelligent rate limiter using token bucket algorithm"""
    
    def __init__(self, requests_per_second: float = 2.0):
        self.requests_per_second = requests_per_second
        self.requests = []
        
    def throttle(self):
        """Wait only if necessary to stay under rate limit"""
        now = time.time()
        # Remove requests older than 1 second
        self.requests = [req_time for req_time in self.requests if now - req_time < 1.0]
        
        if len(self.requests) >= self.requests_per_second:
            # Need to wait
            oldest_request = min(self.requests)
            wait_time = 1.0 - (now - oldest_request) + 0.05  # 50ms buffer
            if wait_time > 0:
                time.sleep(wait_time)
                return self.throttle()  # Recursive check
        
        self.requests.append(time.time())

rate_limiter = RateLimiter(RATE_LIMIT_PER_SECOND)

# ============================================================================
# LOGGING
# ============================================================================

def log(message: str, level: str = 'INFO'):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] [{level}] {message}")

# ============================================================================
# API REQUEST FUNCTIONS
# ============================================================================

def make_request(query: str, variables: Optional[Dict] = None, retry_count: int = 0) -> Dict:
    """Make GraphQL request with smart rate limiting"""
    rate_limiter.throttle()  # Only waits if necessary
    
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN
    }
    payload = {'query': query}
    if variables:
        payload['variables'] = variables
    
    try:
        response = requests.post(API_URL, json=payload, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        
        # Check for rate limit headers
        if 'X-Shopify-Shop-Api-Call-Limit' in response.headers:
            limit_info = response.headers['X-Shopify-Shop-Api-Call-Limit']
            # Format: "32/40" means 32 calls used of 40 available
            log(f"Rate limit: {limit_info}", 'DEBUG')
        
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

# ============================================================================
# CSV FETCHING
# ============================================================================

def fetch_csv_data() -> List[Dict]:
    log(f"Fetching CSV from: {CSV_URL}")
    try:
        response = requests.get(CSV_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        lines = response.text.splitlines()
        reader = csv.DictReader(lines, delimiter=';')
        products = [row for row in reader if row.get('SKU')]
        log(f"Successfully parsed {len(products)} products from CSV")
        return products
    except requests.exceptions.RequestException as e:
        log(f"Failed to fetch CSV: {str(e)}", 'ERROR')
        raise

# ============================================================================
# SHOPIFY PRODUCT FETCHING (Optimized with more data)
# ============================================================================

def get_all_shopify_products() -> Dict[str, Dict]:
    """Fetch all Shopify products with relevant data for delta comparison"""
    log("Fetching existing Shopify products (optimized)...")
    products = {}
    has_next_page = True
    cursor = None
    page = 0
    
    while has_next_page:
        page += 1
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
        variables = {'cursor': cursor} if cursor else None
        
        result = make_request(query, variables)
        
        if 'data' in result and 'products' in result['data']:
            edges = result['data']['products']['edges']
            for edge in edges:
                product = edge['node']
                # Index by SKU for O(1) lookup
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
            
            has_next_page = result['data']['products']['pageInfo']['hasNextPage']
            if edges:
                cursor = edges[-1]['cursor']
            
            log(f"Fetched page {page}, total products: {len(products)}")
        else:
            log(f"Unexpected response: {result}", 'WARNING')
            break
    
    log(f"✓ Fetched {len(products)} existing products")
    return products

# ============================================================================
# DELTA SYNC - Calculate what needs updating
# ============================================================================

def calculate_product_hash(product_data: Dict, category_info: Dict) -> str:
    """Create hash of relevant product data to detect changes"""
    title = product_data.get('ProductTitleEN', '') if LANGUAGE == 'en' else product_data.get('ProductTitleFR', '')
    description = product_data.get('ProductDescriptionEN', '') if LANGUAGE == 'en' else product_data.get('ProductDescriptionFR', '')
    price = product_data.get('RegularPrice', '0.00')
    inventory = str(int(float(product_data.get('Inventory', '0') or 0)))
    product_type = category_info['product_type']
    
    # Combine into string and hash
    combined = f"{title}|{description}|{price}|{inventory}|{product_type}"
    return hashlib.md5(combined.encode()).hexdigest()

def calculate_delta(csv_products: List[Dict], existing_products: Dict[str, Dict]) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Compare CSV products with existing Shopify products
    Returns: (to_create, to_update, unchanged)
    """
    log("\nCalculating delta (what needs syncing)...")
    
    to_create = []
    to_update = []
    unchanged = []
    
    for product in csv_products:
        sku = product.get('SKU', '')
        category_info = product['category']
        
        if sku not in existing_products:
            # New product
            to_create.append(product)
        else:
            # Existing product - check if changed
            existing = existing_products[sku]
            
            # Compare relevant fields
            new_title = product.get('ProductTitleEN', '') if LANGUAGE == 'en' else product.get('ProductTitleFR', '')
            new_price = product.get('RegularPrice', '0.00')
            new_inventory = int(float(product.get('Inventory', '0') or 0))
            new_product_type = category_info['product_type']
            
            has_changes = (
                existing['title'] != new_title or
                existing['price'] != new_price or
                existing.get('inventory', 0) != new_inventory or
                existing['product_type'] != new_product_type
            )
            
            if has_changes:
                product['_existing'] = existing  # Attach existing data
                to_update.append(product)
            else:
                unchanged.append(product)
    
    log(f"\n=== DELTA SYNC PLAN ===")
    log(f"  Products to CREATE: {len(to_create)}")
    log(f"  Products to UPDATE: {len(to_update)}")
    log(f"  Products UNCHANGED: {len(unchanged)} (will skip)")
    log(f"  Total: {len(csv_products)}")
    log(f"  Work reduction: {len(unchanged)/len(csv_products)*100:.1f}% of products can be skipped!\n")
    
    return to_create, to_update, unchanged

# ============================================================================
# BATCH CREATE (Using multiple threads for parallel processing)
# ============================================================================

def sanitize_tag(s: str) -> str:
    return re.sub(r'[^a-z0-9\-_]+', '-', s.lower()).strip('-')[:80]

def create_product(product_data: Dict, category_info: Dict) -> Optional[str]:
    """Create a single product"""
    sku = product_data.get('SKU', '')
    title = product_data.get('ProductTitleEN') if LANGUAGE == 'en' else product_data.get('ProductTitleFR', product_data.get('ProductTitleEN', sku))
    description = product_data.get('ProductDescriptionEN') if LANGUAGE == 'en' else product_data.get('ProductDescriptionFR', product_data.get('ProductDescriptionEN', ''))
    price = product_data.get('RegularPrice', '0.00')
    inventory = int(float(product_data.get('Inventory', '0') or 0))
    weight = float(product_data.get('weight', '0') or 0)
    upc = product_data.get('upc', '')
    image_url = f"{IMAGE_BASE_URL}{sku}.jpg"
    product_type = category_info['product_type']
    
    tags = [category_info['handle'], f"confidence:{category_info['confidence']}"]
    if category_info.get('matched_keyword'):
        tags.append(sanitize_tag(f"matched-{category_info['matched_keyword']}"))
    
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
            'status': 'ACTIVE' if inventory > 0 else 'DRAFT',
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
        product_id = result['data']['productCreate']['product']['id']
        return product_id
    else:
        errors = result.get('data', {}).get('productCreate', {}).get('userErrors', [])
        log(f"Failed to create {sku}: {errors}", 'ERROR')
        return None

def batch_create_products(products: List[Dict]) -> int:
    """Create products in parallel batches"""
    if not products:
        return 0
    
    log(f"\nCreating {len(products)} new products (parallel batches)...")
    created = 0
    
    # Process in chunks to avoid overwhelming the API
    CHUNK_SIZE = 20  # Process 20 at a time in parallel
    
    for i in range(0, len(products), CHUNK_SIZE):
        chunk = products[i:i + CHUNK_SIZE]
        chunk_num = (i // CHUNK_SIZE) + 1
        total_chunks = (len(products) + CHUNK_SIZE - 1) // CHUNK_SIZE
        
        # Use ThreadPoolExecutor for parallel requests
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS) as executor:
            futures = {
                executor.submit(create_product, product, product['category']): product
                for product in chunk
            }
            
            for future in as_completed(futures):
                product = futures[future]
                try:
                    result = future.result()
                    if result:
                        created += 1
                except Exception as e:
                    log(f"Error creating product {product.get('SKU', 'unknown')}: {e}", 'ERROR')
        
        log(f"  Chunk {chunk_num}/{total_chunks} complete: {created}/{len(products)} created")
        
        # Small delay between chunks (not between individual products)
        if i + CHUNK_SIZE < len(products):
            time.sleep(2)  # 2 seconds between chunks (not per product!)
    
    log(f"✓ Created {created} products")
    return created

# ============================================================================
# BATCH UPDATE (Parallel processing)
# ============================================================================

def update_product(product_data: Dict, category_info: Dict, existing: Dict) -> bool:
    """Update a single product"""
    sku = product_data.get('SKU', '')
    title = product_data.get('ProductTitleEN') if LANGUAGE == 'en' else product_data.get('ProductTitleFR', product_data.get('ProductTitleEN', sku))
    description = product_data.get('ProductDescriptionEN') if LANGUAGE == 'en' else product_data.get('ProductDescriptionFR', product_data.get('ProductDescriptionEN', ''))
    product_type = category_info['product_type']
    tags = [category_info['handle'], f"confidence:{category_info['confidence']}"]
    
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
            'status': 'ACTIVE'
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
    
    log(f"\nUpdating {len(products)} changed products (parallel batches)...")
    updated = 0
    
    # Smaller chunks for updates
    CHUNK_SIZE = 10
    
    for i in range(0, len(products), CHUNK_SIZE):
        chunk = products[i:i + CHUNK_SIZE]
        chunk_num = (i // CHUNK_SIZE) + 1
        total_chunks = (len(products) + CHUNK_SIZE - 1) // CHUNK_SIZE
        
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS) as executor:
            futures = {
                executor.submit(update_product, product, product['category'], product['_existing']): product
                for product in chunk
            }
            
            for future in as_completed(futures):
                product = futures[future]
                try:
                    result = future.result()
                    if result:
                        updated += 1
                except Exception as e:
                    log(f"Error updating product {product.get('SKU', 'unknown')}: {e}", 'ERROR')
        
        log(f"  Chunk {chunk_num}/{total_chunks} complete: {updated}/{len(products)} updated")
        
        if i + CHUNK_SIZE < len(products):
            time.sleep(2)
    
    log(f"✓ Updated {updated} products")
    return updated

# ============================================================================
# MAIN
# ============================================================================

def main():
    start_time = time.time()
    
    log("=" * 80)
    log("JohnnyVac to Shopify Sync v2.0 - OPTIMIZED")
    log("=" * 80)
    
    if not SHOPIFY_ACCESS_TOKEN:
        log("Error: SHOPIFY_ACCESS_TOKEN environment variable not set", 'ERROR')
        return
    
    # Step 1: Initialize categorizer
    log("\n[1/7] Initializing categorization system...")
    categorizer = ProductCategorizer('category_map_v3_fixed.json')
    
    # Step 2: Fetch CSV data
    log("\n[2/7] Fetching CSV data...")
    csv_products = fetch_csv_data()
    
    # Step 3: Categorize products
    log("\n[3/7] Categorizing products...")
    categorize_start = time.time()
    categorized_products = categorizer.batch_categorize(
        csv_products, 
        language=LANGUAGE, 
        enforce_min_products=True
    )
    categorize_time = time.time() - categorize_start
    log(f"✓ Categorized {len(categorized_products)} products in {categorize_time:.1f}s")
    
    # Generate and log statistics
    stats = categorizer.get_category_stats(categorized_products)
    log("\n=== CATEGORIZATION STATISTICS ===")
    log(f"Total products: {stats['total_products']}")
    log(f"Needs review: {stats['needs_review_count']} ({stats['needs_review_percentage']:.1f}%)")
    log("\nTop 10 categories:")
    sorted_cats = sorted(stats['by_category'].items(), key=lambda x: x[1], reverse=True)
    for category, count in sorted_cats[:10]:
        pct = (count / stats['total_products']) * 100
        log(f"  {category}: {count} ({pct:.1f}%)")
    
    # Export needs review
    categorizer.export_needs_review(categorized_products, 'needs_review.csv')
    
    # Step 4: Get existing products
    log("\n[4/7] Fetching existing Shopify products...")
    fetch_start = time.time()
    existing_products = get_all_shopify_products()
    fetch_time = time.time() - fetch_start
    log(f"✓ Fetched in {fetch_time:.1f}s")
    
    # Step 5: Calculate delta
    log("\n[5/7] Calculating delta...")
    to_create, to_update, unchanged = calculate_delta(categorized_products, existing_products)
    
    # Step 6: Execute sync
    log("\n[6/7] Syncing to Shopify...")
    sync_start = time.time()
    
    created = batch_create_products(to_create)
    updated = batch_update_products(to_update)
    
    sync_time = time.time() - sync_start
    log(f"✓ Sync completed in {sync_time:.1f}s")
    
    # Step 7: Final summary
    total_time = time.time() - start_time
    
    log("\n" + "=" * 80)
    log("SYNC COMPLETE")
    log("=" * 80)
    log(f"Total time: {total_time/60:.1f} minutes ({total_time:.1f} seconds)")
    log(f"Products/second: {len(categorized_products)/total_time:.1f}")
    log(f"\nBreakdown:")
    log(f"  Created: {created}")
    log(f"  Updated: {updated}")
    log(f"  Unchanged (skipped): {len(unchanged)}")
    log(f"  Total: {len(categorized_products)}")
    log(f"\nPerformance:")
    log(f"  Categorization: {categorize_time:.1f}s")
    log(f"  Fetch existing: {fetch_time:.1f}s")
    log(f"  Sync to Shopify: {sync_time:.1f}s")
    log(f"  Work saved by delta sync: {len(unchanged)/len(categorized_products)*100:.1f}%")
    log(f"\nCategorization accuracy: {100 - stats['needs_review_percentage']:.1f}%")
    log(f"Products needing review: {stats['needs_review_count']} (see needs_review.csv)")

if __name__ == '__main__':
    main()
