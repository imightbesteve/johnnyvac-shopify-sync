#!/usr/bin/env python3
# sync_shopify_bulk_v3.py
"""
JohnnyVac to Shopify Sync - BULK OPERATIONS VERSION

Uses Shopify's Bulk Operations API for fast processing of large catalogs.
Can sync 7,000+ products in 10-20 minutes instead of 3+ hours.

How it works:
1. Fetch CSV and categorize products (fast, ~2 seconds)
2. Fetch existing Shopify products via bulk query (fast, ~1-2 minutes)
3. Generate JSONL file with mutations
4. Upload and execute bulk mutation (Shopify processes async)
5. Poll for completion (~5-15 minutes for 7,000 products)
"""

import os
import re
import csv
import json
import time
import requests
from typing import Dict, List, Optional, Tuple, Set
from datetime import datetime

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

API_VERSION = '2024-01'
GRAPHQL_URL = f'https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/graphql.json'

HEADERS = {
    'Content-Type': 'application/json',
    'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN
}

# Bulk operation polling
POLL_INTERVAL = 10  # seconds
MAX_POLL_TIME = 7200  # 2 hours max wait

# =============================================================================
# LOGGING
# =============================================================================

def log(message: str, level: str = 'INFO'):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] [{level}] {message}", flush=True)

# =============================================================================
# GRAPHQL HELPERS
# =============================================================================

def graphql_request(query: str, variables: Optional[Dict] = None) -> Dict:
    """Make a GraphQL request to Shopify"""
    payload = {'query': query}
    if variables:
        payload['variables'] = variables
    
    response = requests.post(GRAPHQL_URL, json=payload, headers=HEADERS, timeout=60)
    response.raise_for_status()
    result = response.json()
    
    if 'errors' in result:
        log(f"GraphQL errors: {result['errors']}", 'ERROR')
    
    return result

# =============================================================================
# HTML CLEANING
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
# BULK QUERY - Get existing products
# =============================================================================

def get_existing_products_bulk() -> Dict[str, Dict]:
    """Use bulk operation to fetch all existing products"""
    log("Starting bulk query for existing products...")
    
    # Start bulk query
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
    
    result = graphql_request(mutation)
    
    if result.get('data', {}).get('bulkOperationRunQuery', {}).get('userErrors'):
        errors = result['data']['bulkOperationRunQuery']['userErrors']
        log(f"Bulk query errors: {errors}", 'ERROR')
        return {}
    
    # Poll for completion
    products = poll_bulk_operation_and_parse_products()
    return products

def poll_bulk_operation_and_parse_products() -> Dict[str, Dict]:
    """Poll bulk operation and parse results into product dict"""
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
        result = graphql_request(query)
        operation = result.get('data', {}).get('currentBulkOperation')
        
        if not operation:
            log("No bulk operation found", 'WARNING')
            return {}
        
        status = operation.get('status')
        log(f"  Bulk operation status: {status}, objects: {operation.get('objectCount', 0)}")
        
        if status == 'COMPLETED':
            url = operation.get('url')
            if url:
                return download_and_parse_bulk_results(url)
            else:
                log("Bulk operation completed but no URL", 'WARNING')
                return {}
        elif status in ['FAILED', 'CANCELED']:
            log(f"Bulk operation failed: {operation.get('errorCode')}", 'ERROR')
            return {}
        
        time.sleep(POLL_INTERVAL)
    
    log("Bulk operation timed out", 'ERROR')
    return {}

def download_and_parse_bulk_results(url: str) -> Dict[str, Dict]:
    """Download JSONL results and parse into product dict"""
    log(f"Downloading bulk results...")
    
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    
    products = {}
    current_product = None
    
    for line in response.text.strip().split('\n'):
        if not line:
            continue
        obj = json.loads(line)
        
        # Product line
        if 'variants' not in obj and 'sku' not in obj and 'id' in obj:
            current_product = {
                'product_id': obj['id'],
                'title': obj.get('title', ''),
                'product_type': obj.get('productType', ''),
                'status': obj.get('status', 'ACTIVE')
            }
        # Variant line (child of product)
        elif 'sku' in obj and current_product:
            sku = obj.get('sku')
            if sku:
                products[sku] = {
                    **current_product,
                    'variant_id': obj['id'],
                    'price': obj.get('price', '0'),
                    'inventory': obj.get('inventoryQuantity', 0)
                }
    
    log(f"✓ Parsed {len(products)} existing products from Shopify")
    return products

# =============================================================================
# ALTERNATIVE: Paginated fetch (if bulk fails)
# =============================================================================

def get_existing_products_paginated() -> Dict[str, Dict]:
    """Fallback: fetch products with pagination"""
    log("Fetching existing products (paginated)...")
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
        variables = {'cursor': cursor} if cursor else None
        result = graphql_request(query, variables)
        
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
        
        if page % 10 == 0:
            log(f"  Page {page}, products: {len(products)}")
        
        if not page_info.get('hasNextPage'):
            break
        cursor = edges[-1]['cursor']
        time.sleep(0.5)  # Rate limiting
    
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
# BULK MUTATION - Create/Update products
# =============================================================================

def generate_jsonl_for_creates(products: List[Dict], filename: str) -> int:
    """Generate JSONL file for bulk product creation"""
    count = 0
    
    with open(filename, 'w', encoding='utf-8') as f:
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
            count += 1
    
    return count

def generate_jsonl_for_updates(products: List[Dict], filename: str) -> int:
    """Generate JSONL file for bulk product updates"""
    count = 0
    
    with open(filename, 'w', encoding='utf-8') as f:
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
                    "status": status,
                    "tags": tags
                }
            }
            
            f.write(json.dumps(mutation_input) + '\n')
            count += 1
    
    return count

def upload_jsonl_and_run_bulk_mutation(jsonl_file: str, mutation_type: str = 'productCreate') -> bool:
    """Upload JSONL file and run bulk mutation"""
    
    # Step 1: Get staged upload URL
    log(f"Getting staged upload URL for {mutation_type}...")
    
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
          parameters {
            name
            value
          }
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    
    result = graphql_request(staged_mutation)
    
    if result.get('data', {}).get('stagedUploadsCreate', {}).get('userErrors'):
        log(f"Staged upload errors: {result['data']['stagedUploadsCreate']['userErrors']}", 'ERROR')
        return False
    
    target = result['data']['stagedUploadsCreate']['stagedTargets'][0]
    upload_url = target['url']
    resource_url = target['resourceUrl']
    params = {p['name']: p['value'] for p in target['parameters']}
    
    # Step 2: Upload the file
    log(f"Uploading JSONL file...")
    
    with open(jsonl_file, 'rb') as f:
        files = {'file': ('bulk_input.jsonl', f, 'text/jsonl')}
        upload_response = requests.post(upload_url, data=params, files=files, timeout=300)
        upload_response.raise_for_status()
    
    log(f"✓ File uploaded successfully")
    
    # Step 3: Run bulk mutation
    log(f"Starting bulk {mutation_type} operation...")
    
    bulk_mutation = f"""
    mutation {{
      bulkOperationRunMutation(
        mutation: "mutation call($input: ProductInput!) {{ {mutation_type}(input: $input) {{ product {{ id }} userErrors {{ field message }} }} }}",
        stagedUploadPath: "{resource_url}"
      ) {{
        bulkOperation {{
          id
          status
        }}
        userErrors {{
          field
          message
        }}
      }}
    }}
    """
    
    result = graphql_request(bulk_mutation)
    
    if result.get('data', {}).get('bulkOperationRunMutation', {}).get('userErrors'):
        errors = result['data']['bulkOperationRunMutation']['userErrors']
        log(f"Bulk mutation errors: {errors}", 'ERROR')
        return False
    
    # Step 4: Poll for completion
    return poll_bulk_mutation_completion()

def poll_bulk_mutation_completion() -> bool:
    """Poll until bulk mutation completes"""
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
    last_count = 0
    
    while time.time() - start_time < MAX_POLL_TIME:
        result = graphql_request(query)
        operation = result.get('data', {}).get('currentBulkOperation')
        
        if not operation:
            time.sleep(POLL_INTERVAL)
            continue
        
        status = operation.get('status')
        count = operation.get('objectCount', 0)
        root_count = operation.get('rootObjectCount', 0)
        
        if count != last_count:
            log(f"  Status: {status}, processed: {count} objects, {root_count} products")
            last_count = count
        
        if status == 'COMPLETED':
            log(f"✓ Bulk operation completed! Processed {root_count} products")
            return True
        elif status in ['FAILED', 'CANCELED']:
            log(f"Bulk operation failed: {operation.get('errorCode')}", 'ERROR')
            # Try to get error details
            if operation.get('url'):
                try:
                    err_response = requests.get(operation['url'], timeout=60)
                    log(f"Error details: {err_response.text[:500]}", 'ERROR')
                except:
                    pass
            return False
        
        time.sleep(POLL_INTERVAL)
    
    log("Bulk operation timed out", 'ERROR')
    return False

# =============================================================================
# ARCHIVE MISSING PRODUCTS (one by one, but usually small count)
# =============================================================================

def archive_missing_products(missing_skus: List[str], existing_products: Dict[str, Dict]) -> int:
    """Archive products no longer in CSV"""
    if not missing_skus or not ARCHIVE_MISSING:
        return 0
    
    log(f"Archiving {len(missing_skus)} missing products...")
    archived = 0
    
    mutation = """
    mutation updateProduct($input: ProductInput!) {
        productUpdate(input: $input) {
            product { id }
            userErrors { field message }
        }
    }
    """
    
    for sku in missing_skus:
        existing = existing_products.get(sku)
        if not existing or existing.get('status') == 'DRAFT':
            continue
        
        if DRY_RUN:
            archived += 1
            continue
        
        result = graphql_request(mutation, {
            'input': {
                'id': existing['product_id'],
                'status': 'DRAFT'
            }
        })
        
        if result.get('data', {}).get('productUpdate', {}).get('product'):
            archived += 1
        
        time.sleep(0.5)  # Rate limit
    
    log(f"✓ Archived {archived} products")
    return archived

# =============================================================================
# MAIN
# =============================================================================

def main():
    start_time = time.time()
    
    log("=" * 70)
    log("JohnnyVac to Shopify Sync v3.0 - BULK OPERATIONS")
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
    
    # Step 4: Fetch existing products
    log("\n[4/7] Fetching existing Shopify products...")
    try:
        existing_products = get_existing_products_bulk()
    except Exception as e:
        log(f"Bulk query failed, using paginated fallback: {e}", 'WARNING')
        existing_products = get_existing_products_paginated()
    
    # Step 5: Calculate delta
    log("\n[5/7] Calculating delta...")
    to_create, to_update, unchanged, missing_skus = calculate_delta(
        categorized_products, existing_products
    )
    
    # Step 6: Execute sync
    log("\n[6/7] Syncing to Shopify...")
    
    created = 0
    updated = 0
    
    if DRY_RUN:
        log(f"[DRY RUN] Would create {len(to_create)} products")
        log(f"[DRY RUN] Would update {len(to_update)} products")
        created = len(to_create)
        updated = len(to_update)
    else:
        # Create new products
        if to_create:
            log(f"\nCreating {len(to_create)} new products...")
            jsonl_file = 'bulk_creates.jsonl'
            count = generate_jsonl_for_creates(to_create, jsonl_file)
            log(f"Generated {count} mutations in {jsonl_file}")
            
            if upload_jsonl_and_run_bulk_mutation(jsonl_file, 'productCreate'):
                created = count
            else:
                log("Bulk create failed!", 'ERROR')
        
        # Update existing products
        if to_update:
            log(f"\nUpdating {len(to_update)} products...")
            jsonl_file = 'bulk_updates.jsonl'
            count = generate_jsonl_for_updates(to_update, jsonl_file)
            log(f"Generated {count} mutations in {jsonl_file}")
            
            if upload_jsonl_and_run_bulk_mutation(jsonl_file, 'productUpdate'):
                updated = count
            else:
                log("Bulk update failed!", 'ERROR')
    
    # Step 7: Archive missing
    log("\n[7/7] Archiving missing products...")
    archived = archive_missing_products(missing_skus, existing_products)
    
    # Summary
    total_time = time.time() - start_time
    
    log("\n" + "=" * 70)
    log("SYNC COMPLETE")
    log("=" * 70)
    log(f"Total time: {total_time/60:.1f} minutes")
    log(f"\nResults:")
    log(f"  ✅ Created: {created}")
    log(f"  ✏️  Updated: {updated}")
    log(f"  ⏭️  Unchanged: {len(unchanged)}")
    log(f"  🗑️  Archived: {archived}")
    log(f"  ⛔ Skipped: {len(skipped_products)}")
    
    if DRY_RUN:
        log("\n🔸 DRY RUN - No actual changes were made")

if __name__ == '__main__':
    main()
