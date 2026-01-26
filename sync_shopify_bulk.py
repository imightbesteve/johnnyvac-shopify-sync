#!/usr/bin/env python3
# sync_shopify_v2.py
"""
JohnnyVac to Shopify Sync Script v2.0 (updated to use categorizer_v3 + category_map_v3)
"""

import os
import csv
import json
import time
import requests
from typing import Dict, List, Optional
from datetime import datetime
from categorizer_v3 import ProductCategorizer

# Configuration
SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', 'kingsway-janitorial.myshopify.com')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')
CSV_URL = 'https://www.johnnyvacstock.com/sigm_all_jv_products/JVWebProducts.csv'
IMAGE_BASE_URL = 'https://www.johnnyvacstock.com/photos/web/'
BATCH_SIZE = 50
RATE_LIMIT_DELAY = 1.0
BATCH_DELAY = 10
LANGUAGE = 'en'
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30
API_URL = f'https://{SHOPIFY_STORE}/admin/api/2024-01/graphql.json'

def log(message: str, level: str = 'INFO'):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] [{level}] {message}")

def make_request(query: str, variables: Optional[Dict] = None, retry_count: int = 0) -> Dict:
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

def fetch_csv_data() -> List[Dict]:
    log(f"Fetching CSV from: {CSV_URL}")
    try:
        response = requests.get(CSV_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        lines = response.text.splitlines()
        reader = csv.DictReader(lines, delimiter=';')
        products = []
        for row in reader:
            if row.get('SKU'):
                products.append(row)
        log(f"Successfully parsed {len(products)} products from CSV")
        return products
    except requests.exceptions.RequestException as e:
        log(f"Failed to fetch CSV: {str(e)}", 'ERROR')
        raise

def get_all_shopify_products() -> Dict[str, str]:
    log("Fetching existing Shopify products (by SKU)...")
    products = {}
    has_next_page = True
    cursor = None
    page = 0
    while has_next_page:
        page += 1
        if cursor:
            query = """
            query getProducts($cursor: String!) {
                products(first: 250, after: $cursor) {
                    edges {
                        node {
                            id
                            variants(first: 1) {
                                edges {
                                    node {
                                        sku
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
            variables = {'cursor': cursor}
        else:
            query = """
            query getProducts {
                products(first: 250) {
                    edges {
                        node {
                            id
                            variants(first: 1) {
                                edges {
                                    node {
                                        sku
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
            variables = None
        result = make_request(query, variables)
        if 'data' in result and 'products' in result['data']:
            edges = result['data']['products']['edges']
            for edge in edges:
                product = edge['node']
                if product['variants']['edges']:
                    sku = product['variants']['edges'][0]['node']['sku']
                    if sku:
                        products[sku] = product['id']
            has_next_page = result['data']['products']['pageInfo']['hasNextPage']
            if edges:
                cursor = edges[-1]['cursor']
            log(f"Fetched page {page}, total products known: {len(products)}")
            time.sleep(RATE_LIMIT_DELAY)
        else:
            log(f"Unexpected response when fetching products: {result}", 'WARNING')
            break
    log(f"Total existing products in Shopify: {len(products)}")
    return products

def sanitize_tag(s: str) -> str:
    # Make short, safe tag tokens
    return re.sub(r'[^a-z0-9\-_]+', '-', s.lower()).strip('-')[:80]

def create_product(product_data: Dict, category_info: Dict) -> Optional[str]:
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
        log(f"Created: {sku} -> {product_type} (confidence: {category_info['confidence']})")
        # inventory update placeholder - implement inventory item lookup & update as needed
        return product_id
    else:
        errors = result.get('data', {}).get('productCreate', {}).get('userErrors', [])
        log(f"Failed to create {sku}: {errors}", 'ERROR')
        return None

def update_product(product_id: str, product_data: Dict, category_info: Dict) -> bool:
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
            'id': product_id,
            'title': title,
            'descriptionHtml': description,
            'productType': product_type,
            'tags': tags,
            'status': 'ACTIVE'
        }
    }
    result = make_request(mutation, variables)
    if 'data' in result and result['data'].get('productUpdate', {}).get('product'):
        log(f"Updated: {sku} -> {product_type}")
        return True
    else:
        errors = result.get('data', {}).get('productUpdate', {}).get('userErrors', [])
        log(f"Failed to update {sku}: {errors}", 'ERROR')
        return False

def main():
    log("=" * 80)
    log("JohnnyVac to Shopify Sync v2.0 - Starting")
    log("=" * 80)
    if not SHOPIFY_ACCESS_TOKEN:
        log("Error: SHOPIFY_ACCESS_TOKEN environment variable not set", 'ERROR')
        return
    # Initialize categorizer
    log("Initializing categorization system using category_map_v3.json...")
    categorizer = ProductCategorizer('category_map_v3.json')
    # Fetch CSV data
    csv_products = fetch_csv_data()
    # Categorize all products (enforce min_products: True)
    log("Categorizing products (this may take a few minutes)...")
    categorized_products = categorizer.batch_categorize(csv_products, language=LANGUAGE, enforce_min_products=True)
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
    # Export needs review products
    categorizer.export_needs_review(categorized_products, 'needs_review.csv')
    # Get existing products
    existing_products = get_all_shopify_products()
    # Process in batches
    total = len(categorized_products)
    created = 0
    updated = 0
    skipped = 0
    failed = 0
    log(f"\nProcessing {total} products in batches of {BATCH_SIZE}...")
    for i in range(0, total, BATCH_SIZE):
        batch = categorized_products[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
        log(f"\n--- Batch {batch_num}/{total_batches} ---")
        for product in batch:
            sku = product.get('SKU', '')
            category_info = product['category']
            if sku in existing_products:
                if update_product(existing_products[sku], product, category_info):
                    updated += 1
                else:
                    failed += 1
            else:
                if create_product(product, category_info):
                    created += 1
                else:
                    failed += 1
            time.sleep(RATE_LIMIT_DELAY)
        log(f"Batch {batch_num} complete. Waiting {BATCH_DELAY}s before next batch...")
        time.sleep(BATCH_DELAY)
    # Final summary
    log("\n" + "=" * 80)
    log("SYNC COMPLETE")
    log("=" * 80)
    log(f"Total products processed: {total}")
    log(f"Created: {created}")
    log(f"Updated: {updated}")
    log(f"Failed: {failed}")
    log(f"Categorization accuracy: {100 - stats['needs_review_percentage']:.1f}%")
    log(f"Products needing review: {stats['needs_review_count']} (see needs_review.csv)")

if __name__ == '__main__':
    main()
