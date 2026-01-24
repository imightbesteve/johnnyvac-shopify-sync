#!/usr/bin/env python3
"""
JohnnyVac to Shopify Bulk Sync (Refactored)

Uses category_map.json as single source of truth for categorization.
Stateless, CI-compatible, GraphQL bulk operations with hash optimization.

Environment Variables:
    SHOPIFY_STORE: Store URL (e.g., kingsway-janitorial.myshopify.com)
    SHOPIFY_ACCESS_TOKEN: Admin API access token (shpat_...)
"""

import os
import sys
import csv
import json
import hashlib
import requests
import time
from urllib.request import urlopen
from typing import Dict, List, Optional

# Configuration
SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', '')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN', '')
CSV_URL = "https://www.johnnyvacstock.com/sigm_all_jv_products/JVWebProducts.csv"
IMAGE_BASE_URL = "https://www.johnnyvacstock.com/photos/web/"

GRAPHQL_URL = f"https://{SHOPIFY_STORE}/admin/api/2024-01/graphql.json"
HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN
}

# Sync settings
BATCH_SIZE = 50
RATE_LIMIT_DELAY = 1.0
BATCH_DELAY = 10
LANGUAGE = 'en'
CATEGORY_MAP_FILE = 'category_map.json'

# Metafield namespace for hash tracking
HASH_NAMESPACE = "kingsway"
HASH_KEY = "sync_hash"


def log(msg: str):
    """Simple logging with timestamp"""
    from datetime import datetime
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def load_category_map() -> List[Dict]:
    """Load category taxonomy from category_map.json"""
    try:
        with open(CATEGORY_MAP_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            categories = data.get('categories', [])
            log(f"✅ Loaded {len(categories)} categories from {CATEGORY_MAP_FILE}")
            return categories
    except FileNotFoundError:
        log(f"❌ Error: {CATEGORY_MAP_FILE} not found")
        sys.exit(1)
    except json.JSONDecodeError as e:
        log(f"❌ Error parsing {CATEGORY_MAP_FILE}: {e}")
        sys.exit(1)


def classify_product(title_en: str, title_fr: str, desc_en: str, desc_fr: str, categories: List[Dict]) -> str:
    """
    Classify product into canonical productType using keyword matching.
    Returns the productType with the highest score, or fallback category.
    """
    title_en = title_en.lower()
    title_fr = title_fr.lower()
    desc_en = desc_en.lower()
    desc_fr = desc_fr.lower()
    
    best_score = 0
    best_category = None
    
    for category in categories:
        # Skip the fallback category initially
        if category['productType'] == 'Other > Needs Review':
            continue
        
        score = 0
        
        # Check English keywords
        for keyword in category.get('keywords_en', []):
            keyword = keyword.lower()
            if keyword in title_en:
                score += 10  # Title matches are weighted higher
            if keyword in desc_en:
                score += 3
        
        # Check French keywords
        for keyword in category.get('keywords_fr', []):
            keyword = keyword.lower()
            if keyword in title_fr:
                score += 10
            if keyword in desc_fr:
                score += 3
        
        if score > best_score:
            best_score = score
            best_category = category['productType']
    
    # Fallback if no matches
    if best_category is None:
        log(f"⚠️  No category match for: {title_en[:50]}... (using fallback)")
        return 'Other > Needs Review'
    
    return best_category


def compute_product_hash(product_data: Dict) -> str:
    """Compute hash of product data for change detection"""
    # Hash key fields that matter for updates
    hash_input = {
        'title': product_data.get('title', ''),
        'body_html': product_data.get('body_html', ''),
        'product_type': product_data.get('product_type', ''),
        'tags': product_data.get('tags', ''),
        'price': product_data.get('price', ''),
        'inventory': product_data.get('inventory', 0),
        'weight': product_data.get('weight', 0),
        'image_src': product_data.get('image_src', '')
    }
    
    hash_str = json.dumps(hash_input, sort_keys=True)
    return hashlib.sha256(hash_str.encode()).hexdigest()[:16]


def fetch_csv_products(categories: List[Dict]) -> List[Dict]:
    """Fetch and parse JohnnyVac CSV, classify products"""
    log(f"📥 Fetching CSV from {CSV_URL}...")
    
    try:
        response = urlopen(CSV_URL, timeout=60)
        content = response.read().decode('utf-8')
        lines = content.strip().split('\n')
        
        reader = csv.DictReader(lines, delimiter=';')
        products = []
        
        for row in reader:
            sku = row.get('SKU', '').strip()
            if not sku:
                continue
            
            # Get bilingual content
            title_en = row.get('ProductTitleEN', '').strip()
            title_fr = row.get('ProductTitleFR', '').strip()
            desc_en = row.get('ProductDescriptionEN', '').strip()
            desc_fr = row.get('ProductDescriptionFR', '').strip()
            
            # Use language preference
            title = title_en if LANGUAGE == 'en' else title_fr
            description = desc_en if LANGUAGE == 'en' else desc_fr
            
            if not title:
                title = title_fr if title_fr else sku
            
            # Classify using category map
            product_type = classify_product(title_en, title_fr, desc_en, desc_fr, categories)
            
            # Parse product data
            try:
                price = float(row.get('RegularPrice', '0').strip() or '0')
                inventory = int(row.get('Inventory', '0').strip() or '0')
                weight = float(row.get('weight', '0').strip() or '0')
            except ValueError:
                price = 0
                inventory = 0
                weight = 0
            
            product_data = {
                'sku': sku,
                'title': title,
                'body_html': f"<p>{description}</p>" if description else "",
                'product_type': product_type,
                'vendor': "JohnnyVac",
                'tags': "JohnnyVac",
                'price': f"{price:.2f}",
                'inventory': inventory,
                'weight': weight,
                'image_src': f"{IMAGE_BASE_URL}{sku}.jpg"
            }
            
            products.append(product_data)
        
        log(f"✅ Parsed {len(products)} products from CSV")
        return products
        
    except Exception as e:
        log(f"❌ Error fetching CSV: {e}")
        sys.exit(1)


def get_existing_products() -> Dict[str, Dict]:
    """Fetch all existing products from Shopify with their metafields"""
    log("📡 Fetching existing products from Shopify...")
    
    query = '''
    query ($cursor: String) {
      products(first: 250, after: $cursor) {
        pageInfo {
          hasNextPage
          endCursor
        }
        edges {
          node {
            id
            handle
            variants(first: 1) {
              edges {
                node {
                  id
                  sku
                }
              }
            }
            metafields(namespace: "kingsway", first: 10) {
              edges {
                node {
                  key
                  value
                }
              }
            }
          }
        }
      }
    }
    '''
    
    products = {}
    cursor = None
    has_next = True
    
    while has_next:
        try:
            response = requests.post(
                GRAPHQL_URL,
                headers=HEADERS,
                json={"query": query, "variables": {"cursor": cursor}},
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            
            edges = data['data']['products']['edges']
            page_info = data['data']['products']['pageInfo']
            
            for edge in edges:
                product = edge['node']
                variant_edges = product.get('variants', {}).get('edges', [])
                
                if variant_edges:
                    sku = variant_edges[0]['node']['sku']
                    
                    # Extract sync hash from metafields
                    sync_hash = None
                    metafield_edges = product.get('metafields', {}).get('edges', [])
                    for mf_edge in metafield_edges:
                        mf = mf_edge['node']
                        if mf['key'] == HASH_KEY:
                            sync_hash = mf['value']
                            break
                    
                    products[sku] = {
                        'id': product['id'],
                        'handle': product['handle'],
                        'variant_id': variant_edges[0]['node']['id'],
                        'sync_hash': sync_hash
                    }
            
            has_next = page_info['hasNextPage']
            cursor = page_info['endCursor']
            
            time.sleep(RATE_LIMIT_DELAY)
            
        except Exception as e:
            log(f"❌ Error fetching products: {e}")
            break
    
    log(f"✅ Found {len(products)} existing products")
    return products


def create_product_graphql(product_data: Dict) -> bool:
    """Create a new product using GraphQL"""
    mutation = '''
    mutation productCreate($input: ProductInput!) {
      productCreate(input: $input) {
        userErrors {
          field
          message
        }
        product {
          id
          handle
        }
      }
    }
    '''
    
    product_hash = compute_product_hash(product_data)
    
    variables = {
        "input": {
            "title": product_data['title'],
            "descriptionHtml": product_data['body_html'],
            "productType": product_data['product_type'],
            "vendor": product_data['vendor'],
            "tags": [product_data['tags']],
            "variants": [
                {
                    "sku": product_data['sku'],
                    "price": product_data['price'],
                    "inventoryQuantities": {
                        "availableQuantity": product_data['inventory'],
                        "locationId": "gid://shopify/Location/1"  # Adjust if needed
                    },
                    "weight": product_data['weight'],
                    "weightUnit": "POUNDS"
                }
            ],
            "metafields": [
                {
                    "namespace": HASH_NAMESPACE,
                    "key": HASH_KEY,
                    "value": product_hash,
                    "type": "single_line_text_field"
                }
            ]
        }
    }
    
    # Add image if exists
    if product_data.get('image_src'):
        variables['input']['images'] = [{"src": product_data['image_src']}]
    
    try:
        response = requests.post(
            GRAPHQL_URL,
            headers=HEADERS,
            json={"query": mutation, "variables": variables},
            timeout=30
        )
        response.raise_for_status()
        result = response.json()
        
        errors = result.get('data', {}).get('productCreate', {}).get('userErrors', [])
        if errors:
            log(f"  ❌ Create errors: {errors}")
            return False
        
        return True
        
    except Exception as e:
        log(f"  ❌ Error creating product: {e}")
        return False


def update_product_graphql(product_id: str, variant_id: str, product_data: Dict) -> bool:
    """Update an existing product using GraphQL"""
    mutation = '''
    mutation productUpdate($input: ProductInput!) {
      productUpdate(input: $input) {
        userErrors {
          field
          message
        }
        product {
          id
        }
      }
    }
    '''
    
    product_hash = compute_product_hash(product_data)
    
    variables = {
        "input": {
            "id": product_id,
            "title": product_data['title'],
            "descriptionHtml": product_data['body_html'],
            "productType": product_data['product_type'],
            "tags": [product_data['tags']],
            "metafields": [
                {
                    "namespace": HASH_NAMESPACE,
                    "key": HASH_KEY,
                    "value": product_hash,
                    "type": "single_line_text_field"
                }
            ]
        }
    }
    
    # Update variant separately (price, inventory, weight)
    variant_mutation = '''
    mutation productVariantUpdate($input: ProductVariantInput!) {
      productVariantUpdate(input: $input) {
        userErrors {
          field
          message
        }
        productVariant {
          id
        }
      }
    }
    '''
    
    variant_variables = {
        "input": {
            "id": variant_id,
            "price": product_data['price'],
            "inventoryQuantities": {
                "availableQuantity": product_data['inventory'],
                "locationId": "gid://shopify/Location/1"
            },
            "weight": product_data['weight'],
            "weightUnit": "POUNDS"
        }
    }
    
    try:
        # Update product
        response = requests.post(
            GRAPHQL_URL,
            headers=HEADERS,
            json={"query": mutation, "variables": variables},
            timeout=30
        )
        response.raise_for_status()
        
        # Update variant
        response2 = requests.post(
            GRAPHQL_URL,
            headers=HEADERS,
            json={"query": variant_mutation, "variables": variant_variables},
            timeout=30
        )
        response2.raise_for_status()
        
        return True
        
    except Exception as e:
        log(f"  ❌ Error updating product: {e}")
        return False


def sync_products(csv_products: List[Dict], existing_products: Dict[str, Dict]):
    """Sync CSV products to Shopify with hash-based change detection"""
    log("🔄 Starting product sync...")
    
    created = 0
    updated = 0
    skipped = 0
    failed = 0
    
    total = len(csv_products)
    
    for i, product_data in enumerate(csv_products, 1):
        sku = product_data['sku']
        product_hash = compute_product_hash(product_data)
        
        log(f"[{i}/{total}] Processing: {sku} - {product_data['title'][:50]}")
        log(f"         Category: {product_data['product_type']}")
        
        if sku in existing_products:
            # Product exists - check if update needed
            existing = existing_products[sku]
            
            if existing.get('sync_hash') == product_hash:
                log(f"  ⏭️  Skipped (unchanged)")
                skipped += 1
            else:
                log(f"  🔄 Updating...")
                if update_product_graphql(existing['id'], existing['variant_id'], product_data):
                    log(f"  ✅ Updated")
                    updated += 1
                else:
                    failed += 1
        else:
            # Create new product
            log(f"  🆕 Creating...")
            if create_product_graphql(product_data):
                log(f"  ✅ Created")
                created += 1
            else:
                failed += 1
        
        # Rate limiting
        time.sleep(RATE_LIMIT_DELAY)
        
        # Batch delay
        if i % BATCH_SIZE == 0:
            log(f"💤 Batch complete ({i}/{total}). Pausing {BATCH_DELAY}s...")
            time.sleep(BATCH_DELAY)
    
    # Summary
    log("")
    log("=" * 70)
    log("SYNC COMPLETE")
    log("=" * 70)
    log(f"✅ Created:  {created}")
    log(f"🔄 Updated:  {updated}")
    log(f"⏭️  Skipped:  {skipped}")
    log(f"❌ Failed:   {failed}")
    log(f"📊 Total:    {total}")
    log("=" * 70)


def main():
    log("=" * 70)
    log("JohnnyVac → Shopify Bulk Sync (Refactored)")
    log("=" * 70)
    log("")
    
    # Validate environment
    if not SHOPIFY_STORE or not SHOPIFY_ACCESS_TOKEN:
        log("❌ Error: SHOPIFY_STORE and SHOPIFY_ACCESS_TOKEN must be set")
        sys.exit(1)
    
    # Load category taxonomy
    categories = load_category_map()
    
    # Fetch and classify CSV products
    csv_products = fetch_csv_products(categories)
    
    # Get existing Shopify products
    existing_products = get_existing_products()
    
    # Sync products
    sync_products(csv_products, existing_products)
    
    log("")
    log("✅ Sync job complete!")


if __name__ == "__main__":
    main()
