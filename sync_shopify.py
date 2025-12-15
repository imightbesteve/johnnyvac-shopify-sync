import os
import csv
import time
import requests
from datetime import datetime
from http.client import RemoteDisconnected
from urllib.error import URLError

# Configuration
SHOPIFY_STORE = os.environ['SHOPIFY_STORE']
SHOPIFY_ACCESS_TOKEN = os.environ['SHOPIFY_ACCESS_TOKEN']
CSV_URL = 'https://www.johnnyvacstock.com/sigm_all_jv_products/JVWebProducts.csv'
IMAGE_BASE_URL = 'https://www.johnnyvacstock.com/photos/web/'

# Performance settings
BATCH_SIZE = 100
RATE_LIMIT_DELAY = 0.5
BATCH_DELAY = 3

# Chunking support for full syncs
CHUNK_NUMBER = int(os.environ.get('CHUNK_NUMBER', '0'))
TOTAL_CHUNKS = int(os.environ.get('TOTAL_CHUNKS', '1'))

# Change detection
PREVIOUS_CSV_FILE = 'previous_products.csv'
FORCE_FULL_SYNC = os.environ.get('FORCE_FULL_SYNC', 'false').lower() == 'true'

LANGUAGE = 'en'

def make_shopify_request(method, endpoint, data=None, max_retries=3):
    """Make a Shopify API request with retry logic"""
    url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/{endpoint}"
    headers = {
        'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
        'Content-Type': 'application/json'
    }
    
    for attempt in range(max_retries):
        try:
            if method == 'GET':
                response = requests.get(url, headers=headers, timeout=30)
            elif method == 'POST':
                response = requests.post(url, headers=headers, json=data, timeout=30)
            elif method == 'PUT':
                response = requests.put(url, headers=headers, json=data, timeout=30)
            
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 2))
                print(f"Rate limited. Waiting {retry_after}s...")
                time.sleep(retry_after)
                continue
            
            response.raise_for_status()
            return response.json()
            
        except (RemoteDisconnected, URLError, requests.exceptions.RequestException) as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5
                print(f"Connection error (attempt {attempt + 1}/{max_retries}): {e}")
                print(f"Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise
    
    return None

def download_csv():
    """Download the CSV from JohnnyVac"""
    print("Downloading CSV from JohnnyVac...")
    response = requests.get(CSV_URL, timeout=60)
    response.raise_for_status()
    return response.text

def parse_csv(csv_content):
    """Parse CSV content into list of dictionaries"""
    lines = csv_content.strip().split('\n')
    reader = csv.DictReader(lines, delimiter=';')
    return list(reader)

def load_previous_csv():
    """Load the previous CSV if it exists"""
    if not os.path.exists(PREVIOUS_CSV_FILE):
        return None
    
    with open(PREVIOUS_CSV_FILE, 'r', encoding='utf-8') as f:
        content = f.read()
    
    return {row['SKU']: row for row in parse_csv(content)}

def save_current_csv(csv_content):
    """Save current CSV for next comparison"""
    with open(PREVIOUS_CSV_FILE, 'w', encoding='utf-8') as f:
        f.write(csv_content)
    print(f"\n✓ Saved current CSV to {PREVIOUS_CSV_FILE}")

def detect_changes(current_products, previous_products_dict):
    """Detect which products have changed"""
    if previous_products_dict is None:
        if FORCE_FULL_SYNC:
            print("⚠️  FORCE_FULL_SYNC enabled - will sync all products")
            return current_products
        else:
            print("ℹ️  First run - no previous CSV found")
            print("   Run with FORCE_FULL_SYNC=true to create all products")
            print("   Or commit an empty previous_products.csv to skip initial sync")
            return []
    
    if FORCE_FULL_SYNC:
        print("⚠️  FORCE_FULL_SYNC enabled - will sync all products")
        return current_products
    
    changed = []
    new_products = []
    
    # Fields to check for changes
    check_fields = ['RegularPrice', 'Inventory', 'ProductTitleEN', 'ProductTitleFR', 
                    'ProductDescriptionEN', 'ProductDescriptionFR']
    
    for product in current_products:
        sku = product['SKU']
        
        if sku not in previous_products_dict:
            # New product
            new_products.append(product)
        else:
            # Check if any important fields changed
            prev_product = previous_products_dict[sku]
            has_changes = False
            
            for field in check_fields:
                if product.get(field, '').strip() != prev_product.get(field, '').strip():
                    has_changes = True
                    break
            
            if has_changes:
                changed.append(product)
    
    print(f"\n📊 Change Detection Summary:")
    print(f"   Total products in CSV: {len(current_products)}")
    print(f"   New products: {len(new_products)}")
    print(f"   Changed products: {len(changed)}")
    print(f"   Total to sync: {len(new_products) + len(changed)}")
    
    return new_products + changed

def get_shopify_products_for_chunk(chunk_products):
    """Fetch all JohnnyVac products from Shopify - optimized for chunks"""
    print(f"\nFetching existing Shopify products...")
    all_products = {}
    page_info = None
    page = 1
    
    while True:
        # Build URL with pagination
        if page_info:
            url = f"products.json?vendor=JohnnyVac&limit=250&page_info={page_info}"
        else:
            url = f"products.json?vendor=JohnnyVac&limit=250"
        
        print(f"  Page {page}...", end=' ')
        result = make_shopify_request('GET', url)
        
        if not result or 'products' not in result:
            break
        
        products = result['products']
        print(f"got {len(products)} products")
        
        # Index by SKU
        for product in products:
            if product.get('variants'):
                for variant in product['variants']:
                    sku = variant.get('sku', '')
                    if sku:
                        all_products[sku] = {
                            'id': product['id'],
                            'variant': variant
                        }
        
        # Check for next page
        if len(products) < 250:
            break
        
        page += 1
        time.sleep(0.5)
    
    print(f"✓ Loaded {len(all_products)} existing products from Shopify\n")
    return all_products

def find_product_by_sku_graphql(sku):
    """Use GraphQL to quickly find a product by SKU"""
    query = """
    {
      productVariants(first: 1, query: "sku:%s") {
        edges {
          node {
            id
            product {
              id
              legacyResourceId
            }
            legacyResourceId
            inventoryItem {
              id
              legacyResourceId
            }
          }
        }
      }
    }
    """ % sku
    
    url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/graphql.json"
    headers = {
        'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.post(url, headers=headers, json={'query': query}, timeout=30)
        if response.status_code == 200:
            data = response.json()
            edges = data.get('data', {}).get('productVariants', {}).get('edges', [])
            if edges:
                node = edges[0]['node']
                return {
                    'id': int(node['product']['legacyResourceId']),
                    'variant': {
                        'id': int(node['legacyResourceId']),
                        'inventory_item_id': int(node['inventoryItem']['legacyResourceId']),
                        'sku': sku
                    }
                }
    except Exception as e:
        print(f"  GraphQL error for {sku}: {e}")
    
    return None

def get_inventory_location():
    """Get the default inventory location ID (cached)"""
    if not hasattr(get_inventory_location, 'location_id'):
        result = make_shopify_request('GET', 'locations.json')
        if result and result.get('locations'):
            get_inventory_location.location_id = result['locations'][0]['id']
            print(f"✓ Using inventory location: {result['locations'][0]['name']}\n")
        else:
            get_inventory_location.location_id = None
    return get_inventory_location.location_id

def create_or_update_product(product_data, existing_products, fetch_on_demand=False):
    """Create a new product or update an existing one"""
    sku = product_data['SKU']
    title = product_data.get(f'ProductTitle{LANGUAGE.upper()}', product_data.get('ProductTitleEN', sku))
    description = product_data.get(f'ProductDescription{LANGUAGE.upper()}', '')
    price = product_data.get('RegularPrice', '0')
    inventory = product_data.get('Inventory', '0')
    
    # Clean and validate data
    try:
        price = float(price) if price else 0
        inventory = int(inventory) if inventory else 0
    except ValueError:
        price = 0
        inventory = 0
    
    # Fetch product info if not pre-loaded (incremental mode using GraphQL)
    if fetch_on_demand and sku not in existing_products:
        product_info = find_product_by_sku_graphql(sku)
        if product_info:
            existing_products[sku] = product_info
        time.sleep(0.3)  # Small delay for GraphQL
    
    if sku in existing_products:
        # Update existing product
        product_info = existing_products[sku]
        product_id = product_info['id']
        variant = product_info['variant']
        variant_id = variant['id']
        inventory_item_id = variant.get('inventory_item_id')
        
        # Update product title/description
        update_payload = {
            "product": {
                "id": product_id,
                "title": title[:255],
                "body_html": description
            }
        }
        make_shopify_request('PUT', f"products/{product_id}.json", update_payload)
        
        # Update variant price
        variant_payload = {
            "variant": {
                "id": variant_id,
                "price": str(price)
            }
        }
        make_shopify_request('PUT', f"variants/{variant_id}.json", variant_payload)
        
        # Update inventory
        if inventory_item_id:
            location_id = get_inventory_location()
            if location_id:
                inventory_payload = {
                    "location_id": location_id,
                    "inventory_item_id": inventory_item_id,
                    "available": inventory
                }
                make_shopify_request('POST', 'inventory_levels/set.json', inventory_payload)
        
        return 'updated'
    else:
        # Create new product
        product_payload = {
            "product": {
                "title": title[:255],
                "body_html": description,
                "vendor": "JohnnyVac",
                "product_type": product_data.get('ProductCategory', ''),
                "variants": [{
                    "sku": sku,
                    "price": str(price),
                    "inventory_management": "shopify",
                    "inventory_quantity": inventory
                }],
                "images": [{
                    "src": f"{IMAGE_BASE_URL}{sku}.jpg"
                }]
            }
        }
        make_shopify_request('POST', 'products.json', product_payload)
        return 'created'

def apply_chunking(products, chunk_num, total_chunks):
    """Split products into chunks for parallel processing"""
    if total_chunks <= 1 or chunk_num <= 0:
        return products
    
    chunk_size = len(products) // total_chunks
    start_idx = (chunk_num - 1) * chunk_size
    
    if chunk_num == total_chunks:
        end_idx = len(products)
    else:
        end_idx = start_idx + chunk_size
    
    return products[start_idx:end_idx]

def sync_products():
    """Main sync function"""
    is_chunked = CHUNK_NUMBER > 0 and TOTAL_CHUNKS > 1
    
    print(f"\n{'='*70}")
    if is_chunked:
        print(f"JohnnyVac Full Sync - Chunk {CHUNK_NUMBER}/{TOTAL_CHUNKS}")
    else:
        print(f"JohnnyVac Smart Sync (Changes Only)")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")
    
    # Download current CSV
    csv_content = download_csv()
    current_products = parse_csv(csv_content)
    
    # For chunked full syncs, skip change detection
    if is_chunked:
        print(f"📦 Full sync mode - processing chunk {CHUNK_NUMBER}/{TOTAL_CHUNKS}")
        products_to_sync = apply_chunking(current_products, CHUNK_NUMBER, TOTAL_CHUNKS)
        print(f"   This chunk: {len(products_to_sync)} products\n")
    else:
        # Regular mode: detect changes only
        previous_products = load_previous_csv()
        products_to_sync = detect_changes(current_products, previous_products)
        
        if len(products_to_sync) == 0:
            print("\n✅ No changes detected! All products are up to date.")
            save_current_csv(csv_content)
            return
        
        print(f"\n🔄 Proceeding to sync {len(products_to_sync)} products...")
    
    # Get existing Shopify products
    if is_chunked:
        # For full syncs, fetch all products for this chunk's processing
        print("⏳ Fetching existing products for full sync...")
        existing_products = get_shopify_products_for_chunk(products_to_sync)
    else:
        # For incremental syncs, fetch products on-demand (much faster!)
        print("⚡ Incremental mode - will check products individually\n")
        existing_products = {}  # Will be populated on-demand
    
    # Process in batches
    total_products = len(products_to_sync)
    total_batches = (total_products + BATCH_SIZE - 1) // BATCH_SIZE
    
    created_count = 0
    updated_count = 0
    error_count = 0
    start_time = time.time()
    
    for batch_num in range(1, total_batches + 1):
        start_idx = (batch_num - 1) * BATCH_SIZE
        end_idx = min(start_idx + BATCH_SIZE, total_products)
        batch = products_to_sync[start_idx:end_idx]
        
        elapsed_time = time.time() - start_time
        
        print(f"\n{'='*70}")
        print(f"Batch {batch_num}/{total_batches} ({(batch_num/total_batches*100):.1f}%)")
        print(f"Products {start_idx + 1} to {end_idx} of {total_products}")
        print(f"Elapsed: {elapsed_time/60:.1f}m | Created: {created_count} | Updated: {updated_count}")
        print(f"{'='*70}\n")
        
        for product in batch:
            try:
                result = create_or_update_product(product, existing_products, fetch_on_demand=not is_chunked)
                if result == 'created':
                    created_count += 1
                    print(f"✓ Created: {product['SKU']}")
                else:
                    updated_count += 1
                    print(f"↻ Updated: {product['SKU']}")
                
                time.sleep(RATE_LIMIT_DELAY)
                
            except Exception as e:
                error_count += 1
                print(f"✗ Error with {product['SKU']}: {str(e)}")
        
        if batch_num < total_batches:
            print(f"\nWaiting {BATCH_DELAY}s before next batch...")
            time.sleep(BATCH_DELAY)
    
    # Save current CSV for next run (only in non-chunked mode)
    if not is_chunked:
        save_current_csv(csv_content)
    
    # Summary
    total_time = time.time() - start_time
    print(f"\n{'='*70}")
    if is_chunked:
        print(f"✅ Chunk {CHUNK_NUMBER}/{TOTAL_CHUNKS} Complete!")
    else:
        print(f"✅ Sync Complete!")
    print(f"{'='*70}")
    print(f"Created: {created_count}")
    print(f"Updated: {updated_count}")
    print(f"Errors: {error_count}")
    print(f"Total time: {total_time/60:.1f} minutes")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    sync_products()
