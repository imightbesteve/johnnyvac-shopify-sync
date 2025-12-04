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

# Chunking support (set via environment variable)
CHUNK_NUMBER = int(os.environ.get('CHUNK_NUMBER', '0'))  # 0 = all products
TOTAL_CHUNKS = int(os.environ.get('TOTAL_CHUNKS', '1'))   # 1 = no chunking

# Change detection
PREVIOUS_CSV_FILE = 'previous_products.csv'
FORCE_FULL_SYNC = os.environ.get('FORCE_FULL_SYNC', 'false').lower() == 'true'

LANGUAGE = 'en'  # or 'fr'

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
    print(f"Saved current CSV to {PREVIOUS_CSV_FILE}")

def detect_changes(current_products, previous_products_dict):
    """Detect which products have changed"""
    if previous_products_dict is None or FORCE_FULL_SYNC:
        print("No previous CSV found or FORCE_FULL_SYNC set - syncing all products")
        return current_products, []
    
    changed = []
    unchanged = []
    
    # Fields to check for changes
    check_fields = ['RegularPrice', 'Inventory', 'ProductTitleEN', 'ProductTitleFR', 
                    'ProductDescriptionEN', 'ProductDescriptionFR']
    
    for product in current_products:
        sku = product['SKU']
        
        if sku not in previous_products_dict:
            # New product
            changed.append(product)
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
            else:
                unchanged.append(product)
    
    print(f"\n📊 Change Detection Summary:")
    print(f"   Total products: {len(current_products)}")
    print(f"   Changed/New: {len(changed)}")
    print(f"   Unchanged: {len(unchanged)}")
    print()
    
    return changed, unchanged

def apply_chunking(products):
    """Apply chunking if configured"""
    if TOTAL_CHUNKS <= 1 or CHUNK_NUMBER <= 0:
        return products
    
    chunk_size = len(products) // TOTAL_CHUNKS
    start_idx = (CHUNK_NUMBER - 1) * chunk_size
    
    if CHUNK_NUMBER == TOTAL_CHUNKS:
        # Last chunk gets remaining products
        end_idx = len(products)
    else:
        end_idx = start_idx + chunk_size
    
    chunk = products[start_idx:end_idx]
    print(f"\n🔹 Chunking enabled: Processing chunk {CHUNK_NUMBER}/{TOTAL_CHUNKS}")
    print(f"   Chunk range: {start_idx + 1} to {end_idx} ({len(chunk)} products)\n")
    
    return chunk

def get_all_shopify_products():
    """Fetch all existing products from Shopify"""
    print("Fetching existing Shopify products...")
    all_products = {}
    
    params = {'limit': 250}
    page = 1
    
    while True:
        print(f"Fetching page {page}...", end=' ')
        result = make_shopify_request('GET', f"products.json?limit={params['limit']}")
        
        if not result or 'products' not in result:
            break
        
        products = result['products']
        print(f"Got {len(products)} products")
        
        for product in products:
            if product.get('variants') and len(product['variants']) > 0:
                sku = product['variants'][0].get('sku', '')
                if sku:
                    all_products[sku] = product
        
        if len(products) < params['limit']:
            break
        
        page += 1
        time.sleep(0.5)
    
    print(f"Found {len(all_products)} existing products in Shopify\n")
    return all_products

def create_or_update_product(product_data, existing_products):
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
    
    if sku in existing_products:
        # Update existing product
        shopify_product = existing_products[sku]
        product_id = shopify_product['id']
        variant_id = shopify_product['variants'][0]['id']
        
        # Update product details
        update_payload = {
            "product": {
                "id": product_id,
                "title": title[:255],
                "body_html": description
            }
        }
        make_shopify_request('PUT', f"products/{product_id}.json", update_payload)
        
        # Update variant (price, inventory)
        variant_payload = {
            "variant": {
                "id": variant_id,
                "price": str(price),
                "inventory_management": "shopify"
            }
        }
        make_shopify_request('PUT', f"variants/{variant_id}.json", variant_payload)
        
        # Update inventory
        inventory_payload = {
            "location_id": shopify_product['variants'][0].get('inventory_item_id'),
            "inventory_item_id": shopify_product['variants'][0].get('inventory_item_id'),
            "available": inventory
        }
        
        return 'updated'
    else:
        # Create new product
        make_shopify_request('POST', 'products.json', product_payload)
        return 'created'

def sync_products():
    """Main sync function"""
    print(f"\n{'='*70}")
    print(f"JohnnyVac to Shopify Smart Sync")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")
    
    # Download current CSV
    csv_content = download_csv()
    current_products = parse_csv(csv_content)
    
    # Load previous CSV and detect changes
    previous_products = load_previous_csv()
    products_to_sync, unchanged = detect_changes(current_products, previous_products)
    
    # Apply chunking if configured
    products_to_sync = apply_chunking(products_to_sync)
    
    if len(products_to_sync) == 0:
        print("✅ No changes detected! All products are up to date.")
        save_current_csv(csv_content)
        return
    
    # Get existing Shopify products
    existing_products = get_all_shopify_products()
    
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
        avg_time_per_batch = elapsed_time / batch_num if batch_num > 0 else 0
        remaining_batches = total_batches - batch_num
        estimated_remaining = avg_time_per_batch * remaining_batches
        
        print(f"\n{'='*70}")
        print(f"Batch {batch_num}/{total_batches} ({(batch_num/total_batches*100):.1f}%)")
        print(f"Products {start_idx + 1} to {end_idx}")
        print(f"Elapsed: {elapsed_time/60:.1f}m | Est. remaining: {estimated_remaining/60:.1f}m")
        print(f"Created: {created_count} | Updated: {updated_count} | Errors: {error_count}")
        print(f"{'='*70}\n")
        
        for product in batch:
            try:
                result = create_or_update_product(product, existing_products)
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
    
    # Save current CSV for next run
    save_current_csv(csv_content)
    
    # Summary
    total_time = time.time() - start_time
    print(f"\n{'='*70}")
    print(f"Sync Complete!")
    print(f"{'='*70}")
    print(f"Created: {created_count}")
    print(f"Updated: {updated_count}")
    print(f"Errors: {error_count}")
    print(f"Total time: {total_time/60:.1f} minutes")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    sync_products()
