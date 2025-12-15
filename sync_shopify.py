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

def get_shopify_products_by_sku(sku_list):
    """Fetch specific products from Shopify by SKU - OPTIMIZED"""
    if not sku_list:
        return {}
    
    print(f"\nChecking {len(sku_list)} products in Shopify...")
    existing = {}
    
    # Process in batches of 50 SKUs
    for i in range(0, len(sku_list), 50):
        batch_skus = sku_list[i:i+50]
        print(f"  Checking batch {i//50 + 1}/{(len(sku_list) + 49)//50}...", end=' ')
        
        batch_found = 0
        for sku in batch_skus:
            try:
                result = make_shopify_request('GET', f'products.json?limit=1&fields=id,variants&vendor=JohnnyVac')
                
                # Search through results for matching SKU
                if result and result.get('products'):
                    for product in result['products']:
                        for variant in product.get('variants', []):
                            if variant.get('sku') == sku:
                                existing[sku] = {
                                    'id': product['id'],
                                    'variant': variant
                                }
                                batch_found += 1
                                break
                
                time.sleep(0.3)  # Rate limiting
            except Exception as e:
                print(f"\n  Error fetching {sku}: {e}")
        
        print(f"found {batch_found}")
    
    print(f"✓ Found {len(existing)}/{len(sku_list)} existing products\n")
    return existing

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
    products_to_sync = detect_changes(current_products, previous_products)
    
    if len(products_to_sync) == 0:
        print("\n✅ No changes detected! All products are up to date.")
        save_current_csv(csv_content)
        return
    
    print(f"\n🔄 Proceeding to sync {len(products_to_sync)} products...")
    
    # Get existing Shopify products for changed SKUs only
    sku_list = [p['SKU'] for p in products_to_sync]
    existing_products = get_shopify_products_by_sku(sku_list)
    
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
