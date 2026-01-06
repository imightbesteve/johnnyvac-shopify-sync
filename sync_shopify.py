"""
JohnnyVac to Shopify Sync - Daily Full Mode
- Checks ALL products every run
- Only sends updates to Shopify if data has changed (Smart Sync)
"""
import requests
import csv
import time
import os
import sys
from datetime import datetime

# Configuration
SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', 'kingsway-janitorial.myshopify.com')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN', '')
JOHNNYVAC_CSV_URL = "https://www.johnnyvacstock.com/sigm_all_jv_products/JVWebProducts.csv"
IMAGE_BASE_URL = "https://www.johnnyvacstock.com/photos/web/"

# Settings
LANGUAGE = 'en'
MAX_RUNTIME_SECONDS = 5.5 * 3600 # 5.5 Hours safe limit
START_TIME = time.time()
LOCATION_ID = None

def check_time_limit():
    """Returns True if we are approaching the GitHub Actions time limit"""
    elapsed = time.time() - START_TIME
    if elapsed > MAX_RUNTIME_SECONDS:
        print(f"\n⚠️  Time limit reached ({elapsed/3600:.2f} hours). Stopping gracefully.")
        return True
    return False

def smart_sleep(response):
    """Dynamic rate limiting based on Shopify headers"""
    try:
        call_limit = response.headers.get('X-Shopify-Shop-Api-Call-Limit', '0/40')
        used, total = map(int, call_limit.split('/'))
        if used >= 35:
            time.sleep(2.0)
        elif used >= 30:
            time.sleep(1.0)
        else:
            time.sleep(0.1)
    except:
        time.sleep(0.5)

def get_primary_location():
    """Fetch primary Shopify location ID"""
    global LOCATION_ID
    if LOCATION_ID: return LOCATION_ID
        
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Fetching primary Shopify location...")
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN}
    url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/locations.json"
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        LOCATION_ID = response.json()['locations'][0]['id']
        print(f"  ✓ Using location ID: {LOCATION_ID}")
        return LOCATION_ID
    except Exception as e:
        print(f"  ❌ Error fetching location: {e}")
        # We cannot update inventory without a location, so we must exit
        sys.exit(1)

def get_all_shopify_products():
    """Fetch ALL products from Shopify with pagination"""
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Fetching all products from Shopify...")
    products = {}
    url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/products.json?limit=250"
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN}
    page_count = 0
    
    while url:
        if check_time_limit(): break
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            batch = data.get('products', [])
            if not batch: break
            
            page_count += 1
            for product in batch:
                if product.get('variants'):
                    sku = product['variants'][0].get('sku', '').strip()
                    if sku:
                        products[sku] = product
            
            print(f"  Page {page_count}: Loaded {len(products)} products...", end='\r')
            
            # Pagination
            link_header = response.headers.get('Link', '')
            url = None
            if link_header:
                links = link_header.split(',')
                for link in links:
                    if 'rel="next"' in link:
                        url = link.split(';')[0].strip().strip('<>')
                        break
            smart_sleep(response)
        except Exception as e:
            print(f"\n❌ Error fetching products: {e}")
            break
    
    print(f"\n  ✓ Loaded {len(products)} unique products from Shopify")
    return products

def download_johnnyvac_csv():
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Downloading CSV from JohnnyVac...")
    try:
        response = requests.get(JOHNNYVAC_CSV_URL, timeout=60)
        response.raise_for_status()
        content = response.content.decode('utf-8-sig', errors='replace')
        lines = content.strip().splitlines()
        reader = csv.DictReader(lines, delimiter=';')
        products = list(reader)
        print(f"  ✓ Downloaded {len(products)} rows")
        return products
    except Exception as e:
        print(f"  ❌ Error downloading CSV: {e}")
        return None

def needs_update(shopify_product, csv_product):
    """Compare CSV data vs Shopify data to see if update is required"""
    # 1. Check Title / Description
    title_field = 'ProductTitleEN' if LANGUAGE == 'en' else 'ProductTitleFR'
    desc_field = 'ProductDescriptionEN' if LANGUAGE == 'en' else 'ProductDescriptionFR'
    
    csv_title = csv_product.get(title_field, '').strip()
    if shopify_product.get('title') != csv_title: return True

    # 2. Check Variant Data (Price, Inventory)
    variant = shopify_product['variants'][0]
    
    try:
        csv_price = float(csv_product.get('RegularPrice', '0').strip().replace(',', '.'))
        shopify_price = float(variant.get('price', '0'))
        if csv_price != shopify_price: return True
    except:
        pass # If price conversion fails, skip price check

    try:
        csv_inventory = int(csv_product.get('Inventory', '0').strip())
        shopify_inventory = int(variant.get('inventory_quantity', 0))
        if csv_inventory != shopify_inventory: return True
    except:
        pass

    return False

def create_or_update_product(csv_product, shopify_product=None, location_id=None):
    sku = csv_product.get('SKU', '').strip()
    title_field = 'ProductTitleEN' if LANGUAGE == 'en' else 'ProductTitleFR'
    desc_field = 'ProductDescriptionEN' if LANGUAGE == 'en' else 'ProductDescriptionFR'
    
    title = csv_product.get(title_field, '').strip() or f"Product {sku}"
    description = csv_product.get(desc_field, '').strip()
    price = csv_product.get('RegularPrice', '0').strip().replace(',', '.')
    inventory = int(csv_product.get('Inventory', '0').strip())
    weight = csv_product.get('weight', '0').strip().replace(',', '.')
    category = csv_product.get('ProductCategory', '').strip()
    
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN, "Content-Type": "application/json"}

    # --- UPDATE EXISTING PRODUCT ---
    if shopify_product:
        product_id = shopify_product['id']
        variant = shopify_product['variants'][0]
        
        # Payload for main product data
        update_data = {
            "product": {
                "id": product_id,
                "title": title,
                "body_html": description,
                "product_type": category,
                "variants": [{
                    "id": variant['id'],
                    "price": price,
                    "weight": weight
                }]
            }
        }
        
        url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/products/{product_id}.json"
        response = requests.put(url, headers=headers, json=update_data, timeout=30)
        smart_sleep(response)
        
        # Update Inventory (Requires separate call)
        if response.status_code == 200 and location_id and variant.get('inventory_item_id'):
            inv_url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/inventory_levels/set.json"
            inv_payload = {
                "location_id": location_id,
                "inventory_item_id": variant['inventory_item_id'],
                "available": inventory
            }
            requests.post(inv_url, headers=headers, json=inv_payload, timeout=30)
            
        return 'updated'

    # --- CREATE NEW PRODUCT ---
    else:
        product_data = {
            "product": {
                "title": title,
                "body_html": description,
                "vendor": "JohnnyVac",
                "product_type": category,
                "variants": [{
                    "sku": sku,
                    "price": price,
                    "inventory_management": "shopify",
                    "inventory_quantity": inventory,
                    "weight": weight,
                    "weight_unit": "kg"
                }],
                "images": [{"src": f"{IMAGE_BASE_URL}{sku}.jpg"}]
            }
        }
        url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/products.json"
        response = requests.post(url, headers=headers, json=product_data, timeout=30)
        smart_sleep(response)
        return 'created'

def sync_products():
    print("=" * 60)
    print("JohnnyVac → Shopify Sync (FULL DAILY MODE)")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # 1. Fetch Location ID FIRST (Fail fast if permissions wrong)
    loc_id = get_primary_location()
    
    # 2. Download Source Data
    csv_products = download_johnnyvac_csv()
    if not csv_products: return
    
    # 3. Fetch Destination Data
    shopify_products = get_all_shopify_products()
    
    # 4. Compare and Build Lists
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Comparing {len(csv_products)} source products vs {len(shopify_products)} existing...")
    
    to_create = []
    to_update = []
    
    for p in csv_products:
        sku = p.get('SKU', '').strip()
        if not sku: continue
        
        if sku in shopify_products:
            # If product exists, check if it needs an update
            if needs_update(shopify_products[sku], p):
                to_update.append((p, shopify_products[sku]))
        else:
            # If product doesn't exist, create it
            to_create.append(p)

    print(f"  ✓ Products to Create: {len(to_create)}")
    print(f"  ✓ Products to Update: {len(to_update)}")
    print(f"  ✓ Unchanged (Skipping): {len(csv_products) - len(to_create) - len(to_update)}")

    # 5. Process Creates
    created_count = 0
    if to_create:
        print(f"\n--- Creating {len(to_create)} new products ---")
        for p in to_create:
            if check_time_limit(): break
            res = create_or_update_product(p, None, loc_id)
            if res:
                created_count += 1
                if created_count % 5 == 0: print(f"  Created {created_count}/{len(to_create)}...", end='\r')

    # 6. Process Updates
    updated_count = 0
    if to_update:
        print(f"\n--- Updating {len(to_update)} existing products ---")
        for p, shop_p in to_update:
            if check_time_limit(): break
            res = create_or_update_product(p, shop_p, loc_id)
            if res:
                updated_count += 1
                if updated_count % 10 == 0: print(f"  Updated {updated_count}/{len(to_update)}...", end='\r')

    print(f"\n\n{'='*60}")
    print("SYNC COMPLETED")
    print(f"Created: {created_count}")
    print(f"Updated: {updated_count}")
    print(f"{'='*60}")

if __name__ == "__main__":
    sync_products()
