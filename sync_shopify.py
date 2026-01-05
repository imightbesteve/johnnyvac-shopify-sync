"""
JohnnyVac to Shopify Sync Script - Category Batching Mode
- Processes one category per run to avoid timeouts
- Automatically cycles through categories daily
- Tracks progress via environment variable or falls back to date-based rotation
"""
import requests
import csv
import time
import json
import os
import sys
from datetime import datetime
from collections import defaultdict

# Configuration
SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', 'kingsway-janitorial.myshopify.com')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN', 'YOUR_TOKEN_HERE')
JOHNNYVAC_CSV_URL = "https://www.johnnyvacstock.com/sigm_all_jv_products/JVWebProducts.csv"
IMAGE_BASE_URL = "https://www.johnnyvacstock.com/photos/web/"

# Settings
BATCH_SIZE = 50
LANGUAGE = 'en'
MAX_RUNTIME_SECONDS = 5.5 * 3600
START_TIME = time.time()

# Category Batching Configuration
CATEGORY_MODE = os.environ.get('CATEGORY_MODE', 'alternate')  # 'alternate', 'rotate', 'all', or specific category name
SYNC_MODE = os.environ.get('SYNC_MODE', 'daily')  # Only used for 'rotate' mode

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
    """Fetch primary Shopify location once (cached globally)"""
    global LOCATION_ID
    if LOCATION_ID:
        return LOCATION_ID
        
    print("\nFetching primary Shopify location...")
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
        sys.exit(1)

def get_all_shopify_products():
    """Fetch ALL products from Shopify with pagination"""
    print("\nFetching all products from Shopify...")
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
    """Download and parse JohnnyVac product CSV"""
    print("\nDownloading product CSV from JohnnyVac...")
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

def group_products_by_category(csv_products):
    """Group products by their ProductCategory"""
    categories = defaultdict(list)
    
    for product in csv_products:
        category = product.get('ProductCategory', '').strip() or 'Uncategorized'
        categories[category].append(product)
    
    # Sort categories by product count (largest first)
    sorted_categories = sorted(categories.items(), key=lambda x: len(x[1]), reverse=True)
    
    print("\n📊 Product Distribution by Category:")
    print("=" * 60)
    for category, products in sorted_categories:
        print(f"  {category}: {len(products)} products")
    print("=" * 60)
    
    return dict(sorted_categories)

def get_category_to_sync(categories):
    """Determine which category to sync based on CATEGORY_MODE"""
    
    if CATEGORY_MODE == 'all':
        print("\n🔄 Mode: Syncing ALL categories")
        return list(categories.keys())
    
    if CATEGORY_MODE != 'rotate' and CATEGORY_MODE != 'alternate':
        # Specific category requested
        if CATEGORY_MODE in categories:
            print(f"\n🎯 Mode: Syncing specific category '{CATEGORY_MODE}'")
            return [CATEGORY_MODE]
        else:
            print(f"\n⚠️  Category '{CATEGORY_MODE}' not found. Available categories:")
            for cat in categories.keys():
                print(f"    - {cat}")
            sys.exit(1)
    
    category_list = list(categories.keys())
    
    # Alternate mode - Split into Group A and Group B
    if CATEGORY_MODE == 'alternate':
        # Split categories in half
        mid_point = (len(category_list) + 1) // 2
        group_a = category_list[:mid_point]
        group_b = category_list[mid_point:]
        
        # Determine which group based on day of week
        day_of_week = datetime.now().weekday()  # 0=Monday, 6=Sunday
        
        if day_of_week in [0, 2, 4]:  # Monday, Wednesday, Friday
            selected_group = group_a
            group_name = "A"
            next_day = "Tuesday"
        else:  # Tuesday, Thursday, Saturday, Sunday
            selected_group = group_b
            group_name = "B"
            next_day = "Monday"
        
        print(f"\n🔄 Mode: Alternating schedule")
        print(f"  📅 Today: {datetime.now().strftime('%A')} - Running Group {group_name}")
        print(f"  📦 Group {group_name} categories: {', '.join(selected_group)}")
        print(f"  ⏭️  Next sync: Group {'B' if group_name == 'A' else 'A'} on {next_day}")
        
        return selected_group
    
    # Rotate mode - cycle through categories one at a time
    if SYNC_MODE == 'weekly':
        # Cycle every 7 days (Week number determines category)
        week_num = datetime.now().isocalendar()[1]
        index = week_num % len(category_list)
    else:
        # Daily rotation (Day of year determines category)
        day_of_year = datetime.now().timetuple().tm_yday
        index = day_of_year % len(category_list)
    
    selected_category = category_list[index]
    print(f"\n🔄 Mode: Daily rotation")
    print(f"  📅 Today's category: '{selected_category}' ({index+1}/{len(category_list)})")
    print(f"  ⏭️  Next category: '{category_list[(index+1) % len(category_list)]}'")
    
    return [selected_category]

def needs_update(shopify_product, csv_product):
    """Check if update is needed. Returns list of changed fields."""
    changes = []
    
    title_field = 'ProductTitleEN' if LANGUAGE == 'en' else 'ProductTitleFR'
    desc_field = 'ProductDescriptionEN' if LANGUAGE == 'en' else 'ProductDescriptionFR'
    
    csv_title = csv_product.get(title_field, '').strip()
    csv_desc = csv_product.get(desc_field, '').strip()
    
    if csv_title and shopify_product.get('title') != csv_title:
        changes.append('title')
        
    if csv_desc and shopify_product.get('body_html') != csv_desc:
        changes.append('description')

    variant = shopify_product['variants'][0]
    csv_price = csv_product.get('RegularPrice', '0').strip().replace(',', '.')
    csv_inventory = csv_product.get('Inventory', '0').strip()
    
    shopify_price = variant.get('price', '0')
    shopify_inventory = str(variant.get('inventory_quantity', 0))
    
    if float(csv_price) != float(shopify_price):
        changes.append('price')
        
    if int(csv_inventory) != int(shopify_inventory):
        changes.append('inventory')
        
    return changes

def create_or_update_product(csv_product, shopify_product=None, location_id=None, retries=3):
    """Create or Update product in Shopify with retry logic"""
    sku = csv_product.get('SKU', '').strip()
    title_field = 'ProductTitleEN' if LANGUAGE == 'en' else 'ProductTitleFR'
    desc_field = 'ProductDescriptionEN' if LANGUAGE == 'en' else 'ProductDescriptionFR'
    
    title = csv_product.get(title_field, '').strip() or f"Product {sku}"
    description = csv_product.get(desc_field, '').strip()
    price = csv_product.get('RegularPrice', '0').strip().replace(',', '.')
    inventory = int(csv_product.get('Inventory', '0').strip())
    weight = csv_product.get('weight', '0').strip().replace(',', '.')
    category = csv_product.get('ProductCategory', '').strip()
    
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }

    # -- UPDATE FLOW --
    if shopify_product:
        product_id = shopify_product['id']
        variant = shopify_product['variants'][0]
        variant_id = variant['id']
        inventory_item_id = variant.get('inventory_item_id')
        
        update_data = {
            "product": {
                "id": product_id,
                "title": title,
                "body_html": description,
                "product_type": category,
                "variants": [{
                    "id": variant_id,
                    "price": price,
                    "weight": weight
                }]
            }
        }

        for attempt in range(retries):
            try:
                url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/products/{product_id}.json"
                response = requests.put(url, headers=headers, json=update_data, timeout=30)
                response.raise_for_status()
                smart_sleep(response)

                if inventory_item_id and location_id:
                    inv_url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/inventory_levels/set.json"
                    inv_payload = {
                        "location_id": location_id,
                        "inventory_item_id": inventory_item_id,
                        "available": inventory
                    }
                    inv_response = requests.post(inv_url, headers=headers, json=inv_payload, timeout=30)
                    inv_response.raise_for_status()
                    smart_sleep(inv_response)
                    
                return 'updated'

            except requests.exceptions.RequestException as e:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    print(f"    ❌ Update failed: {e}")
                    return None

    # -- CREATE FLOW --
    else:
        image_url = f"{IMAGE_BASE_URL}{sku}.jpg"
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
                "images": [{"src": image_url}]
            }
        }
        
        for attempt in range(retries):
            try:
                url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/products.json"
                response = requests.post(url, headers=headers, json=product_data, timeout=30)
                response.raise_for_status()
                smart_sleep(response)
                return 'created'
            except requests.exceptions.RequestException as e:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    print(f"    ❌ Create failed: {e}")
                    return None

def sync_products():
    print("=" * 60)
    print("JohnnyVac → Shopify Sync (Category Batching Mode)")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # 1. Load Data
    csv_products = download_johnnyvac_csv()
    if not csv_products: return
    
    # 2. Group by categories
    categories = group_products_by_category(csv_products)
    
    # 3. Determine which category/categories to sync
    categories_to_sync = get_category_to_sync(categories)
    
    # 4. Filter products to only selected categories
    filtered_products = []
    for category_name in categories_to_sync:
        filtered_products.extend(categories[category_name])
    
    print(f"\n📦 Processing {len(filtered_products)} products from {len(categories_to_sync)} category(ies)")
    
    # 5. Fetch Shopify products and location
    shopify_products = get_all_shopify_products()
    location_id = get_primary_location()
    
    # 6. Compare
    print("\nCalculating differences...")
    to_create = []
    to_update = []
    
    for csv_p in filtered_products:
        sku = csv_p.get('SKU', '').strip()
        if not sku: continue
        
        if sku in shopify_products:
            changes = needs_update(shopify_products[sku], csv_p)
            if changes:
                to_update.append((csv_p, shopify_products[sku], changes))
        else:
            to_create.append(csv_p)
            
    print(f"  ✓ Create: {len(to_create)}")
    print(f"  ✓ Update: {len(to_update)}")
    print(f"  ✓ Already synced: {len(filtered_products) - len(to_create) - len(to_update)}")

    # 7. Process Creates
    created = 0
    if to_create:
        print(f"\n--- Creating {len(to_create)} new products ---")
        for p in to_create:
            if check_time_limit(): 
                print(f"\n⏰ Stopped at {created}/{len(to_create)} creates")
                break
            
            sku = p.get('SKU')
            res = create_or_update_product(p, location_id=location_id)
            if res:
                created += 1
                if created % 10 == 0:
                    print(f"  Progress: {created}/{len(to_create)} created")
            
    # 8. Process Updates
    updated = 0
    if to_update:
        print(f"\n--- Updating {len(to_update)} existing products ---")
        for csv_p, shop_p, changes in to_update:
            if check_time_limit():
                print(f"\n⏰ Stopped at {updated}/{len(to_update)} updates")
                break
            
            sku = csv_p.get('SKU')
            res = create_or_update_product(csv_p, shop_p, location_id)
            if res:
                updated += 1
                if updated % 10 == 0:
                    print(f"  Progress: {updated}/{len(to_update)} updated")

    print(f"\n{'='*60}")
    print("SYNC COMPLETED")
    print(f"Category: {', '.join(categories_to_sync)}")
    print(f"Created: {created}, Updated: {updated}")
    print(f"Runtime: {(time.time() - START_TIME)/3600:.2f} hours")
    print(f"{'='*60}")

if __name__ == "__main__":
    sync_products()
