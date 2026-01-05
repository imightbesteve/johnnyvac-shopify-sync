"""
JohnnyVac to Shopify Sync Script - Category Batching Mode
- Processes one category per run to avoid timeouts
- Automatically cycles through categories daily
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
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN', '')
JOHNNYVAC_CSV_URL = "https://www.johnnyvacstock.com/sigm_all_jv_products/JVWebProducts.csv"
IMAGE_BASE_URL = "https://www.johnnyvacstock.com/photos/web/"

# Settings
BATCH_SIZE = 50
LANGUAGE = 'en'
MAX_RUNTIME_SECONDS = 5.5 * 3600 # 5.5 Hours
START_TIME = time.time()

# Category Batching Configuration
CATEGORY_MODE = os.environ.get('CATEGORY_MODE', 'alternate')
SYNC_MODE = os.environ.get('SYNC_MODE', 'daily')

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
    """Fetch primary Shopify location once"""
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
    categories = defaultdict(list)
    for product in csv_products:
        category = product.get('ProductCategory', '').strip() or 'Uncategorized'
        categories[category].append(product)
    sorted_categories = sorted(categories.items(), key=lambda x: len(x[1]), reverse=True)
    return dict(sorted_categories)

def get_category_to_sync(categories):
    if CATEGORY_MODE == 'all':
        return list(categories.keys())
    
    if CATEGORY_MODE not in ['rotate', 'alternate']:
        if CATEGORY_MODE in categories:
            return [CATEGORY_MODE]
        else:
            print(f"\n⚠️ Category '{CATEGORY_MODE}' not found.")
            sys.exit(1)
    
    category_list = list(categories.keys())
    
    if CATEGORY_MODE == 'alternate':
        mid_point = (len(category_list) + 1) // 2
        group_a = category_list[:mid_point]
        group_b = category_list[mid_point:]
        day_of_week = datetime.now().weekday()
        
        if day_of_week in [0, 2, 4]: # Mon, Wed, Fri
            return group_a
        else:
            return group_b
            
    # Rotate mode
    day_of_year = datetime.now().timetuple().tm_yday
    index = day_of_year % len(category_list)
    return [category_list[index]]

def needs_update(shopify_product, csv_product):
    title_field = 'ProductTitleEN' if LANGUAGE == 'en' else 'ProductTitleFR'
    desc_field = 'ProductDescriptionEN' if LANGUAGE == 'en' else 'ProductDescriptionFR'
    
    csv_title = csv_product.get(title_field, '').strip()
    csv_price = float(csv_product.get('RegularPrice', '0').strip().replace(',', '.'))
    csv_inventory = int(csv_product.get('Inventory', '0').strip())
    
    variant = shopify_product['variants'][0]
    if shopify_product.get('title') != csv_title: return True
    if float(variant.get('price', '0')) != csv_price: return True
    if int(variant.get('inventory_quantity', 0)) != csv_inventory: return True
    
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

    if shopify_product:
        product_id = shopify_product['id']
        variant = shopify_product['variants'][0]
        update_data = {
            "product": {
                "id": product_id, "title": title, "body_html": description,
                "variants": [{"id": variant['id'], "price": price, "weight": weight}]
            }
        }
        url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/products/{product_id}.json"
        response = requests.put(url, headers=headers, json=update_data, timeout=30)
        
        if response.status_code == 200 and location_id and variant.get('inventory_item_id'):
            inv_url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/inventory_levels/set.json"
            inv_payload = {"location_id": location_id, "inventory_item_id": variant['inventory_item_id'], "available": inventory}
            requests.post(inv_url, headers=headers, json=inv_payload, timeout=30)
        
        smart_sleep(response)
        return 'updated'
    else:
        # Create Flow
        product_data = {
            "product": {
                "title": title, "body_html": description, "vendor": "JohnnyVac", "product_type": category,
                "variants": [{"sku": sku, "price": price, "inventory_management": "shopify", "inventory_quantity": inventory}],
                "images": [{"src": f"{IMAGE_BASE_URL}{sku}.jpg"}]
            }
        }
        url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/products.json"
        response = requests.post(url, headers=headers, json=product_data, timeout=30)
        smart_sleep(response)
        return 'created'

def sync_products():
    csv_products = download_johnnyvac_csv()
    if not csv_products: return
    
    categories = group_products_by_category(csv_products)
    categories_to_sync = get_category_to_sync(categories)
    
    filtered_products = []
    for cat in categories_to_sync:
        filtered_products.extend(categories[cat])
        
    shopify_products = get_all_shopify_products()
    loc_id = get_primary_location()
    
    for p in filtered_products:
        if check_time_limit(): break
        sku = p.get('SKU', '').strip()
        if not sku: continue
        
        if sku in shopify_products:
            if needs_update(shopify_products[sku], p):
                create_or_update_product(p, shopify_products[sku], loc_id)
        else:
            create_or_update_product(p, None, loc_id)

if __name__ == "__main__":
    sync_products()
