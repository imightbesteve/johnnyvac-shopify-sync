"""
JohnnyVac to Shopify Sync Script
Always verifies against actual Shopify inventory (CSV is source of truth)
"""
import requests
import csv
import time
import json
from io import StringIO
from datetime import datetime

# Configuration
SHOPIFY_STORE = "kingsway-janitorial.myshopify.com"
SHOPIFY_ACCESS_TOKEN = "YOUR_TOKEN_HERE"  # Will be replaced by GitHub Actions
JOHNNYVAC_CSV_URL = "https://www.johnnyvacstock.com/sigm_all_jv_products/JVWebProducts.csv"
IMAGE_BASE_URL = "https://www.johnnyvacstock.com/photos/web/"

# Sync settings
BATCH_SIZE = 50
RATE_LIMIT_DELAY = 1.0
BATCH_DELAY = 10
LANGUAGE = 'en'

def get_all_shopify_products():
    """Fetch ALL products from Shopify with pagination"""
    print("\nFetching all products from Shopify...")
    products = {}
    url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/products.json?limit=250"
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN}
    page_count = 0
    
    while url:
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            page_count += 1
            for product in data.get('products', []):
                # Index by SKU from first variant
                if product.get('variants'):
                    sku = product['variants'][0].get('sku', '').strip()
                    if sku:
                        products[sku] = product
            
            print(f"  Page {page_count}: {len(products)} products loaded...", end='\r')
            
            # Check for next page
            link_header = response.headers.get('Link', '')
            if 'rel="next"' in link_header:
                next_url = link_header.split(';')[0].strip('<>')
                url = next_url
                time.sleep(0.5)  # Rate limit protection
            else:
                url = None
                
        except Exception as e:
            print(f"\n❌ Error fetching products: {e}")
            break
    
    print(f"\n  ✓ Loaded {len(products)} products from Shopify")
    return products

def download_johnnyvac_csv():
    """Download and parse JohnnyVac product CSV"""
    print("\nDownloading product CSV from JohnnyVac...")
    try:
        response = requests.get(JOHNNYVAC_CSV_URL, timeout=30)
        response.raise_for_status()
        print("  ✓ Downloaded")
        
        lines = response.text.strip().split('\n')
        reader = csv.DictReader(lines, delimiter=';')
        products = list(reader)
        
        return products
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return None

def needs_update(shopify_product, csv_product):
    """Check if a Shopify product needs updating based on CSV data"""
    title_field = 'ProductTitleEN' if LANGUAGE == 'en' else 'ProductTitleFR'
    desc_field = 'ProductDescriptionEN' if LANGUAGE == 'en' else 'ProductDescriptionFR'
    
    csv_title = csv_product.get(title_field, '').strip()
    csv_desc = csv_product.get(desc_field, '').strip()
    csv_price = csv_product.get('RegularPrice', '0').strip()
    csv_inventory = csv_product.get('Inventory', '0').strip()
    
    # Get Shopify values
    shopify_title = shopify_product.get('title', '').strip()
    shopify_desc = shopify_product.get('body_html', '').strip()
    
    variant = shopify_product.get('variants', [{}])[0]
    shopify_price = str(variant.get('price', '0')).strip()
    shopify_inventory = str(variant.get('inventory_quantity', '0')).strip()
    
    # Check if any field differs
    changes = []
    if csv_title and shopify_title != csv_title:
        changes.append('title')
    if csv_desc and shopify_desc != csv_desc:
        changes.append('description')
    if csv_price != shopify_price:
        changes.append('price')
    if csv_inventory != shopify_inventory:
        changes.append('inventory')
    
    return changes

def create_or_update_product(csv_product, shopify_product=None, retries=3):
    """Create new product or update existing one"""
    sku = csv_product.get('SKU', '').strip()
    title_field = 'ProductTitleEN' if LANGUAGE == 'en' else 'ProductTitleFR'
    desc_field = 'ProductDescriptionEN' if LANGUAGE == 'en' else 'ProductDescriptionFR'
    
    title = csv_product.get(title_field, '').strip() or f"Product {sku}"
    description = csv_product.get(desc_field, '').strip()
    price = csv_product.get('RegularPrice', '0').strip()
    inventory = csv_product.get('Inventory', '0').strip()
    weight = csv_product.get('weight', '0').strip()
    
    # Image URL
    image_url = f"{IMAGE_BASE_URL}{sku}.jpg"
    
    # Prepare product data
    product_data = {
        "product": {
            "title": title,
            "body_html": description,
            "vendor": "JohnnyVac",
            "product_type": csv_product.get('ProductCategory', '').strip(),
            "variants": [{
                "sku": sku,
                "price": price,
                "inventory_management": "shopify",
                "inventory_quantity": int(inventory) if inventory.isdigit() else 0,
                "weight": float(weight) if weight else 0,
                "weight_unit": "kg"
            }],
            "images": [{"src": image_url}]
        }
    }
    
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    
    for attempt in range(retries):
        try:
            if shopify_product:
                # Update existing product
                product_id = shopify_product['id']
                variant_id = shopify_product['variants'][0]['id']
                inventory_item_id = shopify_product['variants'][0]['inventory_item_id']
                
                # Update product details
                url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/products/{product_id}.json"
                product_data['product']['variants'][0]['id'] = variant_id
                response = requests.put(url, headers=headers, json=product_data, timeout=30)
                response.raise_for_status()
                
                # Update inventory level separately
                inv_url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/inventory_levels/set.json"
                inv_data = {
                    "location_id": shopify_product['variants'][0].get('inventory_item', {}).get('locations', [{}])[0].get('id'),
                    "inventory_item_id": inventory_item_id,
                    "available": int(inventory) if inventory.isdigit() else 0
                }
                
                # Get location first
                loc_url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/locations.json"
                loc_response = requests.get(loc_url, headers=headers, timeout=30)
                if loc_response.status_code == 200:
                    locations = loc_response.json().get('locations', [])
                    if locations:
                        inv_data['location_id'] = locations[0]['id']
                        requests.post(inv_url, headers=headers, json=inv_data, timeout=30)
                
                return 'updated'
            else:
                # Create new product
                url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/products.json"
                response = requests.post(url, headers=headers, json=product_data, timeout=30)
                response.raise_for_status()
                return 'created'
                
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                wait_time = (attempt + 1) * 5
                print(f"\n  ⚠️  Retry {attempt + 1}/{retries} in {wait_time}s... ({str(e)[:50]})")
                time.sleep(wait_time)
            else:
                raise
    
    return None

def sync_products():
    """Main sync function - always compares CSV to actual Shopify inventory"""
    print("=" * 70)
    print("JohnnyVac → Shopify Sync (Source of Truth: CSV)")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # Download CSV
    csv_products = download_johnnyvac_csv()
    if not csv_products:
        print("\n❌ Failed to download CSV. Exiting.")
        return
    
    print(f"  ✓ CSV contains {len(csv_products)} products")
    
    # Get actual Shopify inventory
    shopify_products = get_all_shopify_products()
    
    # Compare and identify what needs syncing
    print("\nAnalyzing differences...")
    to_create = []
    to_update = []
    
    for csv_product in csv_products:
        sku = csv_product.get('SKU', '').strip()
        if not sku:
            continue
        
        if sku in shopify_products:
            # Check if update needed
            changes = needs_update(shopify_products[sku], csv_product)
            if changes:
                to_update.append((csv_product, shopify_products[sku], changes))
        else:
            # Product doesn't exist in Shopify
            to_create.append(csv_product)
    
    print(f"\n  ✓ Products to CREATE: {len(to_create)}")
    print(f"  ✓ Products to UPDATE: {len(to_update)}")
    print(f"  ✓ Products already in sync: {len(csv_products) - len(to_create) - len(to_update)}")
    
    if not to_create and not to_update:
        print("\n✅ All products are in sync! Nothing to do.")
        return
    
    # Process creates and updates
    total_changes = len(to_create) + len(to_update)
    processed = 0
    created = 0
    updated = 0
    errors = 0
    
    print(f"\n{'=' * 70}")
    print(f"Processing {total_changes} changes in batches of {BATCH_SIZE}")
    print(f"{'=' * 70}\n")
    
    # Process creates
    for i in range(0, len(to_create), BATCH_SIZE):
        batch = to_create[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        print(f"Batch {batch_num} - Creating {len(batch)} new products...")
        
        for product in batch:
            sku = product.get('SKU', '').strip()
            try:
                result = create_or_update_product(product)
                if result == 'created':
                    created += 1
                    processed += 1
                    print(f"  ✓ Created: {sku} ({processed}/{total_changes})")
                time.sleep(RATE_LIMIT_DELAY)
            except Exception as e:
                errors += 1
                print(f"  ❌ Error creating {sku}: {str(e)[:60]}")
        
        if i + BATCH_SIZE < len(to_create):
            print(f"  Waiting {BATCH_DELAY}s before next batch...")
            time.sleep(BATCH_DELAY)
    
    # Process updates
    for i in range(0, len(to_update), BATCH_SIZE):
        batch = to_update[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1 + (len(to_create) // BATCH_SIZE)
        print(f"\nBatch {batch_num} - Updating {len(batch)} products...")
        
        for csv_product, shopify_product, changes in batch:
            sku = csv_product.get('SKU', '').strip()
            try:
                result = create_or_update_product(csv_product, shopify_product)
                if result == 'updated':
                    updated += 1
                    processed += 1
                    print(f"  ✓ Updated: {sku} ({', '.join(changes)}) ({processed}/{total_changes})")
                time.sleep(RATE_LIMIT_DELAY)
            except Exception as e:
                errors += 1
                print(f"  ❌ Error updating {sku}: {str(e)[:60]}")
        
        if i + BATCH_SIZE < len(to_update):
            print(f"  Waiting {BATCH_DELAY}s before next batch...")
            time.sleep(BATCH_DELAY)
    
    # Final summary
    print(f"\n{'=' * 70}")
    print("SYNC COMPLETE")
    print(f"{'=' * 70}")
    print(f"✓ Created: {created}")
    print(f"✓ Updated: {updated}")
    print(f"❌ Errors: {errors}")
    print(f"Total processed: {processed}/{total_changes}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 70}\n")

if __name__ == "__main__":
    sync_products()
