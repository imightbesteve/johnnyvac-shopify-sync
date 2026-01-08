"""
JohnnyVac to Shopify Sync - Daily Full Mode + Smart Auto-Categorization
- Checks ALL products every run
- Assigns specific Product Types (for Vacuum collections)
- Generates specific Tags (for Waste/Food/Safety collections)
- Only sends updates to Shopify if data has changed
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

def classify_product(title, csv_category_fallback):
    """
    Analyzes Product Title to determine the correct Shopify Product Type AND Tags.
    Matches the user's specific Collection rules.
    """
    t = title.lower()
    product_type = csv_category_fallback # Default to CSV category if no match found
    tags = []

    # --- 1. DETERMINE PRODUCT TYPE (Prioritized for Vacuum Collections) ---
    
    # Vacuum-Related
    if "bag" in t and ("vac" in t or "dust" in t): product_type = "Vacuum Bags"
    elif "filter" in t and ("vac" in t or "hepa" in t): product_type = "Vacuum Filters"
    elif "belt" in t and "vac" in t: product_type = "Vacuum Belts"
    elif "part" in t and "vac" in t: product_type = "Vacuum Parts"
    elif "hose" in t and "vac" in t: product_type = "Vacuum Parts"
    elif "motor" in t and "vac" in t: product_type = "Vacuum Parts"
    elif "cleaner" in t and "vac" in t: product_type = "Vacuum Cleaners"
    
    # Floor Care
    elif "mop" in t: product_type = "Mops"
    elif "broom" in t: product_type = "Brooms"
    elif "floor pad" in t or ("pad" in t and "floor" in t): product_type = "Floor Pads"
    elif "floor machine" in t: product_type = "Floor Machines"
    elif "brush" in t: product_type = "Brushes"
    elif "squeegee" in t: product_type = "Squeegees"
    elif "scrubber" in t: product_type = "Scrubbers"
    elif "burnisher" in t: product_type = "Burnishers"
    elif "extractor" in t: product_type = "Extractors"
    elif "dust pan" in t: product_type = "Dust Pans"
    elif "scraper" in t: product_type = "Scrapers"

    # Paper Products
    elif "toilet paper" in t or "tissue" in t or "bath tissue" in t: product_type = "Toilet Paper"
    elif "paper towel" in t or "roll towel" in t: product_type = "Paper Towels"
    elif "napkin" in t: product_type = "Napkins"
    elif "facial tissue" in t: product_type = "Tissues"
    elif "dispenser" in t: product_type = "Dispensers"

    # Chemicals
    elif "disinfectant" in t: product_type = "Disinfectants"
    elif "finish" in t and "floor" in t: product_type = "Floor Finish"
    elif "stripper" in t and "floor" in t: product_type = "Floor Stripper"
    elif "degreaser" in t: product_type = "Degreaser"
    elif "soap" in t or "sanitizer" in t: product_type = "Hand Soap"
    elif "glass" in t and "cleaner" in t: product_type = "Glass Cleaners"
    elif "bowl" in t and "cleaner" in t: product_type = "Bathroom Cleaners"
    elif "odor" in t or "deodor" in t: product_type = "Odor Control"
    elif "cleaner" in t: product_type = "Cleaners" # Catch-all for chemicals

    # Equipment
    elif "cart" in t or "trolley" in t: product_type = "Carts"
    elif "pressure washer" in t: product_type = "Pressure Washers"
    elif "waste" in t and "container" in t: product_type = "Waste Containers"

    # Cleaning Accessories
    elif "microfiber" in t: product_type = "Microfiber"
    elif "cloth" in t: product_type = "Cloths"
    elif "sponge" in t: product_type = "Sponges"
    elif "duster" in t: product_type = "Dusters"
    elif "bucket" in t: product_type = "Buckets"
    elif "glove" in t: product_type = "Work Gloves" # Default type (Tags will refine specific safety gloves)

    # Safety
    elif "mask" in t or "respirator" in t: product_type = "Masks"
    elif "vest" in t: product_type = "Vests"
    elif "first aid" in t: product_type = "First Aid"

    # Waste
    elif "trash bag" in t or "garbage bag" in t: product_type = "Trash Bags"
    elif "liner" in t: product_type = "Can Liners"
    elif "recycling" in t: product_type = "Recycling"

    # Food Service
    elif "cutlery" in t or "fork" in t or "spoon" in t or "knife" in t: product_type = "Cutlery"
    elif "kitchen" in t: product_type = "Kitchen"
    
    # --- 2. GENERATE TAGS (For Newer Automated Collections) ---
    
    # Safety Tags
    if "safety" in t or "protection" in t or "vest" in t or "glove" in t or "mask" in t or "first aid" in t:
        tags.append("safety")
    if "vest" in t or "visibility" in t:
        tags.extend(["vests", "visibility"])
    if "glove" in t:
        tags.append("gloves") # Common tag

    # Waste Tags
    if "waste" in t or "trash" in t or "garbage" in t or "recycle" in t or "bin" in t:
        tags.append("waste")
    if "trash bag" in t or "garbage bag" in t:
        tags.extend(["trash-bags", "disposal"])
    if "can liner" in t or "liner" in t:
        tags.extend(["can-liners", "disposal"])
    if "receptacle" in t or "bin" in t or "container" in t:
        tags.extend(["receptacles", "bins"])
    if "recycle" in t or "recycling" in t:
        tags.extend(["recycling", "eco-friendly"])

    # Food Service Tags
    if "food" in t or "kitchen" in t or "plate" in t or "cutlery" in t or "cup" in t:
        tags.append("food-service")
    if "disposable" in t or "single use" in t or "single-use" in t:
        tags.extend(["disposables", "single-use"])
    if "cutlery" in t or "fork" in t or "spoon" in t:
        tags.append("cutlery")
    if "plate" in t or "bowl" in t:
        tags.extend(["plates", "tableware"])
    if "kitchen" in t:
        tags.extend(["kitchen", "supplies"])

    # Format tags
    unique_tags = list(set(tags))
    return product_type, ", ".join(unique_tags)

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

def needs_update(shopify_product, csv_product, calc_type, calc_tags):
    """Compare CSV data + Calculated Type/Tags vs Shopify data"""
    title_field = 'ProductTitleEN' if LANGUAGE == 'en' else 'ProductTitleFR'
    
    # 1. Check basic fields
    csv_title = csv_product.get(title_field, '').strip()
    if shopify_product.get('title') != csv_title: return True

    # 2. Check Product Type (Calculated)
    if shopify_product.get('product_type') != calc_type:
        return True

    # 3. Check Tags (Calculated)
    current_shopify_tags = shopify_product.get('tags', '')
    gen_set = set([t.strip() for t in calc_tags.split(',') if t.strip()])
    curr_set = set([t.strip() for t in current_shopify_tags.split(',') if t.strip()])
    
    # Only update if our NEW tags are missing from Shopify
    if not gen_set.issubset(curr_set):
        return True

    # 4. Check Price/Inventory
    variant = shopify_product['variants'][0]
    try:
        csv_price = float(csv_product.get('RegularPrice', '0').strip().replace(',', '.'))
        shopify_price = float(variant.get('price', '0'))
        if csv_price != shopify_price: return True
    except: pass

    try:
        csv_inventory = int(csv_product.get('Inventory', '0').strip())
        shopify_inventory = int(variant.get('inventory_quantity', 0))
        if csv_inventory != shopify_inventory: return True
    except: pass

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
    
    # Fallback category from CSV if our logic finds nothing
    csv_raw_category = csv_product.get('ProductCategory', '').strip()
    
    # --- AUTO-CATEGORIZATION ---
    final_type, final_tags = classify_product(title, csv_raw_category)
    
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN, "Content-Type": "application/json"}

    # --- UPDATE ---
    if shopify_product:
        product_id = shopify_product['id']
        variant = shopify_product['variants'][0]
        
        # Merge existing tags with new tags
        current_tags = shopify_product.get('tags', '')
        combined_tags = ", ".join(list(set(current_tags.split(',') + final_tags.split(',')))).strip(', ')

        update_data = {
            "product": {
                "id": product_id,
                "title": title,
                "body_html": description,
                "product_type": final_type, # Update Type
                "tags": combined_tags,      # Update Tags
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
        
        if response.status_code == 200 and location_id and variant.get('inventory_item_id'):
            inv_url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/inventory_levels/set.json"
            inv_payload = {
                "location_id": location_id,
                "inventory_item_id": variant['inventory_item_id'],
                "available": inventory
            }
            requests.post(inv_url, headers=headers, json=inv_payload, timeout=30)
            
        return 'updated'

    # --- CREATE ---
    else:
        product_data = {
            "product": {
                "title": title,
                "body_html": description,
                "vendor": "JohnnyVac",
                "product_type": final_type, # Set Type
                "tags": final_tags,         # Set Tags
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
    print("JohnnyVac → Shopify Sync (FULL DAILY + AUTO-CATEGORIZATION)")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    loc_id = get_primary_location()
    csv_products = download_johnnyvac_csv()
    if not csv_products: return
    shopify_products = get_all_shopify_products()
    
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Comparing source vs existing...")
    
    to_create = []
    to_update = []
    
    for p in csv_products:
        sku = p.get('SKU', '').strip()
        if not sku: continue
        
        # Determine what the type/tags SHOULD be for this product
        csv_raw_category = p.get('ProductCategory', '').strip()
        title = p.get('ProductTitleEN', '') if LANGUAGE == 'en' else p.get('ProductTitleFR', '')
        calc_type, calc_tags = classify_product(title, csv_raw_category)

        if sku in shopify_products:
            if needs_update(shopify_products[sku], p, calc_type, calc_tags):
                to_update.append((p, shopify_products[sku]))
        else:
            to_create.append(p)

    print(f"  ✓ Products to Create: {len(to_create)}")
    print(f"  ✓ Products to Update: {len(to_update)}")
    print(f"  ✓ Unchanged: {len(csv_products) - len(to_create) - len(to_update)}")

    created_count = 0
    if to_create:
        print(f"\n--- Creating {len(to_create)} new products ---")
        for p in to_create:
            if check_time_limit(): break
            res = create_or_update_product(p, None, loc_id)
            if res:
                created_count += 1
                if created_count % 5 == 0: print(f"  Created {created_count}/{len(to_create)}...", end='\r')

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
