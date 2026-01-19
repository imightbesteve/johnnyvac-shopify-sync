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
    Enhanced JohnnyVac Categorization Logic aligned with JV Website Navigation.
    Returns: (Product Type, Tags String)
    """
    t = str(title).upper()
    current_cat = str(csv_category_fallback).upper()
    tags = []

    # --- 1. ADMINSTRATIVE: DISCONTINUED / SPECIAL ORDER ---
    discontinued_keywords = ['RARELY ORDERED', 'RAREMENT COMMANDE', 'ADJUSTED PRICE', 'DISCONTINUED']
    if any(k in t for k in discontinued_keywords):
        return "Discontinued / Special Order", "admin, discontinued"

    # --- 2. VACUUM CATEGORIES (MAIN NAV) ---
    if any(k in t for k in ["COMMERCIAL VAC", "BACKPACK", "CANISTER", "UPRIGHT"]):
        return "Commercial Vacuums", "vacuum, commercial"
    
    if any(k in t for k in ["CENTRAL VAC", "ASPIRATEUR CENTRAL"]):
        return "Central Vacuums", "vacuum, central-vac"

    # --- 3. FLOOR EQUIPMENT (MAIN NAV) ---
    if any(k in t for k in ["EXTRACTOR", "EXTRACTEUR", "CARPET CLEANER"]):
        return "Carpet Extractors", "equipment, extractors"
    
    if any(k in t for k in ["SCRUBBER", "AUTO-SCRUBBER", "RECURREUSE"]):
        return "Automatic Scrubbers", "equipment, scrubbers"

    if any(k in t for k in ["BURNISHER", "POLISHER", "FLOOR MACHINE", "POLISSEUSE"]):
        return "Floor Machines & Burnishers", "equipment, floor-machines"

    if any(k in t for k in ["PRESSURE WASHER", "NETTOYEUR HAUTE PRESSION"]):
        return "Pressure Washers", "equipment, pressure-washers"

    # --- 4. CONSUMABLES (BAGS, FILTERS, BELTS) ---
    if any(k in t for k in ["BAG", "SAC"]):
        return "Vacuum Bags", "consumable, bags"
    
    if any(k in t for k in ["FILTER", "FILTRE", "HEPA"]):
        return "Vacuum Filters", "consumable, filters"
    
    if any(k in t for k in ["BELT", "COURROIE"]):
        return "Vacuum Belts", "consumable, belts"

    # --- 5. CLEANING SUPPLIES & CHEMICALS ---
    if any(k in t for k in ["PAPER TOWEL", "ESSUIE-TOUT", "TOILET PAPER", "PAPIER TOILETTE"]):
        return "Paper Products", "supplies, paper"
    
    if any(k in t for k in ["DEGREASER", "DISINFECTANT", "SOAP", "CLEANER", "SAVON", "NETTOYANT"]):
        return "Chemicals & Cleaners", "supplies, chemicals"

    # --- 6. MAINTENANCE REFINEMENT (Hardware, Body, Electrical) ---
    maintenance_map = {
        "Hardware & Fasteners": ["SCREW", "BOLT", "NUT", "WASHER", "VIS", "ECROU", "RONDELLE", "RIVET"],
        "Gaskets, Seals & Valves": ["GASKET", "SEAL", "JOINT", "O-RING", "DIAPHRAGM", "VALVE", "FLAP"],
        "Body Components & Housing": ["HANDLE", "COVER", "LID", "BASE", "HOUSING", "BUMPER", "LATCH", "POIGNÉE", "COUVERCLE"],
        "Electrical Components": ["WIRE", "CABLE", "PLUG", "SOCKET", "PCB", "CIRCUIT", "RELAY", "SENSOR", "FICHE", "FIL", "CORD"],
        "Mounting & Tension Hardware": ["SPRING", "CLIP", "BRACKET", "PLATE", "RESSORT", "ATTACHE", "SUPPORT"]
    }

    for p_type, keywords in maintenance_map.items():
        if any(k in t for k in keywords):
            return p_type, "maintenance"

    # --- 7. FALLBACK LOGIC ---
    # If the CSV category is generic, use "General Maintenance"
    generic_names = ["JOHNNY VAC PARTS (*)", "ALL PARTS", "MISCELLANEOUS", "", "NAN"]
    if current_cat in generic_names:
        return "General Maintenance", "needs-review"

    # Otherwise, keep the existing category if it seems specific
    return csv_category_fallback, ""

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
    
    csv_title = csv_product.get(title_field, '').strip()
    if shopify_product.get('title') != csv_title: return True
    if shopify_product.get('product_type') != calc_type: return True

    current_shopify_tags = shopify_product.get('tags', '')
    gen_set = set([t.strip() for t in calc_tags.split(',') if t.strip()])
    curr_set = set([t.strip() for t in current_shopify_tags.split(',') if t.strip()])
    if not gen_set.issubset(curr_set): return True

    variant = shopify_product['variants'][0]
    try:
        csv_price = float(csv_product.get('RegularPrice', '0').strip().replace(',', '.'))
        if csv_price != float(variant.get('price', '0')): return True
    except: pass

    try:
        csv_inventory = int(csv_product.get('Inventory', '0').strip())
        if csv_inventory != int(variant.get('inventory_quantity', 0)): return True
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
    
    csv_raw_category = csv_product.get('ProductCategory', '').strip()
    final_type, final_tags = classify_product(title, csv_raw_category)
    
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN, "Content-Type": "application/json"}

    if shopify_product:
        product_id = shopify_product['id']
        variant = shopify_product['variants'][0]
        current_tags = shopify_product.get('tags', '')
        combined_tags = ", ".join(list(set(current_tags.split(',') + final_tags.split(',')))).strip(', ')

        update_data = {
            "product": {
                "id": product_id,
                "title": title,
                "body_html": description,
                "product_type": final_type,
                "tags": combined_tags,
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
            inv_payload = {"location_id": location_id, "inventory_item_id": variant['inventory_item_id'], "available": inventory}
            requests.post(inv_url, headers=headers, json=inv_payload, timeout=30)
        return 'updated'
    else:
        product_data = {
            "product": {
                "title": title,
                "body_html": description,
                "vendor": "JohnnyVac",
                "product_type": final_type,
                "tags": final_tags,
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
    print("JohnnyVac → Shopify Sync (FULL DAILY + ENHANCED AUTO-CAT)")
    loc_id = get_primary_location()
    csv_products = download_johnnyvac_csv()
    if not csv_products: return
    shopify_products = get_all_shopify_products()
    
    to_create, to_update = [], []
    for p in csv_products:
        sku = p.get('SKU', '').strip()
        if not sku: continue
        calc_type, calc_tags = classify_product(p.get('ProductTitleEN', ''), p.get('ProductCategory', ''))

        if sku in shopify_products:
            if needs_update(shopify_products[sku], p, calc_type, calc_tags):
                to_update.append((p, shopify_products[sku]))
        else:
            to_create.append(p)

    print(f"\n--- Syncing: {len(to_create)} New, {len(to_update)} Updates ---")
    for p in to_create:
        if check_time_limit(): break
        create_or_update_product(p, None, loc_id)
    for p, shop_p in to_update:
        if check_time_limit(): break
        create_or_update_product(p, shop_p, loc_id)

if __name__ == "__main__":
    sync_products()
if __name__ == "__main__":
    sync_products()
