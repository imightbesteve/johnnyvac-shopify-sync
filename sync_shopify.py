import csv
import time
import requests
import io
import re
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURATION ---
# Pulls credentials from your environment variables
SHOP_URL = os.getenv("SHOPIFY_STORE", "").replace("https://", "").strip()
API_PASSWORD = os.getenv("SHOPIFY_ACCESS_TOKEN", "").strip()
LOCATION_ID = "107962957846"
CSV_URL = "https://www.johnnyvacstock.com/sigm_all_jv_products/JVWebProducts.csv"
API_VERSION = "2025-10" 

HEADERS = {
    "X-Shopify-Access-Token": API_PASSWORD,
    "Content-Type": "application/json"
}

# --- 1. ROBUST SESSION SETUP ---
def get_smart_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "PUT", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session

session = get_smart_session()

# --- 2. DATA HELPER FUNCTIONS ---
def normalize_price(price_str):
    if not price_str: return 0.0
    try:
        # Handle "12,50" -> 12.50
        return float(str(price_str).replace(',', '.').strip())
    except ValueError:
        return 0.0

def get_next_link(link_header):
    if not link_header: return None
    match = re.search(r'<([^>]+)>;\s*rel="next"', link_header)
    return match.group(1) if match else None

# --- 3. CATEGORIZATION ENGINE ---
def classify_product(title_en, title_fr, csv_cat):
    # Combine EN/FR titles for richer keyword matching
    t = f"{str(title_en)} {str(title_fr)}".upper()
    
    # Administrative
    if any(k in t for k in ["RARELY ORDERED", "DISCONTINUED", "ADJUSTED PRICE"]):
        return "Discontinued / Special Order", "admin, discontinued"

    # Equipment
    equipment = {
        "Commercial Vacuums": ["COMMERCIAL VAC", "BACKPACK", "CANISTER", "UPRIGHT"],
        "Central Vacuums": ["CENTRAL VAC", "ASPIRATEUR CENTRAL"],
        "Carpet Extractors": ["EXTRACTOR", "CARPET CLEANER"],
        "Automatic Scrubbers": ["SCRUBBER", "AUTO-SCRUBBER"],
        "Floor Machines & Burnishers": ["BURNISHER", "POLISHER", "FLOOR MACHINE"],
        "Pressure Washers": ["PRESSURE WASHER", "NETTOYEUR HAUTE PRESSION"]
    }
    for p_type, keywords in equipment.items():
        if any(k in t for k in keywords):
            return p_type, f"equipment, {p_type.lower().replace(' ', '-').replace('&', 'and')}"

    # Consumables
    consumables = {
        "Vacuum Bags": ["BAG", "SAC"],
        "Vacuum Filters": ["FILTER", "FILTRE", "HEPA", "CARTRIDGE"],
        "Vacuum Belts": ["BELT", "COURROIE"],
        "Vacuum Hoses": ["HOSE", "BOYAU", "MANCHON", "CUFF"],
        "Paper Products": [
            "TOILET", "TISSUE", "PAPER TOWEL", "HAND TOWEL", "ROLL TOWEL", 
            "NAPKIN", "KLEENEX", "PAPIER HYGIENIQUE", "ESSUIE-MAINS", "MOUCHOIR"
        ]
    }
    for p_type, keywords in consumables.items():
        if any(k in t for k in keywords):
            return p_type, "consumable"

    # Chemicals & Solutions
    if any(k in t for k in ["DETERGENT", "SOAP", "SAVON", "DEGREASER", "SHAMPOO", 
                            "CHIMIQUE", "CLEANER", "NETTOYANT", "SANITIZER", "DESINFECTANT"]):
        return "Chemicals & Solutions", "janitorial, chemicals-solutions"

    # Maintenance Parts - Use specific Product Types
    maintenance_map = {
        "Motors & Fans": ["MOTOR", "MOTEUR", "CARBON", "CHARBON", "ARMATURE", "FAN"],
        "Brushes & Tools": ["BRUSH", "BROSSE", "TOOL", "OUTIL", "NOZZLE", "SUCEUR", "WAND", "SQUEEGEE"],
        "Hardware & Fasteners": ["SCREW", "BOLT", "NUT", "WASHER", "RIVET", "VIS", "ECROU"],
        "Gaskets, Seals & Valves": ["GASKET", "SEAL", "O-RING", "DIAPHRAGM", "VALVE", "JOINT"],
        "Body Components & Housing": ["HANDLE", "COVER", "LID", "BASE", "HOUSING", "BUMPER", "LATCH", "TANK"],
        "Electrical Components": ["WIRE", "CABLE", "PLUG", "SOCKET", "PCB", "RELAY", "CORD", "CIRCUIT", "FUSE"],
        "Switches": ["SWITCH", "INTERRUPTEUR", "BUTTON"],
        "Wheels & Casters": ["WHEEL", "CASTER", "ROULETTE"]
    }
    
    for p_type, keywords in maintenance_map.items():
        if any(k in t for k in keywords):
            # Create clean tag handling commas and ampersands
            tag_slug = p_type.lower().replace(" & ", "-").replace(", ", "-").replace(" ", "-")
            return p_type, f"parts, {tag_slug}"

    # Fallback
    if csv_cat in ["All Parts", "Miscellaneous", "", "Parts"]:
        return "General Maintenance", "needs-review"
    
    return csv_cat, "imported"

# --- 4. SYNC LOGIC ---
def sync_johnnyvac():
    print("Step 1: Downloading JohnnyVac CSV...")
    try:
        r = requests.get(CSV_URL, timeout=60)
        r.raise_for_status()
    except Exception as e:
        print(f"CRITICAL: Could not download CSV. {e}")
        return

    r.encoding = 'utf-8-sig'
    csv_reader = csv.DictReader(io.StringIO(r.text), delimiter=';')
    
    headers = csv_reader.fieldnames
    if not headers:
        print("CRITICAL: CSV has no headers!")
        return

    # Helper to find column ignoring case
    def find_col(candidates):
        for h in headers:
            if h in candidates:
                return h
        return None

    sku_key = find_col(['SKU', 'sku'])
    title_en_key = find_col(['ProductTitleEN', 'producttitleen'])
    title_fr_key = find_col(['ProductTitleFR', 'producttitlefr'])
    price_key = find_col(['RegularPrice', 'regularprice'])
    qty_key = find_col(['Inventory', 'inventory'])
    cat_key = find_col(['ProductCategory', 'productcategory'])
    img_key = find_col(['ImageUrl', 'imageurl'])

    if not sku_key or not title_en_key:
        print(f"CRITICAL: Missing required columns.\nFound: {headers}")
        return

    print(f"✓ Columns mapped: SKU='{sku_key}', Title='{title_en_key}', Price='{price_key}'")

    jv_products = {}
    for row in csv_reader:
        if row.get(sku_key):
            jv_products[row[sku_key]] = {
                'sku': row[sku_key],
                'title_en': row.get(title_en_key, ''),
                'title_fr': row.get(title_fr_key, ''),
                'price': row.get(price_key, '0'),
                'inventory': row.get(qty_key, '0'),
                'category': row.get(cat_key, ''),
                'imageurl': row.get(img_key, '')
            }

    print(f"✓ Parsed {len(jv_products)} products.")
    
    # Debug: Check one product to ensure title is reading
    first_sku = list(jv_products.keys())[0]
    print(f"DEBUG: Sample Product [{first_sku}] Title: {jv_products[first_sku]['title_en']}")

    print(f"Step 2: Fetching Shopify Products from {SHOP_URL}...")
    
    shopify_products = {}
    url = f"https://{SHOP_URL}/admin/api/{API_VERSION}/products.json?limit=250"
    
    while url:
        try:
            resp = session.get(url)
            if resp.status_code == 401:
                print("CRITICAL: 401 Unauthorized. Check credentials.")
                return
            resp.raise_for_status()
            
            data = resp.json()
            for p in data.get('products', []):
                if not p['variants']: continue
                variant = p['variants'][0]
                if variant.get('sku'):
                    shopify_products[variant['sku']] = {
                        'product_id': p['id'],
                        'variant_id': variant['id'],
                        'inventory_item_id': variant['inventory_item_id'],
                        'product_type': p['product_type'],
                        'tags': p['tags'],
                        'price': variant['price'],
                        'inventory_quantity': variant['inventory_quantity'],
                        'image_count': len(p.get('images', []))
                    }
            
            url = get_next_link(resp.headers.get('Link'))
            if url: print(".", end="", flush=True)
            
        except Exception as e:
            print(f"\nError fetching products: {e}")
            break

    print(f"\n✓ Loaded {len(shopify_products)} matched products.")
    print("Step 3: Comparing and Syncing...")

    update_count = 0
    
    for sku, jv_item in jv_products.items():
        if sku in shopify_products:
            sh_data = shopify_products[sku]
            
            # --- PREPARE DATA ---
            new_type, new_tags = classify_product(jv_item['title_en'], jv_item['title_fr'], jv_item['category'])
            
            jv_price = normalize_price(jv_item['price'])
            sh_price = float(sh_data['price'])
            
            # Clean inventory string
            qty_clean = jv_item['inventory'].replace(',', '.').strip()
            jv_qty = int(float(qty_clean)) if qty_clean else 0
            
            # Image Check
            csv_img_url = jv_item['imageurl'].strip()
            needs_image = (sh_data['image_count'] == 0) and (csv_img_url != "") and (csv_img_url.startswith('http'))

            # --- DETECT CHANGES ---
            type_changed = sh_data['product_type'] != new_type
            tags_changed = sh_data['tags'] != new_tags
            price_changed = abs(jv_price - sh_price) > 0.01
            qty_changed = sh_data['inventory_quantity'] != jv_qty

            if type_changed or tags_changed or price_changed or qty_changed or needs_image:
                update_count += 1
                if update_count % 10 == 0: print(f"Processing update #{update_count} ({sku})...")

                # 1. Update Inventory
                if qty_changed:
                    inv_payload = {
                        "location_id": LOCATION_ID,
                        "inventory_item_id": sh_data['inventory_item_id'],
                        "available": jv_qty
                    }
                    session.post(f"https://{SHOP_URL}/admin/api/{API_VERSION}/inventory_levels/set.json", json=inv_payload)
                    time.sleep(0.3)

                # 2. Update Metadata
                prod_payload = {"id": sh_data['product_id']}
                if type_changed: prod_payload["product_type"] = new_type
                if tags_changed: prod_payload["tags"] = new_tags
                if price_changed:
                    prod_payload["variants"] = [{"id": sh_data['variant_id'], "price": str(jv_price)}]

                if type_changed or tags_changed or price_changed:
                    session.put(f"https://{SHOP_URL}/admin/api/{API_VERSION}/products/{sh_data['product_id']}.json", json={"product": prod_payload})
                    time.sleep(0.5)

                # 3. Update Image
                if needs_image:
                    print(f"  + Uploading image for {sku}")
                    image_payload = {"image": {"src": csv_img_url}}
                    session.post(f"https://{SHOP_URL}/admin/api/{API_VERSION}/products/{sh_data['product_id']}/images.json", json=image_payload)
                    time.sleep(1.0) 

    print(f"\nSync Complete. {update_count} products updated.")

if __name__ == "__main__":
    sync_johnnyvac()
