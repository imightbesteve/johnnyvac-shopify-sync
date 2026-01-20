import csv
import time
import requests
import io
import re
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURATION ---
# Pulls from your environment variables as seen in your logs
SHOP_URL = os.getenv("SHOPIFY_STORE", "").replace("https://", "").strip()
API_PASSWORD = os.getenv("SHOPIFY_ACCESS_TOKEN", "").strip()
LOCATION_ID = "107962957846"  # Your Shopify Location ID
CSV_URL = "https://www.johnnyvacstock.com/sigm_all_jv_products/JVWebProducts.csv"
API_VERSION = "2025-10" 

HEADERS = {
    "X-Shopify-Access-Token": API_PASSWORD,
    "Content-Type": "application/json"
}

# --- 1. ROBUST SESSION SETUP ---
def get_smart_session():
    """Sets up a session with automatic retries and backoff for Shopify's API."""
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,  # Exponential backoff (2s, 4s, 8s, 16s...)
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
    """Converts price strings like '12,50' to float 12.50."""
    if not price_str: return 0.0
    try:
        return float(str(price_str).replace(',', '.').strip())
    except ValueError:
        return 0.0

def get_next_link(link_header):
    """Parses Shopify pagination headers."""
    if not link_header: return None
    match = re.search(r'<([^>]+)>;\s*rel="next"', link_header)
    return match.group(1) if match else None

# --- 3. CATEGORIZATION ENGINE ---
def classify_product(title_en, title_fr, csv_cat):
    """
    Assigns Product Type and Tags based on your Smart Collection conditions.
    Matches Equipment, Consumables, Maintenance Parts, and Supplies.
    """
    t = f"{str(title_en)} {str(title_fr)}".upper()
    cat = str(csv_cat).upper()
    
    # A. Administrative (Smart Collection: Discontinued)
    if any(k in t for k in ["RARELY ORDERED", "DISCONTINUED", "ADJUSTED PRICE"]):
        return "Discontinued / Special Order", "discontinued"

    # B. Main Equipment (Smart Collections: Commercial Vacuums, Central Vacuums, etc.)
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
            return p_type, "equipment"

    # C. Consumables (Smart Collections: Vacuum Bags, Filters, Belts, Hoses)
    consumables = {
        "Vacuum Bags": ["BAG", "SAC"],
        "Vacuum Filters": ["FILTER", "FILTRE", "HEPA", "CARTRIDGE"],
        "Vacuum Belts": ["BELT", "COURROIE"],
        "Vacuum Hoses": ["HOSE", "BOYAU", "MANCHON", "CUFF"]
    }
    for p_type, keywords in consumables.items():
        if any(k in t for k in keywords):
            return p_type, "consumable"

    # D. Maintenance Parts (Smart Collections: Hardware, Seals, Electrical, Wheels)
    # Hardware & Fasteners
    if any(k in t for k in ["SCREW", "BOLT", "NUT", "WASHER", "RIVET", "VIS", "ECROU"]):
        return "Hardware & Fasteners", "maintenance, parts"
    
    # Seals & Gaskets
    if any(k in t for k in ["GASKET", "SEAL", "O-RING", "DIAPHRAGM", "VALVE", "JOINT"]):
        return "Gaskets, Seals & Valves", "maintenance, parts"
        
    # Electrical Parts
    if any(k in t for k in ["MOTOR", "MOTEUR", "WIRE", "CABLE", "PLUG", "SOCKET", "PCB", "RELAY", "CORD", "CIRCUIT", "SWITCH", "INTERRUPTEUR"]):
        return "Electrical Components", "maintenance, parts"
        
    # Wheels & Casters
    if any(k in t for k in ["WHEEL", "CASTER", "ROULETTE"]):
        return "Wheels & Casters", "maintenance, parts"

    # E. Cleaning Supplies (Smart Collections: Chemicals, Paper Products)
    if any(k in t for k in ["CHEMICAL", "CLEANER", "SOAP", "DETERGENT", "DEGREASER"]):
        return "Chemicals & Cleaners", "cleaning-supplies"
    
    if any(k in t for k in ["PAPER", "TOWEL", "TISSUE", "PAPIER"]):
        return "Paper Products", "cleaning-supplies"

    # F. Fallback (Smart Collection: Needs Review)
    if cat in ["ALL PARTS", "MISCELLANEOUS", "", "PARTS"] or not title_en:
        return "General Maintenance", "needs-review"
    
    return csv_cat if csv_cat else "General Maintenance", "imported"

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
    
    # Exact Mapping from JVWebProducts structure
    jv_products = {}
    for row in csv_reader:
        sku = row.get('SKU')
        if sku:
            jv_products[sku] = {
                'sku': sku,
                'title_en': row.get('ProductTitleEN', ''),
                'title_fr': row.get('ProductTitleFR', ''),
                'price': row.get('RegularPrice', '0'),
                'inventory': row.get('Inventory', '0'),
                'category': row.get('ProductCategory', ''),
                'imageurl': row.get('ImageUrl', '')
            }

    print(f"✓ Parsed {len(jv_products)} products from JohnnyVac.")
    if not jv_products: return

    print(f"Step 2: Fetching Shopify Products from {SHOP_URL}...")
    shopify_products = {}
    url = f"https://{SHOP_URL}/admin/api/{API_VERSION}/products.json?limit=250"
    
    while url:
        resp = session.get(url)
        if resp.status_code != 200:
            print(f"Error fetching products: {resp.status_code}")
            break
            
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

    print(f"\n✓ Loaded {len(shopify_products)} matching products from Shopify.")
    print("Step 3: Processing Sync...")

    update_count = 0
    for sku, jv_item in jv_products.items():
        if sku in shopify_products:
            sh_data = shopify_products[sku]
            
            # --- PREPARE TARGET DATA ---
            new_type, new_tags = classify_product(jv_item['title_en'], jv_item['title_fr'], jv_item['category'])
            jv_price = normalize_price(jv_item['price'])
            sh_price = float(sh_data['price'])
            
            qty_clean = jv_item['inventory'].replace(',', '.').strip()
            jv_qty = int(float(qty_clean)) if qty_clean else 0
            
            # Image check: Only upload if product has 0 images and CSV has an URL
            csv_img_url = jv_item['imageurl'].strip()
            needs_image = (sh_data['image_count'] == 0) and (csv_img_url != "") and (csv_img_url.startswith('http'))

            # --- DETECT CHANGES ---
            type_changed = sh_data['product_type'] != new_type
            tags_changed = sh_data['tags'] != new_tags
            price_changed = abs(jv_price - sh_price) > 0.01
            qty_changed = sh_data['inventory_quantity'] != jv_qty

            if type_changed or tags_changed or price_changed or qty_changed or needs_image:
                update_count += 1
                if update_count % 20 == 0: print(f"Update #{update_count}: Syncing {sku}")

                # 1. Update Inventory Level
                if qty_changed:
                    inv_payload = {
                        "location_id": LOCATION_ID,
                        "inventory_item_id": sh_data['inventory_item_id'],
                        "available": jv_qty
                    }
                    session.post(f"https://{SHOP_URL}/admin/api/{API_VERSION}/inventory_levels/set.json", json=inv_payload)
                    time.sleep(0.4)

                # 2. Update Product Metadata (Type, Tags, Price)
                if type_changed or tags_changed or price_changed:
                    prod_payload = {"id": sh_data['product_id']}
                    if type_changed: prod_payload["product_type"] = new_type
                    if tags_changed: prod_payload["tags"] = new_tags
                    if price_changed:
                        prod_payload["variants"] = [{"id": sh_data['variant_id'], "price": str(jv_price)}]
                    
                    session.put(f"https://{SHOP_URL}/admin/api/{API_VERSION}/products/{sh_data['product_id']}.json", json={"product": prod_payload})
                    time.sleep(0.5)

                # 3. Handle Missing Images
                if needs_image:
                    img_payload = {"image": {"src": csv_img_url}}
                    session.post(f"https://{SHOP_URL}/admin/api/{API_VERSION}/products/{sh_data['product_id']}/images.json", json=img_payload)
                    time.sleep(1.0)

    print(f"\nSync Complete. Total Updates: {update_count}")

if __name__ == "__main__":
    sync_johnnyvac()
