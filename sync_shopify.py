import csv
import time
import requests
import io
import re
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURATION ---
SHOP_URL = os.getenv("SHOPIFY_STORE", "").replace("https://", "").strip()
API_PASSWORD = os.getenv("SHOPIFY_ACCESS_TOKEN", "").strip()
LOCATION_ID = "107962957846" # Your Shopify Location ID
CSV_URL = "https://www.johnnyvacstock.com/sigm_all_jv_products/JVWebProducts.csv"
API_VERSION = "2024-01" 

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
        return float(str(price_str).replace(',', '.').strip())
    except ValueError:
        return 0.0

def get_next_link(link_header):
    if not link_header: return None
    match = re.search(r'<([^>]+)>;\s*rel="next"', link_header)
    return match.group(1) if match else None

# --- 3. CATEGORIZATION ENGINE ---
def classify_product(title_en, title_fr, csv_cat):
    """
    Enhanced Categorization Engine including PPE, Food Service, and Dispensers.
    Assigns Product Type and Tags based on keyword matching.
    """
    t = f"{str(title_en)} {str(title_fr)}".upper()
    cat = str(csv_cat).upper()
    
    # A. Administrative (Smart Collection: Discontinued)
    if any(k in t for k in ["RARELY ORDERED", "DISCONTINUED", "ADJUSTED PRICE", "RAREMENT COMMANDE"]):
        return "Discontinued / Special Order", "discontinued"

    # B. Main Equipment
    equipment = {
        "Commercial Vacuums": ["COMMERCIAL VAC", "BACKPACK", "CANISTER", "UPRIGHT", "ASPIRATEUR DORSAL"],
        "Central Vacuums": ["CENTRAL VAC", "ASPIRATEUR CENTRAL"],
        "Carpet Extractors": ["EXTRACTOR", "CARPET CLEANER", "EXTRACTEUR"],
        "Automatic Scrubbers": ["SCRUBBER", "AUTO-SCRUBBER", "RECURREUSE"],
        "Floor Machines & Burnishers": ["BURNISHER", "POLISHER", "FLOOR MACHINE", "POLISSEUSE"],
        "Pressure Washers": ["PRESSURE WASHER", "NETTOYEUR HAUTE PRESSION"]
    }
    for p_type, keywords in equipment.items():
        if any(k in t for k in keywords):
            return p_type, "equipment"

    # C. Consumables
    consumables = {
        "Vacuum Bags": ["BAG", "SAC"],
        "Vacuum Filters": ["FILTER", "FILTRE", "HEPA", "CARTRIDGE"],
        "Vacuum Belts": ["BELT", "COURROIE"],
        "Vacuum Hoses": ["HOSE", "BOYAU", "MANCHON", "CUFF"]
    }
    for p_type, keywords in consumables.items():
        if any(k in t for k in keywords):
            return p_type, "consumable"

    # D. Facilities & Cleaning Supplies (PPE, Food Service, Dispensers)
    # Safety & PPE
    if any(k in t for k in ["GLOVE", "GANT", "MASK", "MASQUE", "VEST", "VESTE", "GLASSES", "LUNETTE", "FIRST AID", "SECURIT"]):
        return "Safety & PPE", "cleaning-supplies, safety"

    # Food Service
    if any(k in t for k in ["CUTLERY", "COUTELLERIE", "PLATE", "ASSIETTE", "CUP", "VERRE", "NAPKIN", "SERVIETTE", "TRAY", "PLATEAU", "UTENSIL"]):
        return "Food Service", "cleaning-supplies, food-service"

    # Dispensers
    if any(k in t for k in ["DISPENSER", "DISTRIBUTEUR", "TORK", "SANIS", "KIMBERLY"]):
        return "Dispensers", "cleaning-supplies, dispensers"

    # Paper Products
    if any(k in t for k in ["PAPER", "TOWEL", "TISSUE", "PAPIER", "ESSUIE-TOUT", "TOILET PAPER"]):
        return "Paper Products", "cleaning-supplies, paper"

    # Chemicals
    if any(k in t for k in ["CHEMICAL", "CLEANER", "SOAP", "DETERGENT", "DEGREASER", "SAVON", "NETTOYANT"]):
        return "Chemicals & Cleaners", "cleaning-supplies, chemicals"

    # E. Maintenance Parts
    # Hardware & Fasteners
    if any(k in t for k in ["SCREW", "BOLT", "NUT", "WASHER", "RIVET", "VIS", "ECROU", "RONDELLE"]):
        return "Hardware & Fasteners", "maintenance, parts"
    
    # Seals & Gaskets
    if any(k in t for k in ["GASKET", "SEAL", "O-RING", "DIAPHRAGM", "VALVE", "JOINT"]):
        return "Gaskets, Seals & Valves", "maintenance, parts"
        
    # Electrical Parts
    if any(k in t for k in ["MOTOR", "MOTEUR", "WIRE", "CABLE", "PLUG", "SOCKET", "PCB", "RELAY", "CORD", "CIRCUIT", "SWITCH", "INTERRUPTEUR"]):
        return "Electrical Components", "maintenance, parts"
        
    # Wheels & Casters
    if any(k in t for k in ["WHEEL", "CASTER", "ROULETTE", "ROUE"]):
        return "Wheels & Casters", "maintenance, parts"

    # F. Fallback (Smart Collection: Needs Review)
    if cat in ["JOHNNY VAC PARTS (*)", "ALL PARTS", "MISCELLANEOUS", "", "PARTS"] or not title_en:
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

    print(f"Step 2: Fetching Shopify Products...")
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

    print(f"✓ Loaded {len(shopify_products)} products from Shopify.")
    print("Step 3: Processing Sync...")

    update_count = 0
    for sku, jv_item in jv_products.items():
        if sku in shopify_products:
            sh_data = shopify_products[sku]
            new_type, new_tags = classify_product(jv_item['title_en'], jv_item['title_fr'], jv_item['category'])
            jv_price = normalize_price(jv_item['price'])
            sh_price = float(sh_data['price'])
            
            qty_clean = jv_item['inventory'].replace(',', '.').strip()
            jv_qty = int(float(qty_clean)) if qty_clean else 0
            
            type_changed = sh_data['product_type'] != new_type
            tags_changed = sh_data['tags'] != new_tags
            price_changed = abs(jv_price - sh_price) > 0.01
            qty_changed = sh_data['inventory_quantity'] != jv_qty

            if type_changed or tags_changed or price_changed or qty_changed:
                update_count += 1
                
                # Update Inventory
                if qty_changed:
                    inv_payload = {"location_id": LOCATION_ID, "inventory_item_id": sh_data['inventory_item_id'], "available": jv_qty}
                    session.post(f"https://{SHOP_URL}/admin/api/{API_VERSION}/inventory_levels/set.json", json=inv_payload)

                # Update Meta
                if type_changed or tags_changed or price_changed:
                    prod_payload = {"id": sh_data['product_id']}
                    if type_changed: prod_payload["product_type"] = new_type
                    if tags_changed: prod_payload["tags"] = new_tags
                    if price_changed: prod_payload["variants"] = [{"id": sh_data['variant_id'], "price": str(jv_price)}]
                    session.put(f"https://{SHOP_URL}/admin/api/{API_VERSION}/products/{sh_data['product_id']}.json", json={"product": prod_payload})

    print(f"\nSync Complete. Total Updates: {update_count}")

if __name__ == "__main__":
    sync_johnnyvac()
