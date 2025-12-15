import os
import csv
import time
import json
import requests
from datetime import datetime

# Configuration
SHOPIFY_STORE = os.environ['SHOPIFY_STORE']
SHOPIFY_ACCESS_TOKEN = os.environ['SHOPIFY_ACCESS_TOKEN']
CSV_URL = 'https://www.johnnyvacstock.com/sigm_all_jv_products/JVWebProducts.csv'
IMAGE_BASE_URL = 'https://www.johnnyvacstock.com/photos/web/'

# Files
PREVIOUS_CSV_FILE = 'previous_products.csv'
STATE_FILE = 'sync_state.json'

# Settings
LANGUAGE = 'en'
BATCH_SIZE = 50
RATE_LIMIT_DELAY = 0.5

def load_state():
    """Load previous sync state"""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {'last_sync': None, 'products_synced': 0}

def save_state(state):
    """Save sync state"""
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)

def make_request(method, endpoint, data=None, max_retries=3):
    """Make Shopify API request with retry logic"""
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
                print(f"  Rate limited. Waiting {retry_after}s...")
                time.sleep(retry_after)
                continue
            
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 3
                print(f"  Retry {attempt + 1}/{max_retries} in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise
    return None

def graphql_request(query, variables=None):
    """Make GraphQL request"""
    url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/graphql.json"
    headers = {
        'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
        'Content-Type': 'application/json'
    }
    
    payload = {'query': query}
    if variables:
        payload['variables'] = variables
    
    response = requests.post(url, headers=headers, json=payload, timeout=60)
    response.raise_for_status()
    return response.json()

def fetch_all_shopify_products_bulk():
    """
    Fetch ALL products using Shopify's Bulk Operations API
    This is the proper way to sync large catalogs - no rate limits!
    """
    print("Fetching all Shopify products using Bulk Operations API...")
    
    # Step 1: Start bulk operation
    query = """
    mutation {
      bulkOperationRunQuery(
        query: \"\"\"
        {
          products(query: "vendor:JohnnyVac") {
            edges {
              node {
                id
                legacyResourceId
                variants(first: 1) {
                  edges {
                    node {
                      id
                      legacyResourceId
                      sku
                      inventoryItem {
                        id
                        legacyResourceId
                      }
                    }
                  }
                }
              }
            }
          }
        }
        \"\"\"
      ) {
        bulkOperation {
          id
          status
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    
    result = graphql_request(query)
    
    if result.get('data', {}).get('bulkOperationRunQuery', {}).get('userErrors'):
        errors = result['data']['bulkOperationRunQuery']['userErrors']
        print(f"  Error starting bulk operation: {errors}")
        return {}
    
    operation_id = result['data']['bulkOperationRunQuery']['bulkOperation']['id']
    print(f"  ✓ Bulk operation started (ID: {operation_id})")
    
    # Step 2: Poll until complete
    poll_query = """
    {
      currentBulkOperation {
        id
        status
        errorCode
        createdAt
        completedAt
        objectCount
        fileSize
        url
        partialDataUrl
      }
    }
    """
    
    print("  Waiting for bulk operation to complete...", end='')
    while True:
        time.sleep(3)
        result = graphql_request(poll_query)
        operation = result['data']['currentBulkOperation']
        status = operation['status']
        
        print('.', end='', flush=True)
        
        if status == 'COMPLETED':
            print(' Done!')
            url = operation['url']
            count = operation['objectCount']
            print(f"  ✓ Fetched {count} products")
            break
        elif status in ['FAILED', 'CANCELED']:
            print(f' Failed!')
            print(f"  Error: {operation.get('errorCode', 'Unknown')}")
            return {}
    
    # Step 3: Download and parse results
    print("  Downloading results...")
    response = requests.get(url)
    
    products = {}
    for line in response.text.strip().split('\n'):
        if not line:
            continue
        obj = json.loads(line)
        
        # Parse the JSONL format from bulk operation
        if obj.get('variants'):
            variant = obj['variants']['edges'][0]['node']
            sku = variant.get('sku')
            if sku:
                products[sku] = {
                    'id': int(obj['legacyResourceId']),
                    'variant': {
                        'id': int(variant['legacyResourceId']),
                        'sku': sku,
                        'inventory_item_id': int(variant['inventoryItem']['legacyResourceId'])
                    }
                }
    
    print(f"  ✓ Indexed {len(products)} products by SKU\n")
    return products

def download_csv():
    """Download CSV from JohnnyVac"""
    print("Downloading product CSV from JohnnyVac...")
    response = requests.get(CSV_URL, timeout=60)
    response.raise_for_status()
    print(f"  ✓ Downloaded\n")
    return response.text

def parse_csv(csv_content):
    """Parse CSV into list of dicts"""
    lines = csv_content.strip().split('\n')
    reader = csv.DictReader(lines, delimiter=';')
    return list(reader)

def detect_changes(current_products, previous_csv_content):
    """Detect what changed since last run"""
    if not previous_csv_content:
        print("No previous data - will sync all products")
        return current_products
    
    previous_products = {row['SKU']: row for row in parse_csv(previous_csv_content)}
    
    changed = []
    check_fields = ['RegularPrice', 'Inventory', 'ProductTitleEN', 'ProductTitleFR']
    
    for product in current_products:
        sku = product['SKU']
        
        if sku not in previous_products:
            changed.append(product)  # New product
        else:
            prev = previous_products[sku]
            for field in check_fields:
                if product.get(field, '').strip() != prev.get(field, '').strip():
                    changed.append(product)
                    break
    
    print(f"Change Detection:")
    print(f"  Total products: {len(current_products)}")
    print(f"  Changed/New: {len(changed)}")
    print(f"  Unchanged: {len(current_products) - len(changed)}\n")
    
    return changed

def get_inventory_location():
    """Get inventory location ID (cached)"""
    if not hasattr(get_inventory_location, 'location_id'):
        result = make_request('GET', 'locations.json')
        get_inventory_location.location_id = result['locations'][0]['id']
    return get_inventory_location.location_id

def create_or_update_product(product_data, shopify_products):
    """Create or update a product"""
    sku = product_data['SKU']
    title = product_data.get(f'ProductTitle{LANGUAGE.upper()}', sku)[:255]
    description = product_data.get(f'ProductDescription{LANGUAGE.upper()}', '')
    
    try:
        price = float(product_data.get('RegularPrice', 0) or 0)
        inventory = int(product_data.get('Inventory', 0) or 0)
    except:
        price = 0
        inventory = 0
    
    if sku in shopify_products:
        # Update existing
        info = shopify_products[sku]
        product_id = info['id']
        variant_id = info['variant']['id']
        
        # Update product
        make_request('PUT', f"products/{product_id}.json", {
            "product": {
                "id": product_id,
                "title": title,
                "body_html": description
            }
        })
        
        # Update variant
        make_request('PUT', f"variants/{variant_id}.json", {
            "variant": {
                "id": variant_id,
                "price": str(price)
            }
        })
        
        # Update inventory
        make_request('POST', 'inventory_levels/set.json', {
            "location_id": get_inventory_location(),
            "inventory_item_id": info['variant']['inventory_item_id'],
            "available": inventory
        })
        
        return 'updated'
    else:
        # Create new
        make_request('POST', 'products.json', {
            "product": {
                "title": title,
                "body_html": description,
                "vendor": "JohnnyVac",
                "product_type": product_data.get('ProductCategory', ''),
                "variants": [{
                    "sku": sku,
                    "price": str(price),
                    "inventory_management": "shopify",
                    "inventory_quantity": inventory
                }],
                "images": [{"src": f"{IMAGE_BASE_URL}{sku}.jpg"}]
            }
        })
        
        return 'created'

def main():
    """Main sync process"""
    print(f"\n{'='*70}")
    print(f"JohnnyVac → Shopify Sync")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")
    
    start_time = time.time()
    state = load_state()
    
    # Download current CSV
    csv_content = download_csv()
    current_products = parse_csv(csv_content)
    
    # Load previous CSV
    previous_csv = None
    if os.path.exists(PREVIOUS_CSV_FILE):
        with open(PREVIOUS_CSV_FILE, 'r') as f:
            previous_csv = f.read()
    
    # Detect changes
    products_to_sync = detect_changes(current_products, previous_csv)
    
    if not products_to_sync:
        print("✅ No changes detected - nothing to sync!\n")
        with open(PREVIOUS_CSV_FILE, 'w') as f:
            f.write(csv_content)
        return
    
    print(f"Syncing {len(products_to_sync)} products...\n")
    
    # Fetch existing products using Bulk API
    shopify_products = fetch_all_shopify_products_bulk()
    
    # Process in batches
    created = 0
    updated = 0
    errors = 0
    
    total_batches = (len(products_to_sync) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for batch_num in range(total_batches):
        start_idx = batch_num * BATCH_SIZE
        end_idx = min(start_idx + BATCH_SIZE, len(products_to_sync))
        batch = products_to_sync[start_idx:end_idx]
        
        print(f"Batch {batch_num + 1}/{total_batches} (products {start_idx + 1}-{end_idx})")
        
        for product in batch:
            try:
                result = create_or_update_product(product, shopify_products)
                if result == 'created':
                    created += 1
                    print(f"  ✓ Created: {product['SKU']}")
                else:
                    updated += 1
                    print(f"  ↻ Updated: {product['SKU']}")
                
                time.sleep(RATE_LIMIT_DELAY)
            except Exception as e:
                errors += 1
                print(f"  ✗ Error {product['SKU']}: {e}")
        
        print()
    
    # Save state
    with open(PREVIOUS_CSV_FILE, 'w') as f:
        f.write(csv_content)
    
    state['last_sync'] = datetime.now().isoformat()
    state['products_synced'] = created + updated
    save_state(state)
    
    # Summary
    elapsed = time.time() - start_time
    print(f"{'='*70}")
    print(f"✅ Sync Complete!")
    print(f"{'='*70}")
    print(f"Created:  {created}")
    print(f"Updated:  {updated}")
    print(f"Errors:   {errors}")
    print(f"Time:     {elapsed/60:.1f} minutes")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    main()
