import os
import csv
import requests
import time
from datetime import datetime, timedelta
from io import StringIO

# Configuration
SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')
CSV_URL = 'https://www.johnnyvacstock.com/sigm_all_jv_products/JVWebProducts.csv'
IMAGE_BASE_URL = 'https://www.johnnyvacstock.com/photos/web/'

BATCH_SIZE = 50
RATE_LIMIT_DELAY = 1.0
BATCH_DELAY = 10
LANGUAGE = 'en'
OUT_OF_STOCK_DAYS_THRESHOLD = 30
REQUEST_TIMEOUT = 30

# Shopify API helpers
def shopify_request(method, endpoint, data=None, retries=3):
    url = f'https://{SHOPIFY_STORE}/admin/api/2024-01/{endpoint}'
    headers = {
        'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
        'Content-Type': 'application/json'
    }
    
    for attempt in range(retries):
        try:
            if method == 'GET':
                response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            elif method == 'POST':
                response = requests.post(url, headers=headers, json=data, timeout=REQUEST_TIMEOUT)
            elif method == 'PUT':
                response = requests.put(url, headers=headers, json=data, timeout=REQUEST_TIMEOUT)
            elif method == 'DELETE':
                response = requests.delete(url, headers=headers, timeout=REQUEST_TIMEOUT)
            
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 5))
                print(f"Rate limited. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                continue
                
            response.raise_for_status()
            return response.json() if response.content else {}
            
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < retries - 1:
                wait_time = (attempt + 1) * 5
                print(f"Connection error (attempt {attempt + 1}/{retries}): {e}")
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                raise
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429 and attempt < retries - 1:
                continue
            raise
    
    return None

def get_all_shopify_products():
    products = {}
    endpoint = 'products.json?limit=250'
    
    while endpoint:
        print(f"Fetching products...")
        response = shopify_request('GET', endpoint)
        
        if not response:
            break
            
        for product in response.get('products', []):
            sku = None
            for variant in product.get('variants', []):
                if variant.get('sku'):
                    sku = variant['sku']
                    break
            
            if sku:
                products[sku] = {
                    'id': product['id'],
                    'variant_id': product['variants'][0]['id'] if product.get('variants') else None,
                    'inventory_item_id': product['variants'][0].get('inventory_item_id') if product.get('variants') else None,
                    'metafields': {}
                }
        
        link_header = response.get('link')
        endpoint = None
        if link_header:
            links = link_header.split(',')
            for link in links:
                if 'rel="next"' in link:
                    endpoint = link[link.find('<')+1:link.find('>')].split('/admin/api/2024-01/')[1]
                    break
        
        time.sleep(RATE_LIMIT_DELAY)
    
    print(f"Found {len(products)} existing products in Shopify")
    return products

def get_product_metafields(product_id):
    metafields = {}
    try:
        response = shopify_request('GET', f'products/{product_id}/metafields.json')
        for mf in response.get('metafields', []):
            if mf['namespace'] == 'custom':
                metafields[mf['key']] = mf['value']
    except Exception as e:
        print(f"Error fetching metafields for product {product_id}: {e}")
    
    time.sleep(RATE_LIMIT_DELAY)
    return metafields

def set_product_metafield(product_id, key, value):
    try:
        data = {
            'metafield': {
                'namespace': 'custom',
                'key': key,
                'value': value,
                'type': 'single_line_text_field'
            }
        }
        shopify_request('POST', f'products/{product_id}/metafields.json', data)
    except Exception as e:
        print(f"Error setting metafield {key} for product {product_id}: {e}")
    
    time.sleep(RATE_LIMIT_DELAY)

def check_image_exists(url):
    try:
        response = requests.head(url, timeout=5)
        return response.status_code == 200
    except:
        return False

def get_all_product_images(sku):
    images = []
    
    # Try main image first
    main_url = f"{IMAGE_BASE_URL}{sku}.jpg"
    if check_image_exists(main_url):
        images.append({'src': main_url})
    
    # Try numbered images (-1, -2, -3, etc.)
    for i in range(1, 6):  # Check up to 5 additional images
        image_url = f"{IMAGE_BASE_URL}{sku}-{i}.jpg"
        if check_image_exists(image_url):
            images.append({'src': image_url})
        else:
            break  # Stop at first missing image
    
    return images

def update_inventory(variant_id, quantity):
    """Update inventory by setting it directly on the variant"""
    try:
        data = {
            'variant': {
                'id': variant_id,
                'inventory_quantity': quantity,
                'inventory_management': 'shopify'
            }
        }
        shopify_request('PUT', f'variants/{variant_id}.json', data)
        
    except Exception as e:
        print(f"Error updating inventory: {e}")
    
    time.sleep(RATE_LIMIT_DELAY)

def create_product(row, images):
    title = row.get('ProductTitleEN' if LANGUAGE == 'en' else 'ProductTitleFR', '')
    description = row.get('ProductDescriptionEN' if LANGUAGE == 'en' else 'ProductDescriptionFR', '')
    
    product_data = {
        'product': {
            'title': title,
            'body_html': description,
            'vendor': 'JohnnyVac',
            'product_type': row.get('ProductCategory', ''),
            'variants': [{
                'sku': row['SKU'],
                'price': row.get('RegularPrice', '0'),
                'inventory_management': 'shopify',
                'inventory_policy': 'deny',
                'weight': float(row.get('weight', 0)),
                'weight_unit': 'kg',
                'barcode': row.get('upc', '')
            }],
            'images': images
        }
    }
    
    try:
        response = shopify_request('POST', 'products.json', product_data)
        if response and 'product' in response:
            product = response['product']
            product_id = product['id']
            variant_id = product['variants'][0]['id']
            
            # Set initial inventory using variant update
            quantity = int(row.get('Inventory', 0))
            if variant_id:
                update_inventory(variant_id, quantity)
            
            # Set out_of_stock_since metafield if quantity is 0
            if quantity == 0:
                today = datetime.now().strftime('%Y-%m-%d')
                set_product_metafield(product_id, 'out_of_stock_since', today)
            
            return True
    except Exception as e:
        print(f"Error creating product {row['SKU']}: {e}")
        return False
    
    time.sleep(RATE_LIMIT_DELAY)
    return False

def update_product(product_info, row, images):
    product_id = product_info['id']
    variant_id = product_info['variant_id']
    
    title = row.get('ProductTitleEN' if LANGUAGE == 'en' else 'ProductTitleFR', '')
    description = row.get('ProductDescriptionEN' if LANGUAGE == 'en' else 'ProductDescriptionFR', '')
    quantity = int(row.get('Inventory', 0))
    
    try:
        # Update product details
        product_data = {
            'product': {
                'id': product_id,
                'title': title,
                'body_html': description,
                'product_type': row.get('ProductCategory', ''),
                'images': images
            }
        }
        shopify_request('PUT', f'products/{product_id}.json', product_data)
        
        # Update variant with price, weight, barcode AND inventory
        if variant_id:
            variant_data = {
                'variant': {
                    'id': variant_id,
                    'price': row.get('RegularPrice', '0'),
                    'weight': float(row.get('weight', 0)),
                    'barcode': row.get('upc', ''),
                    'inventory_quantity': quantity,
                    'inventory_management': 'shopify'
                }
            }
            shopify_request('PUT', f'variants/{variant_id}.json', variant_data)
        
        # Get existing metafields
        metafields = get_product_metafields(product_id)
        out_of_stock_since = metafields.get('out_of_stock_since')
        
        # Handle out_of_stock_since metafield
        if quantity == 0:
            if not out_of_stock_since:
                # Product just went out of stock
                today = datetime.now().strftime('%Y-%m-%d')
                set_product_metafield(product_id, 'out_of_stock_since', today)
        else:
            if out_of_stock_since:
                # Product back in stock - clear the date
                set_product_metafield(product_id, 'out_of_stock_since', '')
        
        return True
        
    except Exception as e:
        print(f"Error updating product {row['SKU']}: {e}")
        return False
    
    time.sleep(RATE_LIMIT_DELAY)
    return False

def delete_product(product_id, sku):
    try:
        shopify_request('DELETE', f'products/{product_id}.json')
        print(f"Deleted product {sku} (out of stock for 30+ days)")
        return True
    except Exception as e:
        print(f"Error deleting product {sku}: {e}")
        return False
    
    time.sleep(RATE_LIMIT_DELAY)
    return False

def cleanup_old_out_of_stock(existing_products):
    print("\nChecking for products to remove (out of stock 30+ days)...")
    threshold_date = datetime.now() - timedelta(days=OUT_OF_STOCK_DAYS_THRESHOLD)
    deleted_count = 0
    
    for sku, product_info in list(existing_products.items()):
        product_id = product_info['id']
        
        # Get metafields
        metafields = get_product_metafields(product_id)
        out_of_stock_since = metafields.get('out_of_stock_since')
        
        if out_of_stock_since:
            try:
                out_date = datetime.strptime(out_of_stock_since, '%Y-%m-%d')
                if out_date < threshold_date:
                    if delete_product(product_id, sku):
                        deleted_count += 1
                        del existing_products[sku]
            except ValueError:
                print(f"Invalid date format for product {sku}: {out_of_stock_since}")
    
    print(f"Removed {deleted_count} products that were out of stock for 30+ days")

def main():
    print(f"Starting JohnnyVac to Shopify sync at {datetime.now()}")
    print(f"CSV URL: {CSV_URL}")
    
    # Fetch CSV
    print("\nFetching CSV...")
    response = requests.get(CSV_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    
    # Parse CSV with semicolon delimiter
    csv_text = response.content.decode('utf-8')
    lines = StringIO(csv_text)
    reader = csv.DictReader(lines, delimiter=';')
    products = list(reader)
    
    print(f"Found {len(products)} products in CSV")
    
    # Get existing Shopify products
    print("\nFetching existing Shopify products...")
    existing_products = get_all_shopify_products()
    
    # Cleanup old out-of-stock products first
    cleanup_old_out_of_stock(existing_products)
    
    # Process products in batches
    print(f"\nProcessing products in batches of {BATCH_SIZE}...")
    created = 0
    updated = 0
    skipped = 0
    
    for i in range(0, len(products), BATCH_SIZE):
        batch = products[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(products) + BATCH_SIZE - 1) // BATCH_SIZE
        
        print(f"\nProcessing batch {batch_num}/{total_batches}")
        batch_start = time.time()
        
        for row in batch:
            sku = row.get('SKU', '').strip()
            if not sku:
                continue
            
            # Get all available images for this product
            images = get_all_product_images(sku)
            if not images:
                print(f"No images found for {sku}, skipping...")
                skipped += 1
                continue
            
            if sku in existing_products:
                if update_product(existing_products[sku], row, images):
                    updated += 1
                else:
                    skipped += 1
            else:
                if create_product(row, images):
                    created += 1
                else:
                    skipped += 1
            
            time.sleep(RATE_LIMIT_DELAY)
        
        batch_time = time.time() - batch_start
        print(f"Batch {batch_num} completed in {batch_time:.1f}s")
        print(f"Progress: {created} created, {updated} updated, {skipped} skipped")
        
        if i + BATCH_SIZE < len(products):
            print(f"Waiting {BATCH_DELAY}s before next batch...")
            time.sleep(BATCH_DELAY)
    
    print(f"\n=== Sync Complete ===")
    print(f"Created: {created}")
    print(f"Updated: {updated}")
    print(f"Skipped: {skipped}")
    print(f"Total processed: {created + updated + skipped}")
    print(f"Finished at {datetime.now()}")

if __name__ == '__main__':
    main()
