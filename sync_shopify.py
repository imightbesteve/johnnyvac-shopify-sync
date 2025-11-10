import os
import csv
import requests
from datetime import datetime
import json
import time

# Configuration
JOHNNYVAC_CSV_URL = "https://www.johnnyvacstock.com/sigm_all_jv_products/JVWebProducts.csv"
JOHNNYVAC_IMAGE_BASE = "https://www.johnnyvacstock.com/photos/web/"
SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE')  # e.g., 'your-store.myshopify.com'
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')

# Batch Processing Settings
BATCH_SIZE = 100  # Process 100 products at a time
RATE_LIMIT_DELAY = 0.5  # Wait 0.5 seconds between API calls (2 requests/sec)
BATCH_DELAY = 5  # Wait 5 seconds between batches

# CSV Column Mapping - JohnnyVac CSV Structure
CSV_COLUMNS = {
    'sku': 'SKU',
    'title_en': 'ProductTitleEN',
    'title_fr': 'ProductTitleFR',
    'description_en': 'ProductDescriptionEN',
    'description_fr': 'ProductDescriptionFR',
    'price': 'RegularPrice',
    'quantity': 'Inventory',
    'barcode': 'upc',
    'weight': 'weight',
    'image_url': 'ImageUrl',
    'product_type': 'ProductCategory',
    'associated_skus': 'AssociatedSkus'
}

# Language setting - change to 'fr' for French
LANGUAGE = 'en'  # Options: 'en' or 'fr'

class ShopifySync:
    def __init__(self):
        self.store = SHOPIFY_STORE
        self.token = SHOPIFY_ACCESS_TOKEN
        self.api_version = '2024-10'
        self.base_url = f"https://{self.store}/admin/api/{self.api_version}"
        self.headers = {
            'X-Shopify-Access-Token': self.token,
            'Content-Type': 'application/json'
        }
        self.stats = {
            'created': 0,
            'updated': 0,
            'errors': 0,
            'skipped': 0
        }

    def fetch_csv(self):
        """Fetch CSV from JohnnyVac"""
        print(f"Fetching CSV from {JOHNNYVAC_CSV_URL}...")
        response = requests.get(JOHNNYVAC_CSV_URL)
        response.raise_for_status()
        
        # Parse CSV
        lines = response.text.splitlines()
        reader = csv.DictReader(lines)
        products = list(reader)
        print(f"Found {len(products)} products in CSV")
        return products

    def test_connection(self):
        """Test Shopify API connection"""
        print("Testing Shopify API connection...")
        url = f"{self.base_url}/shop.json"
        response = requests.get(url, headers=self.headers)
        
        if response.status_code == 200:
            shop_data = response.json().get('shop', {})
            print(f"✓ Connected to: {shop_data.get('name', 'Unknown')}")
            print(f"  Store: {shop_data.get('domain', 'Unknown')}")
            return True
        else:
            print(f"✗ Connection failed: {response.status_code}")
            print(f"  Error: {response.text}")
            if response.status_code == 401:
                print("\n⚠️  Authentication Error!")
                print("  Check that:")
                print("  1. SHOPIFY_STORE is correct (e.g., 'your-store.myshopify.com')")
                print("  2. SHOPIFY_ACCESS_TOKEN is valid and not expired")
                print("  3. The app has proper API permissions")
            return False

    def get_all_shopify_products(self):
        """Get all existing products from Shopify and build SKU lookup"""
        print("Fetching existing Shopify products...")
        sku_lookup = {}
        url = f"{self.base_url}/products.json"
        params = {'fields': 'id,variants', 'limit': 250}
        
        while url:
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code != 200:
                print(f"Warning: Could not fetch products: {response.status_code}")
                break
                
            products = response.json().get('products', [])
            
            for product in products:
                for variant in product.get('variants', []):
                    if variant.get('sku'):
                        sku_lookup[variant['sku']] = (product['id'], variant['id'])
            
            # Handle pagination
            link_header = response.headers.get('Link', '')
            if 'rel="next"' in link_header:
                # Extract next URL from Link header
                next_link = [l for l in link_header.split(',') if 'rel="next"' in l]
                if next_link:
                    url = next_link[0].split(';')[0].strip('<> ')
                else:
                    url = None
            else:
                url = None
        
        print(f"Found {len(sku_lookup)} existing products in Shopify")
        return sku_lookup
        """Get all existing products from Shopify and build SKU lookup"""
        print("Fetching existing Shopify products...")
        sku_lookup = {}
        url = f"{self.base_url}/products.json"
        params = {'fields': 'id,variants', 'limit': 250}
        
        while url:
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code != 200:
                print(f"Warning: Could not fetch products: {response.status_code}")
                break
                
            products = response.json().get('products', [])
            
            for product in products:
                for variant in product.get('variants', []):
                    if variant.get('sku'):
                        sku_lookup[variant['sku']] = (product['id'], variant['id'])
            
            # Handle pagination
            link_header = response.headers.get('Link', '')
            if 'rel="next"' in link_header:
                # Extract next URL from Link header
                next_link = [l for l in link_header.split(',') if 'rel="next"' in l]
                if next_link:
                    url = next_link[0].split(';')[0].strip('<> ')
                else:
                    url = None
            else:
                url = None
        
        print(f"Found {len(sku_lookup)} existing products in Shopify")
        return sku_lookup

    def create_product(self, csv_row):
        """Create new product in Shopify"""
        sku = csv_row.get(CSV_COLUMNS['sku'], '').strip()
        
        if not sku:
            print("Skipping product with no SKU")
            self.stats['skipped'] += 1
            return False

        # Use language setting for title/description
        title_key = 'title_en' if LANGUAGE == 'en' else 'title_fr'
        desc_key = 'description_en' if LANGUAGE == 'en' else 'description_fr'
        
        title = csv_row.get(CSV_COLUMNS[title_key], sku)
        description = csv_row.get(CSV_COLUMNS[desc_key], '')
        price = csv_row.get(CSV_COLUMNS['price'], '0.00')
        
        # Handle quantity - convert to int, default to 0
        try:
            quantity = int(float(csv_row.get(CSV_COLUMNS['quantity'], 0)))
        except (ValueError, TypeError):
            quantity = 0
            
        vendor = 'JohnnyVac'
        product_type = csv_row.get(CSV_COLUMNS.get('product_type', ''), '')
        barcode = csv_row.get(CSV_COLUMNS.get('barcode', ''), '')
        weight = csv_row.get(CSV_COLUMNS.get('weight', ''), '')

        # Use ImageUrl from CSV, or build from SKU
        image_url = csv_row.get(CSV_COLUMNS.get('image_url', ''), '').strip()
        if not image_url:
            image_url = f"{JOHNNYVAC_IMAGE_BASE}{sku}.jpg"

        product_data = {
            "product": {
                "title": title,
                "body_html": description,
                "vendor": vendor,
                "product_type": product_type,
                "variants": [{
                    "sku": sku,
                    "price": price,
                    "inventory_quantity": quantity,
                    "inventory_management": "shopify",
                    "barcode": barcode,
                    "weight": float(weight) if weight else None,
                    "weight_unit": "kg"
                }],
                "images": [{
                    "src": image_url
                }] if image_url else []
            }
        }

        url = f"{self.base_url}/products.json"
        
        # Rate limiting
        time.sleep(RATE_LIMIT_DELAY)
        
        response = requests.post(url, headers=self.headers, json=product_data)

        if response.status_code == 201:
            print(f"✓ Created: {sku} - {title[:50]}")
            self.stats['created'] += 1
            return True
        elif response.status_code == 429:
            # Rate limited - wait and retry once
            print(f"⚠ Rate limited, waiting 2 seconds...")
            time.sleep(2)
            response = requests.post(url, headers=self.headers, json=product_data)
            if response.status_code == 201:
                print(f"✓ Created: {sku} - {title[:50]}")
                self.stats['created'] += 1
                return True
        
        print(f"✗ Failed to create {sku}: {response.status_code} - {response.text[:100]}")
        self.stats['errors'] += 1
        return False

    def update_product(self, product_id, variant_id, csv_row):
        """Update existing product in Shopify"""
        sku = csv_row.get(CSV_COLUMNS['sku'], '').strip()
        price = csv_row.get(CSV_COLUMNS['price'], '0.00')
        
        # Handle quantity - convert to int, default to 0
        try:
            quantity = int(float(csv_row.get(CSV_COLUMNS['quantity'], 0)))
        except (ValueError, TypeError):
            quantity = 0

        # Update variant (price, quantity)
        variant_data = {
            "variant": {
                "id": variant_id,
                "price": price,
                "inventory_quantity": quantity
            }
        }

        url = f"{self.base_url}/variants/{variant_id}.json"
        
        # Rate limiting
        time.sleep(RATE_LIMIT_DELAY)
        
        response = requests.put(url, headers=self.headers, json=variant_data)

        if response.status_code == 200:
            print(f"✓ Updated: {sku} (${price}, Qty: {quantity})")
            self.stats['updated'] += 1
            return True
        elif response.status_code == 429:
            # Rate limited - wait and retry once
            print(f"⚠ Rate limited, waiting 2 seconds...")
            time.sleep(2)
            response = requests.put(url, headers=self.headers, json=variant_data)
            if response.status_code == 200:
                print(f"✓ Updated: {sku} (${price}, Qty: {quantity})")
                self.stats['updated'] += 1
                return True
        
        print(f"✗ Failed to update {sku}: {response.status_code}")
        self.stats['errors'] += 1
        return False

    def sync(self):
        """Main sync function with batch processing"""
        print("=" * 60)
        print(f"JohnnyVac → Shopify Sync Started: {datetime.now()}")
        print("=" * 60)

        try:
            # Test connection first
            if not self.test_connection():
                print("\n❌ Cannot proceed without valid Shopify connection")
                return False
            
            print()
            
            # Fetch CSV
            csv_products = self.fetch_csv()
            total_products = len(csv_products)
            
            # Get existing Shopify products (build lookup once)
            shopify_products = self.get_all_shopify_products()

            # Calculate batches
            num_batches = (total_products + BATCH_SIZE - 1) // BATCH_SIZE
            print(f"\nProcessing {total_products} products in {num_batches} batches of {BATCH_SIZE}")
            print("=" * 60)

            # Process in batches
            for batch_num in range(num_batches):
                start_idx = batch_num * BATCH_SIZE
                end_idx = min(start_idx + BATCH_SIZE, total_products)
                batch = csv_products[start_idx:end_idx]
                
                print(f"\n{'='*60}")
                print(f"BATCH {batch_num + 1}/{num_batches} - Products {start_idx + 1} to {end_idx}")
                print(f"{'='*60}")
                
                batch_start_time = time.time()
                
                # Process each product in batch
                for idx, row in enumerate(batch, start_idx + 1):
                    sku = row.get(CSV_COLUMNS['sku'], '').strip()
                    
                    if not sku:
                        continue

                    print(f"[{idx}/{total_products}] {sku}...", end=" ")

                    # Check if product exists
                    existing = shopify_products.get(sku)

                    if existing:
                        product_id, variant_id = existing
                        self.update_product(product_id, variant_id, row)
                    else:
                        self.create_product(row)
                
                batch_time = time.time() - batch_start_time
                
                # Batch summary
                print(f"\nBatch {batch_num + 1} complete in {batch_time:.1f}s")
                print(f"Progress: Created={self.stats['created']}, Updated={self.stats['updated']}, Errors={self.stats['errors']}")
                
                # Wait between batches (except for last batch)
                if batch_num < num_batches - 1:
                    print(f"Waiting {BATCH_DELAY} seconds before next batch...")
                    time.sleep(BATCH_DELAY)

            # Print final summary
            print("\n" + "=" * 60)
            print("🎉 SYNC COMPLETE!")
            print("=" * 60)
            print(f"Total Products Processed: {total_products}")
            print(f"✓ Created: {self.stats['created']}")
            print(f"✓ Updated: {self.stats['updated']}")
            print(f"✗ Errors: {self.stats['errors']}")
            print(f"⊘ Skipped: {self.stats['skipped']}")
            print("=" * 60)

            return self.stats['errors'] == 0

        except Exception as e:
            print(f"\n❌ FATAL ERROR: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

if __name__ == "__main__":
    # Validate environment variables
    if not SHOPIFY_STORE or not SHOPIFY_ACCESS_TOKEN:
        print("ERROR: Missing required environment variables:")
        print("- SHOPIFY_STORE")
        print("- SHOPIFY_ACCESS_TOKEN")
        exit(1)

    syncer = ShopifySync()
    success = syncer.sync()
    exit(0 if success else 1)
