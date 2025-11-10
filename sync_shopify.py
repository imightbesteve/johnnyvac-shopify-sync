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
        response = requests.post(url, headers=self.headers, json=product_data)

        if response.status_code == 201:
            print(f"✓ Created product: {sku} - {title}")
            self.stats['created'] += 1
            return True
        else:
            print(f"✗ Failed to create {sku}: {response.status_code} - {response.text}")
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
        response = requests.put(url, headers=self.headers, json=variant_data)

        if response.status_code == 200:
            print(f"✓ Updated product: {sku} (Price: ${price}, Qty: {quantity})")
            self.stats['updated'] += 1
            return True
        else:
            print(f"✗ Failed to update {sku}: {response.status_code}")
            self.stats['errors'] += 1
            return False

    def sync(self):
        """Main sync function"""
        print("=" * 60)
        print(f"JohnnyVac → Shopify Sync Started: {datetime.now()}")
        print("=" * 60)

        try:
            # Fetch CSV
            csv_products = self.fetch_csv()
            
            # Get existing Shopify products (build lookup once)
            shopify_products = self.get_all_shopify_products()

            # Process each product
            for idx, row in enumerate(csv_products, 1):
                sku = row.get(CSV_COLUMNS['sku'], '').strip()
                
                if not sku:
                    continue

                print(f"\n[{idx}/{len(csv_products)}] Processing SKU: {sku}")

                # Check if product exists
                existing = shopify_products.get(sku)

                if existing:
                    product_id, variant_id = existing
                    self.update_product(product_id, variant_id, row)
                else:
                    self.create_product(row)

            # Print summary
            print("\n" + "=" * 60)
            print("Sync Complete!")
            print(f"Created: {self.stats['created']}")
            print(f"Updated: {self.stats['updated']}")
            print(f"Errors: {self.stats['errors']}")
            print(f"Skipped: {self.stats['skipped']}")
            print("=" * 60)

            return self.stats['errors'] == 0

        except Exception as e:
            print(f"ERROR: {str(e)}")
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
