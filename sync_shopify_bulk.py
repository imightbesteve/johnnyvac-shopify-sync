import csv
import io
import json
import os
import time
import hashlib
import requests
from typing import Dict, List, Tuple

# Configuration
SHOP = os.environ["SHOPIFY_STORE"].replace("https://", "").replace("/", "")
TOKEN = os.environ["SHOPIFY_ACCESS_TOKEN"]
API_VERSION = "2025-01"
CSV_URL = "https://www.johnnyvacstock.com/sigm_all_jv_products/JVWebProducts.csv"

GRAPHQL_URL = f"https://{SHOP}/admin/api/{API_VERSION}/graphql.json"
REST_BASE = f"https://{SHOP}/admin/api/{API_VERSION}"

HEADERS = {
    "X-Shopify-Access-Token": TOKEN,
    "Content-Type": "application/json"
}

LOCATION_ID = "107962957846"

# Load category mapping
try:
    with open("category_map.json", "r") as f:
        CATEGORY_MAP = json.load(f)
except FileNotFoundError:
    print("⚠️  category_map.json not found, using empty category map")
    CATEGORY_MAP = {}

# ---------------- UTILITIES ----------------

def sha1_hash(*values):
    """Create consistent hash from product data"""
    raw = "|".join(str(v).strip() for v in values)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()

def classify_category(text: str) -> str:
    """Match product to category using keyword mapping"""
    t = text.upper()
    for key, cat in CATEGORY_MAP.items():
        if key.upper() in t:
            return cat
    return "Misc / Uncategorized"

def graphql_request(query: str, variables: dict = None) -> dict:
    """Make GraphQL request with error handling"""
    try:
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
            
        response = requests.post(
            GRAPHQL_URL, 
            headers=HEADERS, 
            json=payload,
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        
        # Check for GraphQL errors
        if "errors" in data:
            raise Exception(f"GraphQL errors: {data['errors']}")
            
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"❌ GraphQL request failed: {e}")
        raise

# ---------------- FETCH CSV ----------------

def load_csv() -> List[dict]:
    """Download and parse JohnnyVac CSV feed"""
    print("📥 Downloading CSV from JohnnyVac...")
    
    try:
        response = requests.get(CSV_URL, timeout=60)
        response.raise_for_status()
        
        # Parse CSV with semicolon delimiter
        reader = csv.DictReader(
            io.StringIO(response.text), 
            delimiter=";"
        )
        rows = list(reader)
        
        print(f"✅ Loaded {len(rows)} products from CSV")
        return rows
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to download CSV: {e}")
        raise

# ---------------- FETCH SHOPIFY STATE (WITH PAGINATION) ----------------

def fetch_shopify_products() -> Dict[str, dict]:
    """Fetch all products from Shopify with pagination"""
    print("📥 Fetching existing Shopify products...")
    
    products = {}
    has_next = True
    cursor = None
    page = 1
    
    while has_next:
        query = """
        query($cursor: String) {
          products(first: 250, after: $cursor) {
            pageInfo {
              hasNextPage
              endCursor
            }
            edges {
              node {
                id
                title
                variants(first: 1) {
                  edges {
                    node {
                      id
                      sku
                      price
                      inventoryItem {
                        id
                        inventoryLevels(first: 1) {
                          edges {
                            node {
                              available
                            }
                          }
                        }
                      }
                    }
                  }
                }
                metafield(namespace: "kingsway", key: "source_hash") {
                  value
                }
              }
            }
          }
        }
        """
        
        try:
            data = graphql_request(query, {"cursor": cursor})
            products_data = data["data"]["products"]
            
            # Process products
            for edge in products_data["edges"]:
                node = edge["node"]
                
                # Skip products without variants
                if not node["variants"]["edges"]:
                    continue
                    
                variant = node["variants"]["edges"][0]["node"]
                sku = variant["sku"]
                
                if not sku:
                    continue
                
                # Get inventory if available
                inventory = 0
                if variant["inventoryItem"]["inventoryLevels"]["edges"]:
                    inventory = variant["inventoryItem"]["inventoryLevels"]["edges"][0]["node"]["available"]
                
                products[sku] = {
                    "product_id": node["id"],
                    "variant_id": variant["id"],
                    "inventory_item_id": variant["inventoryItem"]["id"],
                    "inventory": inventory,
                    "price": float(variant["price"]),
                    "source_hash": node["metafield"]["value"] if node["metafield"] else None
                }
            
            has_next = products_data["pageInfo"]["hasNextPage"]
            cursor = products_data["pageInfo"]["endCursor"]
            
            print(f"  Page {page}: {len(products_data['edges'])} products (total: {len(products)})")
            page += 1
            
            # Rate limit protection
            time.sleep(0.5)
            
        except Exception as e:
            print(f"❌ Error fetching products page {page}: {e}")
            raise
    
    print(f"✅ Fetched {len(products)} existing products from Shopify")
    return products

# ---------------- BUILD BULK JSONL ----------------

def build_bulk_operations(jv_rows: List[dict], shopify_products: Dict[str, dict]) -> Tuple[List[dict], List[dict]]:
    """Build operations for products that need creating/updating"""
    print("🔨 Building bulk operations...")
    
    creates = []
    updates = []
    inventory_updates = []
    skipped = 0
    
    for row in jv_rows:
        sku = row.get("SKU", "").strip()
        if not sku:
            continue
        
        try:
            # Build product data
            title_en = row.get("ProductTitleEN", "").strip()
            title_fr = row.get("ProductTitleFR", "").strip()
            title = f"{title_en} / {title_fr}" if title_en and title_fr else (title_en or title_fr or sku)
            
            category_text = " ".join([
                row.get("ProductCategory", ""),
                title_en,
                title_fr,
                row.get("ProductDescriptionEN", "")[:100],
                row.get("ProductDescriptionFR", "")[:100]
            ])
            category = classify_category(category_text)
            
            # Parse price and inventory
            price_str = str(row.get("RegularPrice", "0")).replace(",", ".")
            price = float(price_str) if price_str else 0.0
            
            inv_str = str(row.get("Inventory", "0")).replace(",", ".")
            qty = int(float(inv_str)) if inv_str else 0
            
            # Image URL
            image_url = row.get("ImageUrl", "").strip()
            if not image_url:
                image_url = f"https://www.johnnyvacstock.com/photos/web/{sku}.jpg"
            
            # Calculate source hash
            source_hash = sha1_hash(price, qty, image_url, category, title)
            
            # Check if product exists
            if sku in shopify_products:
                existing = shopify_products[sku]
                
                # Skip if unchanged
                if existing["source_hash"] == source_hash:
                    skipped += 1
                    continue
                
                # Update existing product
                updates.append({
                    "input": {
                        "id": existing["product_id"],
                        "productType": category,
                        "variants": [{
                            "id": existing["variant_id"],
                            "price": str(price)
                        }],
                        "metafields": [{
                            "namespace": "kingsway",
                            "key": "source_hash",
                            "type": "single_line_text_field",
                            "value": source_hash
                        }]
                    }
                })
                
                # Queue inventory update if changed
                if existing["inventory"] != qty:
                    inventory_updates.append({
                        "inventory_item_id": existing["inventory_item_id"],
                        "available": qty,
                        "sku": sku
                    })
            else:
                # Create new product
                creates.append({
                    "input": {
                        "title": title,
                        "productType": category,
                        "vendor": "JohnnyVac",
                        "status": "ACTIVE",
                        "variants": [{
                            "sku": sku,
                            "price": str(price),
                            "inventoryManagement": "SHOPIFY",
                            "inventoryPolicy": "DENY"
                        }],
                        "images": [{
                            "src": image_url
                        }],
                        "metafields": [{
                            "namespace": "kingsway",
                            "key": "source_hash",
                            "type": "single_line_text_field",
                            "value": source_hash
                        }]
                    }
                })
                
        except Exception as e:
            print(f"⚠️  Error processing SKU {sku}: {e}")
            continue
    
    print(f"✅ Operations: {len(creates)} creates, {len(updates)} updates, {skipped} skipped")
    print(f"📦 Inventory updates queued: {len(inventory_updates)}")
    
    return creates + updates, inventory_updates

# ---------------- RUN BULK OPERATION ----------------

def run_bulk_operation(operations: List[dict]) -> bool:
    """Execute bulk mutation and wait for completion"""
    if not operations:
        print("ℹ️  No operations to run")
        return True
    
    print(f"🚀 Starting bulk operation with {len(operations)} mutations...")
    
    try:
        # Step 1: Create staged upload
        print("  1/4 Creating staged upload...")
        stage_mutation = """
        mutation {
          stagedUploadsCreate(
            input: {
              resource: BULK_MUTATION_VARIABLES,
              filename: "bulk_operation.jsonl",
              mimeType: "text/jsonl",
              httpMethod: PUT
            }
          ) {
            userErrors {
              field
              message
            }
            stagedTargets {
              url
              resourceUrl
              parameters {
                name
                value
              }
            }
          }
        }
        """
        
        stage_result = graphql_request(stage_mutation)
        
        if stage_result["data"]["stagedUploadsCreate"]["userErrors"]:
            errors = stage_result["data"]["stagedUploadsCreate"]["userErrors"]
            raise Exception(f"Staging errors: {errors}")
        
        staged_target = stage_result["data"]["stagedUploadsCreate"]["stagedTargets"][0]
        upload_url = staged_target["url"]
        resource_url = staged_target["resourceUrl"]
        
        # Step 2: Create JSONL file
        print("  2/4 Uploading operations...")
        jsonl_content = "\n".join(json.dumps(op) for op in operations)
        
        upload_response = requests.put(
            upload_url,
            data=jsonl_content.encode('utf-8'),
            headers={"Content-Type": "text/jsonl"},
            timeout=120
        )
        upload_response.raise_for_status()
        
        # Step 3: Start bulk operation
        print("  3/4 Starting bulk mutation...")
        bulk_mutation = f"""
        mutation {{
          bulkOperationRunMutation(
            mutation: "mutation call($input: ProductInput!) {{ productSet(input: $input) {{ product {{ id }} userErrors {{ message field }} }} }}",
            stagedUploadPath: "{resource_url}"
          ) {{
            bulkOperation {{
              id
              status
            }}
            userErrors {{
              field
              message
            }}
          }}
        }}
        """
        
        bulk_result = graphql_request(bulk_mutation)
        
        if bulk_result["data"]["bulkOperationRunMutation"]["userErrors"]:
            errors = bulk_result["data"]["bulkOperationRunMutation"]["userErrors"]
            raise Exception(f"Bulk operation errors: {errors}")
        
        operation_id = bulk_result["data"]["bulkOperationRunMutation"]["bulkOperation"]["id"]
        print(f"  Operation ID: {operation_id}")
        
        # Step 4: Poll for completion
        print("  4/4 Waiting for completion...")
        return poll_bulk_operation(operation_id)
        
    except Exception as e:
        print(f"❌ Bulk operation failed: {e}")
        return False

def poll_bulk_operation(operation_id: str, max_wait: int = 1800) -> bool:
    """Poll bulk operation status until complete"""
    start_time = time.time()
    
    while True:
        # Check timeout
        if time.time() - start_time > max_wait:
            print(f"⏱️  Timeout waiting for bulk operation")
            return False
        
        query = f"""
        {{
          node(id: "{operation_id}") {{
            ... on BulkOperation {{
              id
              status
              errorCode
              createdAt
              completedAt
              objectCount
              fileSize
              url
            }}
          }}
        }}
        """
        
        try:
            result = graphql_request(query)
            operation = result["data"]["node"]
            
            status = operation["status"]
            count = operation.get("objectCount", 0)
            
            if status == "COMPLETED":
                print(f"✅ Bulk operation completed: {count} objects processed")
                return True
                
            elif status in ["FAILED", "CANCELED"]:
                error_code = operation.get("errorCode", "UNKNOWN")
                print(f"❌ Bulk operation {status.lower()}: {error_code}")
                return False
                
            elif status in ["RUNNING", "CREATED"]:
                elapsed = int(time.time() - start_time)
                print(f"  ⏳ Status: {status}, Objects: {count}, Elapsed: {elapsed}s")
                time.sleep(10)
                
            else:
                print(f"  ⏳ Status: {status}")
                time.sleep(5)
                
        except Exception as e:
            print(f"⚠️  Error polling status: {e}")
            time.sleep(5)

# ---------------- UPDATE INVENTORY ----------------

def update_inventory(updates: List[dict]) -> None:
    """Update inventory levels for existing products"""
    if not updates:
        print("ℹ️  No inventory updates needed")
        return
    
    print(f"📦 Updating inventory for {len(updates)} products...")
    
    success = 0
    failed = 0
    
    for update in updates:
        try:
            response = requests.post(
                f"{REST_BASE}/inventory_levels/set.json",
                headers=HEADERS,
                json={
                    "location_id": LOCATION_ID,
                    "inventory_item_id": update["inventory_item_id"],
                    "available": update["available"]
                },
                timeout=10
            )
            response.raise_for_status()
            success += 1
            
            # Rate limiting
            time.sleep(0.3)
            
        except Exception as e:
            failed += 1
            print(f"  ⚠️  Failed to update inventory for {update.get('sku', 'unknown')}: {e}")
    
    print(f"✅ Inventory updates: {success} successful, {failed} failed")

# ---------------- MAIN ----------------

def main():
    """Main sync process"""
    print("=" * 60)
    print("JohnnyVac → Shopify Sync (GraphQL)")
    print("=" * 60)
    
    try:
        # Step 1: Load CSV
        jv_rows = load_csv()
        
        # Step 2: Fetch Shopify state
        shopify_products = fetch_shopify_products()
        
        # Step 3: Build operations
        operations, inventory_updates = build_bulk_operations(jv_rows, shopify_products)
        
        # Step 4: Run bulk operation
        if operations:
            success = run_bulk_operation(operations)
            if not success:
                print("⚠️  Bulk operation did not complete successfully")
                return
        
        # Step 5: Update inventory (only for existing products)
        if inventory_updates:
            update_inventory(inventory_updates)
        
        print("=" * 60)
        print("✅ Sync completed successfully")
        print("=" * 60)
        
    except Exception as e:
        print("=" * 60)
        print(f"❌ Sync failed: {e}")
        print("=" * 60)
        raise

if __name__ == "__main__":
    main()
