#!/usr/bin/env python3
"""
JohnnyVac to Shopify - Automated Collection Creator v2.1

Creates smart collections based on category_map_v4.json taxonomy.
Stateless, CI-compatible, fully idempotent.

FIXED: Better error handling to show actual GraphQL errors

Environment Variables:
    SHOPIFY_STORE: Store URL (e.g., kingsway-janitorial.myshopify.com)
    SHOPIFY_ACCESS_TOKEN: Admin API access token (shpat_...)
    AUTO_PUBLISH: Set to 'true' to auto-publish collections (optional)
"""

import os
import sys
import json
import requests
import time
from typing import Dict, List, Optional
from datetime import datetime

# Configuration
SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', '')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN', '')
AUTO_PUBLISH = os.environ.get('AUTO_PUBLISH', 'false').lower() == 'true'
CATEGORY_MAP_FILE = 'category_map_v4.json'

API_VERSION = '2026-01'
GRAPHQL_URL = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/graphql.json"
REST_BASE_URL = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}"

HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN
}

RATE_LIMIT_DELAY = 0.5


def log(msg: str):
    """Simple logging with timestamp"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def load_category_map() -> List[Dict]:
    """Load category taxonomy from category_map_v4.json"""
    try:
        with open(CATEGORY_MAP_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            categories = data.get('categories', [])
            
            # Filter out fallback categories (priority <= 10)
            active_categories = [
                c for c in categories 
                if c.get('priority', 0) > 10
            ]
            
            log(f"✅ Loaded {len(active_categories)} active categories from {CATEGORY_MAP_FILE}")
            return active_categories
    except FileNotFoundError:
        log(f"❌ Error: {CATEGORY_MAP_FILE} not found")
        sys.exit(1)
    except json.JSONDecodeError as e:
        log(f"❌ Error parsing {CATEGORY_MAP_FILE}: {e}")
        sys.exit(1)


def get_product_counts_by_type() -> Dict[str, int]:
    """Query Shopify to get product counts per productType"""
    log("📊 Fetching product counts from Shopify...")
    
    query = '''
    query ($cursor: String) {
      products(first: 250, after: $cursor) {
        pageInfo {
          hasNextPage
          endCursor
        }
        edges {
          node {
            productType
          }
        }
      }
    }
    '''
    
    product_types = []
    cursor = None
    has_next = True
    
    while has_next:
        try:
            response = requests.post(
                GRAPHQL_URL,
                headers=HEADERS,
                json={"query": query, "variables": {"cursor": cursor}},
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            
            # Check for GraphQL errors
            if 'errors' in data:
                log(f"⚠️  GraphQL errors: {data['errors']}")
            
            if 'data' not in data:
                log(f"❌ No data in response: {data}")
                break
            
            edges = data['data']['products']['edges']
            page_info = data['data']['products']['pageInfo']
            
            for edge in edges:
                product_type = edge['node'].get('productType', '')
                if product_type:
                    product_types.append(product_type)
            
            has_next = page_info['hasNextPage']
            cursor = page_info['endCursor']
            
            time.sleep(RATE_LIMIT_DELAY)
            
        except Exception as e:
            log(f"❌ Error fetching products: {e}")
            break
    
    # Count occurrences
    counts = {}
    for pt in product_types:
        counts[pt] = counts.get(pt, 0) + 1
    
    log(f"✅ Found {len(product_types)} products across {len(counts)} productTypes")
    return counts


def collection_exists_by_handle(handle: str) -> Optional[Dict]:
    """Check if a collection already exists by handle"""
    query = '''
    query ($handle: String!) {
      collectionByHandle(handle: $handle) {
        id
        handle
        title
        productsCount {
          count
        }
      }
    }
    '''
    
    try:
        response = requests.post(
            GRAPHQL_URL,
            headers=HEADERS,
            json={"query": query, "variables": {"handle": handle}},
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        
        if 'errors' in data:
            log(f"⚠️  Check collection errors: {data['errors']}")
            
        if 'data' not in data:
            return None
            
        collection = data.get("data", {}).get("collectionByHandle")
        if collection:
            # Normalize productsCount
            products_count = collection.get('productsCount', {})
            if isinstance(products_count, dict):
                collection['productsCount'] = products_count.get('count', 0)
        return collection
    except Exception as e:
        log(f"⚠️  Error checking collection '{handle}': {e}")
        return None


def create_automated_collection(
    title: str, 
    handle: str, 
    product_type: str, 
    description: str = None
) -> Optional[Dict]:
    """Create an automated collection with productType rule"""
    
    # Create a nice description
    if not description:
        parts = product_type.split(' > ')
        if len(parts) > 1:
            description = f"<p>Browse our selection of {parts[-1].lower()}.</p>"
        else:
            description = f"<p>All products in the {title} category.</p>"
    
    mutation = '''
    mutation CollectionCreate($input: CollectionInput!) {
      collectionCreate(input: $input) {
        userErrors {
          field
          message
        }
        collection {
          id
          title
          handle
          ruleSet {
            appliedDisjunctively
            rules {
              column
              relation
              condition
            }
          }
        }
      }
    }
    '''
    
    variables = {
        "input": {
            "title": title,
            "handle": handle,
            "descriptionHtml": description,
            "ruleSet": {
                "appliedDisjunctively": False,
                "rules": [
                    {
                        "column": "PRODUCT_TYPE",
                        "relation": "EQUALS",
                        "condition": product_type
                    }
                ]
            }
        }
    }
    
    try:
        response = requests.post(
            GRAPHQL_URL,
            headers=HEADERS,
            json={"query": mutation, "variables": variables},
            timeout=30
        )
        response.raise_for_status()
        payload = response.json()
        
        # DEBUG: Print full response if there's an issue
        if 'errors' in payload:
            log(f"  ❌ GraphQL errors: {payload['errors']}")
            return None
        
        if 'data' not in payload:
            log(f"  ❌ No 'data' in response. Full response: {json.dumps(payload, indent=2)}")
            return None
        
        collection_create = payload.get("data", {}).get("collectionCreate")
        if not collection_create:
            log(f"  ❌ No 'collectionCreate' in response data")
            return None
        
        errors = collection_create.get("userErrors", [])
        if errors:
            log(f"  ❌ Collection create errors: {errors}")
            return None
        
        return collection_create.get("collection")
        
    except requests.exceptions.HTTPError as e:
        log(f"  ❌ HTTP Error creating collection '{title}': {e}")
        log(f"     Response: {e.response.text if e.response else 'No response'}")
        return None
    except Exception as e:
        log(f"  ❌ Error creating collection '{title}': {e}")
        return None


def publish_collection(collection_gid: str) -> bool:
    """Publish collection to Online Store using publishablePublish mutation"""
    
    mutation = '''
    mutation publishablePublish($id: ID!, $input: [PublicationInput!]!) {
      publishablePublish(id: $id, input: $input) {
        publishable {
          availablePublicationsCount {
            count
          }
        }
        userErrors {
          field
          message
        }
      }
    }
    '''
    
    # First, get the Online Store publication ID
    pub_query = '''
    query {
      publications(first: 10) {
        edges {
          node {
            id
            name
          }
        }
      }
    }
    '''
    
    try:
        # Get publications
        response = requests.post(
            GRAPHQL_URL,
            headers=HEADERS,
            json={"query": pub_query},
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        
        if 'errors' in data:
            log(f"  ⚠️  Error getting publications: {data['errors']}")
            return False
        
        publications = data.get('data', {}).get('publications', {}).get('edges', [])
        
        # Find Online Store publication
        online_store_pub = None
        for pub in publications:
            name = pub['node'].get('name', '').lower()
            if 'online store' in name or 'online_store' in name:
                online_store_pub = pub['node']['id']
                break
        
        if not online_store_pub:
            # Just use the first publication if we can't find Online Store
            if publications:
                online_store_pub = publications[0]['node']['id']
            else:
                log(f"  ⚠️  No publications found")
                return False
        
        # Publish the collection
        variables = {
            "id": collection_gid,
            "input": [{"publicationId": online_store_pub}]
        }
        
        response = requests.post(
            GRAPHQL_URL,
            headers=HEADERS,
            json={"query": mutation, "variables": variables},
            timeout=30
        )
        response.raise_for_status()
        result = response.json()
        
        if 'errors' in result:
            log(f"  ⚠️  Publish errors: {result['errors']}")
            return False
        
        user_errors = result.get('data', {}).get('publishablePublish', {}).get('userErrors', [])
        if user_errors:
            log(f"  ⚠️  Publish user errors: {user_errors}")
            return False
        
        return True
        
    except Exception as e:
        log(f"  ⚠️  Error publishing collection: {e}")
        return False


def create_collections(categories: List[Dict], product_counts: Dict[str, int]):
    """Create collections for categories meeting minimum thresholds"""
    log("")
    log("=" * 70)
    log("Creating Collections")
    log("=" * 70)
    log("")
    
    created = 0
    skipped_exists = 0
    skipped_threshold = 0
    failed = 0
    
    for category in categories:
        product_type = category['productType']
        handle = category['handle']
        title = category.get('title', product_type.split(' > ')[-1])
        min_products = category.get('min_products', 1)
        
        # Get actual product count
        actual_count = product_counts.get(product_type, 0)
        
        log(f"📦 {product_type}")
        log(f"   Handle: {handle}")
        log(f"   Products: {actual_count} (min: {min_products})")
        
        # Skip if below threshold
        if actual_count < min_products:
            log(f"   ⏭️  Skipped (below threshold)")
            skipped_threshold += 1
            continue
        
        # Check if already exists
        existing = collection_exists_by_handle(handle)
        if existing:
            count = existing.get('productsCount', 0)
            log(f"   ✅ Already exists ({count} products)")
            skipped_exists += 1
            time.sleep(RATE_LIMIT_DELAY)
            continue
        
        # Create collection
        log(f"   🔨 Creating...")
        collection = create_automated_collection(title, handle, product_type)
        
        if collection:
            created += 1
            log(f"   ✅ Created: {collection['handle']}")
            
            # Publish if AUTO_PUBLISH is set
            if AUTO_PUBLISH:
                log(f"   📢 Publishing...")
                if publish_collection(collection['id']):
                    log(f"   ✅ Published")
                else:
                    log(f"   ⚠️  Created but not published")
        else:
            failed += 1
            log(f"   ❌ Failed")
        
        # Rate limiting
        time.sleep(RATE_LIMIT_DELAY)
    
    # Summary
    log("")
    log("=" * 70)
    log("SUMMARY")
    log("=" * 70)
    log(f"✅ Created:             {created}")
    log(f"⏭️  Skipped (exists):    {skipped_exists}")
    log(f"⏭️  Skipped (threshold): {skipped_threshold}")
    log(f"❌ Failed:              {failed}")
    log(f"📊 Total categories:    {len(categories)}")
    log("=" * 70)
    
    if created > 0 and not AUTO_PUBLISH:
        log("")
        log("ℹ️  Collections created but not published.")
        log("   Set AUTO_PUBLISH=true to auto-publish in future runs.")


def main():
    log("=" * 70)
    log("JohnnyVac Automated Collection Creator v2.1")
    log("=" * 70)
    log("")
    
    # Validate environment
    if not SHOPIFY_STORE or not SHOPIFY_ACCESS_TOKEN:
        log("❌ Error: SHOPIFY_STORE and SHOPIFY_ACCESS_TOKEN must be set")
        sys.exit(1)
    
    log(f"Store: {SHOPIFY_STORE}")
    log(f"API Version: {API_VERSION}")
    
    if AUTO_PUBLISH:
        log("📢 AUTO_PUBLISH enabled - collections will be published")
    else:
        log("ℹ️  AUTO_PUBLISH disabled - collections will be created unpublished")
    log("")
    
    # Test connection first
    log("Testing API connection...")
    test_query = '{ shop { name } }'
    try:
        response = requests.post(
            GRAPHQL_URL,
            headers=HEADERS,
            json={"query": test_query},
            timeout=10
        )
        response.raise_for_status()
        result = response.json()
        
        if 'errors' in result:
            log(f"❌ API Error: {result['errors']}")
            sys.exit(1)
        
        shop_name = result.get('data', {}).get('shop', {}).get('name', 'Unknown')
        log(f"✅ Connected to: {shop_name}")
    except Exception as e:
        log(f"❌ Connection failed: {e}")
        sys.exit(1)
    
    log("")
    
    # Load category taxonomy
    categories = load_category_map()
    
    # Get product counts from Shopify
    product_counts = get_product_counts_by_type()
    
    # Create collections
    create_collections(categories, product_counts)
    
    log("")
    log("✅ Collection creation complete!")


if __name__ == "__main__":
    main()
