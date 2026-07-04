#!/usr/bin/env python3
"""
JohnnyVac to Shopify - Automated Collection Creator v3.0

Creates smart collections based on category_map_v4.json taxonomy, plus
"Shop by Brand" collections for every detected vendor with enough products.
Stateless, CI-compatible, fully idempotent.

v3.0 CHANGES:
  - Brand collections (vendor-rule smart collections, min 5 products)
  - SEO title/description set on every created collection
  - Existing collections with missing/thin descriptions get backfilled
    (manually written collection descriptions are never touched)

Environment Variables:
    SHOPIFY_STORE: Store URL (e.g., kingsway-janitorial.myshopify.com)
    SHOPIFY_CLIENT_ID / SHOPIFY_CLIENT_SECRET: preferred (runtime-minted token)
    SHOPIFY_ACCESS_TOKEN: static token fallback
    AUTO_PUBLISH: Set to 'true' to auto-publish collections (optional)
"""

import os
import re
import sys
import json
import requests
import time
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from shopify_auth import get_access_token

# Configuration
SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', '')
SHOPIFY_ACCESS_TOKEN = get_access_token() or ''
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

# A vendor needs at least this many products to get a brand collection
BRAND_MIN_PRODUCTS = 5

# Existing collections with fewer text characters than this get a generated
# description (anything longer is assumed to be hand-written and is kept)
MIN_COLLECTION_DESCRIPTION = 40


def log(msg: str):
    """Simple logging with timestamp"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def strip_html(html_str: str) -> str:
    return re.sub(r'<[^>]+>', '', html_str or '').strip()


def category_description(title: str, product_type: str) -> str:
    """Storefront description for a category collection."""
    leaf = product_type.split(' > ')[-1].strip() or title
    return (
        f"<p>Browse our complete selection of {leaf.lower()}. "
        f"We carry options for commercial and residential use from trusted "
        f"brands, with new stock added as it arrives. Every product page "
        f"lists the part number (MPN) and compatible models where available, "
        f"so you can be sure you're ordering the right fit.</p>"
    )


def brand_description(vendor: str, count: int) -> str:
    """Storefront description for a brand collection."""
    return (
        f"<p>Shop all {vendor} products in one place — vacuum bags, filters, "
        f"belts, parts and accessories. Every listing includes the "
        f"manufacturer part number so you can match it to your machine.</p>"
    )


def collection_seo(title: str, kind: str = 'category') -> Dict:
    if kind == 'brand':
        seo_title = f"{title} Vacuum Parts & Supplies"
        seo_desc = (f"Shop genuine and compatible {title} vacuum bags, filters, "
                    f"belts, parts and accessories. Find the right fit by part "
                    f"number and model.")
    else:
        seo_title = f"{title} — Janitorial & Vacuum Supplies"
        seo_desc = (f"Browse {title.lower()} for commercial and home use. "
                    f"Quality brands, clear part numbers, and stock updated daily.")
    return {"title": seo_title[:70], "description": seo_desc[:160]}


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


def get_product_counts() -> Tuple[Dict[str, int], Dict[str, int]]:
    """Query Shopify for ACTIVE product counts per productType and per vendor"""
    log("📊 Fetching product counts from Shopify...")

    query = '''
    query ($cursor: String) {
      products(first: 250, after: $cursor, query: "status:active") {
        pageInfo {
          hasNextPage
          endCursor
        }
        edges {
          node {
            productType
            vendor
          }
        }
      }
    }
    '''

    type_counts: Dict[str, int] = {}
    vendor_counts: Dict[str, int] = {}
    total = 0
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

            if not data.get('data'):
                log(f"❌ No data in response: {data}")
                break

            edges = data['data']['products']['edges']
            page_info = data['data']['products']['pageInfo']

            for edge in edges:
                total += 1
                product_type = edge['node'].get('productType', '')
                if product_type:
                    type_counts[product_type] = type_counts.get(product_type, 0) + 1
                vendor = (edge['node'].get('vendor') or '').strip()
                if vendor:
                    vendor_counts[vendor] = vendor_counts.get(vendor, 0) + 1

            has_next = page_info['hasNextPage']
            cursor = page_info['endCursor']

            time.sleep(RATE_LIMIT_DELAY)

        except Exception as e:
            log(f"❌ Error fetching products: {e}")
            break

    log(f"✅ Found {total} active products across {len(type_counts)} productTypes "
        f"and {len(vendor_counts)} vendors")
    return type_counts, vendor_counts


def collection_exists_by_handle(handle: str) -> Optional[Dict]:
    """Check if a collection already exists by handle"""
    query = '''
    query ($handle: String!) {
      collectionByHandle(handle: $handle) {
        id
        handle
        title
        descriptionHtml
        seo { title description }
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
    condition: str,
    description: str = None,
    rule_column: str = 'TYPE',
    seo: Optional[Dict] = None
) -> Optional[Dict]:
    """Create an automated collection with a productType or vendor rule"""

    # Create a nice description
    if not description:
        description = category_description(title, condition)

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
            "seo": seo or collection_seo(title, 'brand' if rule_column == 'VENDOR' else 'category'),
            "ruleSet": {
                "appliedDisjunctively": False,
                "rules": [
                    {
                        "column": rule_column,
                        "relation": "EQUALS",
                        "condition": condition
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


def update_collection_content(collection_gid: str, description: str, seo: Dict) -> bool:
    """Backfill description + SEO on an existing collection (used only when
    the current description is missing/thin — manual copy is never replaced)."""
    mutation = '''
    mutation collectionUpdate($input: CollectionInput!) {
      collectionUpdate(input: $input) {
        collection { id }
        userErrors { field message }
      }
    }
    '''
    variables = {
        "input": {
            "id": collection_gid,
            "descriptionHtml": description,
            "seo": seo
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
        if 'errors' in payload:
            log(f"  ⚠️  Collection update errors: {payload['errors']}")
            return False
        errors = (payload.get('data') or {}).get('collectionUpdate', {}).get('userErrors', [])
        if errors:
            log(f"  ⚠️  Collection update user errors: {errors}")
            return False
        return True
    except Exception as e:
        log(f"  ⚠️  Error updating collection: {e}")
        return False


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


def sync_collections(specs: List[Dict]) -> Dict[str, int]:
    """Create (or content-backfill) one collection per spec.

    spec = {title, handle, condition, rule_column, count, min_products, kind}
    """
    stats = {'created': 0, 'updated': 0, 'skipped_exists': 0,
             'skipped_threshold': 0, 'failed': 0}

    for spec in specs:
        title = spec['title']
        handle = spec['handle']
        kind = spec.get('kind', 'category')

        log(f"📦 {spec['condition']}")
        log(f"   Handle: {handle}")
        log(f"   Products: {spec['count']} (min: {spec['min_products']})")

        # Skip if below threshold
        if spec['count'] < spec['min_products']:
            log(f"   ⏭️  Skipped (below threshold)")
            stats['skipped_threshold'] += 1
            continue

        description = (brand_description(title, spec['count'])
                       if kind == 'brand'
                       else category_description(title, spec['condition']))
        seo = collection_seo(title, kind)

        # Check if already exists
        existing = collection_exists_by_handle(handle)
        if existing:
            count = existing.get('productsCount', 0)
            # Backfill description + SEO when missing/thin; never replace
            # a hand-written description.
            existing_desc = strip_html(existing.get('descriptionHtml', ''))
            existing_seo = (existing.get('seo') or {}).get('description') or ''
            if len(existing_desc) < MIN_COLLECTION_DESCRIPTION or not existing_seo:
                log(f"   ✏️  Exists ({count} products) — backfilling description/SEO...")
                if update_collection_content(existing['id'], description, seo):
                    stats['updated'] += 1
                    log(f"   ✅ Updated")
                else:
                    stats['failed'] += 1
            else:
                log(f"   ✅ Already exists ({count} products)")
                stats['skipped_exists'] += 1
            # Re-attempt publishing (idempotent): collections created while the
            # token lacked write_publications stay unpublished; this picks them
            # up on the first run after the scope is granted.
            if AUTO_PUBLISH:
                publish_collection(existing['id'])
            time.sleep(RATE_LIMIT_DELAY)
            continue

        # Create collection
        log(f"   🔨 Creating...")
        collection = create_automated_collection(
            title, handle, spec['condition'],
            description=description,
            rule_column=spec.get('rule_column', 'TYPE'),
            seo=seo
        )

        if collection:
            stats['created'] += 1
            log(f"   ✅ Created: {collection['handle']}")

            # Publish if AUTO_PUBLISH is set
            if AUTO_PUBLISH:
                log(f"   📢 Publishing...")
                if publish_collection(collection['id']):
                    log(f"   ✅ Published")
                else:
                    log(f"   ⚠️  Created but not published")
        else:
            stats['failed'] += 1
            log(f"   ❌ Failed")

        # Rate limiting
        time.sleep(RATE_LIMIT_DELAY)

    return stats


def slugify(text: str) -> str:
    slug = re.sub(r'[^a-z0-9]+', '-', (text or '').lower()).strip('-')
    return slug or 'unknown'


def build_specs(categories: List[Dict], type_counts: Dict[str, int],
                vendor_counts: Dict[str, int]) -> List[Dict]:
    """Category specs from the taxonomy + brand specs from live vendor data."""
    specs = []
    for category in categories:
        product_type = category['productType']
        specs.append({
            'kind': 'category',
            'title': category.get('title', product_type.split(' > ')[-1]),
            'handle': category['handle'],
            'condition': product_type,
            'rule_column': 'TYPE',
            'count': type_counts.get(product_type, 0),
            'min_products': category.get('min_products', 1),
        })

    for vendor, count in sorted(vendor_counts.items(), key=lambda kv: -kv[1]):
        specs.append({
            'kind': 'brand',
            'title': vendor,
            'handle': f"brand-{slugify(vendor)}",
            'condition': vendor,
            'rule_column': 'VENDOR',
            'count': count,
            'min_products': BRAND_MIN_PRODUCTS,
        })
    return specs


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
    type_counts, vendor_counts = get_product_counts()

    # Category collections + brand collections
    specs = build_specs(categories, type_counts, vendor_counts)
    n_brands = sum(1 for s in specs if s['kind'] == 'brand')
    log("")
    log("=" * 70)
    log(f"Syncing Collections ({len(specs) - n_brands} categories + {n_brands} brands)")
    log("=" * 70)
    log("")
    stats = sync_collections(specs)

    # Summary
    log("")
    log("=" * 70)
    log("SUMMARY")
    log("=" * 70)
    log(f"✅ Created:             {stats['created']}")
    log(f"✏️  Content backfilled:  {stats['updated']}")
    log(f"⏭️  Skipped (exists):    {stats['skipped_exists']}")
    log(f"⏭️  Skipped (threshold): {stats['skipped_threshold']}")
    log(f"❌ Failed:              {stats['failed']}")
    log(f"📊 Total specs:         {len(specs)}")
    log("=" * 70)

    if stats['created'] > 0 and not AUTO_PUBLISH:
        log("")
        log("ℹ️  Collections created but not published.")
        log("   Set AUTO_PUBLISH=true to auto-publish in future runs.")

    log("")
    log("✅ Collection sync complete!")


if __name__ == "__main__":
    main()
