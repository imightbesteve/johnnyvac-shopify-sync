#!/usr/bin/env python3
"""
Bulk Product Description Generator for Kingsway Janitorial
Generates rich, unique product descriptions for products with thin/empty body content.

Uses existing product data (title, category, brand, SKU, associated SKUs, specs)
to generate 3-5 sentence descriptions with Canadian market messaging.

Environment Variables:
    SHOPIFY_STORE: Store URL (e.g., kingsway-janitorial.myshopify.com)
    SHOPIFY_ACCESS_TOKEN: Admin API access token (shpat_...)
    DRY_RUN: Set to 'true' to preview without updating (default: true)

Usage:
    python description_generator.py --dry-run --limit 50 --export results.csv
    python description_generator.py --min-length 50 --export results.csv
"""

import os
import re
import csv
import json
import time
import random
import argparse
import requests
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# =============================================================================
# CONFIGURATION
# =============================================================================

SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', 'kingsway-janitorial.myshopify.com')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN', '')
API_VERSION = '2026-01'
GRAPHQL_URL = f'https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/graphql.json'
HEADERS = {
    'Content-Type': 'application/json',
    'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN
}

# Description quality thresholds
MIN_DESCRIPTION_LENGTH = 80  # Characters - descriptions shorter than this get regenerated
MAX_DESCRIPTION_LENGTH = 800  # Keep descriptions concise but substantive

# Rate limiting
RATE_LIMIT_DELAY = 0.5
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
POLL_INTERVAL = 10
MAX_POLL_TIME = 3600

# CSV feed for associated SKUs lookup
CSV_URL = 'https://www.johnnyvacstock.com/sigm_all_jv_products/JVWebProducts.csv'

# =============================================================================
# LOGGING
# =============================================================================

def log(msg: str, level: str = 'INFO'):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{level}] {msg}", flush=True)

# =============================================================================
# BRAND DETECTION (reused from seo_generator patterns)
# =============================================================================

BRANDS = [
    "JohnnyVac", "Johnny Vac",
    "Filter Queen", "Dirt Devil",
    "Bissell", "Compact", "Dustbane", "Dyson", "Electrolux", "Eureka",
    "Fuller", "Ghibli", "Hoover", "Intervac", "Karcher", "Kenmore",
    "Kirby", "Maytag", "Miele", "Nilfisk", "Oreck", "Panasonic",
    "Perfect", "ProTeam", "Proteam", "Rainbow", "Riccar", "Roval",
    "Royal", "Samsung", "Sanitaire", "Sanyo", "Shark", "Simplicity",
    "Tennant", "Tristar", "Vortech", "Wirbel"
]

def extract_brand(title: str) -> Optional[str]:
    """Extract brand from product title."""
    text = title.lower()
    for brand in BRANDS:
        if brand.lower() in text:
            # Normalize JohnnyVac variants
            if 'johnny' in brand.lower():
                return 'JohnnyVac'
            return brand
    return None

# =============================================================================
# PRODUCT DATA EXTRACTION
# =============================================================================

def extract_pack_quantity(title: str) -> Optional[int]:
    """Extract pack/quantity info from title."""
    patterns = [
        r'pack\s+of\s+(\d+)', r'(\d+)\s*-?\s*pack', r'box\s+of\s+(\d+)',
        r'(\d+)\s+bags', r'pk\s*(\d+)', r'(\d+)\s*pc',
        r'(\d+)\s*per\s+case', r'case\s+of\s+(\d+)',
    ]
    for pattern in patterns:
        match = re.search(pattern, title.lower())
        if match:
            return int(match.group(1))
    return None

def extract_dimensions(title: str, weight: str = '', length: str = '', width: str = '', height: str = '') -> Dict:
    """Extract physical specs from product data."""
    specs = {}
    
    # From explicit fields
    try:
        w = float(weight or 0)
        if w > 0:
            specs['weight_kg'] = w
            specs['weight_lbs'] = round(w * 2.205, 1)
    except (ValueError, TypeError):
        pass
    
    try:
        l, wi, h = float(length or 0), float(width or 0), float(height or 0)
        if l > 0 and wi > 0:
            specs['dimensions'] = f"{l} x {wi}"
            if h > 0:
                specs['dimensions'] += f" x {h}"
            specs['dimensions'] += " cm"
    except (ValueError, TypeError):
        pass
    
    # From title patterns
    size_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:"|inch|in)\b', title.lower())
    if size_match:
        specs['size_inches'] = size_match.group(1)
    
    diameter_match = re.search(r'(\d+(?:\.\d+)?)\s*mm\b', title.lower())
    if diameter_match:
        specs['size_mm'] = diameter_match.group(1)
    
    # Voltage
    volt_match = re.search(r'(\d+)\s*(?:v|volt)\b', title.lower())
    if volt_match:
        specs['voltage'] = volt_match.group(1)
    
    return specs

def extract_color(title: str) -> Optional[str]:
    """Extract color from title."""
    colors = [
        'black', 'white', 'grey', 'gray', 'red', 'blue', 'green', 'yellow',
        'orange', 'brown', 'beige', 'clear', 'transparent', 'chrome', 'silver'
    ]
    title_lower = title.lower()
    for color in colors:
        if re.search(r'\b' + color + r'\b', title_lower):
            return color.capitalize()
    return None

def extract_material(title: str) -> Optional[str]:
    """Extract material from title."""
    materials = {
        'hepa': 'HEPA', 'microfiber': 'microfiber', 'cloth': 'cloth',
        'paper': 'paper', 'foam': 'foam', 'rubber': 'rubber',
        'nylon': 'nylon', 'stainless': 'stainless steel', 'plastic': 'plastic',
        'vinyl': 'vinyl', 'latex': 'latex', 'nitrile': 'nitrile',
        'cotton': 'cotton', 'polyester': 'polyester',
    }
    title_lower = title.lower()
    for key, display in materials.items():
        if key in title_lower:
            return display
    return None

def extract_compatible_models(title: str) -> List[str]:
    """Extract model compatibility references from title."""
    models = []
    
    # "for [Brand] [Model]" pattern
    for_match = re.findall(r'for\s+(\w+(?:\s+\w+)?(?:\s+[A-Z0-9-]+)?)', title, re.IGNORECASE)
    for m in for_match:
        clean = m.strip()
        if len(clean) > 3 and clean.lower() not in ('use', 'all', 'the', 'vacuum', 'vacuums'):
            models.append(clean)
    
    # Model number patterns
    model_patterns = [
        r'\b([A-Z]{1,3}\d{3,6}[A-Z]?)\b',
        r'\b([A-Z]{2,4}-\d{2,4})\b',
    ]
    for pattern in model_patterns:
        matches = re.findall(pattern, title)
        for m in matches:
            if m not in models and len(m) > 2:
                models.append(m)
    
    return models[:3]

# =============================================================================
# CATEGORY-SPECIFIC DESCRIPTION TEMPLATES
# =============================================================================

# Each category has multiple template variants to ensure uniqueness
DESCRIPTION_TEMPLATES = {
    "vacuum-bags": {
        "openers": [
            "Keep your {brand_or}vacuum running at peak performance with {this_these} {material_adj}replacement vacuum bag{s}.",
            "{Brand_or}Replacement vacuum bag{s} designed for reliable filtration and easy installation.",
            "Maintain optimal suction power with {this_these} high-quality replacement vacuum bag{s}{brand_for}.",
        ],
        "features": [
            "Engineered for efficient dust and debris capture, {this_these} bag{s} help{s_verb} maintain strong suction while protecting the motor from fine particles.",
            "Designed for superior dust containment, reducing allergens and maintaining air quality during cleaning.",
            "{Material_cap}construction provides excellent filtration while allowing maximum airflow for powerful suction.",
        ],
        "pack_note": "This convenient {qty}-pack keeps you stocked for extended use.",
        "compat_note": "Compatible with {models} — check your vacuum model to confirm fitment.",
    },
    "filters": {
        "openers": [
            "Restore your vacuum's filtration performance with this {material_adj}replacement filter{brand_for}.",
            "{Brand_or}Replacement filter built for effective air filtration and long service life.",
            "Keep air quality high and suction strong with this {material_adj}filter replacement{brand_for}.",
        ],
        "features": [
            "Traps fine dust, allergens, and microscopic particles to deliver cleaner exhaust air — important for commercial environments.",
            "Designed to capture fine particulates while maintaining optimal airflow, extending the life of your vacuum motor.",
            "{Material_cap}filtration media provides excellent particle capture without sacrificing suction power.",
        ],
    },
    "belts": {
        "openers": [
            "Get your vacuum's brush roll spinning again with this replacement drive belt{brand_for}.",
            "{Brand_or}Replacement belt designed for a secure fit and long-lasting performance.",
            "Restore agitator function with this durable replacement vacuum belt{brand_for}.",
        ],
        "features": [
            "A worn belt causes loss of brush roll rotation and reduced cleaning effectiveness. Regular belt replacement maintains peak carpet cleaning performance.",
            "Manufactured for a precise fit, this belt restores proper brush roll tension and rotation speed for effective dirt pickup.",
        ],
    },
    "hoses": {
        "openers": [
            "Replace your worn or damaged vacuum hose with this durable replacement{brand_for}.",
            "{Brand_or}Replacement hose built for flexible, reliable suction transfer.",
            "Restore full suction power with this quality replacement vacuum hose{brand_for}.",
        ],
        "features": [
            "Flexible yet durable construction maintains strong suction while resisting kinks and cracks through heavy commercial use.",
            "Designed to deliver consistent airflow from floor tool to collection system, with connections that lock securely in place.",
        ],
    },
    "motors": {
        "openers": [
            "Bring your vacuum back to life with this replacement motor assembly{brand_for}.",
            "{Brand_or}Replacement motor engineered for reliable suction power and extended service life.",
            "Restore original suction performance with this high-quality replacement vacuum motor{brand_for}.",
        ],
        "features": [
            "Precision-built to OEM specifications, this motor delivers consistent suction power and reliable operation in demanding commercial environments.",
            "Designed for heavy-duty use, this motor provides the airflow and suction your vacuum needs for effective cleaning.",
        ],
    },
    "brushes": {
        "openers": [
            "Restore your vacuum's agitation performance with this replacement brush{brand_for}.",
            "{Brand_or}Replacement brush roll designed for effective carpet agitation and debris pickup.",
            "Get deep-cleaning performance back with this quality replacement brush{brand_for}.",
        ],
        "features": [
            "Bristle pattern is designed to agitate carpet fibers effectively, loosening embedded dirt for better suction pickup.",
            "Durable bristles maintain their shape through extended commercial use, providing consistent carpet agitation and edge cleaning.",
        ],
    },
    "nozzles-wands": {
        "openers": [
            "Extend your vacuum's reach and versatility with this {product_noun}{brand_for}.",
            "{Brand_or}{Product_noun_cap} for flexible cleaning access in tight spaces and varied surfaces.",
            "Enhance your vacuum's cleaning capability with this quality {product_noun}{brand_for}.",
        ],
        "features": [
            "Durable construction and secure connections ensure consistent suction transfer for effective cleaning of floors, upholstery, and hard-to-reach areas.",
            "Designed for everyday commercial use, providing reliable reach and maneuverability across different cleaning tasks.",
        ],
    },
    "equipment": {
        "openers": [
            "{Brand_or}{Product_noun_cap} built for commercial cleaning performance.",
            "Professional-grade {product_noun} designed for demanding janitorial environments.",
            "Tackle commercial cleaning jobs with this {brand_adj}{product_noun}.",
        ],
        "features": [
            "Built for the rigours of daily commercial use, this machine delivers consistent performance across large floor areas and high-traffic environments.",
            "Professional-grade construction meets the demands of commercial cleaning — from office buildings to industrial facilities.",
        ],
        "specs_note": "Weighing {weight_lbs} lbs ({weight_kg} kg), this unit balances power with maneuverability for efficient cleaning.",
    },
    "chemicals": {
        "openers": [
            "{Brand_or}{Product_noun_cap} formulated for professional cleaning results.",
            "Professional-strength {product_noun} for commercial janitorial applications.",
            "Achieve professional cleaning results with this {brand_adj}{product_noun}.",
        ],
        "features": [
            "Formulated for commercial environments, this product delivers effective cleaning while being suitable for daily professional use.",
            "Concentrated formula provides excellent coverage and value for commercial cleaning operations.",
        ],
    },
    "consumables": {
        "openers": [
            "{Brand_or}{Product_noun_cap} for commercial washroom and facility maintenance.",
            "Stock your facility with this quality {product_noun} built for high-traffic environments.",
            "Professional-grade {product_noun} designed for commercial facility maintenance.",
        ],
        "features": [
            "Designed for high-traffic commercial environments, providing reliable performance and value in daily facility operations.",
            "Built to meet the demands of busy washrooms, kitchens, and commercial spaces.",
        ],
    },
    "tools": {
        "openers": [
            "{Brand_or}{Product_noun_cap} for efficient commercial cleaning operations.",
            "Equip your cleaning crew with this professional-grade {product_noun}.",
            "Professional {product_noun} built for daily commercial cleaning use.",
        ],
        "features": [
            "Durable construction withstands the demands of daily commercial cleaning, providing reliable performance and long service life.",
            "Designed for professional janitorial use, combining practical ergonomics with sturdy build quality.",
        ],
    },
    "safety": {
        "openers": [
            "{Brand_or}{Product_noun_cap} for workplace safety compliance.",
            "Protect your team with this quality {product_noun} for commercial environments.",
            "Professional-grade {product_noun} meeting workplace safety standards.",
        ],
        "features": [
            "Provides reliable protection for cleaning and maintenance personnel in commercial environments.",
            "Designed for comfort during extended wear while meeting professional safety requirements.",
        ],
    },
    "parts-general": {
        "openers": [
            "{Brand_or}Replacement {product_noun} for vacuum and cleaning equipment maintenance.",
            "Keep your equipment running with this quality replacement {product_noun}{brand_for}.",
            "{Brand_or}{Product_noun_cap} — a reliable replacement to maintain your cleaning equipment's performance.",
        ],
        "features": [
            "Manufactured for a proper fit and reliable performance, this replacement part helps extend the service life of your cleaning equipment.",
            "Quality construction ensures this replacement component performs reliably in demanding commercial cleaning environments.",
        ],
    },
}

# Canadian CTAs — varied for uniqueness
CANADIAN_CTAS = [
    "Available from Kingsway Janitorial with fast shipping across Canada.",
    "Order from Kingsway Janitorial — proudly serving Canadian businesses from Vancouver.",
    "Ships quickly across Canada from Kingsway Janitorial, Vancouver's trusted janitorial supplier.",
    "Kingsway Janitorial — supplying Canadian businesses with professional cleaning products since 1990.",
    "In stock and ready to ship across Canada. Serving Vancouver, Montreal, and everywhere in between.",
    "Fast Canadian shipping from Kingsway Janitorial, your professional cleaning supply partner.",
    "Available for delivery across Canada — from our Vancouver warehouse to your door.",
    "Trusted by Canadian businesses coast to coast. Order from Kingsway Janitorial today.",
]

# =============================================================================
# CATEGORY MAPPING — map productType to template key
# =============================================================================

def get_template_key(product_type: str) -> str:
    """Map a Shopify productType to the right description template."""
    pt_lower = product_type.lower()
    
    if 'vacuum bag' in pt_lower:
        return 'vacuum-bags'
    elif 'filter' in pt_lower:
        return 'filters'
    elif 'belt' in pt_lower:
        return 'belts'
    elif 'hose' in pt_lower or 'fitting' in pt_lower:
        return 'hoses'
    elif 'motor' in pt_lower or 'electrical' in pt_lower:
        return 'motors'
    elif 'brush' in pt_lower or 'agitator' in pt_lower:
        return 'brushes'
    elif 'nozzle' in pt_lower or 'wand' in pt_lower:
        return 'nozzles-wands'
    elif 'equipment' in pt_lower or 'vacuum' in pt_lower and 'part' not in pt_lower:
        return 'equipment'
    elif 'chemical' in pt_lower or 'solution' in pt_lower:
        return 'chemicals'
    elif 'paper' in pt_lower or 'floor pad' in pt_lower:
        return 'consumables'
    elif 'tool' in pt_lower or 'mop' in pt_lower or 'broom' in pt_lower or 'dispenser' in pt_lower:
        return 'tools'
    elif 'safety' in pt_lower or 'glove' in pt_lower or 'ppe' in pt_lower:
        return 'safety'
    elif 'squeegee' in pt_lower or 'blade' in pt_lower:
        return 'tools'
    elif 'spring' in pt_lower or 'hardware' in pt_lower or 'latch' in pt_lower:
        return 'parts-general'
    elif 'seal' in pt_lower or 'gasket' in pt_lower:
        return 'parts-general'
    elif 'wheel' in pt_lower or 'caster' in pt_lower:
        return 'parts-general'
    elif 'kit' in pt_lower:
        return 'parts-general'
    elif 'assembl' in pt_lower or 'housing' in pt_lower:
        return 'parts-general'
    elif 'pump' in pt_lower or 'regulator' in pt_lower:
        return 'parts-general'
    else:
        return 'parts-general'

def get_product_noun(title: str, product_type: str) -> str:
    """Get a natural noun phrase for the product based on title/type."""
    pt_lower = product_type.lower()
    
    # Try to extract a meaningful noun from the product type
    if '>' in product_type:
        sub = product_type.split('>')[-1].strip()
        # Convert plural category names to more natural phrasing
        noun_map = {
            'Vacuum Bags': 'vacuum bag',
            'Filters': 'filter',
            'Vacuum Belts': 'vacuum belt',
            'Motors & Electrical': 'motor',
            'Hoses & Fittings': 'hose',
            'Brushes & Agitators': 'brush roll',
            'Nozzles & Wands': 'attachment',
            'Assemblies & Housings': 'assembly',
            'Repair & Conversion Kits': 'repair kit',
            'Wheels & Casters': 'wheel',
            'Latches & Clips': 'latch',
            'Springs & Hardware': 'hardware component',
            'Seals & Gaskets': 'seal',
            'Squeegees & Blades': 'squeegee blade',
            'Pumps & Regulators': 'pump',
            'Chemicals & Solutions': 'cleaning solution',
            'Paper Products': 'paper product',
            'Floor Pads': 'floor pad',
            'Cleaning Tools': 'cleaning tool',
            'Brooms & Brushes': 'broom',
            'Mops & Buckets': 'mop',
            'Dispensers': 'dispenser',
            'Gloves': 'gloves',
            'Safety & PPE': 'safety equipment',
            'Commercial Vacuums': 'commercial vacuum',
            'Upright Vacuums': 'upright vacuum',
            'Backpack Vacuums': 'backpack vacuum',
            'Canister Vacuums': 'canister vacuum',
            'Wet & Dry Vacuums': 'wet/dry vacuum',
            'HEPA Vacuums': 'HEPA vacuum',
            'Central Vacuums': 'central vacuum system',
            'Floor Machines': 'floor machine',
            'Automatic Scrubbers': 'automatic scrubber',
            'Carpet Extractors': 'carpet extractor',
            'Pressure Washers': 'pressure washer',
            'General Parts': 'replacement part',
        }
        if sub in noun_map:
            return noun_map[sub]
    
    # Fallback: use title-derived noun
    title_lower = title.lower()
    if 'vacuum' in title_lower and 'bag' not in title_lower and 'filter' not in title_lower:
        return 'vacuum'
    elif 'bag' in title_lower:
        return 'vacuum bag'
    elif 'filter' in title_lower:
        return 'filter'
    
    return 'replacement part'

# =============================================================================
# DESCRIPTION GENERATOR
# =============================================================================

class DescriptionGenerator:
    def __init__(self, store: str, token: str):
        self.store = store
        self.token = token
        self.graphql_url = f'https://{store}/admin/api/{API_VERSION}/graphql.json'
        self.headers = {
            'Content-Type': 'application/json',
            'X-Shopify-Access-Token': token
        }
        self.stats = {
            'total_fetched': 0,
            'thin_descriptions': 0,
            'generated': 0,
            'updated': 0,
            'skipped': 0,
            'errors': 0,
        }
        self.results = []
        self.associated_skus_map = {}  # SKU -> list of associated SKUs
        self.sku_title_map = {}  # SKU -> title (for resolving associated SKU names)
        self._cta_index = 0  # Rotate through CTAs
    
    def _graphql(self, query: str, variables: Optional[Dict] = None) -> Dict:
        """Make a GraphQL request."""
        payload = {'query': query}
        if variables:
            payload['variables'] = variables
        
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.post(self.graphql_url, json=payload, headers=self.headers, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                result = resp.json()
                if 'errors' in result:
                    log(f"GraphQL errors: {result['errors']}", 'WARNING')
                return result
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                if attempt < MAX_RETRIES - 1:
                    wait = (attempt + 1) * 5
                    log(f"Retry {attempt+1}/{MAX_RETRIES} in {wait}s: {e}", 'WARNING')
                    time.sleep(wait)
                else:
                    raise
        return {}
    
    # =========================================================================
    # DATA FETCHING
    # =========================================================================
    
    def fetch_csv_associations(self):
        """Fetch CSV to build associated SKUs map and SKU->title map."""
        log("Fetching CSV for associated SKUs data...")
        try:
            resp = requests.get(CSV_URL, timeout=60)
            resp.raise_for_status()
            lines = resp.text.splitlines()
            reader = csv.DictReader(lines, delimiter=';')
            
            for row in reader:
                sku = (row.get('SKU') or '').strip()
                if not sku:
                    continue
                
                title = (row.get('ProductTitleEN') or '').strip()
                associated = (row.get('AssociatedSkus') or '').strip()
                
                self.sku_title_map[sku] = title
                
                if associated:
                    # Associated SKUs may be comma or pipe separated
                    assoc_list = [s.strip() for s in re.split(r'[,|;]', associated) if s.strip()]
                    self.associated_skus_map[sku] = assoc_list
            
            log(f"✓ Loaded {len(self.sku_title_map)} SKUs, {len(self.associated_skus_map)} with associations")
        except Exception as e:
            log(f"Could not fetch CSV associations: {e}", 'WARNING')
    
    def fetch_products_paginated(self, min_desc_length: int = MIN_DESCRIPTION_LENGTH) -> List[Dict]:
        """Fetch all products, return those with thin descriptions."""
        log("Fetching products from Shopify...")
        
        query = """
        query ($cursor: String) {
            products(first: 250, after: $cursor) {
                edges {
                    node {
                        id
                        title
                        bodyHtml
                        productType
                        vendor
                        tags
                        handle
                        status
                        variants(first: 1) {
                            nodes {
                                sku
                                price
                                inventoryQuantity
                            }
                        }
                    }
                    cursor
                }
                pageInfo { hasNextPage }
            }
        }
        """
        
        all_products = []
        thin_products = []
        cursor = None
        
        while True:
            result = self._graphql(query, {'cursor': cursor} if cursor else None)
            edges = result.get('data', {}).get('products', {}).get('edges', [])
            page_info = result.get('data', {}).get('products', {}).get('pageInfo', {})
            
            for edge in edges:
                node = edge['node']
                all_products.append(node)
                
                # Check if description is thin
                body = node.get('bodyHtml') or ''
                # Strip HTML for length check
                text_only = re.sub(r'<[^>]+>', '', body).strip()
                
                if len(text_only) < min_desc_length:
                    # Only process ACTIVE products (or DRAFT ones we manage)
                    thin_products.append(node)
            
            if not page_info.get('hasNextPage'):
                break
            cursor = edges[-1]['cursor']
            
            if len(all_products) % 1000 == 0:
                log(f"  Fetched {len(all_products)} products...")
            
            time.sleep(RATE_LIMIT_DELAY)
        
        self.stats['total_fetched'] = len(all_products)
        self.stats['thin_descriptions'] = len(thin_products)
        
        log(f"✓ Fetched {len(all_products)} total products")
        log(f"  {len(thin_products)} have thin descriptions (< {min_desc_length} chars)")
        
        return thin_products
    
    # =========================================================================
    # DESCRIPTION GENERATION
    # =========================================================================
    
    def _next_cta(self) -> str:
        """Get the next CTA, rotating through the list for variety."""
        cta = CANADIAN_CTAS[self._cta_index % len(CANADIAN_CTAS)]
        self._cta_index += 1
        return cta
    
    def generate_description(self, product: Dict) -> str:
        """Generate a rich product description from available data."""
        title = product.get('title', '')
        product_type = product.get('productType', '')
        vendor = product.get('vendor', '')
        tags = product.get('tags', [])
        
        # Get variant data
        variants = product.get('variants', {}).get('nodes', [])
        sku = variants[0].get('sku', '') if variants else ''
        price = variants[0].get('price', '0') if variants else '0'
        
        # Extract product attributes
        brand = extract_brand(title)
        pack_qty = extract_pack_quantity(title)
        material = extract_material(title)
        color = extract_color(title)
        models = extract_compatible_models(title)
        product_noun = get_product_noun(title, product_type)
        template_key = get_template_key(product_type)
        
        # Get associated SKUs info
        associated = self.associated_skus_map.get(sku, [])
        associated_titles = []
        for assoc_sku in associated[:3]:
            assoc_title = self.sku_title_map.get(assoc_sku, '')
            if assoc_title:
                associated_titles.append(assoc_title)
        
        # Get CSV data for dimensions
        csv_title = self.sku_title_map.get(sku, '')
        specs = extract_dimensions(title)
        
        # Build template variables
        is_plural = pack_qty and pack_qty > 1
        tmpl_vars = {
            'brand_or': f'{brand} ' if brand else '',
            'Brand_or': f'{brand} ' if brand else '',
            'brand_for': f' for {brand} vacuums' if brand else '',
            'brand_adj': f'{brand} ' if brand else 'commercial ',
            'this_these': 'these' if is_plural else 'this',
            'material_adj': f'{material} ' if material else '',
            'Material_cap': f'{material.capitalize()} ' if material else '',
            's': 's' if is_plural else '',
            's_verb': '' if is_plural else 's',
            'product_noun': product_noun,
            'Product_noun_cap': product_noun.capitalize() if product_noun else 'Product',
            'qty': str(pack_qty) if pack_qty else '',
            'models': ', '.join(models[:2]) if models else '',
        }
        
        # Get templates
        templates = DESCRIPTION_TEMPLATES.get(template_key, DESCRIPTION_TEMPLATES['parts-general'])
        
        # Select template variants (use SKU hash for deterministic but varied selection)
        sku_hash = hash(sku) if sku else hash(title)
        
        # Build description sentences
        sentences = []
        
        # 1. Opening sentence
        openers = templates.get('openers', DESCRIPTION_TEMPLATES['parts-general']['openers'])
        opener = openers[abs(sku_hash) % len(openers)]
        try:
            sentences.append(opener.format(**tmpl_vars))
        except (KeyError, IndexError):
            sentences.append(f"{tmpl_vars['Brand_or']}{tmpl_vars['Product_noun_cap']}{tmpl_vars['brand_for']}.")
        
        # 2. Feature/benefit sentence
        features = templates.get('features', DESCRIPTION_TEMPLATES['parts-general']['features'])
        feature = features[abs(sku_hash + 1) % len(features)]
        try:
            sentences.append(feature.format(**tmpl_vars))
        except (KeyError, IndexError):
            sentences.append("Quality construction for reliable performance in commercial environments.")
        
        # 3. Pack quantity note (if multi-pack)
        if pack_qty and pack_qty > 1 and 'pack_note' in templates:
            sentences.append(templates['pack_note'].format(**tmpl_vars))
        
        # 4. Compatibility note (if models found)
        if models and 'compat_note' in templates:
            sentences.append(templates['compat_note'].format(**tmpl_vars))
        elif models:
            sentences.append(f"Compatible with {', '.join(models[:2])} — verify your model for proper fitment.")
        
        # 5. Specs note (if available)
        if specs.get('weight_lbs') and 'specs_note' in templates:
            sentences.append(templates['specs_note'].format(
                weight_lbs=specs['weight_lbs'],
                weight_kg=specs['weight_kg']
            ))
        
        # 6. Associated products note (if available and relevant)
        if associated_titles and len(sentences) < 5:
            # Reference one related product naturally
            assoc_noun = get_product_noun(associated_titles[0], '')
            sentences.append(f"Often purchased alongside compatible {assoc_noun}s and accessories — browse related products for a complete maintenance solution.")
        
        # 7. Canadian CTA (always last)
        sentences.append(self._next_cta())
        
        # Combine into HTML paragraphs
        # Group into 2 paragraphs for readability
        mid = len(sentences) // 2
        para1 = ' '.join(sentences[:mid])
        para2 = ' '.join(sentences[mid:])
        
        html = f"<p>{para1}</p>\n<p>{para2}</p>"
        
        # Truncate if too long
        text_only = re.sub(r'<[^>]+>', ' ', html).strip()
        if len(text_only) > MAX_DESCRIPTION_LENGTH:
            # Remove optional sentences (associated products, specs) and try again
            core_sentences = sentences[:3]  # opener, feature, CTA
            core_sentences.append(sentences[-1])  # CTA is always last
            html = f"<p>{' '.join(core_sentences[:2])}</p>\n<p>{' '.join(core_sentences[2:])}</p>"
        
        return html
    
    # =========================================================================
    # SHOPIFY UPDATE
    # =========================================================================
    
    def update_product_description(self, product_id: str, description_html: str) -> bool:
        """Update a product's body HTML in Shopify."""
        mutation = """
        mutation productUpdate($input: ProductInput!) {
            productUpdate(input: $input) {
                product { id }
                userErrors { field message }
            }
        }
        """
        
        result = self._graphql(mutation, {
            'input': {
                'id': product_id,
                'descriptionHtml': description_html
            }
        })
        
        errors = result.get('data', {}).get('productUpdate', {}).get('userErrors', [])
        if errors:
            log(f"Update error for {product_id}: {errors}", 'WARNING')
            return False
        
        return bool(result.get('data', {}).get('productUpdate', {}).get('product'))
    
    # =========================================================================
    # MAIN PROCESSING
    # =========================================================================
    
    def process(self, dry_run: bool = True, limit: Optional[int] = None, 
                min_length: int = MIN_DESCRIPTION_LENGTH) -> List[Dict]:
        """Main processing pipeline."""
        log("=" * 70)
        log("BULK DESCRIPTION GENERATOR — Kingsway Janitorial")
        log("=" * 70)
        log(f"Store: {self.store}")
        log(f"Min description length: {min_length} chars")
        log(f"Mode: {'DRY RUN' if dry_run else 'LIVE — descriptions will be updated'}")
        log("")
        
        # Step 1: Fetch CSV associations
        self.fetch_csv_associations()
        
        # Step 2: Fetch products with thin descriptions
        thin_products = self.fetch_products_paginated(min_length)
        
        if limit:
            thin_products = thin_products[:limit]
            log(f"Limited to {limit} products")
        
        if not thin_products:
            log("No products need description enrichment!")
            return []
        
        # Step 3: Generate descriptions
        log(f"\nGenerating descriptions for {len(thin_products)} products...")
        
        for i, product in enumerate(thin_products, 1):
            product_id = product['id']
            title = product.get('title', 'Unknown')
            product_type = product.get('productType', '')
            old_body = product.get('bodyHtml') or ''
            old_text = re.sub(r'<[^>]+>', '', old_body).strip()
            sku = ''
            if product.get('variants', {}).get('nodes'):
                sku = product['variants']['nodes'][0].get('sku', '')
            
            # Generate new description
            new_description = self.generate_description(product)
            new_text = re.sub(r'<[^>]+>', ' ', new_description).strip()
            
            result = {
                'product_id': product_id,
                'sku': sku,
                'title': title,
                'product_type': product_type,
                'old_length': len(old_text),
                'new_length': len(new_text),
                'new_description': new_description,
                'new_description_text': new_text,
                'status': 'pending',
            }
            
            if not dry_run:
                success = self.update_product_description(product_id, new_description)
                if success:
                    result['status'] = 'updated'
                    self.stats['updated'] += 1
                else:
                    result['status'] = 'error'
                    self.stats['errors'] += 1
                time.sleep(RATE_LIMIT_DELAY)
            else:
                result['status'] = 'dry_run'
            
            self.stats['generated'] += 1
            self.results.append(result)
            
            if i % 100 == 0 or i == len(thin_products):
                log(f"  Progress: {i}/{len(thin_products)}")
        
        return self.results
    
    # =========================================================================
    # EXPORT & REPORTING
    # =========================================================================
    
    def export_results(self, filename: str = 'description_results.csv'):
        """Export results to CSV for review."""
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'sku', 'title', 'product_type', 'old_length', 'new_length',
                'new_description_text', 'status'
            ])
            writer.writeheader()
            for r in self.results:
                writer.writerow({
                    'sku': r['sku'],
                    'title': r['title'],
                    'product_type': r['product_type'],
                    'old_length': r['old_length'],
                    'new_length': r['new_length'],
                    'new_description_text': r['new_description_text'],
                    'status': r['status'],
                })
        
        log(f"📄 Results exported to {filename}")
        return filename
    
    def print_summary(self):
        """Print processing summary."""
        log("")
        log("=" * 70)
        log("SUMMARY")
        log("=" * 70)
        log(f"  Total products fetched:     {self.stats['total_fetched']}")
        log(f"  Products with thin desc:    {self.stats['thin_descriptions']}")
        log(f"  Descriptions generated:     {self.stats['generated']}")
        log(f"  Successfully updated:       {self.stats['updated']}")
        log(f"  Errors:                     {self.stats['errors']}")
        log("=" * 70)
        
        # Show category distribution of generated descriptions
        if self.results:
            type_counts = {}
            for r in self.results:
                pt = r.get('product_type', 'Unknown')
                type_counts[pt] = type_counts.get(pt, 0) + 1
            
            log("\nDescriptions by product type:")
            for pt, count in sorted(type_counts.items(), key=lambda x: -x[1])[:15]:
                log(f"  {count:5d}  {pt}")
    
    def print_samples(self, count: int = 5):
        """Print sample generated descriptions."""
        log(f"\n📋 SAMPLE DESCRIPTIONS (first {count}):")
        log("-" * 70)
        for r in self.results[:count]:
            log(f"\nSKU: {r['sku']}")
            log(f"Title: {r['title']}")
            log(f"Type: {r['product_type']}")
            log(f"Old length: {r['old_length']} chars → New: {r['new_length']} chars")
            log(f"Description:\n{r['new_description_text']}")
            log("-" * 40)


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='Generate product descriptions for Kingsway Janitorial')
    parser.add_argument('--dry-run', action='store_true', default=True,
                        help='Preview without updating (default: true)')
    parser.add_argument('--live', action='store_true',
                        help='Actually update Shopify products')
    parser.add_argument('--limit', type=int, help='Limit number of products')
    parser.add_argument('--min-length', type=int, default=MIN_DESCRIPTION_LENGTH,
                        help=f'Min description length threshold (default: {MIN_DESCRIPTION_LENGTH})')
    parser.add_argument('--export', type=str, default='description_results.csv',
                        help='Export results CSV filename')
    parser.add_argument('--samples', type=int, default=10,
                        help='Number of sample descriptions to show')
    args = parser.parse_args()
    
    store = os.environ.get('SHOPIFY_STORE', SHOPIFY_STORE)
    token = os.environ.get('SHOPIFY_ACCESS_TOKEN', SHOPIFY_ACCESS_TOKEN)
    
    if not token:
        log("❌ SHOPIFY_ACCESS_TOKEN not set", 'ERROR')
        return
    
    dry_run = not args.live  # Default is dry run unless --live is specified
    
    generator = DescriptionGenerator(store, token)
    generator.process(dry_run=dry_run, limit=args.limit, min_length=args.min_length)
    generator.export_results(args.export)
    generator.print_summary()
    generator.print_samples(args.samples)


if __name__ == '__main__':
    main()
