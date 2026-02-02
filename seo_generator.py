#!/usr/bin/env python3
"""
SEO Metadata Generator for Kingsway Janitorial Shopify Store
Generates optimized SEO titles and descriptions for all products
Emphasizes: Canadian shipping, commercial/professional focus, JohnnyVac compatibility
"""

import requests
import json
import re
import time
import csv
from datetime import datetime

# Configuration
SHOPIFY_STORE = "kingsway-janitorial.myshopify.com"
SHOPIFY_ACCESS_TOKEN = ""  # Set via environment or paste here
API_VERSION = "2024-01"

# Rate limiting
RATE_LIMIT_DELAY = 0.5  # seconds between API calls
BATCH_SIZE = 50

# SEO Character limits
MAX_TITLE_LENGTH = 60
MAX_DESCRIPTION_LENGTH = 160

# Brand keywords to detect - ORDERED BY SPECIFICITY (longer/more specific first)
BRANDS = [
    # JohnnyVac variants - most specific first
    "JohnnyVac", "Johnny Vac",
    # Multi-word brands
    "Filter Queen", "Dirt Devil",
    # Standard brands (alphabetical for maintainability)
    "Bissell", "Compact", "Dustbane", "Dyson", "Electrolux", "Eureka",
    "Fuller", "Ghibli", "Hoover", "Intervac", "Karcher", "Kenmore",
    "Kirby", "Maytag", "Miele", "Nilfisk", "Oreck", "Panasonic",
    "Perfect", "ProTeam", "Proteam", "Rainbow", "Riccar", "Roval",
    "Royal", "Samsung", "Sanitaire", "Sanyo", "Shark", "Simplicity",
    "Tennant", "Tristar", "Vortech", "Wirbel"
]

# Product type patterns for categorization - ORDERED BY PRIORITY
# More specific categories first to avoid false positives
PRODUCT_TYPE_PRIORITY = [
    "machines",    # Central vacuums, extractors - check first
    "chemicals",   # Cleaners, detergents - check early (avoid "cleaner" matching elsewhere)
    "wands",       # Telescopic wands - specific
    "hoses",       # Hoses - specific
    "motors",      # Motors - specific
    "bags",        # Most common replacement part
    "filters",     # Common but check after bags
    "brushes",     # Brush rolls, carbon brushes
    "belts",       # Drive belts - check after brushes (both often in same products)
    "cords",       # Power cords
    "wheels",      # Wheels, casters
    "attachments", # Generic attachments
    "parts",       # Fallback category
]

PRODUCT_PATTERNS = {
    "machines": [
        r"\bcentral\s+vacuum",              # Central vacuums - check first!
        r"\bvacuum\s+cleaner\b",
        r"\bcanister\b(?!\s*(bag|filter))", # canister but not "canister bag/filter"
        r"\bupright\b(?!\s*(bag|filter))",  # upright but not "upright bag"
        r"\bbackpack\b(?!\s*(bag|filter))", # backpack but not "backpack bag"
        r"\bcommercial\s+vacuum",
        r"\bwet.*dry\b",
        r"\bextractor\b(?!\s*bag)",
        r"\bscrubber\b", r"\bsweeper\b", r"\bpolisher\b", r"\bburnisher\b"
    ],
    "chemicals": [
        r"\bdetergent\b", r"\bdegreaser\b", r"\bsanitizer\b", r"\bdisinfectant\b",
        r"\bglass\s+cleaner", r"\bfloor\s+cleaner", r"\bcleaning\s+solution",
        r"\bpolish\b(?!\s*brush)", r"\bsprayway\b", r"\bfabric\s+cleaner",
        r"\bleather\s+cleaner", r"\bvinyl\s+cleaner", r"\bneutral\s+cleaner",
        r"\ball\s+purpose\s+cleaner",
    ],
    "wands": [
        r"\bwand[s]?\b", r"\btelescopic\b", r"\bextension\s+wand",
        r"\btelescop\b",  # Handle typos like "telescop wand"
    ],
    "hoses": [
        r"\bhose[s]?\b", r"\bstretch\s+hose", r"\bextension\s+hose", 
        r"\bcrushproof\b"
    ],
    "motors": [
        r"\bmotor\b(?!\s*filter)", r"\bsuction\s+motor", r"\bbrush\s+motor", 
        r"\barmature\b", r"\btangential\s+vacuum\s+motor"
    ],
    "bags": [
        r"\bvacuum\s+bag", r"\bdust\s+bag", r"\bpaper\s+bag",
        r"\bhepa\s+bag", r"\bmicrofilter\s+bag",
        r"\bbag[s]?\s+for\b",               # "bags for X vacuum"
        r"\bpack\s+of\s+\d+\s+bag",         # "pack of 6 bags"
        r"^bags?\s+hepa",                   # "BAGS HEPA" at start of title
        r"\bbags?\b.*\bvacuum",             # "bags ... vacuum" anywhere
    ],
    "filters": [
        r"\bmicrofilter\b(?!\s*bag)",       # microfilter but not "microfilter bag"
        r"\bhepa\s+filter", r"\bmotor\s+filter", r"\bexhaust\s+filter",
        r"\bpre-?filter\b", r"\bpost-?filter\b", r"\bfoam\s+filter",
        r"\bfilter\b(?!\s*(bag|queen))",    # filter but not "filter bag" or "Filter Queen"
    ],
    "brushes": [
        r"\bbrush\s*roll", r"\broller\s+brush", r"\bagitator\b", r"\bbeater\s+bar",
        r"\bnylon\s+brush", r"\bcarbon\s+brush", r"\bcomplete\s+roller\s+brush",
        r"\bbrush\b(?!\s*(motor|belt))",    # brush but not "brush motor" or "brush belt"
    ],
    "belts": [
        r"\bdrive\s+belt", r"\bflat\s+belt", r"\bround\s+belt",
        r"\bvacuum\s+belt", r"\breplacement\s+belt",
        r"(?<!\w)belt[s]?(?!\s*(clip|brush))\b",  # belt but not "belt clip" or "belt brush"
    ],
    "cords": [
        r"\bpower\s+cord", r"\belectrical\s+cord", 
        r"\bcord\s+\d+[\'\"]\b",             # "cord 6'" 
        r"\b\d+[\'\"]\s*cord\b",             # "6' cord"
    ],
    "wheels": [r"\bwheel[s]?\b", r"\bcaster[s]?\b"],
    "attachments": [
        r"\battachment[s]?\b", r"\bnozzle[s]?\b", r"\bcrevice\b",
        r"\bupholstery\s+tool", r"\bdusting\s+brush",
        r"\btool\s+kit\b", r"\bfloor\s+tool",
    ],
    "parts": [
        r"\breplacement\b(?!\s*(belt|bag|filter|brush|motor))",
        r"\bfitting[s]?\b", r"\badaptor\b", r"\bgasket\b",
        r"\bconnector\b", r"\bcoupling\b", r"\blatch\b", r"\bswitch\b", 
        r"\bhandle\b", r"\bpedal\b", r"\bvalve\b", r"\bdome\s+assembly",
        r"\bsilencer\b", r"\bbearing\b",
    ]
}

# Category-specific SEO phrases
CATEGORY_SEO = {
    "bags": {
        "suffix": "Vacuum Bags",
        "descriptors": ["replacement bags", "commercial grade", "quality filtration"]
    },
    "filters": {
        "suffix": "Filter Replacement",
        "descriptors": ["replacement filter", "commercial grade", "premium filtration"]
    },
    "belts": {
        "suffix": "Belt Replacement",
        "descriptors": ["replacement belt", "durable construction", "OEM compatible"]
    },
    "brushes": {
        "suffix": "Brush Replacement",
        "descriptors": ["replacement brush", "commercial grade", "durable bristles"]
    },
    "hoses": {
        "suffix": "Hose Replacement",
        "descriptors": ["replacement hose", "flexible construction", "durable material"]
    },
    "cords": {
        "suffix": "Power Cord",
        "descriptors": ["replacement cord", "heavy-duty", "commercial grade"]
    },
    "motors": {
        "suffix": "Motor Replacement",
        "descriptors": ["replacement motor", "commercial grade", "high performance"]
    },
    "wheels": {
        "suffix": "Wheel Replacement",
        "descriptors": ["replacement wheel", "durable construction", "smooth rolling"]
    },
    "wands": {
        "suffix": "Wand Attachment",
        "descriptors": ["extension wand", "durable construction", "commercial grade"]
    },
    "attachments": {
        "suffix": "Vacuum Attachment",
        "descriptors": ["vacuum tool", "versatile cleaning", "commercial grade"]
    },
    "parts": {
        "suffix": "Replacement Part",
        "descriptors": ["replacement part", "OEM compatible", "quality construction"]
    },
    "machines": {
        "suffix": "Commercial Vacuum",
        "descriptors": ["commercial vacuum", "professional grade", "heavy-duty"]
    },
    "chemicals": {
        "suffix": "Cleaning Solution",
        "descriptors": ["professional cleaner", "commercial grade", "effective formula"]
    }
}


class SEOGenerator:
    def __init__(self, store, token):
        self.store = store
        self.token = token
        self.base_url = f"https://{store}/admin/api/{API_VERSION}"
        self.headers = {
            "X-Shopify-Access-Token": token,
            "Content-Type": "application/json"
        }
        self.stats = {
            "processed": 0,
            "updated": 0,
            "skipped": 0,
            "errors": 0
        }
        self.results = []

    def truncate_text(self, text, max_length, use_ellipsis=False):
        """Safely truncate text to max_length, breaking at word boundaries."""
        if len(text) <= max_length:
            return text
        
        # Try to truncate at last space before limit
        truncated = text[:max_length].rsplit(' ', 1)[0]
        
        # If truncation is too aggressive, use hard cut
        if len(truncated) < max_length * 0.6:
            truncated = text[:max_length - 3] if use_ellipsis else text[:max_length]
        
        if use_ellipsis and len(truncated) < len(text):
            return truncated.rstrip('.') + "..."
        
        return truncated

    def extract_brand(self, title, description=""):
        """Extract brand name from product title or description."""
        text = f"{title} {description}".lower()
        for brand in BRANDS:
            if brand.lower() in text:
                return brand
        return None

    def extract_pack_quantity(self, title):
        """Extract pack quantity from title."""
        patterns = [
            r"pack\s+of\s+(\d+)",
            r"(\d+)\s*-?\s*pack",
            r"box\s+of\s+(\d+)",
            r"(\d+)\s+bags",
            r"pk\s*(\d+)",
            r"(\d+)\s*pc",
        ]
        for pattern in patterns:
            match = re.search(pattern, title.lower())
            if match:
                return int(match.group(1))
        return None

    def extract_model_number(self, title, sku=""):
        """Extract model/part numbers from title or SKU."""
        models = []
        text = f"{title} {sku}"
        
        # Patterns ordered by specificity
        patterns = [
            r"#\s*(\d{5,})",                    # # followed by 5+ digits (like #103191)
            r"\b(\d{6,})\b",                    # 6+ digits standalone (like 440001018)
            r"model\s+([A-Z0-9-]+)",            # model XYZ
            r"type\s+([A-Z0-9-]+)",             # type Y, type AB
            r"style\s+([A-Z0-9-]+)",            # style C, style U
            r"\b([A-Z]{1,3}\d{3,6}[A-Z]?)\b",   # AH10040, C105, VAC19
            r"\b([A-Z]{2,4}-\d{2,4})\b",        # XV-10, PB-1006
            r"#\s*(\d{3,})",                    # # followed by 3+ digits
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                # Validate - skip common false positives
                if match.upper() in ['AND', 'FOR', 'THE', 'WITH']:
                    continue
                if len(match) < 2:
                    continue
                # Skip pure numbers that are likely pack quantities
                if match.isdigit() and int(match) < 100:
                    continue
                models.append(match.upper())
        
        # Dedupe while preserving order, prioritize longer/more specific
        seen = set()
        unique_models = []
        for m in sorted(models, key=len, reverse=True):
            if m not in seen:
                seen.add(m)
                unique_models.append(m)
        
        return unique_models[:3]  # Return top 3

    def detect_product_type(self, title, description=""):
        """Detect the product type/category using priority order."""
        text = f"{title} {description}".lower()
        
        # FIRST: Check for definitive machine indicators (these should always win)
        machine_definitive = [
            r"\bcentral\s+vacuum",
            r"\bvacuum\s+cleaner\b",
            r"\bcommercial\s+vacuum\b",
            r"\bwet.*dry\b",
            r"\bextractor\b(?!\s*bag)",     # extractor but not "extractor bag"
            r"\bscrubber\b", r"\bsweeper\b", r"\bpolisher\b", r"\bburnisher\b"
        ]
        for pattern in machine_definitive:
            if re.search(pattern, text, re.IGNORECASE):
                return "machines"
        
        # SECOND: Check for strong matches (2+ patterns) in priority order
        for category in PRODUCT_TYPE_PRIORITY:
            if category == "machines":  # Already checked above
                continue
            patterns = PRODUCT_PATTERNS.get(category, [])
            matches = sum(1 for p in patterns if re.search(p, text, re.IGNORECASE))
            if matches >= 2:  # Strong match
                return category
        
        # THIRD: Accept single matches in priority order
        for category in PRODUCT_TYPE_PRIORITY:
            if category == "machines":  # Already checked above
                continue
            patterns = PRODUCT_PATTERNS.get(category, [])
            for pattern in patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    return category
        
        return "parts"  # Default fallback

    def extract_compatibility(self, title, description=""):
        """Extract compatible vacuum models/brands from product info."""
        text = f"{title} {description}"
        
        compat_patterns = [
            r"compatible\s+with\s+([^.,-]+)",
            r"fits?\s+(?:on\s+)?([^.,-]+(?:vacuum|cleaner|model)s?)",
            r"replacement\s+for\s+([^.,-]+)",
            r"works\s+with\s+([^.,-]+)",
            r"for\s+([A-Z][a-z]+(?:\s+[A-Z0-9-]+)?)\s+(?:vacuum|model)",
        ]
        
        compatibilities = []
        for pattern in compat_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                # Clean up the match
                clean = match.strip()
                if len(clean) > 5 and len(clean) < 50:  # Reasonable length
                    compatibilities.append(clean)
        
        return compatibilities[:2]  # Return top 2 matches

    def extract_style_type(self, title):
        """Extract Style/Type designations like 'Style U' or 'Type Y'."""
        patterns = [
            r"style\s+([A-Z0-9-]+)",
            r"type\s+([A-Z0-9-]+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, title, re.IGNORECASE)
            if match:
                return match.group(0).title()  # Returns "Style U" or "Type Y"
        return None

    def generate_seo_title(self, product):
        """Generate optimized SEO title (max 60 chars)."""
        title = product.get("title", "")
        sku = product.get("variants", [{}])[0].get("sku", "") if product.get("variants") else ""
        
        brand = self.extract_brand(title)
        product_type = self.detect_product_type(title)
        pack_qty = self.extract_pack_quantity(title)
        models = self.extract_model_number(title, sku)
        style_type = self.extract_style_type(title)
        
        category_info = CATEGORY_SEO.get(product_type, CATEGORY_SEO["parts"])
        
        # Build SEO title components
        parts = []
        
        # Add brand if detected
        if brand:
            parts.append(brand)
        
        # Add style/type if found (e.g., "Style U", "Type Y")
        if style_type:
            parts.append(style_type)
        # Add model number if found and no style/type
        elif models:
            parts.append(models[0])
        
        # Add category suffix
        parts.append(category_info["suffix"])
        
        # Add pack info if relevant
        if pack_qty and pack_qty > 1:
            parts.append(f"{pack_qty} Pack")
        
        # Combine and check length
        seo_title = " ".join(parts)
        
        # If too long, try shorter version
        if len(seo_title) > MAX_TITLE_LENGTH:
            parts = []
            if brand:
                parts.append(brand)
            parts.append(category_info["suffix"])
            seo_title = " ".join(parts)
        
        # If too short, add more context
        if len(seo_title) < 25:
            # Try adding "Commercial" prefix (but avoid "Commercial Commercial...")
            if not brand and not seo_title.lower().startswith("commercial"):
                seo_title = f"Commercial {seo_title}"
            if len(seo_title) < 25:
                seo_title = f"{seo_title} | Vacuum Part"
            if len(seo_title) < 25 and models:
                seo_title = f"{seo_title} {models[0]}"
        
        # Final fallback - use cleaned original title
        if len(seo_title) > MAX_TITLE_LENGTH or len(seo_title) < 20:
            clean_title = re.sub(r'\s+', ' ', title).strip()
            # Remove redundant words
            clean_title = re.sub(r'\b(vacuum|for|and|the|pack of \d+)\b', '', clean_title, flags=re.IGNORECASE)
            clean_title = re.sub(r'\s+', ' ', clean_title).strip()
            clean_title = self.truncate_text(clean_title, MAX_TITLE_LENGTH - 15)
            seo_title = f"{clean_title} | Replacement"
        
        return self.truncate_text(seo_title, MAX_TITLE_LENGTH)

    def generate_seo_description(self, product):
        """Generate optimized SEO description (max 160 chars)."""
        title = product.get("title", "")
        description = product.get("body_html", "") or ""
        sku = product.get("variants", [{}])[0].get("sku", "") if product.get("variants") else ""
        
        brand = self.extract_brand(title, description)
        product_type = self.detect_product_type(title, description)
        pack_qty = self.extract_pack_quantity(title)
        models = self.extract_model_number(title, sku)
        style_type = self.extract_style_type(title)
        
        category_info = CATEGORY_SEO.get(product_type, CATEGORY_SEO["parts"])
        
        # Build description components
        opener = ""
        if brand:
            opener = f"{brand} {category_info['suffix'].lower()}"
        else:
            # Avoid "Commercial commercial vacuum" - check if suffix starts with "commercial"
            suffix_lower = category_info['suffix'].lower()
            if suffix_lower.startswith("commercial"):
                opener = category_info['suffix']  # Use as-is, capitalized
            else:
                opener = f"Commercial {suffix_lower}"
        
        # Add style/type or model if found
        if style_type:
            opener = f"{opener} ({style_type})"
        elif models:
            opener = f"{opener} ({models[0]})"
        
        parts = [opener]
        
        # Add pack quantity
        if pack_qty and pack_qty > 1:
            parts.append(f"Pack of {pack_qty}")
        
        # Add quality descriptor - use the one matching the product type
        parts.append(category_info["descriptors"][0].capitalize())
        
        # Canadian shipping CTAs - varied options
        ctas = [
            "Fast shipping across Canada.",
            "Ships Canada-wide.",
            "Canadian janitorial supply.",
            "Professional grade. Ships across Canada."
        ]
        
        # JohnnyVac compatibility note - only for parts, not for JV machines
        jv_note = ""
        if product_type not in ["machines", "chemicals"]:
            if brand and "johnny" in brand.lower():
                jv_note = "JohnnyVac compatible. "
            elif "johnny" in title.lower() or "jvac" in title.lower():
                jv_note = "JohnnyVac compatible. "
        
        # Combine parts
        main_text = ". ".join(parts) + "."
        
        # Insert JohnnyVac note if applicable
        if jv_note:
            main_text = main_text.replace(". ", f". {jv_note}", 1)
        
        # If description is too short, add more context
        if len(main_text) < 60:
            main_text = main_text.rstrip(".")
            main_text += ". Commercial grade quality."
        
        # Select CTA that fits within character limit
        for cta in ctas:
            test_desc = f"{main_text} {cta}"
            if len(test_desc) <= MAX_DESCRIPTION_LENGTH:
                return test_desc
        
        # If still too long, try shorter main text
        if len(main_text) > MAX_DESCRIPTION_LENGTH - 30:
            # Rebuild with fewer parts
            if brand:
                short_text = f"{brand} {category_info['suffix'].lower()}."
            else:
                short_text = f"{category_info['suffix']}."
            
            if pack_qty and pack_qty > 1:
                short_text += f" {pack_qty} pack."
            
            short_text += f" {jv_note}Professional grade. Ships Canada-wide."
            
            if len(short_text) <= MAX_DESCRIPTION_LENGTH:
                return short_text
        
        # Final truncation if needed
        return main_text[:MAX_DESCRIPTION_LENGTH]

    def get_all_products(self):
        """Fetch all products from Shopify."""
        products = []
        url = f"{self.base_url}/products.json?limit=250"
        
        while url:
            try:
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                batch = data.get("products", [])
                products.extend(batch)
                print(f"  Fetched {len(products)} products...")
                
                # Check for pagination
                link_header = response.headers.get("Link", "")
                if 'rel="next"' in link_header:
                    # Extract next URL
                    links = link_header.split(",")
                    for link in links:
                        if 'rel="next"' in link:
                            url = link.split(";")[0].strip().strip("<>")
                            break
                else:
                    url = None
                
                time.sleep(RATE_LIMIT_DELAY)
                
            except requests.exceptions.RequestException as e:
                print(f"  Error fetching products: {e}")
                break
        
        return products

    def update_product_seo(self, product_id, seo_title, seo_description):
        """Update a product's SEO metadata."""
        url = f"{self.base_url}/products/{product_id}.json"
        
        payload = {
            "product": {
                "id": product_id,
                "metafields_global_title_tag": seo_title,
                "metafields_global_description_tag": seo_description
            }
        }
        
        try:
            response = requests.put(url, headers=self.headers, json=payload, timeout=30)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            print(f"  Error updating product {product_id}: {e}")
            return False

    def process_products(self, dry_run=False, limit=None):
        """Process all products and generate/update SEO metadata."""
        print("\n" + "="*60)
        print("SEO METADATA GENERATOR - Kingsway Janitorial")
        print("="*60)
        
        print("\n📥 Fetching products from Shopify...")
        products = self.get_all_products()
        
        if limit:
            products = products[:limit]
        
        print(f"\n✅ Found {len(products)} products to process")
        
        if dry_run:
            print("\n🔍 DRY RUN MODE - No changes will be made")
        
        print("\n" + "-"*60)
        print("Processing products...")
        print("-"*60 + "\n")
        
        for i, product in enumerate(products, 1):
            product_id = product.get("id")
            title = product.get("title", "Unknown")
            handle = product.get("handle", "")
            
            # Generate SEO metadata
            seo_title = self.generate_seo_title(product)
            seo_description = self.generate_seo_description(product)
            
            # Store result
            result = {
                "id": product_id,
                "handle": handle,
                "original_title": title,
                "seo_title": seo_title,
                "seo_description": seo_description,
                "status": "pending"
            }
            
            if not dry_run:
                success = self.update_product_seo(product_id, seo_title, seo_description)
                if success:
                    result["status"] = "updated"
                    self.stats["updated"] += 1
                else:
                    result["status"] = "error"
                    self.stats["errors"] += 1
                
                time.sleep(RATE_LIMIT_DELAY)
            else:
                result["status"] = "dry_run"
            
            self.results.append(result)
            self.stats["processed"] += 1
            
            # Progress update
            if i % 50 == 0 or i == len(products):
                print(f"  Processed {i}/{len(products)} products...")
        
        return self.results

    def export_results(self, filename=None):
        """Export results to CSV for review."""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"seo_results_{timestamp}.csv"
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                "handle", "original_title", "seo_title", "seo_description", "status"
            ])
            writer.writeheader()
            for result in self.results:
                writer.writerow({
                    "handle": result["handle"],
                    "original_title": result["original_title"],
                    "seo_title": result["seo_title"],
                    "seo_description": result["seo_description"],
                    "status": result["status"]
                })
        
        return filename

    def print_summary(self):
        """Print processing summary."""
        print("\n" + "="*60)
        print("PROCESSING SUMMARY")
        print("="*60)
        print(f"  Total processed: {self.stats['processed']}")
        print(f"  Updated:         {self.stats['updated']}")
        print(f"  Errors:          {self.stats['errors']}")
        print("="*60)


def main():
    import os
    
    # Get credentials from environment or use placeholders
    store = os.environ.get("SHOPIFY_STORE", SHOPIFY_STORE)
    token = os.environ.get("SHOPIFY_ACCESS_TOKEN", SHOPIFY_ACCESS_TOKEN)
    
    if not token:
        print("❌ Error: SHOPIFY_ACCESS_TOKEN not set")
        print("   Set it via environment variable or in the script")
        return
    
    # Parse arguments
    import argparse
    parser = argparse.ArgumentParser(description="Generate SEO metadata for Shopify products")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without updating")
    parser.add_argument("--limit", type=int, help="Limit number of products to process")
    parser.add_argument("--export", type=str, help="Export results to CSV file")
    args = parser.parse_args()
    
    # Initialize generator
    generator = SEOGenerator(store, token)
    
    # Process products
    results = generator.process_products(dry_run=args.dry_run, limit=args.limit)
    
    # Export results
    if args.export or args.dry_run:
        filename = generator.export_results(args.export)
        print(f"\n📄 Results exported to: {filename}")
    
    # Print summary
    generator.print_summary()
    
    # Show sample results
    print("\n📋 SAMPLE RESULTS (first 5):")
    print("-"*60)
    for result in results[:5]:
        print(f"\nOriginal: {result['original_title'][:50]}...")
        print(f"SEO Title: {result['seo_title']}")
        print(f"SEO Desc:  {result['seo_description']}")


if __name__ == "__main__":
    main()
