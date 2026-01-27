#!/usr/bin/env python3
# categorizer_v4.py
"""
ProductCategorizer v4
- Uses JohnnyVac ProductCategory as PRIMARY signal (high confidence)
- Falls back to keyword matching only for generic categories (All Parts, Johnny Vac Parts)
- FIXED: Global part keywords are now checked LAST (fallback) not FIRST
- Thread-safe design
- Includes skip pattern detection for placeholder products
"""

import re
import json
import csv
from typing import Dict, List, Tuple, Optional
from collections import Counter


class ProductCategorizer:
    def __init__(self, category_rules_path: str):
        with open(category_rules_path, 'r', encoding='utf-8') as f:
            self.rules = json.load(f)
        
        # Load settings
        settings = self.rules.get('settings', {})
        self.global_part_keywords = settings.get('global_part_keywords', [])
        self.skip_patterns = settings.get('skip_patterns', {})
        
        # Load JohnnyVac category mappings
        self.jv_mappings = self.rules.get('jv_category_mappings', {})
        
        # Load categories sorted by priority desc (for keyword fallback)
        self.categories = sorted(
            self.rules.get('categories', []),
            key=lambda x: x.get('priority', 0),
            reverse=True
        )
        
        # Build quick lookup for category handles
        self.category_by_handle = {
            cat['handle']: cat for cat in self.categories
        }

    # =========================================================================
    # SKIP DETECTION - Identify placeholder products
    # =========================================================================
    
    def should_skip_product(self, product: Dict) -> Tuple[bool, Optional[str]]:
        """
        Determine if a product should be skipped entirely.
        Returns (should_skip, reason)
        """
        title_en = (product.get('ProductTitleEN') or '').lower()
        title_fr = (product.get('ProductTitleFR') or '').lower()
        combined_title = f"{title_en} {title_fr}"
        
        # Check price threshold
        try:
            price = float(product.get('RegularPrice') or 0)
            max_price = self.skip_patterns.get('max_price_threshold', 0.05)
            if price <= max_price and price > 0:
                return True, f"Price ${price:.2f} below threshold ${max_price}"
        except (ValueError, TypeError):
            pass
        
        # Check English skip patterns
        for pattern in self.skip_patterns.get('title_patterns_en', []):
            if pattern.lower() in combined_title:
                return True, f"Matched skip pattern: '{pattern}'"
        
        # Check French skip patterns
        for pattern in self.skip_patterns.get('title_patterns_fr', []):
            if pattern.lower() in combined_title:
                return True, f"Matched skip pattern (FR): '{pattern}'"
        
        return False, None

    # =========================================================================
    # TEXT NORMALIZATION
    # =========================================================================
    
    def normalize_text(self, text: str) -> str:
        """Normalize text for keyword matching, stripping SKU/model noise"""
        if not text:
            return ""
        t = text.lower()
        # Replace common punctuation with space
        t = re.sub(r'[^\wà-ÿ\s]', ' ', t)
        # Collapse whitespace
        t = re.sub(r'\s+', ' ', t).strip()
        # Remove SKU/model noise patterns
        t = re.sub(r'\b[a-z]{1,10}[_-][a-z0-9\-_]{2,}\b', ' ', t)
        t = re.sub(r'\b[a-z]{0,4}\d{2,}\b', ' ', t)
        t = re.sub(r'\b\d{2,}[-_]\w+\b', ' ', t)
        t = re.sub(r'\s+', ' ', t).strip()
        return t

    # =========================================================================
    # KEYWORD MATCHING HELPERS
    # =========================================================================
    
    def match_keywords(self, text: str, keywords: List[str]) -> Tuple[bool, Optional[str]]:
        """Check if any keyword matches in the text"""
        if not text or not keywords:
            return False, None
        
        normalized_text = self.normalize_text(text)
        
        for keyword in keywords:
            if not keyword:
                continue
            normalized_keyword = keyword.lower().strip()
            
            if ' ' in normalized_keyword:
                # Exact phrase match
                if normalized_keyword in normalized_text:
                    return True, keyword
            else:
                # Word boundary match
                pattern = r'\b' + re.escape(normalized_keyword) + r'\b'
                if re.search(pattern, normalized_text):
                    return True, keyword
        
        return False, None

    def has_exclusions(self, text: str, exclusions: List[str]) -> Tuple[bool, Optional[str]]:
        """Check if any exclusion keyword matches"""
        return self.match_keywords(text, exclusions) if exclusions else (False, None)

    # =========================================================================
    # MAIN CATEGORIZATION LOGIC
    # =========================================================================
    
    def categorize_product(
        self,
        title_en: str,
        title_fr: str,
        desc_en: str,
        desc_fr: str,
        sku: str = '',
        jv_category: str = '',
        language: str = 'en'
    ) -> Dict:
        """
        Categorize a product using a two-tier approach:
        1. Try JohnnyVac's ProductCategory first (if available and mapped)
        2. Fall back to keyword matching for generic categories
        3. Use global_part_keywords as final fallback before "Needs Review"
        """
        
        # =====================================================================
        # TIER 1: Try JohnnyVac Category Mapping (Primary)
        # =====================================================================
        
        jv_category_clean = (jv_category or '').strip()
        
        if jv_category_clean and jv_category_clean in self.jv_mappings:
            mapping = self.jv_mappings[jv_category_clean]
            
            # If mapping exists (not null), use it directly
            if mapping is not None:
                return {
                    'product_type': mapping['productType'],
                    'handle': mapping['handle'],
                    'confidence': mapping.get('confidence', 'high'),
                    'matched_keyword': None,
                    'excluded_by': None,
                    'source': 'jv_category',
                    'jv_category': jv_category_clean,
                    'priority': 999,  # Highest priority for JV mappings
                    'reason': f"Mapped from JohnnyVac category: '{jv_category_clean}'"
                }
        
        # =====================================================================
        # TIER 2: Keyword Matching (Fallback for generic JV categories)
        # =====================================================================
        
        # Combine text for keyword matching
        if language == 'fr':
            raw_text = f"{title_fr or ''} {desc_fr or ''} {sku or ''}"
        else:
            raw_text = f"{title_en or ''} {desc_en or ''} {sku or ''}"
        
        if not raw_text.strip():
            return self._needs_review(
                reason='No title or description available',
                jv_category=jv_category_clean
            )
        
        normalized_text = self.normalize_text(raw_text)
        
        # Try each category in priority order
        for category in self.categories:
            # Skip fallback categories (we handle them specially)
            if category.get('priority', 0) <= 10:
                continue
            
            # Choose language-specific keywords/exclusions
            if language == 'fr':
                keywords = category.get('keywords_fr', [])
                exclusions = category.get('exclusions_fr', [])
            else:
                keywords = category.get('keywords_en', [])
                exclusions = category.get('exclusions_en', [])
            
            # Check exclusions first
            has_excl, excl_keyword = self.has_exclusions(normalized_text, exclusions)
            if has_excl:
                continue  # Skip this category if exclusion found
            
            # Check for keyword match
            matched, matched_keyword = self.match_keywords(normalized_text, keywords)
            if matched:
                # Determine confidence based on where match was found
                confidence = 'low'
                if language == 'en':
                    if matched_keyword.lower() in (title_en or '').lower():
                        confidence = 'high'
                    elif matched_keyword.lower() in (desc_en or '').lower():
                        confidence = 'medium'
                else:
                    if matched_keyword.lower() in (title_fr or '').lower():
                        confidence = 'high'
                    elif matched_keyword.lower() in (desc_fr or '').lower():
                        confidence = 'medium'
                
                return {
                    'product_type': category['productType'],
                    'handle': category['handle'],
                    'confidence': confidence,
                    'matched_keyword': matched_keyword,
                    'excluded_by': None,
                    'source': 'keyword_match',
                    'jv_category': jv_category_clean,
                    'priority': category.get('priority', 0),
                    'reason': f"Keyword '{matched_keyword}' matched (priority {category.get('priority', 0)})"
                }
        
        # =====================================================================
        # TIER 3: Global Part Keywords (Fallback to General Parts)
        # =====================================================================
        
        matched, kw = self.match_keywords(raw_text, self.global_part_keywords)
        if matched:
            return {
                'product_type': 'Parts & Replacement Parts > General Parts',
                'handle': 'parts-general',
                'confidence': 'low',
                'matched_keyword': kw,
                'excluded_by': None,
                'source': 'global_part_fallback',
                'jv_category': jv_category_clean,
                'priority': 10,
                'reason': f"Global part keyword '{kw}' matched (fallback)"
            }
        
        # =====================================================================
        # TIER 4: Nothing matched - Needs Review
        # =====================================================================
        
        return self._needs_review(
            reason='No keyword matches found',
            jv_category=jv_category_clean
        )

    def _needs_review(self, reason: str = 'No match', jv_category: str = '') -> Dict:
        """Return a 'Needs Review' categorization"""
        return {
            'product_type': 'Other > Needs Review',
            'handle': 'needs-review',
            'confidence': 'low',
            'matched_keyword': None,
            'excluded_by': None,
            'source': 'no_match',
            'jv_category': jv_category,
            'priority': 0,
            'reason': reason
        }

    # =========================================================================
    # BATCH PROCESSING
    # =========================================================================
    
    def batch_categorize(
        self,
        products: List[Dict],
        language: str = 'en',
        enforce_min_products: bool = True,
        skip_placeholders: bool = True
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Categorize a batch of products.
        
        Returns:
            Tuple of (categorized_products, skipped_products)
        """
        categorized = []
        skipped = []
        
        for product in products:
            # Check if should skip
            if skip_placeholders:
                should_skip, skip_reason = self.should_skip_product(product)
                if should_skip:
                    product['skip_reason'] = skip_reason
                    skipped.append(product)
                    continue
            
            # Categorize the product
            info = self.categorize_product(
                title_en=product.get('ProductTitleEN', ''),
                title_fr=product.get('ProductTitleFR', ''),
                desc_en=product.get('ProductDescriptionEN', ''),
                desc_fr=product.get('ProductDescriptionFR', ''),
                sku=product.get('SKU', ''),
                jv_category=product.get('ProductCategory', ''),
                language=language
            )
            product['category'] = info
            categorized.append(product)
        
        # Optionally enforce min_products threshold
        if enforce_min_products:
            categorized = self._enforce_min_products(categorized)
        
        return categorized, skipped

    def _enforce_min_products(self, categorized: List[Dict]) -> List[Dict]:
        """Demote categories with too few products to 'Needs Review'"""
        # Count products per category
        counts = Counter([p['category']['product_type'] for p in categorized])
        
        # Build min_products lookup
        min_map = {
            c['productType']: c.get('min_products', 1) 
            for c in self.categories
        }
        
        demoted = 0
        for product in categorized:
            pt = product['category']['product_type']
            if pt == 'Other > Needs Review':
                continue
            
            required = min_map.get(pt, 1)
            if counts.get(pt, 0) < required:
                original_pt = pt
                product['category'] = self._needs_review(
                    reason=f"Demoted: category '{original_pt}' has {counts.get(pt, 0)} products (min: {required})"
                )
                demoted += 1
        
        if demoted:
            print(f"  Demoted {demoted} products below min_products threshold")
        
        return categorized

    # =========================================================================
    # STATISTICS & REPORTING
    # =========================================================================
    
    def get_category_stats(self, categorized_products: List[Dict]) -> Dict:
        """Generate statistics about categorization results"""
        stats = {
            'total_products': len(categorized_products),
            'by_category': {},
            'by_confidence': {'high': 0, 'medium': 0, 'low': 0},
            'by_source': {},
            'needs_review_count': 0,
            'needs_review_percentage': 0.0
        }
        
        for product in categorized_products:
            cat_info = product.get('category', {})
            category = cat_info.get('product_type', 'Unknown')
            confidence = cat_info.get('confidence', 'low')
            source = cat_info.get('source', 'unknown')
            
            # Count by category
            stats['by_category'][category] = stats['by_category'].get(category, 0) + 1
            
            # Count by confidence
            if confidence in stats['by_confidence']:
                stats['by_confidence'][confidence] += 1
            
            # Count by source
            stats['by_source'][source] = stats['by_source'].get(source, 0) + 1
            
            # Count needs review
            if category == 'Other > Needs Review':
                stats['needs_review_count'] += 1
        
        if stats['total_products'] > 0:
            stats['needs_review_percentage'] = (
                stats['needs_review_count'] / stats['total_products']
            ) * 100
        
        return stats

    def export_needs_review(
        self, 
        categorized_products: List[Dict], 
        output_file: str = 'needs_review.csv'
    ):
        """Export products that need review to a CSV file"""
        needs_review = [
            p for p in categorized_products 
            if p.get('category', {}).get('product_type') == 'Other > Needs Review'
        ]
        
        if not needs_review:
            print("No products need review!")
            return
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'SKU', 'ProductTitleEN', 'ProductTitleFR',
                'JV_Category', 'Reason'
            ])
            writer.writeheader()
            for product in needs_review:
                writer.writerow({
                    'SKU': product.get('SKU', ''),
                    'ProductTitleEN': product.get('ProductTitleEN', ''),
                    'ProductTitleFR': product.get('ProductTitleFR', ''),
                    'JV_Category': product.get('ProductCategory', ''),
                    'Reason': product.get('category', {}).get('reason', '')
                })
        
        print(f"Exported {len(needs_review)} products to {output_file}")

    def export_skipped(
        self, 
        skipped_products: List[Dict], 
        output_file: str = 'skipped_products.csv'
    ):
        """Export skipped products to a CSV file"""
        if not skipped_products:
            print("No products were skipped!")
            return
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'SKU', 'ProductTitleEN', 'RegularPrice', 'Skip_Reason'
            ])
            writer.writeheader()
            for product in skipped_products:
                writer.writerow({
                    'SKU': product.get('SKU', ''),
                    'ProductTitleEN': product.get('ProductTitleEN', ''),
                    'RegularPrice': product.get('RegularPrice', ''),
                    'Skip_Reason': product.get('skip_reason', '')
                })
        
        print(f"Exported {len(skipped_products)} skipped products to {output_file}")


# =============================================================================
# CLI / TESTING
# =============================================================================

if __name__ == '__main__':
    import sys
    
    # Quick test
    categorizer = ProductCategorizer('category_map_v4.json')
    
    # Test cases
    test_products = [
        {
            'SKU': 'TEST001',
            'ProductTitleEN': 'HEPA Filter Replacement for XV-10',
            'ProductTitleFR': 'Filtre HEPA de remplacement pour XV-10',
            'ProductDescriptionEN': 'High quality HEPA filter',
            'ProductDescriptionFR': '',
            'ProductCategory': 'Filters',
            'RegularPrice': '19.99'
        },
        {
            'SKU': 'TEST002',
            'ProductTitleEN': 'Paper Vacuum Bag Pack of 6',
            'ProductTitleFR': 'Sac en papier paquet de 6',
            'ProductDescriptionEN': '',
            'ProductDescriptionFR': '',
            'ProductCategory': 'Vacuum Bags',
            'RegularPrice': '12.99'
        },
        {
            'SKU': 'TEST003',
            'ProductTitleEN': 'Motor Assembly Complete',
            'ProductTitleFR': 'Assemblage moteur complet',
            'ProductDescriptionEN': '',
            'ProductDescriptionFR': '',
            'ProductCategory': 'Johnny Vac Parts (*)',  # Generic - needs keyword matching
            'RegularPrice': '89.99'
        },
        {
            'SKU': 'TEST004',
            'ProductTitleEN': 'THIS PART IS RARELY ORDERED - Price will be adjusted',
            'ProductTitleFR': '',
            'ProductDescriptionEN': '',
            'ProductDescriptionFR': '',
            'ProductCategory': 'All Parts',
            'RegularPrice': '0.01'
        }
    ]
    
    print("Testing ProductCategorizer v4")
    print("=" * 60)
    
    categorized, skipped = categorizer.batch_categorize(
        test_products, 
        language='en',
        skip_placeholders=True
    )
    
    print(f"\nCategorized: {len(categorized)}")
    for p in categorized:
        cat = p['category']
        print(f"  {p['SKU']}: {cat['product_type']}")
        print(f"    Source: {cat['source']}, Confidence: {cat['confidence']}")
        print(f"    Reason: {cat['reason']}")
    
    print(f"\nSkipped: {len(skipped)}")
    for p in skipped:
        print(f"  {p['SKU']}: {p.get('skip_reason', 'Unknown')}")
    
    stats = categorizer.get_category_stats(categorized)
    print(f"\nStats:")
    print(f"  By source: {stats['by_source']}")
    print(f"  By confidence: {stats['by_confidence']}")
