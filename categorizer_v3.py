#!/usr/bin/env python3
# categorizer_v3.py
"""
ProductCategorizer v3
- Uses category_map_v3.json (settings.global_part_keywords)
- Strips SKU/model noise prior to matching
- Applies global part pre-check (short-circuit to Parts)
- Logs exclusion keywords and reasons
- Enforces min_products post-pass demotion to Needs Review
"""

import re
import json
from typing import Dict, List, Tuple, Optional
from collections import defaultdict, Counter


class ProductCategorizer:
    def __init__(self, category_rules_path: str):
        with open(category_rules_path, 'r', encoding='utf-8') as f:
            self.rules = json.load(f)
        # load settings
        settings = self.rules.get('settings', {})
        self.global_part_keywords = settings.get('global_part_keywords', [])
        # load categories sorted by priority desc
        self.categories = sorted(
            self.rules.get('categories', []),
            key=lambda x: x.get('priority', 0),
            reverse=True
        )

    # ----------------- text normalization & noise stripping -----------------
    def normalize_text(self, text: str) -> str:
        if not text:
            return ""
        t = text.lower()
        # replace common punctuation with space
        t = re.sub(r'[^\wà-ÿ\s]', ' ', t)
        # collapse whitespace
        t = re.sub(r'\s+', ' ', t).strip()
        # remove SKU / model noise patterns (e.g., simpli_B224-0500, JV202, PN600)
        t = re.sub(r'\b[a-z]{1,10}[_-][a-z0-9\-_]{2,}\b', ' ', t)   # prefix_suffix style
        t = re.sub(r'\b[a-z]{0,4}\d{2,}\b', ' ', t)                 # shortalpha + numbers or just numbers tokens like VC5000, JV202
        t = re.sub(r'\b\d{2,}[-_]\w+\b', ' ', t)
        t = re.sub(r'\s+', ' ', t).strip()
        return t

    # ----------------- keyword match helpers -----------------
    def match_keywords(self, text: str, keywords: List[str]) -> Tuple[bool, Optional[str]]:
        if not text or not keywords:
            return False, None
        normalized_text = self.normalize_text(text)
        for keyword in keywords:
            if not keyword:
                continue
            normalized_keyword = keyword.lower().strip()
            if ' ' in normalized_keyword:
                # exact phrase match
                if normalized_keyword in normalized_text:
                    return True, keyword
            else:
                # word boundary
                pattern = r'\b' + re.escape(normalized_keyword) + r'\b'
                if re.search(pattern, normalized_text):
                    return True, keyword
        return False, None

    def has_exclusions(self, text: str, exclusions: List[str]) -> Tuple[bool, Optional[str]]:
        return self.match_keywords(text, exclusions) if exclusions else (False, None)

    # ----------------- core categorization -----------------
    def categorize_product(
        self,
        title_en: str,
        title_fr: str,
        desc_en: str,
        desc_fr: str,
        sku: str = '',
        language: str = 'en'
    ) -> Dict:
        # combine text
        if language == 'fr':
            raw_text = f"{title_fr or ''} {desc_fr or ''} {sku or ''}"
        else:
            raw_text = f"{title_en or ''} {desc_en or ''} {sku or ''}"

        if not raw_text.strip():
            return self._needs_review(reason='No title or description available')

        # --------------- global part pre-check (highest priority) ---------------
        matched, kw = self.match_keywords(raw_text, self.global_part_keywords)
        if matched:
            return {
                'product_type': 'Parts & Replacement Parts > General Parts',
                'handle': 'parts-general',
                'confidence': 'high',
                'matched_keyword': kw,
                'excluded_by': None,
                'priority': 100,
                'reason': f"Global part keyword '{kw}' matched"
            }

        # Normalize once for further operations
        normalized_text = self.normalize_text(raw_text)

        # --------------- category loop (priority decided by JSON) ---------------
        for category in self.categories:
            # skip fallback category here (we handle at end)
            if category.get('priority', 0) == 0:
                continue

            # choose language-specific keywords/exclusions
            if language == 'fr':
                keywords = category.get('keywords_fr', [])
                exclusions = category.get('exclusions_fr', [])
            else:
                keywords = category.get('keywords_en', [])
                exclusions = category.get('exclusions_en', [])

            # check exclusions first (record exclusion)
            has_excl, excl_keyword = self.has_exclusions(normalized_text, exclusions)
            if has_excl:
                # record exclusion reason (so we can debug later)
                # continue to next category
                continue

            # check for keyword match
            matched, matched_keyword = self.match_keywords(normalized_text, keywords)
            if matched:
                # confidence: title match = high, desc match = medium
                confidence = 'low'
                # check if matched keyword appears in title (raw)
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
                    'excluded_by': excl_keyword,
                    'priority': category.get('priority', 0),
                    'reason': f"Matched '{matched_keyword}' (category priority {category.get('priority',0)})"
                }

        # nothing matched -> fallback
        return self._needs_review(reason='No keyword matches found')

    def _needs_review(self, reason: str = 'No match'):
        return {
            'product_type': 'Other > Needs Review',
            'handle': 'needs-review',
            'confidence': 'low',
            'matched_keyword': None,
            'excluded_by': None,
            'priority': 0,
            'reason': reason
        }

    # ----------------- batch helpers -----------------
    def batch_categorize(
        self,
        products: List[Dict],
        language: str = 'en',
        enforce_min_products: bool = True
    ) -> List[Dict]:
        categorized = []
        for product in products:
            info = self.categorize_product(
                title_en=product.get('ProductTitleEN', ''),
                title_fr=product.get('ProductTitleFR', ''),
                desc_en=product.get('ProductDescriptionEN', ''),
                desc_fr=product.get('ProductDescriptionFR', ''),
                sku=product.get('SKU', ''),
                language=language
            )
            product['category'] = info
            categorized.append(product)

        # Optionally enforce min_products: demote categories with too few items to Needs Review
        if enforce_min_products:
            # count category assignments (by productType)
            counts = Counter([p['category']['product_type'] for p in categorized])
            # map productType -> min_products from JSON
            min_map = {c['productType']: c.get('min_products', 1) for c in self.rules.get('categories', [])}
            # for categories below min, demote their products to Needs Review
            demoted = 0
            for product in categorized:
                pt = product['category']['product_type']
                if pt == 'Other > Needs Review':
                    continue
                required = min_map.get(pt, 1)
                if counts.get(pt, 0) < required:
                    # demote
                    product['category'] = self._needs_review(reason=f"Demoted: category '{pt}' below min_products ({required})")
                    demoted += 1
            if demoted:
                # optionally log count (caller sees through stats)
                pass

        return categorized

    def get_category_stats(self, categorized_products: List[Dict]) -> Dict:
        stats = {
            'total_products': len(categorized_products),
            'by_category': {},
            'by_confidence': {'high': 0, 'medium': 0, 'low': 0},
            'needs_review_count': 0,
            'needs_review_percentage': 0.0
        }

        for product in categorized_products:
            category = product['category']['product_type']
            confidence = product['category']['confidence']
            stats['by_category'][category] = stats['by_category'].get(category, 0) + 1
            stats['by_confidence'][confidence] = stats['by_confidence'].get(confidence, 0) + 1
            if category == 'Other > Needs Review':
                stats['needs_review_count'] += 1

        if stats['total_products'] > 0:
            stats['needs_review_percentage'] = (stats['needs_review_count'] / stats['total_products']) * 100

        return stats

    def export_needs_review(self, categorized_products: List[Dict], output_file: str = 'needs_review.csv'):
        import csv
        needs_review = [p for p in categorized_products if p['category']['product_type'] == 'Other > Needs Review']
        if not needs_review:
            print("No products need review!")
            return
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'SKU', 'ProductTitleEN', 'ProductTitleFR',
                'ProductDescriptionEN', 'ProductDescriptionFR',
                'Reason'
            ])
            writer.writeheader()
            for product in needs_review:
                writer.writerow({
                    'SKU': product.get('SKU', ''),
                    'ProductTitleEN': product.get('ProductTitleEN', ''),
                    'ProductTitleFR': product.get('ProductTitleFR', ''),
                    'ProductDescriptionEN': product.get('ProductDescriptionEN', ''),
                    'ProductDescriptionFR': product.get('ProductDescriptionFR', ''),
                    'Reason': product['category'].get('reason', '')
                })
        print(f"Exported {len(needs_review)} products to {output_file}")
