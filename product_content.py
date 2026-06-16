#!/usr/bin/env python3
# product_content.py
"""
Shared product content engine for Kingsway Janitorial.

Single source of truth for:
  - Brand detection (real vendor instead of hardcoded "JohnnyVac")
  - Product attribute extraction (pack qty, materials, dimensions, models)
  - Rich description generation (AI via Claude API when ANTHROPIC_API_KEY is
    set, with template fallback)
  - SEO meta title / meta description generation
  - Shopify Standard Product Taxonomy mapping (powers Google Merchant
    Center categorization and structured data)

Used by: sync_shopify_bulk_v3.py, description_generator.py, seo_generator.py
"""

import os
import re
import json
import html as html_lib
from datetime import datetime
from typing import Dict, List, Optional


def _log(msg, level='INFO'):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [{level}] {msg}", flush=True)


# =============================================================================
# BRANDS
# =============================================================================

BRANDS = [
    "JohnnyVac", "Johnny Vac", "Filter Queen", "Dirt Devil", "Big D",
    "Bissell", "Compact", "Dustbane", "Dyson", "Electrolux", "Eureka",
    "Fuller", "Ghibli", "Hoover", "Intervac", "Karcher", "Kenmore",
    "Kirby", "Maytag", "Miele", "Nilfisk", "Oreck", "Panasonic",
    "Perfect", "ProTeam", "Proteam", "Rainbow", "Riccar", "Roval",
    "Royal", "Samsung", "Sanitaire", "Sanyo", "Shark", "Simplicity",
    "Tennant", "Tristar", "Vortech", "Wirbel"
]

DEFAULT_VENDOR = "JohnnyVac"


def extract_brand(title: str) -> Optional[str]:
    """Extract the real brand from a product title. Returns None if unknown."""
    text = (title or '').lower()
    # "Perfect" is only a brand in specific contexts ("perfect for..." is not)
    if 'perfect' in text:
        if re.search(r'\bperfect\s+(vacuum|canister|upright|c\d+|pb\d+)', text, re.I):
            return "Perfect"
    for brand in BRANDS:
        if brand.lower() == 'perfect':
            continue
        if brand.lower() in text:
            return 'JohnnyVac' if 'johnny' in brand.lower() else brand
    return None


def compute_vendor(title: str) -> str:
    """Vendor for Shopify = detected brand, falling back to JohnnyVac."""
    return extract_brand(title) or DEFAULT_VENDOR


# =============================================================================
# ATTRIBUTE EXTRACTION
# =============================================================================

def extract_pack_quantity(title: str) -> Optional[int]:
    for p in [r'pack\s+of\s+(\d+)', r'(\d+)\s*-?\s*pack', r'box\s+of\s+(\d+)',
              r'(\d+)\s+bags', r'pk\s*(\d+)', r'(\d+)\s*pc']:
        m = re.search(p, (title or '').lower())
        if m:
            qty = int(m.group(1))
            if 1 < qty < 500:
                return qty
    return None


def extract_dimensions(title: str) -> Dict[str, str]:
    specs = {}
    t = (title or '').lower()
    m = re.search(r'(\d+(?:\.\d+)?)\s*(?:"|inch|in)\b', t)
    if m: specs['size_inches'] = m.group(1)
    m = re.search(r'(\d+(?:\.\d+)?)\s*mm\b', t)
    if m: specs['size_mm'] = m.group(1)
    m = re.search(r'(\d+)\s*(?:v|volt)\b', t)
    if m: specs['voltage'] = m.group(1)
    m = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:gal|gallon)', t)
    if m: specs['volume_gal'] = m.group(1).replace(',', '.')
    m = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:l\b|litre|liter)', t)
    if m: specs['volume_l'] = m.group(1).replace(',', '.')
    return specs


def extract_material(title: str) -> Optional[str]:
    materials = {'hepa': 'HEPA', 'microfiber': 'microfiber', 'cloth': 'cloth',
                 'paper': 'paper', 'foam': 'foam', 'rubber': 'rubber',
                 'nylon': 'nylon', 'stainless': 'stainless steel', 'vinyl': 'vinyl',
                 'latex': 'latex', 'nitrile': 'nitrile', 'cotton': 'cotton',
                 'washable': 'washable'}
    t = (title or '').lower()
    for k, v in materials.items():
        if k in t:
            return v
    return None


def extract_compatible_models(title: str, product_type: str = '') -> List[str]:
    pt = (product_type or '').lower()
    if any(x in pt for x in ['equipment', 'machine', 'chemical', 'consumable', 'tool', 'safety', 'ops']):
        return []
    models = []
    if extract_brand(title):
        for p in [r'\b([A-Z]{1,3}\d{3,6}[A-Z]?)\b', r'\b([A-Z]{2,4}-\d{2,4})\b']:
            for m in re.findall(p, title or ''):
                if m not in models and len(m) > 2 and (not m.isdigit() or int(m) >= 100):
                    models.append(m)
    return models[:2]


def extract_model_number(title: str, sku: str = '') -> List[str]:
    """Extract model/part numbers from title or SKU (used for SEO titles)."""
    models = []
    text = f"{title or ''} {sku or ''}"
    patterns = [
        r"#\s*(\d{5,})",
        r"\b(\d{6,})\b",
        r"model\s+([A-Z0-9-]+)",
        r"type\s+([A-Z0-9-]+)",
        r"style\s+([A-Z0-9-]+)",
        r"\b([A-Z]{1,3}\d{3,6}[A-Z]?)\b",
        r"\b([A-Z]{2,4}-\d{2,4})\b",
        r"#\s*(\d{3,})",
    ]
    for pattern in patterns:
        for match in re.findall(pattern, text, re.IGNORECASE):
            if match.upper() in ['AND', 'FOR', 'THE', 'WITH']:
                continue
            if len(match) < 2:
                continue
            if match.isdigit() and int(match) < 100:
                continue
            models.append(match.upper())
    seen = set()
    unique = []
    for m in sorted(models, key=len, reverse=True):
        if m not in seen:
            seen.add(m)
            unique.append(m)
    return unique[:3]


def extract_style_type(title: str) -> Optional[str]:
    for pattern in [r"style\s+([A-Z0-9-]+)", r"type\s+([A-Z0-9-]+)"]:
        m = re.search(pattern, title or '', re.IGNORECASE)
        if m:
            return m.group(0).title()
    return None


def detect_electrical_subtype(title: str) -> str:
    t = (title or '').lower()
    if any(w in t for w in ['motor', 'suction motor', 'brush motor', 'fan motor', 'armature']): return 'motor'
    if any(w in t for w in ['power cord', 'electrical cord', 'cord reel']): return 'power cord'
    if any(w in t for w in ['cord', 'pigtail', 'lead wire', 'wiring', 'cable']): return 'cord/wire'
    if any(w in t for w in ['switch', 'on/off', 'rocker', 'toggle', 'reset']): return 'switch'
    if any(w in t for w in ['relay', 'circuit', 'pcb', 'pc board', 'board']): return 'circuit board'
    if any(w in t for w in ['capacitor', 'transformer', 'solenoid']): return 'electrical component'
    if any(w in t for w in ['carbon brush', 'brush holder']): return 'carbon brush'
    if any(w in t for w in ['battery', 'charger']): return 'battery'
    if any(w in t for w in ['sensor', 'controller']): return 'sensor'
    if any(w in t for w in ['plug', 'terminal', 'connector']): return 'connector'
    return 'electrical part'


# =============================================================================
# TEMPLATE KEYS (maps Shopify productType / raw JV category -> template)
# =============================================================================

RAW_JV_TO_TEMPLATE = {
    "Vacuum Bags": "vacuum-bags", "Filters": "filters", "Vacuum Filters": "filters",
    "Vacuum Belts": "belts", "Motors, Carbone Brushes & Pumps": "motors",
    "Switches, Relays & Circuit Boards": "electrical", "Roller Brushes & Agitators": "brushes",
    "Brushes, Accessories & Adapters": "brushes", "Brushes & Dusters": "tools-brooms", "Brushes": "brushes",
    "Wand & Handles": "nozzles-wands", "Power & Air Nozzles": "nozzles-wands",
    "Canister & Upright Vacuum Hoses": "hoses", "Hoses for Central Vacuums": "hoses",
    "Universal Hoses for central vacuums": "hoses", "Hose Covers & Brackets": "hoses",
    "End Cuffs": "hoses", "Electrical Cord Winders": "electrical",
    "Tools, Bearings ": "parts-general", "Tools, Bearings": "parts-general",
    "Accessories & Extractor Tools": "nozzles-wands",
    "Installation Kits and Accessories for Central Vacuums": "parts-kits",
    "Accessories Kits for Central Vacuums": "parts-kits",
    "Kits and Accessories for Retractable Hoses": "parts-kits",
    "Gaskets, Seals & Valves": "parts-general", "Vacuum Parts": "parts-general",
    "All Parts": "parts-general", "Johnny Vac Parts (*)": "parts-general",
    "Odor Control": "chemicals", "Glass Cleaning": "chemicals",
    "Floor Maintenance Products": "chemicals", "Carpet Products": "chemicals",
    "Car Products": "chemicals", "General Purpose Products": "chemicals",
    "Kitchen Products": "chemicals", "Sanitizers": "chemicals",
    "Ecologo products": "chemicals", "Speciality Products": "chemicals",
    "Laundry Products": "chemicals", "Cleaners": "chemicals",
    "Paper Hand Towels & Bathroom Tissues": "consumables-paper",
    "Garbage bags": "consumables-paper", "Washroom Products": "consumables-paper",
    "Hygiene Products": "consumables-paper", "Paper distributors": "dispensers",
    "Floor Pads": "consumables-pads", "Floor Machine Pads": "consumables-pads",
    "Sponges & Pads": "tools",
    "Wet Mops, Dry Mops & Handles": "tools-mops", "Mops": "tools-mops",
    "Buckets & Wringers": "tools-mops", "Brooms & Dustpans": "tools-brooms",
    "Cloths, Rags & Dusters": "tools", "Soap Dispensers": "dispensers",
    "Bottles & Sprayers": "tools", "Garbage Pails": "tools", "Utility Carts": "tools",
    "Ashtrays": "tools", "General Maintenance": "tools",
    "Gloves & Masks": "safety", "Work Gloves": "safety",
    "Commercial Vacuums and Equipment ": "equipment-vacuum", "Central Vacuums": "equipment-central",
    "Steam Cleaners": "equipment", "Carpet Extractors": "equipment",
    "Autoscrubbers": "equipment", "Scrubbers": "equipment", "Floor Dryers": "equipment",
    "Floor Machines": "equipment", "Specialty Cleaning Equipment": "equipment",
    "Stick Vacuums": "equipment-vacuum", "Upright Vacuums": "equipment-vacuum",
    "Backpack Vacuums": "equipment-vacuum", "Canister Vacuums": "equipment-vacuum",
    "Wet & Dry Vacuums": "equipment-vacuum", "HEPA Specialized Vacuums": "equipment-vacuum",
    "Residential Vacuums": "equipment-vacuum", "Delivery and cleaning robots": "equipment",
    "Promotional products": "parts-general", "Johnny Vac Clearance & Overstock Products": "parts-general",
}


def get_template_key(product_type: str, title: str = '') -> str:
    if product_type in RAW_JV_TO_TEMPLATE:
        key = RAW_JV_TO_TEMPLATE[product_type]
        if key in ('motors', 'electrical'):
            sub = detect_electrical_subtype(title)
            if sub == 'motor': return 'motors'
            elif sub in ('power cord', 'cord/wire'): return 'electrical-cords'
            elif sub in ('switch', 'sensor', 'connector'): return 'electrical-switches'
            elif sub in ('circuit board', 'electrical component'): return 'electrical-boards'
            elif sub == 'carbon brush': return 'electrical-carbon-brushes'
            else: return 'electrical'
        return key
    pt = (product_type or '').lower()
    if 'vacuum bag' in pt: return 'vacuum-bags'
    elif 'filter' in pt: return 'filters'
    elif 'belt' in pt: return 'belts'
    elif 'hose' in pt or 'fitting' in pt: return 'hoses'
    elif 'motor' in pt or 'electrical' in pt:
        sub = detect_electrical_subtype(title)
        if sub == 'motor': return 'motors'
        elif sub in ('power cord', 'cord/wire'): return 'electrical-cords'
        elif sub in ('switch', 'sensor', 'connector'): return 'electrical-switches'
        else: return 'electrical'
    elif 'brush' in pt and 'agitator' in pt: return 'brushes'
    elif 'nozzle' in pt or 'wand' in pt: return 'nozzles-wands'
    elif 'kit' in pt: return 'parts-kits'
    elif 'chemical' in pt or 'solution' in pt: return 'chemicals'
    elif 'paper' in pt: return 'consumables-paper'
    elif 'floor pad' in pt: return 'consumables-pads'
    elif 'mop' in pt or 'bucket' in pt: return 'tools-mops'
    elif 'broom' in pt: return 'tools-brooms'
    elif 'dispenser' in pt: return 'dispensers'
    elif 'cleaning tool' in pt: return 'tools'
    elif 'glove' in pt or 'safety' in pt or 'ppe' in pt: return 'safety'
    elif 'equipment' in pt or ('vacuum' in pt and 'part' not in pt): return 'equipment-vacuum'
    return 'parts-general'


def get_product_noun(title: str, product_type: str, template_key: str) -> str:
    noun_map = {
        'vacuum-bags': 'vacuum bag', 'filters': 'filter', 'belts': 'drive belt',
        'hoses': 'vacuum hose', 'motors': 'vacuum motor', 'electrical': 'electrical component',
        'electrical-cords': 'power cord', 'electrical-switches': 'switch',
        'electrical-boards': 'circuit board', 'electrical-carbon-brushes': 'carbon brush',
        'brushes': 'brush roll', 'nozzles-wands': 'vacuum attachment',
        'equipment': 'cleaning machine', 'equipment-vacuum': 'vacuum cleaner',
        'equipment-central': 'central vacuum system', 'chemicals': 'cleaning product',
        'consumables-paper': 'paper product', 'consumables-pads': 'floor pad',
        'tools': 'cleaning tool', 'tools-mops': 'mop', 'tools-brooms': 'broom',
        'dispensers': 'dispenser', 'safety': 'safety product', 'parts-kits': 'accessory kit',
        'parts-general': 'component',
    }
    base = noun_map.get(template_key, 'product')
    if template_key == 'parts-general':
        t = (title or '').lower()
        for kw, noun in [('wheel', 'wheel'), ('caster', 'caster'), ('bearing', 'bearing'),
                         ('spring', 'spring'), ('latch', 'latch'), ('clip', 'clip'), ('seal', 'seal'),
                         ('gasket', 'gasket'), ('valve', 'valve'), ('handle', 'handle'), ('cover', 'cover'),
                         ('lid', 'lid'), ('tank', 'tank'), ('housing', 'housing'), ('bracket', 'bracket'),
                         ('bumper', 'bumper'), ('pedal', 'pedal'), ('lever', 'lever'), ('knob', 'knob'),
                         ('assembly', 'assembly'), ('pump', 'pump'), ('plate', 'plate'), ('axle', 'axle')]:
            if kw in t:
                return noun
    if template_key == 'chemicals':
        t = (title or '').lower()
        for kw, noun in [('deodorant', 'deodorant'), ('deodorizer', 'deodorizer'),
                         ('disinfectant', 'disinfectant'), ('sanitizer', 'sanitizer'), ('degreaser', 'degreaser'),
                         ('detergent', 'detergent'), ('cleaner', 'cleaner'), ('polish', 'polish'), ('wax', 'wax'),
                         ('stripper', 'floor stripper'), ('finish', 'floor finish'), ('shampoo', 'carpet shampoo'),
                         ('freshener', 'air freshener'), ('soap', 'soap'), ('lotion', 'lotion')]:
            if kw in t:
                return noun
    return base


# =============================================================================
# DESCRIPTION TEMPLATES (fallback when no Claude API key is configured)
# =============================================================================

TEMPLATES = {
    "vacuum-bags": {
        "openers": [
            "Keep your {brand_or}vacuum running at peak performance with {this_these} {material_adj}vacuum bag{s}.",
            "{Brand_or}Vacuum bag{s} designed for reliable filtration and easy installation.",
            "Maintain optimal suction power with {this_these} high-quality vacuum bag{s}{brand_for}.",
            "Effective dust and debris capture with {this_these} {material_adj}vacuum bag{s}{brand_for}.",
            "Dependable {brand_adj}vacuum bag{s} that keep{s_verb} your machine performing at its best.",
        ],
        "features": [
            "Engineered for efficient dust and debris capture, helping maintain strong suction while protecting the motor from fine particles.",
            "Designed for superior dust containment, reducing allergens and maintaining air quality during cleaning.",
            "{Material_sentence}Provides excellent filtration while allowing maximum airflow for powerful suction.",
            "Traps dust and fine particles effectively, extending the time between bag changes while maintaining consistent cleaning performance.",
        ],
        "pack_note": "This convenient {qty}-pack keeps you stocked for extended use.",
    },
    "filters": {
        "openers": [
            "Restore your vacuum's filtration performance with this {material_adj}filter{brand_for}.",
            "{Brand_or}Filter built for effective air filtration and long service life.",
            "Keep air quality high and suction strong with this {material_adj}filter{brand_for}.",
            "Effective {material_adj}filtration to keep your {brand_adj}vacuum performing at its best.",
            "Protect your vacuum motor and improve exhaust air quality with this {material_adj}filter{brand_for}.",
        ],
        "features": [
            "Traps fine dust, allergens, and microscopic particles to deliver cleaner exhaust air — important for commercial environments.",
            "Designed to capture fine particulates while maintaining optimal airflow, extending the life of your vacuum motor.",
            "{Material_sentence}Provides excellent particle capture without sacrificing suction power.",
            "Helps maintain a healthier indoor environment by trapping dust and allergens before they recirculate.",
        ],
    },
    "belts": {
        "openers": [
            "Get your vacuum's brush roll spinning again with this drive belt{brand_for}.",
            "{Brand_or}Drive belt designed for a secure fit and long-lasting performance.",
            "Restore agitator function with this durable vacuum belt{brand_for}.",
            "A properly tensioned belt means effective carpet cleaning — this {brand_adj}belt delivers.",
            "Replace your worn belt and restore full brush roll rotation{brand_for}.",
        ],
        "features": [
            "A worn belt causes loss of brush roll rotation and reduced cleaning effectiveness. Regular replacement maintains peak carpet cleaning performance.",
            "Manufactured for a precise fit, restoring proper brush roll tension and rotation speed for effective dirt pickup.",
            "Durable construction resists stretching and slipping, providing consistent agitator performance through heavy use.",
        ],
    },
    "hoses": {
        "openers": [
            "Replace your worn or damaged vacuum hose with this durable replacement{brand_for}.",
            "{Brand_or}Vacuum hose built for flexible, reliable suction transfer.",
            "Restore full suction power with this quality vacuum hose{brand_for}.",
            "Flexible and durable {brand_adj}hose designed for consistent airflow and easy handling.",
            "Maintain strong suction from tool to tank with this {brand_adj}vacuum hose.",
        ],
        "features": [
            "Flexible yet durable construction maintains strong suction while resisting kinks and cracks through heavy commercial use.",
            "Designed to deliver consistent airflow from floor tool to collection system, with connections that lock securely in place.",
            "Built to withstand the bending and stretching of daily commercial cleaning without losing suction performance.",
        ],
    },
    "motors": {
        "openers": [
            "Bring your vacuum back to life with this motor assembly{brand_for}.",
            "{Brand_or}Vacuum motor engineered for reliable suction power and extended service life.",
            "Restore original suction performance with this high-quality vacuum motor{brand_for}.",
            "Powerful {brand_adj}motor built for the demands of commercial cleaning.",
        ],
        "features": [
            "Precision-built to OEM specifications, delivering consistent suction power and reliable operation in demanding commercial environments.",
            "Designed for heavy-duty use, providing the airflow and suction your vacuum needs for effective cleaning.",
            "Engineered for long service life with sustained suction output, even during extended cleaning sessions.",
        ],
    },
    "electrical": {
        "openers": [
            "Keep your {brand_or}vacuum's electrical system in top shape with this {noun}.",
            "{Brand_or}Reliable {noun} for vacuum electrical system maintenance.",
            "Restore proper electrical function with this quality {noun}{brand_for}.",
            "Dependable {brand_adj}{noun} built for vacuum electrical system repair.",
        ],
        "features": [
            "Manufactured to OEM specifications for reliable electrical performance and safe operation.",
            "Quality construction ensures dependable performance, restoring proper function to your vacuum's electrical system.",
            "Designed for a precise fit, providing reliable electrical connections for safe and consistent operation.",
        ],
    },
    "electrical-cords": {
        "openers": [
            "Replace your damaged or worn power cord with this {brand_adj}vacuum cord.",
            "{Brand_or}Power cord built for safe, reliable power delivery to your vacuum.",
            "Restore safe power delivery with this durable cord{brand_for}.",
            "Heavy-duty {brand_adj}cord designed for the demands of commercial vacuum use.",
        ],
        "features": [
            "Durable insulation and secure connectors ensure reliable power delivery and safe operation during daily commercial use.",
            "Built to withstand the flexing and pulling of everyday vacuum use without fraying or connection issues.",
        ],
    },
    "electrical-switches": {
        "openers": [
            "Restore your vacuum's controls with this {brand_adj}{noun}.",
            "{Brand_or}{Noun_cap} — get your vacuum's controls functioning properly again.",
            "Replace a faulty {noun} and restore proper vacuum operation{brand_for}.",
        ],
        "features": [
            "Manufactured for a precise fit and reliable switching action, restoring proper control to your vacuum.",
            "Quality construction provides consistent, dependable operation through thousands of switching cycles.",
        ],
    },
    "electrical-boards": {
        "openers": [
            "Restore your vacuum's electronic controls with this {brand_adj}{noun}.",
            "{Brand_or}{Noun_cap} for reliable vacuum electronic system repair.",
            "Get your vacuum's electronics back on track with this quality {noun}{brand_for}.",
        ],
        "features": [
            "Precision electronic component designed to restore proper control and function to your vacuum's operating system.",
            "Manufactured to OEM specifications for reliable electronic performance and proper system integration.",
        ],
    },
    "electrical-carbon-brushes": {
        "openers": [
            "Restore motor performance with {this_these} {brand_adj}carbon brush{s}.",
            "{Brand_or}Carbon brush{s} — essential for maintaining vacuum motor performance.",
            "Keep your vacuum motor running smoothly with {this_these} carbon brush{s}{brand_for}.",
        ],
        "features": [
            "Carbon brushes wear over time and need periodic replacement to maintain motor efficiency. Fresh brushes restore proper electrical contact and motor performance.",
            "Quality carbon construction provides consistent motor contact for reliable suction power.",
        ],
    },
    "brushes": {
        "openers": [
            "Restore your vacuum's agitation performance with this brush roll{brand_for}.",
            "{Brand_or}Brush roll designed for effective carpet agitation and debris pickup.",
            "Get deep-cleaning performance back with this quality brush{brand_for}.",
            "Effective carpet agitation starts with a good brush roll — this {brand_adj}brush delivers.",
        ],
        "features": [
            "Bristle pattern designed to agitate carpet fibres effectively, loosening embedded dirt for better suction pickup.",
            "Durable bristles maintain their shape through extended commercial use, providing consistent carpet agitation and edge cleaning.",
        ],
    },
    "nozzles-wands": {
        "openers": [
            "Extend your vacuum's reach and versatility with this {noun}{brand_for}.",
            "{Brand_or}{Noun_cap} for flexible cleaning access in tight spaces and varied surfaces.",
            "Enhance your vacuum's cleaning capability with this quality {noun}{brand_for}.",
            "Reach every corner and surface with this versatile {brand_adj}{noun}.",
        ],
        "features": [
            "Durable construction and secure connections ensure consistent suction transfer for effective cleaning of floors, upholstery, and hard-to-reach areas.",
            "Designed for everyday commercial use, providing reliable reach and manoeuvrability across different cleaning tasks.",
        ],
    },
    "equipment-vacuum": {
        "openers": [
            "{Brand_or}{Noun_cap} built for commercial cleaning performance.",
            "Professional-grade {noun} designed for demanding janitorial environments.",
            "Tackle commercial cleaning jobs with this {brand_adj}{noun}.",
            "{Brand_or}{Noun_cap} — professional cleaning power for commercial spaces.",
        ],
        "features": [
            "Built for the rigours of daily commercial use, delivering consistent performance across large floor areas and high-traffic environments.",
            "Professional-grade construction meets the demands of commercial cleaning — from office buildings to industrial facilities.",
        ],
    },
    "equipment-central": {
        "openers": [
            "{Brand_or}Central vacuum system for whole-building cleaning convenience.",
            "Professional {brand_adj}central vacuum system designed for commercial installation.",
            "Powerful central vacuum system delivering whole-building suction{brand_for}.",
        ],
        "features": [
            "Central vacuum systems provide powerful, quiet cleaning with the convenience of wall-mounted inlets throughout the building.",
            "Delivers strong, consistent suction through a permanent piping system, with simple inlet connections on every floor.",
        ],
    },
    "equipment": {
        "openers": [
            "{Brand_or}{Noun_cap} built for commercial cleaning performance.",
            "Professional-grade {noun} designed for demanding janitorial environments.",
            "Tackle tough jobs with this {brand_adj}{noun}.",
        ],
        "features": [
            "Built for the rigours of daily commercial use, delivering consistent performance in high-traffic environments.",
            "Professional-grade construction meets the demands of commercial cleaning operations.",
        ],
    },
    "chemicals": {
        "openers": [
            "{Brand_or}{Noun_cap} formulated for professional cleaning results.",
            "Professional-strength {noun} for commercial janitorial applications.",
            "Achieve professional cleaning results with this {brand_adj}{noun}.",
            "Commercial-grade {brand_adj}{noun} for effective, reliable results.",
            "{Brand_or}{Noun_cap} — professional cleaning performance for commercial environments.",
        ],
        "features": [
            "Formulated for commercial environments, delivering effective results suitable for daily professional use.",
            "Concentrated formula provides excellent coverage and value for commercial cleaning operations.",
            "Professional formulation balances cleaning effectiveness with practical, everyday usability.",
        ],
    },
    "consumables-paper": {
        "openers": [
            "{Brand_or}{Noun_cap} for commercial washroom and facility maintenance.",
            "Stock your facility with quality {noun} built for high-traffic environments.",
            "Professional-grade {noun} designed for busy commercial facilities.",
        ],
        "features": [
            "Designed for high-traffic commercial environments, providing reliable performance and value in daily facility operations.",
            "Built to meet the demands of busy washrooms, kitchens, and commercial spaces without frequent restocking.",
        ],
    },
    "consumables-pads": {
        "openers": [
            "{Brand_or}{Noun_cap} for floor machine maintenance and refinishing.",
            "Professional {brand_adj}floor pad for buffing, scrubbing, or stripping applications.",
            "Keep your floors looking their best with this {brand_adj}floor pad.",
        ],
        "features": [
            "Designed for consistent performance on commercial floor machines, providing effective scrubbing, buffing, or stripping action.",
            "Durable pad construction maintains its properties through extended use on large floor areas.",
        ],
    },
    "tools": {
        "openers": [
            "{Brand_or}{Noun_cap} for efficient commercial cleaning operations.",
            "Equip your cleaning crew with this professional-grade {noun}.",
            "Professional {noun} built for daily commercial cleaning use.",
            "{Brand_or}{Noun_cap} — practical and durable for everyday janitorial work.",
        ],
        "features": [
            "Durable construction withstands the demands of daily commercial cleaning, providing reliable performance and long service life.",
            "Designed for professional janitorial use, combining practical ergonomics with sturdy build quality.",
        ],
    },
    "tools-mops": {
        "openers": [
            "{Brand_or}{Noun_cap} for efficient commercial floor maintenance.",
            "Keep floors clean with this professional-grade {brand_adj}{noun}.",
            "{Brand_or}{Noun_cap} built for the demands of daily commercial floor care.",
        ],
        "features": [
            "Designed for efficient cleaning of hard floors in commercial environments — from lobbies and corridors to kitchens and warehouses.",
            "Durable construction stands up to daily commercial mopping with consistent performance over time.",
        ],
    },
    "tools-brooms": {
        "openers": [
            "{Brand_or}{Noun_cap} for quick, effective floor sweeping.",
            "Keep floors debris-free with this commercial-grade {brand_adj}{noun}.",
        ],
        "features": [
            "Durable bristles and sturdy handle construction handle daily commercial sweeping duties with ease.",
            "Designed for efficient debris pickup across hard floors, from smooth tile to rough concrete.",
        ],
    },
    "dispensers": {
        "openers": [
            "{Brand_or}{Noun_cap} for commercial washroom or kitchen installation.",
            "Professional {brand_adj}{noun} built for high-traffic commercial environments.",
            "Equip your facility with this durable, easy-to-refill {brand_adj}{noun}.",
        ],
        "features": [
            "Designed for high-traffic commercial washrooms and kitchens, with durable construction and easy refill access.",
            "Wall-mounted design keeps products accessible and hygienic while minimizing counter clutter.",
        ],
    },
    "safety": {
        "openers": [
            "{Brand_or}{Noun_cap} for workplace safety compliance.",
            "Protect your team with this quality {noun} for commercial environments.",
            "Professional-grade {noun} meeting workplace safety standards.",
        ],
        "features": [
            "Provides reliable protection for cleaning and maintenance personnel in commercial environments.",
            "Designed for comfort during extended wear while meeting professional safety requirements.",
        ],
    },
    "parts-kits": {
        "openers": [
            "{Brand_or}{Noun_cap} — everything you need for installation or repair in one package.",
            "Simplify your repair or installation with this complete {brand_adj}{noun}{brand_for}.",
            "{Brand_or}Complete {noun} with all the components you need{brand_for}.",
        ],
        "features": [
            "Includes all necessary components for a complete installation or repair, saving time sourcing individual parts.",
            "Bundled components are matched for compatibility, ensuring a proper fit and reliable performance.",
        ],
    },
    "parts-general": {
        "openers": [
            "Keep your equipment running with this quality {noun}{brand_for}.",
            "{Brand_or}{Noun_cap} — a reliable component for your cleaning equipment.",
            "Maintain your {brand_adj}cleaning equipment with this durable {noun}.",
            "Quality {brand_adj}{noun} built for vacuum and cleaning equipment maintenance.",
        ],
        "features": [
            "Manufactured for a proper fit and reliable performance, helping extend the service life of your cleaning equipment.",
            "Quality construction ensures this component performs reliably in demanding commercial cleaning environments.",
            "Designed as a direct replacement, restoring proper function to your cleaning equipment.",
        ],
    },
}

CANADIAN_CTAS = [
    "Available from Kingsway Janitorial with fast shipping across Canada.",
    "Order from Kingsway Janitorial — proudly serving Canadian businesses from Vancouver.",
    "Ships quickly across Canada from Kingsway Janitorial, Vancouver's trusted janitorial supplier.",
    "Kingsway Janitorial — supplying Canadian businesses with professional cleaning products since 1990.",
    "In stock and ready to ship across Canada. Serving Vancouver, Montreal, and everywhere in between.",
    "Fast Canadian shipping from Kingsway Janitorial, your professional cleaning supply partner.",
    "Available for delivery across Canada — from Vancouver to Montreal and coast to coast.",
    "Trusted by Canadian businesses coast to coast. Order from Kingsway Janitorial today.",
    "Shop with confidence at Kingsway Janitorial — serving Canadian businesses for over 35 years.",
    "Kingsway Janitorial: Vancouver-based, shipping Canada-wide. Professional supplies since 1990.",
]


def strip_html(html_str: str) -> str:
    return re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', html_str or '')).strip()


def generate_description_template(title: str, product_type: str, sku: str = '') -> str:
    """Template-based rich description (deterministic per SKU)."""
    brand = extract_brand(title)
    pack_qty = extract_pack_quantity(title)
    material = extract_material(title)
    specs = extract_dimensions(title)
    template_key = get_template_key(product_type, title)
    product_noun = get_product_noun(title, product_type, template_key)
    models = extract_compatible_models(title, product_type)

    is_plural = pack_qty and pack_qty > 1
    material_sentence = f"{material.capitalize()} construction " if material else ''

    tmpl = {
        'brand_or': f'{brand} ' if brand else '',
        'Brand_or': f'{brand} ' if brand else '',
        'brand_for': f' for {brand} vacuums' if brand else '',
        'brand_adj': f'{brand} ' if brand else '',
        'this_these': 'these' if is_plural else 'this',
        'material_adj': f'{material} ' if material else '',
        'Material_sentence': material_sentence,
        's': 's' if is_plural else '',
        's_verb': '' if is_plural else 's',
        'noun': product_noun,
        'Noun_cap': product_noun.capitalize() if product_noun else 'Component',
        'qty': str(pack_qty) if pack_qty else '',
        'models': ', '.join(models[:2]) if models else '',
    }

    templates = TEMPLATES.get(template_key, TEMPLATES['parts-general'])
    h = abs(hash(sku)) if sku else abs(hash(title))
    sentences = []

    openers = templates.get('openers', TEMPLATES['parts-general']['openers'])
    try:
        sentences.append(openers[h % len(openers)].format(**tmpl))
    except (KeyError, IndexError):
        sentences.append(f"{product_noun.capitalize()}{tmpl['brand_for']}.")

    features = templates.get('features', TEMPLATES['parts-general']['features'])
    try:
        feat = features[(h + 1) % len(features)].format(**tmpl)
        feat = re.sub(r'\s+', ' ', feat).strip()
        if feat and feat[0].islower():
            feat = feat[0].upper() + feat[1:]
        sentences.append(feat)
    except (KeyError, IndexError):
        sentences.append("Quality construction for reliable performance in commercial environments.")

    if pack_qty and pack_qty > 1 and 'pack_note' in templates:
        sentences.append(templates['pack_note'].format(**tmpl))

    if models:
        sentences.append(f"Compatible with {', '.join(models[:2])} — verify your model for proper fitment.")

    if template_key == 'chemicals':
        vp = []
        if specs.get('volume_gal'): vp.append(f"{specs['volume_gal']} gal")
        if specs.get('volume_l'): vp.append(f"{specs['volume_l']} L")
        if vp:
            sentences.append(f"Available in {' / '.join(vp)} size.")

    sentences.append(CANADIAN_CTAS[h % len(CANADIAN_CTAS)])

    mid = max(2, len(sentences) // 2)
    p1 = re.sub(r'\s+', ' ', ' '.join(sentences[:mid])).strip()
    p2 = re.sub(r'\s+', ' ', ' '.join(sentences[mid:])).strip()
    return f"<p>{p1}</p>\n<p>{p2}</p>" if p2 else f"<p>{p1}</p>"


# =============================================================================
# AI DESCRIPTIONS (Claude API — used when ANTHROPIC_API_KEY is set)
# =============================================================================

AI_MODEL = os.environ.get('ANTHROPIC_MODEL', 'claude-opus-4-8')
AI_BATCH_SIZE = 10

AI_SYSTEM_PROMPT = """You write product descriptions for Kingsway Janitorial, a Vancouver-based commercial cleaning and janitorial supply store that ships across Canada. The catalog is vacuum parts (bags, filters, belts, motors, hoses), cleaning chemicals, janitorial tools, and commercial cleaning equipment, much of it distributed by JohnnyVac.

For each product you receive, write a unique, natural product description as exactly two short paragraphs (70-130 words total):
- Paragraph 1: what the product is and the problem it solves for the buyer.
- Paragraph 2: practical details (fit, use, durability) and end with a short sentence about availability from Kingsway Janitorial with shipping across Canada. Vary the wording of this closing line between products.

Rules:
- Use ONLY facts present in the provided title, brand, and category. Never invent dimensions, materials, certifications, pack counts, or compatibility claims.
- If a brand or model number appears in the title, mention it naturally (helps customers searching for that part).
- Write for commercial/professional buyers; plain, confident language. No hype words like "revolutionary" or "best-ever", no exclamation marks, no keyword stuffing.
- Vary sentence structure and openings across products so descriptions don't read as copies of each other.
- Plain text only in each paragraph (no HTML, no markdown, no lists)."""

AI_OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "products": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "sku": {"type": "string"},
                    "paragraph1": {"type": "string"},
                    "paragraph2": {"type": "string"},
                },
                "required": ["sku", "paragraph1", "paragraph2"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["products"],
    "additionalProperties": False,
}


def ai_available() -> bool:
    return bool(os.environ.get('ANTHROPIC_API_KEY'))


def generate_descriptions_ai(items: List[Dict], model: str = None,
                             batch_size: int = AI_BATCH_SIZE) -> Dict[str, str]:
    """
    Generate unique descriptions with the Claude API.

    items: list of {'sku', 'title', 'product_type'} dicts.
    Returns {sku: description_html}. Items that fail are simply absent from
    the result — callers fall back to generate_description_template().
    """
    if not items or not ai_available():
        return {}

    try:
        import anthropic
    except ImportError:
        _log("anthropic package not installed — falling back to templates", 'WARNING')
        return {}

    client = anthropic.Anthropic()
    model = model or AI_MODEL
    results: Dict[str, str] = {}
    total_batches = (len(items) + batch_size - 1) // batch_size

    for batch_num, start in enumerate(range(0, len(items), batch_size), 1):
        chunk = items[start:start + batch_size]
        product_lines = []
        for it in chunk:
            brand = extract_brand(it.get('title', '')) or 'unknown'
            product_lines.append(
                f"- SKU: {it['sku']} | Title: {it.get('title', '')} | "
                f"Brand: {brand} | Category: {it.get('product_type', '')}"
            )
        user_msg = (
            "Write descriptions for these products:\n" + "\n".join(product_lines)
        )

        try:
            response = client.messages.create(
                model=model,
                max_tokens=8000,
                system=AI_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_msg}],
                output_config={"format": {"type": "json_schema", "schema": AI_OUTPUT_SCHEMA}},
            )
            if response.stop_reason == "refusal":
                _log(f"AI batch {batch_num}/{total_batches}: request refused, using templates", 'WARNING')
                continue
            text = next((b.text for b in response.content if b.type == "text"), "")
            data = json.loads(text)
            wanted_skus = {it['sku'] for it in chunk}
            for prod in data.get('products', []):
                sku = prod.get('sku', '')
                if sku not in wanted_skus:
                    continue
                p1 = html_lib.escape((prod.get('paragraph1') or '').strip(), quote=False)
                p2 = html_lib.escape((prod.get('paragraph2') or '').strip(), quote=False)
                if len(p1) + len(p2) < 60:
                    continue
                results[sku] = f"<p>{p1}</p>\n<p>{p2}</p>" if p2 else f"<p>{p1}</p>"
        except Exception as e:
            _log(f"AI batch {batch_num}/{total_batches} failed ({e}) — templates will be used", 'WARNING')
            continue

        if batch_num % 10 == 0 or batch_num == total_batches:
            _log(f"  AI descriptions: batch {batch_num}/{total_batches} ({len(results)} generated)")

    return results


def build_description(title: str, product_type: str, sku: str = '',
                      jv_desc_html: str = '', ai_desc: str = None,
                      min_length: int = 80) -> str:
    """
    Pick the best available description:
      1. JohnnyVac's own description when it has real content (factual specs)
      2. AI-generated description (when provided)
      3. Template-generated description
    """
    if len(strip_html(jv_desc_html)) >= min_length:
        return jv_desc_html
    if ai_desc:
        return ai_desc
    return generate_description_template(title, product_type, sku)


# =============================================================================
# SEO META TITLE / DESCRIPTION
# =============================================================================

MAX_SEO_TITLE = 60
MAX_SEO_DESCRIPTION = 160

PRODUCT_TYPE_PRIORITY = [
    "machines", "chemicals", "wands", "hoses", "motors", "bags", "filters",
    "brushes", "belts", "cords", "wheels", "attachments", "parts",
]

PRODUCT_PATTERNS = {
    "machines": [
        r"\bcentral\s+vacuum", r"\bvacuum\s+cleaner\b",
        r"\bcanister\b(?!\s*(bag|filter))", r"\bupright\b(?!\s*(bag|filter))",
        r"\bbackpack\b(?!\s*(bag|filter))", r"\bcommercial\s+vacuum",
        r"\bwet.*dry\b", r"\bextractor\b(?!\s*bag)",
        r"\bscrubber\b", r"\bsweeper\b", r"\bpolisher\b", r"\bburnisher\b"
    ],
    "chemicals": [
        r"\bdetergent\b", r"\bdegreaser\b", r"\bsanitizer\b", r"\bdisinfectant\b",
        r"\bglass\s+cleaner", r"\bfloor\s+cleaner", r"\bcleaning\s+solution",
        r"\bpolish\b(?!\s*brush)", r"\bsprayway\b", r"\bfabric\s+cleaner",
        r"\bleather\s+cleaner", r"\bvinyl\s+cleaner", r"\bneutral\s+cleaner",
        r"\ball\s+purpose\s+cleaner", r"\blotion\b", r"\bantibacterial\b",
        r"\bhand\s+soap", r"\bsoap\b",
    ],
    "wands": [r"\bwand[s]?\b", r"\btelescopic\b", r"\bextension\s+wand", r"\btelescop\b"],
    "hoses": [r"\bhose[s]?\b", r"\bstretch\s+hose", r"\bextension\s+hose", r"\bcrushproof\b"],
    "motors": [
        r"\bmotor\b(?!\s*filter)", r"\bsuction\s+motor", r"\bbrush\s+motor",
        r"\barmature\b", r"\btangential\s+vacuum\s+motor"
    ],
    "bags": [
        r"\bvacuum\s+bag", r"\bdust\s+bag", r"\bpaper\s+bag",
        r"\bhepa\s+bag", r"\bmicrofilter\s+bag", r"\bbag[s]?\s+for\b",
        r"\bpack\s+of\s+\d+\s+bag", r"^bags?\s+hepa", r"\bbags?\b.*\bvacuum",
    ],
    "filters": [
        r"\bmicrofilter\b(?!\s*bag)", r"\bhepa\s+filter", r"\bmotor\s+filter",
        r"\bexhaust\s+filter", r"\bpre-?filter\b", r"\bpost-?filter\b",
        r"\bfoam\s+filter", r"\bfilter\b(?!\s*(bag|queen))",
    ],
    "brushes": [
        r"\bbrush\s*roll", r"\broller\s+brush", r"\bagitator\b", r"\bbeater\s+bar",
        r"\bnylon\s+brush", r"\bcarbon\s+brush", r"\bcomplete\s+roller\s+brush",
        r"\bbrush\b(?!\s*(motor|belt))",
    ],
    "belts": [
        r"\bdrive\s+belt", r"\bflat\s+belt", r"\bround\s+belt",
        r"\bvacuum\s+belt", r"\breplacement\s+belt",
        r"(?<!\w)belt[s]?(?!\s*(clip|brush))\b",
    ],
    "cords": [
        r"\bpower\s+cord", r"\belectrical\s+cord",
        r"\bcord\s+\d+[\'\"]\b", r"\b\d+[\'\"]\s*cord\b",
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

CATEGORY_SEO = {
    "bags": {"suffix": "Vacuum Bags", "descriptors": ["replacement bags", "commercial grade", "quality filtration"]},
    "filters": {"suffix": "Filter Replacement", "descriptors": ["replacement filter", "commercial grade", "premium filtration"]},
    "belts": {"suffix": "Belt Replacement", "descriptors": ["replacement belt", "durable construction", "OEM compatible"]},
    "brushes": {"suffix": "Brush Replacement", "descriptors": ["replacement brush", "commercial grade", "durable bristles"]},
    "hoses": {"suffix": "Hose Replacement", "descriptors": ["replacement hose", "flexible construction", "durable material"]},
    "cords": {"suffix": "Power Cord", "descriptors": ["replacement cord", "heavy-duty", "commercial grade"]},
    "motors": {"suffix": "Motor Replacement", "descriptors": ["replacement motor", "commercial grade", "high performance"]},
    "wheels": {"suffix": "Wheel Replacement", "descriptors": ["replacement wheel", "durable construction", "smooth rolling"]},
    "wands": {"suffix": "Wand Attachment", "descriptors": ["extension wand", "durable construction", "commercial grade"]},
    "attachments": {"suffix": "Vacuum Attachment", "descriptors": ["vacuum tool", "versatile cleaning", "commercial grade"]},
    "parts": {"suffix": "Replacement Part", "descriptors": ["replacement part", "OEM compatible", "quality construction"]},
    "machines": {"suffix": "Commercial Vacuum", "descriptors": ["commercial vacuum", "professional grade", "heavy-duty"]},
    "chemicals": {"suffix": "Cleaning Solution", "descriptors": ["professional cleaner", "commercial grade", "effective formula"]},
}


def truncate_text(text, max_length, use_ellipsis=False):
    if len(text) <= max_length:
        return text
    truncated = text[:max_length].rsplit(' ', 1)[0]
    if len(truncated) < max_length * 0.6:
        truncated = text[:max_length - 3] if use_ellipsis else text[:max_length]
    if use_ellipsis and len(truncated) < len(text):
        return truncated.rstrip('.') + "..."
    return truncated


def detect_seo_product_type(title: str, description: str = "") -> str:
    text = f"{title or ''} {description or ''}".lower()

    if re.search(r"\bhose[s]?\b", text, re.IGNORECASE):
        return "hoses"
    if re.search(r"\bbags?\s+(for\s+)?central\s+vacuum", text, re.IGNORECASE):
        return "bags"
    if re.search(r"\bbags?\s+for\s+.*\bcentral\b", text, re.IGNORECASE):
        return "bags"

    machine_definitive = [
        r"\bcentral\s+vacuum\b", r"\bvacuum\s+cleaner\b", r"\bcommercial\s+vacuum\b",
        r"\bwet.*dry\b", r"\bextractor\b(?!\s*bag)",
        r"\bscrubber\b", r"\bsweeper\b", r"\bpolisher\b", r"\bburnisher\b"
    ]
    for pattern in machine_definitive:
        if re.search(pattern, text, re.IGNORECASE):
            return "machines"

    for category in PRODUCT_TYPE_PRIORITY:
        if category in ("machines", "hoses"):
            continue
        patterns = PRODUCT_PATTERNS.get(category, [])
        matches = sum(1 for p in patterns if re.search(p, text, re.IGNORECASE))
        if matches >= 2:
            return category

    for category in PRODUCT_TYPE_PRIORITY:
        if category in ("machines", "hoses"):
            continue
        for pattern in PRODUCT_PATTERNS.get(category, []):
            if re.search(pattern, text, re.IGNORECASE):
                return category

    return "parts"


def generate_seo_title(title: str, sku: str = '') -> str:
    """Generate an SEO meta title (max 60 chars)."""
    brand = extract_brand(title)
    product_type = detect_seo_product_type(title)
    pack_qty = extract_pack_quantity(title)
    models = extract_model_number(title, sku)
    style_type = extract_style_type(title)
    category_info = CATEGORY_SEO.get(product_type, CATEGORY_SEO["parts"])

    parts = []
    if brand:
        parts.append(brand)
    if style_type:
        parts.append(style_type)
    elif models:
        parts.append(models[0])
    parts.append(category_info["suffix"])
    if pack_qty and pack_qty > 1:
        parts.append(f"{pack_qty} Pack")

    seo_title = " ".join(parts)

    if len(seo_title) > MAX_SEO_TITLE:
        parts = []
        if brand:
            parts.append(brand)
        parts.append(category_info["suffix"])
        seo_title = " ".join(parts)

    if len(seo_title) < 25:
        if not brand and not seo_title.lower().startswith("commercial"):
            seo_title = f"Commercial {seo_title}"
        if len(seo_title) < 25:
            seo_title = f"{seo_title} | Vacuum Part"
        if len(seo_title) < 25 and models:
            seo_title = f"{seo_title} {models[0]}"

    if len(seo_title) > MAX_SEO_TITLE or len(seo_title) < 20:
        clean_title = re.sub(r'\s+', ' ', title or '').strip()
        clean_title = re.sub(r'\b(vacuum|for|and|the|pack of \d+)\b', '', clean_title, flags=re.IGNORECASE)
        clean_title = re.sub(r'\s+', ' ', clean_title).strip()
        clean_title = truncate_text(clean_title, MAX_SEO_TITLE - 15)
        seo_title = f"{clean_title} | Replacement"

    return truncate_text(seo_title, MAX_SEO_TITLE)


def generate_seo_description(title: str, sku: str = '') -> str:
    """Generate an SEO meta description (max 160 chars)."""
    brand = extract_brand(title)
    product_type = detect_seo_product_type(title)
    pack_qty = extract_pack_quantity(title)
    models = extract_model_number(title, sku)
    style_type = extract_style_type(title)
    category_info = CATEGORY_SEO.get(product_type, CATEGORY_SEO["parts"])

    if brand:
        opener = f"{brand} {category_info['suffix'].lower()}"
    else:
        suffix_lower = category_info['suffix'].lower()
        if suffix_lower.startswith("commercial"):
            opener = category_info['suffix']
        else:
            opener = f"Commercial {suffix_lower}"

    if style_type:
        opener = f"{opener} ({style_type})"
    elif models:
        opener = f"{opener} ({models[0]})"

    parts = [opener]
    if pack_qty and pack_qty > 1:
        parts.append(f"Pack of {pack_qty}")
    parts.append(category_info["descriptors"][0].capitalize())

    ctas = [
        "Fast shipping across Canada.",
        "Ships Canada-wide.",
        "Canadian janitorial supply.",
        "Professional grade. Ships across Canada."
    ]

    jv_note = ""
    if product_type not in ["machines", "chemicals"]:
        if (brand and "johnny" in brand.lower()) or "johnny" in (title or '').lower() or "jvac" in (title or '').lower():
            jv_note = "JohnnyVac compatible. "

    main_text = ". ".join(parts) + "."
    if jv_note:
        main_text = main_text.replace(". ", f". {jv_note}", 1)

    if len(main_text) < 60:
        main_text = main_text.rstrip(".") + ". Commercial grade quality."

    for cta in ctas:
        test_desc = f"{main_text} {cta}"
        if len(test_desc) <= MAX_SEO_DESCRIPTION:
            return test_desc

    if len(main_text) > MAX_SEO_DESCRIPTION - 30:
        if brand:
            short_text = f"{brand} {category_info['suffix'].lower()}."
        else:
            short_text = f"{category_info['suffix']}."
        if pack_qty and pack_qty > 1:
            short_text += f" {pack_qty} pack."
        short_text += f" {jv_note}Professional grade. Ships Canada-wide."
        if len(short_text) <= MAX_SEO_DESCRIPTION:
            return short_text

    return main_text[:MAX_SEO_DESCRIPTION]


# =============================================================================
# SHOPIFY STANDARD PRODUCT TAXONOMY
# =============================================================================
# Maps category_map_v4.json handles to Shopify Standard Product Taxonomy IDs.
# This is what Google Merchant Center / Shopping uses to categorize products.
# IDs verified against github.com/Shopify/product-taxonomy (dist/en/categories.txt).

_T = "gid://shopify/TaxonomyCategory/"

TAXONOMY_BY_HANDLE = {
    # Vacuum parts → Home & Garden > Household Appliance Accessories > Vacuum Accessories
    "parts-motors-electrical":      _T + "hg-8-11",
    "parts-pumps-regulators":       _T + "hg-8-11",
    "parts-hoses-fittings":         _T + "hg-8-11-26",   # Extension Hoses
    "parts-filters":                _T + "hg-8-11-29",   # Filters
    "parts-vacuum-bags":            _T + "hg-8-11-21",   # Dust Bags
    "parts-vacuum-belts":           _T + "hg-8-11",
    "parts-seals-gaskets":          _T + "hg-8-11",
    "parts-brushes-agitators":      _T + "hg-8-11",
    "parts-nozzles-wands":          _T + "hg-8-11",
    "parts-assemblies-housings":    _T + "hg-8-11",
    "parts-repair-conversion-kits": _T + "hg-8-11-2",    # Accessory Kits
    "parts-wheels-casters":         _T + "hg-8-11-62",   # Wheels Replacement Kits
    "parts-latches-clips":          _T + "hg-8-11",
    "parts-springs-hardware":       _T + "hg-8-11",
    "parts-squeegees-blades":       _T + "hg-8-5",       # Floor & Steam Cleaner Accessories
    "parts-general":                _T + "hg-8-11",
    # Consumables
    "consumables-chemicals-solutions": _T + "hg-10-6-11",  # Household Cleaning Products
    "consumables-paper-products":      _T + "hg-10-7",     # Household Paper Products
    "consumables-floor-pads":          _T + "hg-8-5-4",    # Floor Cleaning Pads
    # Tools & Accessories
    "tools-cleaning-tools":  _T + "hg-10-6",    # Household Cleaning Supplies
    "tools-brooms-brushes":  _T + "hg-10-6",
    "tools-mops-buckets":    _T + "hg-10-6",
    "tools-dispensers":      _T + "hg-1",       # Bathroom Accessories
    # Equipment
    "equipment-upright-vacuums":    _T + "hg-9-10-2",    # Upright Vacuums
    "equipment-backpack-vacuums":   _T + "hg-9-10",      # Vacuums
    "equipment-canister-vacuums":   _T + "hg-9-10-1",    # Canister Vacs
    "equipment-wet-dry-vacuums":    _T + "hg-9-10",
    "equipment-hepa-vacuums":       _T + "hg-9-10",
    "equipment-commercial-vacuums": _T + "hg-9-10",
    "equipment-floor-machines":     _T + "hg-9-3",       # Floor & Steam Cleaners
    "equipment-automatic-scrubbers": _T + "hg-9-3-4",    # Floor Scrubbers
    "equipment-carpet-extractors":  _T + "hg-9-3-1-2",   # Carpet Extractors
    "equipment-central-vacuums":    _T + "hg-9-10",
    "equipment-pressure-washers":   _T + "hg-12-3-12",   # Pressure Washers
    # Ops & Safety
    "ops-gloves":     _T + "hg-10-6-6",  # Cleaning Gloves
    "ops-safety-ppe": _T + "bi-25",      # Work Safety Protective Gear
}


def taxonomy_for_handle(handle: str) -> Optional[str]:
    return TAXONOMY_BY_HANDLE.get(handle)


# =============================================================================
# COLLECTION CONTENT (descriptions + SEO for category/collection pages)
# =============================================================================
# Collection pages are a primary ranking surface. A grid with a one-line stub
# is thin content (Google "crawled - currently not indexed"); these give each
# collection two real paragraphs plus SEO meta.

# Per top-level group, the angle the intro takes.
_COLLECTION_GROUP_INTRO = {
    "Parts & Replacement Parts":
        "Keep your equipment running with replacement {leaf_l}. We stock {leaf_l} "
        "for the commercial vacuums and cleaning machines used across Canadian "
        "facilities — matched to the major brands so you can find the right fit by "
        "model or part number.",
    "Consumables":
        "Stock your facility with {leaf_l}. These are the everyday {leaf_l} that "
        "keep commercial washrooms, kitchens, and floors running — bought by the "
        "case and shipped across Canada.",
    "Tools & Accessories":
        "Equip your cleaning crew with {leaf_l}. Durable, professional-grade "
        "{leaf_l} built for the demands of daily commercial cleaning.",
    "Equipment & Machines":
        "Browse commercial {leaf_l} for professional cleaning. From single sites "
        "to large facilities, these {leaf_l} are built for the workloads Canadian "
        "businesses put them through.",
    "Ops & Safety":
        "Protect your team with {leaf_l}. Workplace safety supplies for cleaning "
        "and maintenance crews, in stock and ready to ship across Canada.",
}

_COLLECTION_GROUP_BODY = {
    "Parts & Replacement Parts":
        "Replacing a worn part on schedule restores performance and extends the "
        "life of the machine — far cheaper than running equipment into the ground. "
        "Browse the range below by brand and model.",
    "Consumables":
        "Buying consumables in volume keeps per-unit costs down and avoids running "
        "out mid-shift. Browse the options below to stock your facility.",
    "Tools & Accessories":
        "The right tool makes a cleaning crew faster and the result more "
        "consistent. Browse the selection below for your operation.",
    "Equipment & Machines":
        "Choosing the right machine for your floor type and square footage pays off "
        "in labour saved on every clean. Browse the models below, and reach out if "
        "you'd like help matching a machine to your site.",
    "Ops & Safety":
        "Meeting workplace safety requirements protects your staff and your "
        "business. Browse the range below for your team.",
}

_COLLECTION_CTA = (
    "Available from Kingsway Janitorial — a Vancouver-based janitorial supplier "
    "serving Canadian businesses since 1990, with fast shipping coast to coast."
)


def _collection_parts(product_type: str, title: str) -> tuple:
    segs = [s.strip() for s in (product_type or title or '').split('>')]
    group = segs[0] if segs else 'Tools & Accessories'
    leaf = (segs[-1] if segs else title) or title
    return group, leaf


def generate_collection_description_template(product_type: str, title: str = '',
                                             count: Optional[int] = None) -> str:
    group, leaf = _collection_parts(product_type, title)
    leaf_l = leaf.lower()
    intro = _COLLECTION_GROUP_INTRO.get(group, _COLLECTION_GROUP_INTRO["Tools & Accessories"])
    body = _COLLECTION_GROUP_BODY.get(group, _COLLECTION_GROUP_BODY["Tools & Accessories"])
    p1 = intro.format(leaf_l=leaf_l)
    count_note = ""
    if count and count >= 5:
        count_note = f"This collection brings together {count} {leaf_l} in one place. "
    p2 = f"{count_note}{body} {_COLLECTION_CTA}"
    p1 = re.sub(r'\s+', ' ', p1).strip()
    p2 = re.sub(r'\s+', ' ', p2).strip()
    return f"<p>{p1}</p>\n<p>{p2}</p>"


def generate_collection_seo(product_type: str, title: str = '') -> tuple:
    """Returns (seo_title, seo_description), each within Google's limits."""
    group, leaf = _collection_parts(product_type, title)
    seo_title = f"{leaf} | Kingsway Janitorial"
    if len(seo_title) > MAX_SEO_TITLE:
        seo_title = truncate_text(leaf, MAX_SEO_TITLE)
    leaf_l = leaf.lower()
    desc = (f"Shop commercial {leaf_l} at Kingsway Janitorial. Professional-grade, "
            f"in stock, fast shipping across Canada.")
    if len(desc) > MAX_SEO_DESCRIPTION:
        desc = truncate_text(desc, MAX_SEO_DESCRIPTION)
    return seo_title, desc


COLLECTION_AI_SYSTEM_PROMPT = """You write category-page (collection) descriptions for Kingsway Janitorial, a Vancouver-based commercial cleaning and janitorial supplier shipping across Canada. Each collection groups one product category (e.g. vacuum filters, mops & buckets, automatic scrubbers).

For each collection you receive, write a unique description as exactly two short paragraphs (60-110 words total):
- Paragraph 1: what this category is and who buys it / what problem it solves for a commercial buyer.
- Paragraph 2: a practical buying consideration for the category, then a short closing line about availability from Kingsway Janitorial with shipping across Canada. Vary that closing line between collections.

Rules:
- Use ONLY the category name and group provided. Do not invent brands, specs, counts, or claims.
- Write for commercial/professional buyers; plain, confident language. No hype words, no exclamation marks, no keyword stuffing.
- Vary structure and wording across collections so they don't read as copies.
- Plain text only per paragraph (no HTML, no markdown, no lists)."""

COLLECTION_AI_SCHEMA = {
    "type": "object",
    "properties": {
        "collections": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "handle": {"type": "string"},
                    "paragraph1": {"type": "string"},
                    "paragraph2": {"type": "string"},
                },
                "required": ["handle", "paragraph1", "paragraph2"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["collections"],
    "additionalProperties": False,
}


def generate_collection_descriptions_ai(items: List[Dict], model: str = None) -> Dict[str, str]:
    """items: [{'handle', 'title', 'product_type'}]. Returns {handle: html}."""
    if not items or not ai_available():
        return {}
    try:
        import anthropic
    except ImportError:
        _log("anthropic package not installed — collection descriptions use templates", 'WARNING')
        return {}

    client = anthropic.Anthropic()
    model = model or AI_MODEL
    lines = []
    for it in items:
        group, leaf = _collection_parts(it.get('product_type', ''), it.get('title', ''))
        lines.append(f"- handle: {it['handle']} | Category: {leaf} | Group: {group}")
    try:
        response = client.messages.create(
            model=model,
            max_tokens=8000,
            system=COLLECTION_AI_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": "Write descriptions for these collections:\n" + "\n".join(lines)}],
            output_config={"format": {"type": "json_schema", "schema": COLLECTION_AI_SCHEMA}},
        )
        if response.stop_reason == "refusal":
            _log("Collection description request refused — using templates", 'WARNING')
            return {}
        text = next((b.text for b in response.content if b.type == "text"), "")
        data = json.loads(text)
        wanted = {it['handle'] for it in items}
        out = {}
        for c in data.get('collections', []):
            h = c.get('handle', '')
            if h not in wanted:
                continue
            p1 = html_lib.escape((c.get('paragraph1') or '').strip(), quote=False)
            p2 = html_lib.escape((c.get('paragraph2') or '').strip(), quote=False)
            if len(p1) + len(p2) < 60:
                continue
            out[h] = f"<p>{p1}</p>\n<p>{p2}</p>" if p2 else f"<p>{p1}</p>"
        return out
    except Exception as e:
        _log(f"Collection AI descriptions failed ({e}) — using templates", 'WARNING')
        return {}


def build_collection_description(product_type: str, title: str = '',
                                 count: Optional[int] = None, ai_desc: str = None) -> str:
    if ai_desc:
        return ai_desc
    return generate_collection_description_template(product_type, title, count)

