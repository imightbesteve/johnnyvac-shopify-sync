#!/usr/bin/env python3
"""
Bulk Product Description Generator v2 for Kingsway Janitorial
Generates rich, unique product descriptions for products with thin/empty body content.

v2 FIXES:
  - Raw JV categories (Vacuum Bags, Odor Control, etc.) now mapped to correct templates
  - Motors & Electrical subcategorized: cords, switches, wires get appropriate descriptions
  - "Replacement replacement part" template variable bug fixed
  - Empty material formatting fixed (no more leading spaces)
  - Compatibility extraction improved (filters garbage, skips machines)
  - More template variants for better opener diversity

Environment Variables:
    SHOPIFY_STORE: Store URL (e.g., kingsway-janitorial.myshopify.com)
    SHOPIFY_ACCESS_TOKEN: Admin API access token (shpat_...)

Usage:
    python description_generator.py --dry-run --limit 50 --export results.csv
    python description_generator.py --live --export results.csv
"""

import os, re, csv, json, time, argparse, requests
from datetime import datetime
from typing import Dict, List, Optional, Tuple

SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', 'kingsway-janitorial.myshopify.com')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN', '')
API_VERSION = '2026-01'
GRAPHQL_URL = f'https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/graphql.json'
HEADERS = {'Content-Type': 'application/json', 'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN}
MIN_DESCRIPTION_LENGTH = 80
MAX_DESCRIPTION_LENGTH = 800
RATE_LIMIT_DELAY = 0.75
REQUEST_TIMEOUT = 30
MAX_RETRIES = 5
CSV_URL = 'https://www.johnnyvacstock.com/sigm_all_jv_products/JVWebProducts.csv'

def log(msg, level='INFO'):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [{level}] {msg}", flush=True)

BRANDS = [
    "JohnnyVac","Johnny Vac","Filter Queen","Dirt Devil","Big D",
    "Bissell","Compact","Dustbane","Dyson","Electrolux","Eureka",
    "Fuller","Ghibli","Hoover","Intervac","Karcher","Kenmore",
    "Kirby","Maytag","Miele","Nilfisk","Oreck","Panasonic",
    "Perfect","ProTeam","Proteam","Rainbow","Riccar","Roval",
    "Royal","Samsung","Sanitaire","Sanyo","Shark","Simplicity",
    "Tennant","Tristar","Vortech","Wirbel"
]

def extract_brand(title):
    text = title.lower()
    if 'perfect' in text:
        if re.search(r'\bperfect\s+(vacuum|canister|upright|c\d+|pb\d+)', text, re.I):
            return "Perfect"
    for brand in BRANDS:
        if brand.lower() == 'perfect': continue
        if brand.lower() in text:
            return 'JohnnyVac' if 'johnny' in brand.lower() else brand
    return None

def extract_pack_quantity(title):
    for p in [r'pack\s+of\s+(\d+)',r'(\d+)\s*-?\s*pack',r'box\s+of\s+(\d+)',r'(\d+)\s+bags',r'pk\s*(\d+)',r'(\d+)\s*pc']:
        m = re.search(p, title.lower())
        if m:
            qty = int(m.group(1))
            if 1 < qty < 500: return qty
    return None

def extract_dimensions(title):
    specs = {}
    m = re.search(r'(\d+(?:\.\d+)?)\s*(?:"|inch|in)\b', title.lower())
    if m: specs['size_inches'] = m.group(1)
    m = re.search(r'(\d+(?:\.\d+)?)\s*mm\b', title.lower())
    if m: specs['size_mm'] = m.group(1)
    m = re.search(r'(\d+)\s*(?:v|volt)\b', title.lower())
    if m: specs['voltage'] = m.group(1)
    m = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:gal|gallon)', title.lower())
    if m: specs['volume_gal'] = m.group(1).replace(',','.')
    m = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:l\b|litre|liter)', title.lower())
    if m: specs['volume_l'] = m.group(1).replace(',','.')
    return specs

def extract_material(title):
    materials = {'hepa':'HEPA','microfiber':'microfiber','cloth':'cloth','paper':'paper','foam':'foam',
        'rubber':'rubber','nylon':'nylon','stainless':'stainless steel','vinyl':'vinyl',
        'latex':'latex','nitrile':'nitrile','cotton':'cotton','washable':'washable'}
    t = title.lower()
    for k,v in materials.items():
        if k in t: return v
    return None

def extract_compatible_models(title, product_type=''):
    pt = product_type.lower()
    if any(x in pt for x in ['equipment','machine','chemical','consumable','tool','safety','ops']): return []
    models = []
    brand = extract_brand(title)
    if brand:
        for p in [r'\b([A-Z]{1,3}\d{3,6}[A-Z]?)\b', r'\b([A-Z]{2,4}-\d{2,4})\b']:
            for m in re.findall(p, title):
                if m not in models and len(m) > 2 and (not m.isdigit() or int(m) >= 100):
                    models.append(m)
    return models[:2]

def detect_electrical_subtype(title):
    t = title.lower()
    if any(w in t for w in ['motor','suction motor','brush motor','fan motor','armature']): return 'motor'
    if any(w in t for w in ['power cord','electrical cord','cord reel']): return 'power cord'
    if any(w in t for w in ['cord','pigtail','lead wire','wiring','cable']): return 'cord/wire'
    if any(w in t for w in ['switch','on/off','rocker','toggle','reset']): return 'switch'
    if any(w in t for w in ['relay','circuit','pcb','pc board','board']): return 'circuit board'
    if any(w in t for w in ['capacitor','transformer','solenoid']): return 'electrical component'
    if any(w in t for w in ['carbon brush','brush holder']): return 'carbon brush'
    if any(w in t for w in ['battery','charger']): return 'battery'
    if any(w in t for w in ['sensor','controller']): return 'sensor'
    if any(w in t for w in ['plug','terminal','connector']): return 'connector'
    return 'electrical part'

RAW_JV_TO_TEMPLATE = {
    "Vacuum Bags":"vacuum-bags","Filters":"filters","Vacuum Filters":"filters",
    "Vacuum Belts":"belts","Motors, Carbone Brushes & Pumps":"motors",
    "Switches, Relays & Circuit Boards":"electrical","Roller Brushes & Agitators":"brushes",
    "Brushes, Accessories & Adapters":"brushes","Brushes & Dusters":"tools-brooms","Brushes":"brushes",
    "Wand & Handles":"nozzles-wands","Power & Air Nozzles":"nozzles-wands",
    "Canister & Upright Vacuum Hoses":"hoses","Hoses for Central Vacuums":"hoses",
    "Universal Hoses for central vacuums":"hoses","Hose Covers & Brackets":"hoses",
    "End Cuffs":"hoses","Electrical Cord Winders":"electrical",
    "Tools, Bearings ":"parts-general","Tools, Bearings":"parts-general",
    "Accessories & Extractor Tools":"nozzles-wands",
    "Installation Kits and Accessories for Central Vacuums":"parts-kits",
    "Accessories Kits for Central Vacuums":"parts-kits",
    "Kits and Accessories for Retractable Hoses":"parts-kits",
    "Gaskets, Seals & Valves":"parts-general","Vacuum Parts":"parts-general",
    "All Parts":"parts-general","Johnny Vac Parts (*)":"parts-general",
    "Odor Control":"chemicals","Glass Cleaning":"chemicals",
    "Floor Maintenance Products":"chemicals","Carpet Products":"chemicals",
    "Car Products":"chemicals","General Purpose Products":"chemicals",
    "Kitchen Products":"chemicals","Sanitizers":"chemicals",
    "Ecologo products":"chemicals","Speciality Products":"chemicals",
    "Laundry Products":"chemicals","Cleaners":"chemicals",
    "Paper Hand Towels & Bathroom Tissues":"consumables-paper",
    "Garbage bags":"consumables-paper","Washroom Products":"consumables-paper",
    "Hygiene Products":"consumables-paper","Paper distributors":"dispensers",
    "Floor Pads":"consumables-pads","Floor Machine Pads":"consumables-pads",
    "Sponges & Pads":"tools",
    "Wet Mops, Dry Mops & Handles":"tools-mops","Mops":"tools-mops",
    "Buckets & Wringers":"tools-mops","Brooms & Dustpans":"tools-brooms",
    "Cloths, Rags & Dusters":"tools","Soap Dispensers":"dispensers",
    "Bottles & Sprayers":"tools","Garbage Pails":"tools","Utility Carts":"tools",
    "Ashtrays":"tools","General Maintenance":"tools",
    "Gloves & Masks":"safety","Work Gloves":"safety",
    "Commercial Vacuums and Equipment ":"equipment-vacuum","Central Vacuums":"equipment-central",
    "Steam Cleaners":"equipment","Carpet Extractors":"equipment",
    "Autoscrubbers":"equipment","Scrubbers":"equipment","Floor Dryers":"equipment",
    "Floor Machines":"equipment","Specialty Cleaning Equipment":"equipment",
    "Stick Vacuums":"equipment-vacuum","Upright Vacuums":"equipment-vacuum",
    "Backpack Vacuums":"equipment-vacuum","Canister Vacuums":"equipment-vacuum",
    "Wet & Dry Vacuums":"equipment-vacuum","HEPA Specialized Vacuums":"equipment-vacuum",
    "Residential Vacuums":"equipment-vacuum","Delivery and cleaning robots":"equipment",
    "Promotional products":"parts-general","Johnny Vac Clearance & Overstock Products":"parts-general",
}

def get_template_key(product_type, title=''):
    if product_type in RAW_JV_TO_TEMPLATE:
        key = RAW_JV_TO_TEMPLATE[product_type]
        if key in ('motors','electrical'):
            sub = detect_electrical_subtype(title)
            if sub == 'motor': return 'motors'
            elif sub in ('power cord','cord/wire'): return 'electrical-cords'
            elif sub in ('switch','sensor','connector'): return 'electrical-switches'
            elif sub in ('circuit board','electrical component'): return 'electrical-boards'
            elif sub == 'carbon brush': return 'electrical-carbon-brushes'
            else: return 'electrical'
        return key
    pt = product_type.lower()
    if 'vacuum bag' in pt: return 'vacuum-bags'
    elif 'filter' in pt: return 'filters'
    elif 'belt' in pt: return 'belts'
    elif 'hose' in pt or 'fitting' in pt: return 'hoses'
    elif 'motor' in pt or 'electrical' in pt:
        sub = detect_electrical_subtype(title)
        if sub == 'motor': return 'motors'
        elif sub in ('power cord','cord/wire'): return 'electrical-cords'
        elif sub in ('switch','sensor','connector'): return 'electrical-switches'
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

def get_product_noun(title, product_type, template_key):
    noun_map = {
        'vacuum-bags':'vacuum bag','filters':'filter','belts':'drive belt',
        'hoses':'vacuum hose','motors':'vacuum motor','electrical':'electrical component',
        'electrical-cords':'power cord','electrical-switches':'switch',
        'electrical-boards':'circuit board','electrical-carbon-brushes':'carbon brush',
        'brushes':'brush roll','nozzles-wands':'vacuum attachment',
        'equipment':'cleaning machine','equipment-vacuum':'vacuum cleaner',
        'equipment-central':'central vacuum system','chemicals':'cleaning product',
        'consumables-paper':'paper product','consumables-pads':'floor pad',
        'tools':'cleaning tool','tools-mops':'mop','tools-brooms':'broom',
        'dispensers':'dispenser','safety':'safety product','parts-kits':'accessory kit',
        'parts-general':'component',
    }
    base = noun_map.get(template_key, 'product')
    if template_key == 'parts-general':
        t = title.lower()
        for kw, noun in [('wheel','wheel'),('caster','caster'),('bearing','bearing'),
            ('spring','spring'),('latch','latch'),('clip','clip'),('seal','seal'),
            ('gasket','gasket'),('valve','valve'),('handle','handle'),('cover','cover'),
            ('lid','lid'),('tank','tank'),('housing','housing'),('bracket','bracket'),
            ('bumper','bumper'),('pedal','pedal'),('lever','lever'),('knob','knob'),
            ('assembly','assembly'),('pump','pump'),('plate','plate'),('axle','axle')]:
            if kw in t: return noun
    if template_key == 'chemicals':
        t = title.lower()
        for kw, noun in [('deodorant','deodorant'),('deodorizer','deodorizer'),
            ('disinfectant','disinfectant'),('sanitizer','sanitizer'),('degreaser','degreaser'),
            ('detergent','detergent'),('cleaner','cleaner'),('polish','polish'),('wax','wax'),
            ('stripper','floor stripper'),('finish','floor finish'),('shampoo','carpet shampoo'),
            ('freshener','air freshener'),('soap','soap'),('lotion','lotion')]:
            if kw in t: return noun
    return base

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


class DescriptionGenerator:
    def __init__(self, store, token):
        self.store = store
        self.token = token
        self.graphql_url = f'https://{store}/admin/api/{API_VERSION}/graphql.json'
        self.headers = {'Content-Type':'application/json','X-Shopify-Access-Token':token}
        self.stats = {'total_fetched':0,'thin_descriptions':0,'generated':0,'updated':0,'skipped':0,'errors':0}
        self.results = []
        self.associated_skus_map = {}
        self.sku_title_map = {}
        self._cta_index = 0

    def _graphql(self, query, variables=None):
        payload = {'query': query}
        if variables: payload['variables'] = variables
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.post(self.graphql_url, json=payload, headers=self.headers, timeout=REQUEST_TIMEOUT)
                # Retry on 429 (rate limit) and 503 (service unavailable)
                if resp.status_code in (429, 503):
                    retry_after = int(resp.headers.get('Retry-After', (attempt + 1) * 10))
                    wait = min(retry_after, 60)
                    log(f"HTTP {resp.status_code}, retry {attempt+1}/{MAX_RETRIES} in {wait}s...", 'WARNING')
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(wait)
                        continue
                resp.raise_for_status()
                result = resp.json()
                if 'errors' in result: log(f"GraphQL errors: {result['errors']}", 'WARNING')
                return result
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                if attempt < MAX_RETRIES - 1:
                    wait = (attempt + 1) * 10
                    log(f"Connection error, retry {attempt+1}/{MAX_RETRIES} in {wait}s...", 'WARNING')
                    time.sleep(wait)
                else: raise
        return {}

    def _next_cta(self):
        cta = CANADIAN_CTAS[self._cta_index % len(CANADIAN_CTAS)]
        self._cta_index += 1
        return cta

    def fetch_csv_associations(self):
        log("Fetching CSV for associated SKUs data...")
        try:
            resp = requests.get(CSV_URL, timeout=60)
            resp.raise_for_status()
            reader = csv.DictReader(resp.text.splitlines(), delimiter=';')
            for row in reader:
                sku = (row.get('SKU') or '').strip()
                if not sku: continue
                self.sku_title_map[sku] = (row.get('ProductTitleEN') or '').strip()
                associated = (row.get('AssociatedSkus') or '').strip()
                if associated:
                    self.associated_skus_map[sku] = [s.strip() for s in re.split(r'[,|;]', associated) if s.strip()]
            log(f"✓ Loaded {len(self.sku_title_map)} SKUs, {len(self.associated_skus_map)} with associations")
        except Exception as e:
            log(f"Could not fetch CSV: {e}", 'WARNING')

    def fetch_products_paginated(self, min_desc_length=MIN_DESCRIPTION_LENGTH):
        log("Fetching products from Shopify...")
        query = """query ($cursor: String) { products(first: 250, after: $cursor) {
            edges { node { id title bodyHtml productType vendor tags handle status
                variants(first: 1) { nodes { sku price inventoryQuantity } } } cursor }
            pageInfo { hasNextPage } } }"""
        all_prods, thin = [], []
        cursor = None
        while True:
            result = self._graphql(query, {'cursor': cursor} if cursor else None)
            edges = result.get('data',{}).get('products',{}).get('edges',[])
            pi = result.get('data',{}).get('products',{}).get('pageInfo',{})
            for edge in edges:
                node = edge['node']
                all_prods.append(node)
                body = node.get('bodyHtml') or ''
                if len(re.sub(r'<[^>]+>','',body).strip()) < min_desc_length:
                    thin.append(node)
            if not pi.get('hasNextPage'): break
            cursor = edges[-1]['cursor']
            if len(all_prods) % 1000 == 0: log(f"  Fetched {len(all_prods)} products...")
            time.sleep(RATE_LIMIT_DELAY)
        self.stats['total_fetched'] = len(all_prods)
        self.stats['thin_descriptions'] = len(thin)
        log(f"✓ {len(all_prods)} total, {len(thin)} with thin descriptions (< {min_desc_length} chars)")
        return thin

    def generate_description(self, product):
        title = product.get('title','')
        product_type = product.get('productType','')
        variants = product.get('variants',{}).get('nodes',[])
        sku = variants[0].get('sku','') if variants else ''

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

        # Opener
        openers = templates.get('openers', TEMPLATES['parts-general']['openers'])
        try:
            sentences.append(openers[h % len(openers)].format(**tmpl))
        except (KeyError, IndexError):
            sentences.append(f"{product_noun.capitalize()}{tmpl['brand_for']}.")

        # Feature
        features = templates.get('features', TEMPLATES['parts-general']['features'])
        try:
            feat = features[(h+1) % len(features)].format(**tmpl)
            feat = re.sub(r'\s+', ' ', feat).strip()
            if feat and feat[0].islower(): feat = feat[0].upper() + feat[1:]
            sentences.append(feat)
        except (KeyError, IndexError):
            sentences.append("Quality construction for reliable performance in commercial environments.")

        # Pack note
        if pack_qty and pack_qty > 1 and 'pack_note' in templates:
            sentences.append(templates['pack_note'].format(**tmpl))

        # Compatibility (parts only)
        if models:
            sentences.append(f"Compatible with {', '.join(models[:2])} — verify your model for proper fitment.")

        # Volume (chemicals)
        if template_key == 'chemicals':
            vp = []
            if specs.get('volume_gal'): vp.append(f"{specs['volume_gal']} gal")
            if specs.get('volume_l'): vp.append(f"{specs['volume_l']} L")
            if vp: sentences.append(f"Available in {' / '.join(vp)} size.")

        # CTA
        sentences.append(self._next_cta())

        # Build HTML
        mid = max(2, len(sentences) // 2)
        p1 = re.sub(r'\s+', ' ', ' '.join(sentences[:mid])).strip()
        p2 = re.sub(r'\s+', ' ', ' '.join(sentences[mid:])).strip()
        return f"<p>{p1}</p>\n<p>{p2}</p>" if p2 else f"<p>{p1}</p>"

    def update_product_description(self, product_id, html):
        mutation = """mutation productUpdate($input: ProductInput!) {
            productUpdate(input: $input) { product { id } userErrors { field message } } }"""
        result = self._graphql(mutation, {'input': {'id': product_id, 'descriptionHtml': html}})
        errors = result.get('data',{}).get('productUpdate',{}).get('userErrors',[])
        if errors: log(f"Update error {product_id}: {errors}", 'WARNING'); return False
        return bool(result.get('data',{}).get('productUpdate',{}).get('product'))

    def process(self, dry_run=True, limit=None, min_length=MIN_DESCRIPTION_LENGTH):
        log("="*70)
        log("BULK DESCRIPTION GENERATOR v2 — Kingsway Janitorial")
        log("="*70)
        log(f"Store: {self.store} | Min length: {min_length} | Mode: {'DRY RUN' if dry_run else 'LIVE'}")

        self.fetch_csv_associations()
        thin = self.fetch_products_paginated(min_length)
        if limit: thin = thin[:limit]; log(f"Limited to {limit}")
        if not thin: log("No products need enrichment!"); return []

        log(f"\nGenerating descriptions for {len(thin)} products...")
        for i, prod in enumerate(thin, 1):
            pid = prod['id']
            title = prod.get('title','')
            pt = prod.get('productType','')
            old_text = re.sub(r'<[^>]+>','', prod.get('bodyHtml','') or '').strip()
            sku = prod.get('variants',{}).get('nodes',[{}])[0].get('sku','')
            tk = get_template_key(pt, title)

            new_html = self.generate_description(prod)
            new_text = re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', new_html)).strip()

            r = {'product_id':pid,'sku':sku,'title':title,'product_type':pt,'template_key':tk,
                 'old_length':len(old_text),'new_length':len(new_text),
                 'new_description':new_html,'new_description_text':new_text,'status':'pending'}

            if not dry_run:
                try:
                    if self.update_product_description(pid, new_html):
                        r['status'] = 'updated'; self.stats['updated'] += 1
                    else:
                        r['status'] = 'error'; self.stats['errors'] += 1
                except Exception as e:
                    log(f"Error updating {sku}: {e}", 'WARNING')
                    r['status'] = 'error'; self.stats['errors'] += 1
                time.sleep(RATE_LIMIT_DELAY)
            else:
                r['status'] = 'dry_run'

            self.stats['generated'] += 1
            self.results.append(r)
            if i % 100 == 0 or i == len(thin): log(f"  Progress: {i}/{len(thin)}")
        return self.results

    def export_results(self, filename='description_results.csv'):
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=['sku','title','product_type','template_key','old_length','new_length','new_description_text','status'])
            w.writeheader()
            for r in self.results:
                w.writerow({k: r[k] for k in w.fieldnames})
        log(f"📄 Exported to {filename}")

    def print_summary(self):
        log("\n" + "="*70)
        log("SUMMARY")
        log("="*70)
        for k in ['total_fetched','thin_descriptions','generated','updated','errors']:
            log(f"  {k:25s} {self.stats[k]}")
        if self.results:
            tc = {}
            for r in self.results: tc[r['template_key']] = tc.get(r['template_key'],0)+1
            log("\nBy template:")
            for k,c in sorted(tc.items(), key=lambda x:-x[1]): log(f"  {c:5d}  {k}")

    def print_samples(self, count=5):
        log(f"\n📋 SAMPLES ({count}):")
        log("-"*70)
        for r in self.results[:count]:
            log(f"\nSKU: {r['sku']} | Type: {r['product_type']} → {r['template_key']}")
            log(f"Title: {r['title']}")
            log(f"Desc: {r['new_description_text']}")
            log("-"*40)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true', default=True)
    parser.add_argument('--live', action='store_true')
    parser.add_argument('--limit', type=int)
    parser.add_argument('--min-length', type=int, default=MIN_DESCRIPTION_LENGTH)
    parser.add_argument('--export', type=str, default='description_results.csv')
    parser.add_argument('--samples', type=int, default=10)
    args = parser.parse_args()

    token = os.environ.get('SHOPIFY_ACCESS_TOKEN', SHOPIFY_ACCESS_TOKEN)
    if not token: log("❌ SHOPIFY_ACCESS_TOKEN not set", 'ERROR'); return
    store = os.environ.get('SHOPIFY_STORE', SHOPIFY_STORE)

    gen = DescriptionGenerator(store, token)
    gen.process(dry_run=not args.live, limit=args.limit, min_length=args.min_length)
    gen.export_results(args.export)
    gen.print_summary()
    gen.print_samples(args.samples)

if __name__ == '__main__':
    main()
