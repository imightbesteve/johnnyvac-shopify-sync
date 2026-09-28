#!/usr/bin/env python3
"""Which machines each part fits, and which parts go with each machine.

Feeds the two product metafields the store already defines for this:
  custom.compatible_models  (parts)     "Fits these models" -- machine model numbers
  custom.compatible_parts   (machines)  "Bags, belts, filters and accessories that
                                         go with this machine" -- product references

Two sources, both in the supplier feed:
- AssociatedSkus links a part to the machines it fits (a PEBP5 battery lists
  the PEBP5 backpack vacuum) and a machine to its parts. Only links to a SKU
  categorized as a machine count as "fits"; part-to-part links (a filter
  listing the bag that goes with it) are left out.
- Part titles name models: "Drain Hose - for JVC50, JVC56, JVC70BCT". Any token
  that is a machine SKU in the feed counts, as do JohnnyVac / Ghibli / EDIC
  style model numbers (JV58, JVC50BCN, GH80D70, AS6, PN11, XV10) for machines
  no longer sold -- a JV58 owner still needs the part.

Orders are deterministic (the sync compares against the stored value daily).
"""
import re
from collections import defaultdict
from typing import Callable, Dict, List, Tuple

MACHINE_TYPE_PREFIX = 'Equipment & Machines'
MAX_MODELS = 12
MAX_PARTS = 40

MODEL_TOKEN = re.compile(
    r'\b(JVC?\d{2,4}[A-Z]{0,5}\d{0,2}|AS\d{1,2}|GHM?\d{2}[A-Z]?\d{0,3}|PN\d{2}[A-Z]{0,3}|XV\d{1,2}[A-Z]{0,4})\b')
SKU_LIKE = re.compile(r'[A-Z0-9][A-Z0-9-]{2,}')

# Cross-sell order on a machine page: consumables first, as the definition says.
PART_ORDER = ('Vacuum Bags', 'Filters', 'Vacuum Belts', 'Brushes', 'Hoses', 'Nozzles',
              'Squeegees', 'Wheels', 'Seals', 'Motors')


def _associated(product: Dict) -> List[str]:
    return [a.strip() for a in (product.get('AssociatedSkus') or '').split(',') if a.strip()]


def build_fitment(products: List[Dict], title_of: Callable[[Dict], str]
                  ) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    """(models each part fits, part SKUs for each machine) from categorized
    feed rows. title_of(row) returns the title as the store shows it."""
    types = {p['SKU']: (p.get('category') or {}).get('product_type', '') for p in products}
    machines = {s for s, t in types.items() if t.startswith(MACHINE_TYPE_PREFIX)}
    machine_by_upper = {s.upper(): s for s in machines}

    fits: Dict[str, List[str]] = {}
    parts_of: Dict[str, set] = defaultdict(set)
    for p in products:
        sku = p['SKU']
        linked = _associated(p)
        if sku in machines:
            parts_of[sku].update(a for a in linked if a in types and a not in machines)
            continue
        found = [a for a in linked if a in machines]
        title = (title_of(p) or '').upper()
        found += [machine_by_upper.get(m, m) for m in MODEL_TOKEN.findall(title)]
        found += [machine_by_upper[t] for t in SKU_LIKE.findall(title) if t in machine_by_upper]
        models = [m for m in dict.fromkeys(found) if m.upper() != sku.upper()][:MAX_MODELS]
        if models:
            fits[sku] = models
        for m in models:
            if m in machines:
                parts_of[m].add(sku)

    def order(s: str) -> Tuple[int, str]:
        t = types.get(s, '')
        return next((i for i, k in enumerate(PART_ORDER) if k in t), len(PART_ORDER)), s

    parts = {m: sorted(v, key=order)[:MAX_PARTS] for m, v in parts_of.items() if v}
    return fits, parts
