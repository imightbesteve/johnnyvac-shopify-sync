#!/usr/bin/env python3
"""Machine model numbers for custom.compatible_models.

The homepage part finder and the "Fits model" filter read that metafield, so
two rules hold everywhere in the sync:

- MERGE, never replace. Values already on a product (many are entered by
  hand) are kept exactly as stored; the sync only appends models that are
  not there yet. A list is never written back shorter or empty.
- A model is a machine model. The part's own SKU, MPN or part number is
  never one, and neither is a bag / belt type ("Type AB", "Type Q/C",
  "Style 5800").

For active parts that have no models at all there is a rule-based
back-fill (free, every run): models named in title / description phrases
such as "for Hoover UH70120", "fits ...", "compatible with models 6582 and
6583". A match is kept only if it is already a known model somewhere in the
catalogue, so a part number or a dimension cannot get in. There is no AI
pass; models the rules cannot find are entered in Shopify (Sidekick).

Models are normalized to upper case with no spaces; dashes stay as written
(DCF-15, FBCC-1).

    python model_backfill.py        # run the examples
"""
import re
from typing import Dict, Iterable, List, Set

MAX_MODELS = 128            # Shopify's limit for a list metafield
TAG_RULE = 'models:rule'
MODEL_TAG_PREFIX = 'models:'

# "Type AB", "Types A and Z", "Style 5800", "Type Q/C": consumable types, not machines.
BAG_TYPE = re.compile(
    r'\b(?:types?|styles?)\s+[A-Za-z0-9]{1,6}(?:\s*(?:/|&|,|\band\b|\bet\b)\s*[A-Za-z0-9]{1,3}\b)*',
    re.I)
TRIGGER = re.compile(
    r'\b(?:for|fits?|fitting|to\s+fit|compatible\s+with|models?|series|used\s+(?:on|with|in))\b', re.I)
TOKEN = re.compile(r'[A-Za-z0-9][A-Za-z0-9.\-]*')
# a number followed by a unit is a size, not a model: 120 V, 32 mm, 6', 1 1/4"
UNIT_AFTER = re.compile(
    r'''\s*(?:["'”″°%]|(?:mm|cm|m|in|inch(?:es)?|ft|v|volts?|w|watts?|a|amps?|hz|oz|g|kg|lbs?|l|ml|gal|'''
    r'''psi|cfm|rpm|gsm|pk|pcs?|pack|bags?|filters?|rolls?|sheets?|ply|x)\b)''', re.I)
OWN_ID = re.compile(r'(?:\bpart\s*(?:#|no\.?|number)|\bp/n|\bmpn|\boem|\bitem\s*#|#)\s*:?\s*([A-Za-z0-9][A-Za-z0-9.\-]+)', re.I)
SEGMENT_CHARS = 160


def normalize_model(value: str) -> str:
    """Upper case, no spaces, dashes kept: 'dcf - 15' -> 'DCF-15'."""
    return re.sub(r'\s+', '', str(value or '').upper()).strip('.,;:()[]/')


def model_key(value: str) -> str:
    """Comparison key: DCF-15, DCF15 and 'dcf 15' are the same model."""
    return re.sub(r'[^A-Z0-9]', '', str(value or '').upper())


def merge_models(existing: Iterable[str], found: Iterable[str]) -> List[str]:
    """existing + the found models that are not in it yet. Stored values are
    returned untouched and first, so the result is never shorter than what
    the product already has."""
    merged = [m for m in (existing or []) if str(m).strip()]
    seen = {model_key(m) for m in merged}
    for m in found or []:
        norm = normalize_model(m)
        key = model_key(norm)
        if key and key not in seen and len(merged) < MAX_MODELS:
            seen.add(key)
            merged.append(norm)
    return merged


def own_identifiers(sku: str, *texts: str, extra: Iterable[str] = ()) -> Set[str]:
    """Keys of everything that names the part itself: SKU, MPN, UPC, the
    number behind a supplier code (hoover_38528027) and anything introduced
    as a part number in the text."""
    ids = {model_key(sku)}
    if '_' in (sku or ''):
        ids.add(model_key(sku.split('_', 1)[1]))
    ids.update(model_key(e) for e in extra if e)
    for text in texts:
        ids.update(model_key(m) for m in OWN_ID.findall(text or ''))
    ids.discard('')
    return ids


def build_known_index(models: Iterable[str]) -> Dict[str, str]:
    """key -> the spelling the catalogue already uses for that model."""
    index: Dict[str, str] = {}
    for m in models:
        norm = normalize_model(m)
        key = model_key(norm)
        if len(key) >= 2 and any(c.isdigit() for c in key):
            index.setdefault(key, norm)
    return index


def rule_models(text: str, known: Dict[str, str], own_ids: Set[str], limit: int = 20) -> List[str]:
    """Known machine models named after "for / fits / compatible with /
    models" in the text."""
    text = BAG_TYPE.sub(' ', re.sub(r'<[^>]+>', ' ', text or ''))
    found: List[str] = []
    seen: Set[str] = set()

    def take(candidate: str) -> bool:
        key = model_key(candidate)
        if key in seen or key in own_ids or key not in known:
            return False
        if key.isdigit() and len(key) < 3:
            return False
        seen.add(key)
        # the spelling as written when it is the known one, else the catalogue's
        written = normalize_model(candidate)
        found.append(written if written == known[key] else known[key])
        return True

    for trig in TRIGGER.finditer(text):
        segment = re.split(r'(?<=[a-z0-9)])[.;!?](?:\s|$)', text[trig.end():trig.end() + SEGMENT_CHARS])[0]
        tokens = [(m.group(0).strip('.-'), m.end()) for m in TOKEN.finditer(segment)]
        for i, (tok, end) in enumerate(tokens):
            if not tok:
                continue
            # "DC 07" -> DC07: letters then digits written apart
            if tok.isalpha() and i + 1 < len(tokens) and tokens[i + 1][0][:1].isdigit():
                if take(tok + tokens[i + 1][0]):
                    continue
            if not any(c.isdigit() for c in tok):
                continue
            if tok.replace('.', '').isdigit() and UNIT_AFTER.match(segment[end:]):
                continue
            take(tok)
        if len(found) >= limit:
            break
    return found[:limit]


# =============================================================================
# Examples
# =============================================================================

if __name__ == '__main__':
    known = build_known_index(['DC04', 'DC07', 'DC14', 'DC33', 'UH70120', '6582', '6583',
                               'DCF-15', 'FBCC-1', 'JV58', 'NV350'])
    cases = [
        ('Brush-roller Agitator for Dyson DC04, DC07, DC14, DC33', 'PNDC04',
         ['DC04', 'DC07', 'DC14', 'DC33']),
        ('Belt for Hoover UH70120 - Pack of 2 - 562289001', 'hoover_562289001', ['UH70120']),
        ('Filter. Compatible with models 6582 and 6583.', 'F100', ['6582', '6583']),
        ('Microfilter Bag for Hoover Type AB and Type Q/C - Pack of 3', 'B1', []),
        ('Cartridge Filter fits Kenmore dcf 15 - 82720', '82720', ['DCF-15']),
        ('Microfilter Bag for Fuller Model FBCC-1 and CPCC-1', 'X', ['FBCC-1']),
        ('Wheel for JV58 - 120 V motor side - 6583 mm', 'W1', ['JV58']),
        ('Hose for DC07', 'DC07', []),          # the part's own SKU is never a model
    ]
    bad = 0
    for text, sku, want in cases:
        got = rule_models(text, known, own_identifiers(sku, text))
        ok = got == want
        bad += not ok
        print(('ok  ' if ok else 'FAIL') + f' {text!r} -> {got}' + ('' if ok else f'   want {want}'))
    merged = merge_models(['DC04', 'DC07', 'DC14', 'DC33'], ['dc07', 'DC 41'])
    ok = merged == ['DC04', 'DC07', 'DC14', 'DC33', 'DC41'] and merge_models(['A1'], []) == ['A1']
    bad += not ok
    print(('ok  ' if ok else 'FAIL') + f' merge -> {merged}')
    raise SystemExit(1 if bad else 0)
