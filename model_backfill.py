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

For active parts that have no models at all there is a back-fill:

- Rule pass (free, every run): models named in title / description phrases
  such as "for Hoover UH70120", "fits ...", "compatible with models 6582
  and 6583". A match is kept only if it is already a known model somewhere
  in the catalogue, so a part number or a dimension cannot get in.
- AI pass (needs ANTHROPIC_API_KEY, capped per run): for parts the rules
  leave empty, Claude returns {"models": [...], "confidence": "high|low"}.
  Only models that literally appear in the part's text are accepted.

Models are normalized to upper case with no spaces; dashes stay as written
(DCF-15, FBCC-1).

    python model_backfill.py        # run the examples
"""
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Iterable, List, Optional, Set

MAX_MODELS = 128            # Shopify's limit for a list metafield
# Claude model for the AI pass. Its own setting, separate from the one that
# writes product descriptions (ANTHROPIC_MODEL).
AI_MODEL = os.environ.get('MODEL_BACKFILL_MODEL') or 'claude-sonnet-5-5'
TAG_RULE = 'models:rule'
TAG_AI = 'models:ai'
TAG_AI_LOW = 'models:ai-low'    # AI was unsure: listed for review, not written
TAG_NONE = 'models:none'        # AI found no model in the text: not asked again
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
# AI pass
# =============================================================================

AI_SYSTEM_PROMPT = """You extract vacuum and cleaning-machine MODEL numbers from a replacement part's listing, for a store's "find parts for my machine" search.

You are given one part: its title, description, SKU and part numbers. Reply with JSON only:
{"models": [...], "confidence": "high" | "low"}

models: the machine models this part is stated to fit.
- Only models written in the text. Copy each exactly as written. Never guess, infer from a series name, expand a range ("NV350-NV357") or add models from your own knowledge.
- A model identifies a machine (Hoover UH70120, Dyson DC07, Kenmore 116.21614, Johnny Vac JV58). Do not return the part's own SKU, MPN, OEM or part number, a supplier code, a size, a voltage, a colour or a pack count.
- Bag, belt and filter types are not models: "Type AB", "Type Q/C", "Style 12", "Type Y".
- A brand or product line alone ("Hoover", "WindTunnel", "Navigator") is not a model.
- If the text names no machine model, return an empty list.

confidence: "high" when the text plainly says the part fits those models ("for", "fits", "compatible with"); "low" when you cannot tell whether a number is a machine model or a part number."""

AI_SCHEMA = {
    "type": "object",
    "properties": {
        "models": {"type": "array", "items": {"type": "string"}},
        "confidence": {"type": "string", "enum": ["high", "low"]},
    },
    "required": ["models", "confidence"],
    "additionalProperties": False,
}


def accept_ai_models(models: Iterable[str], text: str, own_ids: Set[str]) -> List[str]:
    """Keep only what the AI was allowed to return: written in the text, not
    the part's own number, not a bag type, and shaped like a model."""
    plain = re.sub(r'<[^>]+>', ' ', text or '')
    haystack = model_key(plain)
    bag_types = {model_key(t) for m in BAG_TYPE.finditer(plain)
                 for t in re.split(r'[\s/&,]+', m.group(0))[1:]}
    accepted: List[str] = []
    seen: Set[str] = set()
    for raw in models or []:
        norm = normalize_model(raw)
        key = model_key(norm)
        if (not 2 <= len(key) <= 24 or not any(c.isdigit() for c in key)
                or key in seen or key in own_ids or key in bag_types
                or key not in haystack):
            continue
        seen.add(key)
        accepted.append(norm)
    return accepted


def ai_models(items: List[Dict], model: Optional[str] = None, workers: int = 4,
              log=print) -> Dict[str, Dict]:
    """Ask Claude for the models each part fits.

    items: [{'sku', 'title', 'description', 'own_ids'}]. Returns
    {sku: {'models': [...], 'confidence': 'high'|'low'}} for the parts that
    got an answer; a part whose request failed is absent and is asked again
    on a later run."""
    if not items or not os.environ.get('ANTHROPIC_API_KEY'):
        return {}
    try:
        import anthropic
    except ImportError:
        log("anthropic package not installed - skipping the AI model pass")
        return {}

    client = anthropic.Anthropic()
    model = model or AI_MODEL
    stop = {'reason': None}

    def one(item: Dict) -> Optional[Dict]:
        if stop['reason']:
            return None
        text = f"{item['title']}\n{item.get('description', '')}"
        user = (f"Title: {item['title']}\n"
                f"Description: {(item.get('description') or '(none)')[:3000]}\n"
                f"SKU / part numbers (never models): {', '.join(sorted(item.get('own_labels', []))) or item['sku']}")
        try:
            response = client.messages.create(
                model=model,
                max_tokens=4000,
                system=AI_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user}],
                # reading a title for model numbers is a simple job: low effort
                output_config={"effort": "low",
                               "format": {"type": "json_schema", "schema": AI_SCHEMA}},
            )
        except (anthropic.AuthenticationError, anthropic.PermissionDeniedError,
                anthropic.NotFoundError, anthropic.BadRequestError) as e:
            # wrong key / model / request shape: every other call fails the same way
            stop['reason'] = f"{type(e).__name__}: {e}"
            return None
        except anthropic.APIError as e:   # rate limit, overload, network - after SDK retries
            log(f"  AI models: {item['sku']} skipped this run ({type(e).__name__})")
            return None
        if response.stop_reason != 'end_turn':
            return None
        try:
            data = json.loads(next((b.text for b in response.content if b.type == 'text'), ''))
        except ValueError:
            return None
        models = accept_ai_models(data.get('models'), text, item.get('own_ids', set()))
        return {'models': models,
                'confidence': 'high' if data.get('confidence') == 'high' else 'low'}

    results: Dict[str, Dict] = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for item, answer in zip(items, pool.map(one, items)):
            if answer is not None:
                results[item['sku']] = answer
    if stop['reason']:
        log(f"  AI model pass stopped: {stop['reason']}")
    return results


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
    acc = accept_ai_models(['UH70120', 'UH99999', 'AB', '562289001'],
                           'Belt for Hoover UH70120 Type AB 562289001', {'562289001'})
    ok = acc == ['UH70120']
    bad += not ok
    print(('ok  ' if ok else 'FAIL') + f' accept -> {acc}')
    raise SystemExit(1 if bad else 0)
