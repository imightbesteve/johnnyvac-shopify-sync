#!/usr/bin/env python3
"""Turn a JohnnyVac feed title into one a shopper can read.

The feed carries three kinds of noise that end up on the storefront verbatim:

- Supplier codes: 1,600 titles end in the distributor's internal part code,
  `Crevice Tool bissel_2036655`. The number after the underscore is the OEM
  part number people search for, so it is kept, behind the brand it belongs
  to: `Crevice Tool - Bissell 2036655`. (As a side effect the vendor, which is
  read from the title, becomes Bissell instead of the JohnnyVac fallback.)
- ALL CAPS: 2,000 titles are shouted. They are title-cased, but anything with
  a digit (JV400, 12V, GHM38), a known acronym (HEPA, PVC) or a single letter
  (bag style Y) keeps its capitals, and units after a number go lower case.
- Typing debris: runs of spaces, '' for inches, 18X12 dimensions, *** and
  SPEC ORD markers (kept as "(Special Order)"), wBrush for w/ Brush.

clean_title() is idempotent -- running it on its own output changes nothing --
because the sync compares the stored title with this output every run, and a
title that kept changing would be rewritten daily.

    python title_cleaner.py            # run the examples
"""
import re

try:
    from product_content import BRANDS
except ImportError:  # standalone use
    BRANDS = []

# Distributor code prefix -> the brand the code belongs to.
CODE_BRANDS = {
    'bissel': 'Bissell', 'hoover': 'Hoover', 'dirtde': 'Dirt Devil',
    'electr': 'Electrolux', 'ghibli': 'Ghibli', 'sanita': 'Sanitaire',
    'edic--': 'EDIC', 'edic': 'EDIC', 'protea': 'ProTeam', 'kenmor': 'Kenmore',
    'kirby-': 'Kirby', 'kirby': 'Kirby', 'simpli': 'Simplicity',
    'koblen': 'Koblenz', 'ametek': 'Ametek',
}
_PREFIX = '|'.join(sorted((re.escape(p) for p in CODE_BRANDS), key=len, reverse=True))
CODE_TOKEN = re.compile(r'(?<![\w-])(' + _PREFIX + r')_([A-Za-z0-9][A-Za-z0-9.\-]*)', re.I)
DANGLING_CODE = re.compile(r'(?<![\w-])(' + _PREFIX + r')_(?=\s|$)', re.I)

DIMENSION = re.compile(r'(?<![A-Za-z0-9.])([\d¼½¾]+(?:\.\d+)?["\']?)\s*[xX]\s*(?=[\d¼½¾])')
SPECIAL_ORDER = re.compile(r'\bSPEC(?:IAL)?\.?\s*O(?:RD(?:ER)?)?\b\.?', re.I)
SPECIAL_ORDER_TAG = '(Special Order)'

# Kept upper case when title-casing a shouted title.
ACRONYMS = {
    'HEPA', 'ULPA', 'PVC', 'ABS', 'OEM', 'UV', 'LED', 'USB', 'AC', 'DC', 'JV',
    'EDIC', 'EZ', 'PN', 'CS', 'SMS', 'NLA', 'GFCI', 'CSA', 'HD', 'XL', 'XXL',
    'II', 'III', 'IV', 'VI', 'DD', 'AS', 'BI', 'SC', 'SD', 'TK', 'MV', 'NTN',
    'PE', 'PP', 'EU', 'CFM', 'RPM', 'PSI', 'GPM', 'USA', 'JVAC', 'ASP', 'PH',
}
# Lower case unless first.
SMALL = {'a', 'an', 'and', 'or', 'of', 'for', 'with', 'to', 'the', 'in', 'on',
         'by', 'per', 'from', 'at', 'x', 'w/'}
# Two-letter words that are ordinary words, not codes.
TWO_LETTER_WORDS = {'of', 'or', 'to', 'in', 'on', 'by', 'an', 'at', 'up', 'no', 'is', 'it', 'do'}
UNITS = {'mm': 'mm', 'cm': 'cm', 'gal': 'gal', 'oz': 'oz', 'ft': 'ft', 'lb': 'lb',
         'lbs': 'lbs', 'ml': 'ml', 'kg': 'kg', 'qt': 'qt', 'pc': 'pc', 'pcs': 'pcs'}
SPECIAL_CASE = {'PROTEAM': 'ProTeam', 'WINDTUNNEL': 'WindTunnel', 'IROBOT': 'iRobot',
                'ON/OFF': 'On/Off', 'PKG': 'Pkg', 'ASSY': 'Assy', 'W/': 'w/'}


def _case_word(word, first, prev_is_number):
    """Case one whitespace-delimited word of a shouted title."""
    core = word.strip('()[],.:;"\'')
    if not core:
        return word
    if core.upper() in SPECIAL_CASE:
        return word.replace(core, SPECIAL_CASE[core.upper()])
    if prev_is_number and core.lower() in UNITS:
        return word.replace(core, UNITS[core.lower()])
    if any(ch.isdigit() for ch in core):
        return word
    if '-' in core or '/' in core:
        parts = re.split(r'([-/])', core)
        cased = ''.join(p if p in '-/' else _case_word(p, first and i == 0, False)
                        for i, p in enumerate(parts))
        return word.replace(core, cased)
    if core.upper() in ACRONYMS or len(core) == 1:
        return word.replace(core, core.upper())
    if len(core) == 2 and core.lower() not in TWO_LETTER_WORDS:
        return word.replace(core, core.upper())
    if not first and core.lower() in SMALL:
        return word.replace(core, core.lower())
    return word.replace(core, core[0].upper() + core[1:].lower())


def _is_shouted(text):
    letters = [c for c in text if c.isalpha()]
    return len(letters) >= 4 and not any(c.islower() for c in letters)


def _title_case(text):
    out, prev_number = [], False
    for i, word in enumerate(text.split(' ')):
        out.append(_case_word(word, i == 0, prev_number))
        prev_number = bool(re.fullmatch(r'[\d.,/]+', word.strip('()')))
    return ' '.join(out)


def clean_title(title, sku=''):
    if not title:
        return title
    t = title

    # Debris first, so later steps see words, not punctuation runs.
    t = re.sub(r'\*{2,}', ' ', t)
    special = SPECIAL_ORDER_TAG in t
    t = t.replace(SPECIAL_ORDER_TAG, ' ')   # our own tag, from a previous pass
    special = special or bool(SPECIAL_ORDER.search(t))
    t = SPECIAL_ORDER.sub(' ', t)
    t = re.sub(r"(\d)\s*''", r'\1"', t)
    t = re.sub(r'\bw([A-Z][a-z]+)', r'w/ \1', t)

    # Supplier codes -> "Brand code", pulled out so casing does not touch them.
    codes = []

    def take(m):
        brand = CODE_BRANDS[m.group(1).lower()]
        codes.append((brand, m.group(2)))
        return ' '
    t = CODE_TOKEN.sub(take, t)
    t = DANGLING_CODE.sub(' ', t)

    t = re.sub(r'\s+', ' ', t).strip(' -')
    if _is_shouted(t):
        t = _title_case(t)
    # 18X12 -> 18 x 12, but only a free-standing number: 1LU0310X00 is a code.
    # Repeated because 18 X18 X 9 only exposes its second X after the first.
    for _ in range(3):
        before = t
        t = DIMENSION.sub(r'\1 x ', t)
        if t == before:
            break

    # Name the code's brand only when the title names none: the vendor is the
    # first brand found in the title, and "Hose Assy Royal SD40020" should stay
    # a Royal part rather than turn Dirt Devil because of a dirtde_ code.
    named = any(b.lower() in t.lower() for b in list(BRANDS) + list(CODE_BRANDS.values()))
    for brand, code in codes:
        ref = code if named else f'{brand} {code}'
        if ref.lower() not in t.lower():
            t = f'{t} - {ref}' if t else ref
    if special:
        t = f'{t} {SPECIAL_ORDER_TAG}'

    t = re.sub(r'\s+', ' ', t)
    t = re.sub(r'(\s-)+\s', ' - ', t)
    return t.strip(' -') or title.strip()


EXAMPLES = [
    ('Crevice Tool bissel_2036655', 'bissel_2036655', 'Crevice Tool - Bissell 2036655'),
    ('BELT COGGED KENMORE kenmor_KS742024', 'KS742024', 'Belt Cogged Kenmore - KS742024'),
    ('FLAT BELT - HOOVER WINDTUNNEL (R) - 38528-033', 'CH033R', 'Flat Belt - Hoover WindTunnel (R) - 38528-033'),
    ('AXLE - JOHNNY VAC JV400 JV58', 'x', 'Axle - Johnny Vac JV400 JV58'),
    ("BONNET 17'' BLUE/ WHITE SCRUB", 'x', 'Bonnet 17" Blue/ White Scrub'),
    ('MICRO FILTER ACTIVE CHARCOAL BAGS HOOVER Y PK2 hoover_AH10165', '2865C',
     'Micro Filter Active Charcoal Bags Hoover Y PK2 - AH10165'),
    (' HOSE ASSY EUREKA STYLE 5800 SPEC ORD sanita_61865-4', 'x',
     'Hose Assy Eureka Style 5800 - 61865-4 (Special Order)'),
    ("***CRUSHPROOF 6' HOSE GREY SPEC ORDER", 'x', "Crushproof 6' Hose Grey (Special Order)"),
    ('Base wBrush Motor  Titanium bissel_2035644', 'x', 'Base w/ Brush Motor Titanium - Bissell 2035644'),
    ('BOX 18 X18 X 9 NO PICTURE NO LOGO', 'x', 'Box 18 x 18 x 9 No Picture No Logo'),
    ('TankInTank Assembly dirtde_2QC0505X00', 'x', 'TankInTank Assembly - Dirt Devil 2QC0505X00'),
    (' NLA  HOSE ASSY ROYAL SD40020   SPEC ORD dirtde_440001731', 'x',
     'NLA Hose Assy Royal SD40020 - 440001731 (Special Order)'),
    ('HEPA BAG INTERVAC  CS6,CS8 PK 5 +1 FILTER', 'x', 'HEPA Bag Intervac CS6,CS8 PK 5 +1 Filter'),
    ('Microfilter Bag for Bissell Zing 4122 Series Canister Vacuum - Pack of 3 Bags - Envirocare 820', 'x',
     'Microfilter Bag for Bissell Zing 4122 Series Canister Vacuum - Pack of 3 Bags - Envirocare 820'),
    ('POWER CORD 50\' FLOOR MACHINE 18 MM', 'x', "Power Cord 50' Floor Machine 18 mm"),
    ('HANDLE + SWITCH HOSE BLACK BOHA GAZ PUMP', 'x', 'Handle + Switch Hose Black Boha Gaz Pump'),
    ('  SEE CY720BELT STYLE 4  5   2PK  ROYAL DD    SPEC O dirtde_', 'x',
     'See CY720BELT Style 4 5 2PK Royal DD (Special Order)'),
]

if __name__ == '__main__':
    bad = 0
    for raw, sku, want in EXAMPLES:
        got = clean_title(raw, sku)
        again = clean_title(got, sku)
        ok = got == want and again == got
        bad += not ok
        print(('ok  ' if ok else 'FAIL') + f' {raw!r}\n     -> {got!r}' + ('' if ok else f'\n   want {want!r}\n  again {again!r}'))
    raise SystemExit(1 if bad else 0)
