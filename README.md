# JohnnyVac → Shopify Sync

Keeps the Kingsway Janitorial Shopify store in step with the JohnnyVac
product feed (`JVWebProducts.csv`, ~7,000 products): inventory, prices,
new products, archived products, categorization, collections, structured
metadata, SEO, and product descriptions.

## Workflows

| Workflow | Trigger | What it does |
|---|---|---|
| `sync.yml` | **Daily 7:00 UTC** + manual | Full catalog sync: create/update/archive products, inventory + prices, taxonomy, tags, brand, compatible models (merge + back-fill), SEO backfill, thin-description enrichment. Scheduled runs also create new collections and rank them. |
| `generate-descriptions.yml` | **Weekly Mon 8:00 UTC** + manual | AI-written descriptions (Claude) for products with thin/empty copy, 300 per scheduled run. Scheduled runs skip if `ANTHROPIC_API_KEY` isn't set. |
| `seo-generate.yml` | Manual | Backfill SEO meta titles/descriptions. |
| `backfill-metafields.yml` | Manual | One-time rollout of structured `custom.*` metafields to every product (dry-run by default). Does not write `compatible_models`. |
| `backfill-mpn.yml` | Manual | Legacy MPN-only backfill (superseded by `backfill-metafields.yml`). |
| `backfill-redirects.yml` | Manual | 301 redirects for previously archived product URLs. |

## Required repository secrets

| Secret | Required | Notes |
|---|---|---|
| `SHOPIFY_STORE` | ✅ | e.g. `kingsway-janitorial.myshopify.com` |
| `SHOPIFY_CLIENT_ID` / `SHOPIFY_CLIENT_SECRET` | ✅ (preferred) | Custom-app client credentials. A fresh Admin API token is minted at runtime, so it always carries the app's *current* scopes. |
| `SHOPIFY_ACCESS_TOKEN` | fallback | Static `shpat_…` token, only used when client credentials aren't set. Beware: static tokens keep their original scopes forever. |
| `ANTHROPIC_API_KEY` | ⚠️ recommended | Enables the AI steps: the model back-fill's AI pass and Claude-written product descriptions. **Currently missing** — without it the back-fill runs its rule pass only, descriptions fall back to templates and the weekly description run skips itself. |

Optional repo *variable*: `ANTHROPIC_MODEL` (defaults to `claude-opus-4-8`).

## Required Shopify app access scopes

Configure these on the custom app (Settings → Apps and sales channels →
Develop apps → your app → API scopes), then reinstall/re-release so the
minted token picks them up:

| Scope | Needed for | Status (last checked) |
|---|---|---|
| `write_products` | products, collections, metafields, SEO | ✅ granted |
| `write_inventory` | inventory quantities | ✅ granted |
| `read_locations` | location details for inventory | ❌ missing (sync now works without it, but grant it anyway) |
| `write_online_store_navigation` | 301 redirects when archiving products | ❌ missing — archived URLs 404 in Google until granted |
| `write_publications` | publishing collections the sync creates to the Online Store | ❌ missing — new collections are created unpublished until granted (the sync logs a warning at start-up while it is missing). Products are published already. |

> **Why the sync failed daily from 2026-06-24 to 2026-07-02:** the runtime-minted
> token only has `write_inventory,write_products`, and the sync queried the
> location **name** (needs `read_locations`). Shopify answered with
> `ACCESS_DENIED` + `data: null` and the script crashed before syncing
> anything. Fixed by querying only the location id and hardening all
> GraphQL null-data handling — but grant the scopes above for full
> functionality.

## Rules the sync always follows

- **Never sold or activated:** $0-priced rows, placeholder rows ("rarely
  ordered"), used items, JV promo merch and shipping boxes
  (`settings.skip_patterns` in `category_map_v4.json`), and zero-stock items.
  A feed row one of these rules excludes is never created; if a product
  already exists for it, it is held in **DRAFT**.
- **Vendor = brand, supplier = tags.** Vendor and `custom.brand` hold the
  brand the product is, or the machine brand a part is made for. Supplier
  routing never reads the vendor. Products tagged `hibbert` (or
  `sync:ignore`) are not in the JohnnyVac feed and are never drafted or
  archived by this sync (`SYNC_PROTECTED_TAGS`).
- **Nothing is hidden for being in a small collection.**

### Stock rule (one rule, every run)

| Feed stock | Product today | Result |
|---|---|---|
| > 0 | anything | **ACTIVE** |
| 0 | ACTIVE | stays **ACTIVE**, shows "Sold out" — inventory tracked, "continue selling when out of stock" off, so it cannot be bought |
| 0 | DRAFT, archived or not created yet | **DRAFT** until stock arrives |

So a product goes live the first time it has stock, a zero-stock item is
never activated, and a page Google has indexed does not 404 every time stock
runs out (about 3,000 feed items are at zero on a given day). The sync also
checks every variant each run and turns tracking on / "continue selling" off
where it finds otherwise. `KEEP_OOS_ACTIVE=false` switches to the stricter
rule (zero stock → DRAFT, always).

### Products that leave the feed

| Situation | Result |
|---|---|
| SKU no longer in the feed (live **or** draft) | **ARCHIVED**, tagged `sync:discontinued`, `/products/<handle>` 301-redirected to its category collection |
| SKU in the feed but excluded by a never-sell rule | held in **DRAFT** (not archived — the row is still there) |
| Archived by the sync, SKU comes back | restored (ACTIVE or DRAFT by the stock rule), tag removed |
| Archived by hand (no `sync:discontinued` tag) | left alone, never recreated |

Safety: at most `archive_cap` (default 50) **live** products may leave the
storefront in one run, and a feed with fewer than `FEED_MIN_SKUS` (5,000)
SKUs is treated as truncated. Either way the run archives nothing and fails,
so the failure email is the alert. Drafts being archived do not count
against the cap, which is how the first run clears the backlog of old drafts.

### Compatible models (`custom.compatible_models`)

The homepage part finder and the "Fits model" filter read this metafield.

- **Merged, never replaced.** Stored values (many are entered by hand) are
  kept exactly as they are; the sync only appends models that are missing.
  A list is never written back shorter or empty.
- The part's own SKU, MPN or part number is never a model, and neither is a
  bag / belt type ("Type AB", "Type Q/C").
- **Back-fill** for active parts with no models (`model_backfill.py`):
  1. *Rule pass* (free, daily): models named after "for", "fits",
     "compatible with", "models" in the title and description, kept only if
     the catalogue already lists that model somewhere. Tag: `models:rule`.
  2. *AI pass* (needs `ANTHROPIC_API_KEY`; `model_ai_cap`, default 500 parts
     per run): Claude returns `{"models": [...], "confidence": "high|low"}`;
     only models that appear in the part's text are accepted. High
     confidence → written, tag `models:ai`. Low confidence → **not**
     written, tag `models:ai-low`. Nothing found → tag `models:none`. The
     last two are not asked again; remove the tag to have a part re-checked.
  3. Every result is listed in `needs_review.csv`.
- Models are upper case with no spaces; dashes stay as written (DCF-15).

## What gets written to each product
## What gets written to each product

- **Core**: title, product type (37-category taxonomy from
  `category_map_v4.json`), vendor (detected brand), status, tags
  (manual tags preserved; only `confidence:*`, `source:*` and the
  category handle are managed).
- **Standard Product Taxonomy** category (powers Google Merchant Center).
- **Metafields** (`custom.*`): `mpn`, `brand`, `pack_quantity`,
  `material`, `compatible_models` (merged, see above), `size_inches`,
  `voltage` — extracted from the title/type. Google accepts Brand + MPN in
  place of GTIN.
- **Brand**: the feed has no brand column, so the brand is read from the
  title: the brand a part is made *for* ("Bag for Hoover …"), else the first
  machine brand named, else the product's maker (Envirocare, Plastiflex …).
  JohnnyVac is used where the title says so (name, a JV model number, a JV
  house model). A title that names no brand gets
  `settings.brands.fallback_vendor`.
- **SEO** meta title/description — only when missing (manual SEO kept).
- **Description** — only when the existing one is thin (< 80 chars of
  text). AI-written via Claude when `ANTHROPIC_API_KEY` is set, template
  fallback otherwise. Rich/manual descriptions are never overwritten.
- **Price + inventory** (absolute quantities, batched).

## Collections

`create_collections.py` runs on every scheduled sync (idempotent):

- **Category collections** — one smart collection per taxonomy category
  (productType rule), created once it meets the category's
  `min_products` threshold.
- **Brand collections** (`/collections/brand-<name>`) — one per vendor with
  ≥ 5 active products (vendor rule).
- **Existing collections keep their description and SEO.** All of them were
  rewritten by hand in Nov 2026, and the step that used to fill "missing or
  thin" ones is gone. Generated text is only used at the moment the sync
  creates a new collection.
- **Merges and renames live in `category_map_v4.json`**, so the sync does not
  recreate what was merged away:
  - `collection_handle` on a category — its collection lives under another
    handle (Filters → `filters-filtration`).
  - `collection_title` on a category — the collection is renamed to match
    (Gloves → "Gloves & Safety").
  - `settings.collections.retired_handles` — old category → replacement
    (Safety & PPE → Gloves, HEPA Vacuums → Commercial Vacuums). The old tag
    is removed from products and archive redirects use the replacement.
  - `settings.collections.never_publish` — handles the sync never publishes
    or edits (`parts-filters`, the "UNUSED - Filters & Filtration" one).
  - `settings.brands.merge` — one brand folded into another
    (Riccar → Simplicity).

## Reports

Each sync run uploads two artifacts: `needs_review.csv` (products that
couldn't be auto-categorized, plus every model the back-fill found) and
`skipped_products.csv` (feed rows the never-sell rules exclude). The job
summary lists what was archived, what was held as a draft, and the
back-fill's counts.

## Tests

    python -m unittest discover -s tests

Runs in the workflow before every sync; no network needed.
