# JohnnyVac → Shopify Sync

Keeps the Kingsway Janitorial Shopify store in step with the JohnnyVac
product feed (`JVWebProducts.csv`, ~7,000 products): inventory, prices,
new products, archived products, categorization, collections, structured
metadata, SEO, and product descriptions.

## Workflows

| Workflow | Trigger | What it does |
|---|---|---|
| `sync.yml` | **Daily 7:00 UTC** + manual | Full catalog sync: create/update/archive products, inventory + prices, taxonomy, tags, SEO backfill, thin-description enrichment. Scheduled runs also create/update collections. |
| `generate-descriptions.yml` | **Weekly Mon 8:00 UTC** + manual | AI-written descriptions (Claude) for products with thin/empty copy, 300 per scheduled run. Scheduled runs skip if `ANTHROPIC_API_KEY` isn't set. |
| `seo-generate.yml` | Manual | Backfill SEO meta titles/descriptions. |
| `backfill-metafields.yml` | Manual | One-time rollout of structured `custom.*` metafields to every product (dry-run by default). |
| `backfill-mpn.yml` | Manual | Legacy MPN-only backfill (superseded by `backfill-metafields.yml`). |
| `backfill-redirects.yml` | Manual | 301 redirects for previously archived product URLs. |

## Required repository secrets

| Secret | Required | Notes |
|---|---|---|
| `SHOPIFY_STORE` | ✅ | e.g. `kingsway-janitorial.myshopify.com` |
| `SHOPIFY_CLIENT_ID` / `SHOPIFY_CLIENT_SECRET` | ✅ (preferred) | Custom-app client credentials. A fresh Admin API token is minted at runtime, so it always carries the app's *current* scopes. |
| `SHOPIFY_ACCESS_TOKEN` | fallback | Static `shpat_…` token, only used when client credentials aren't set. Beware: static tokens keep their original scopes forever. |
| `ANTHROPIC_API_KEY` | ⚠️ recommended | Enables Claude-written product descriptions. **Currently missing** — without it every description falls back to templates and the weekly description run skips itself. |

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
| `write_publications` | auto-publishing new collections to the Online Store | ❌ missing — new collections are created unpublished until granted |

> **Why the sync failed daily from 2026-06-24 to 2026-07-02:** the runtime-minted
> token only has `write_inventory,write_products`, and the sync queried the
> location **name** (needs `read_locations`). Shopify answered with
> `ACCESS_DENIED` + `data: null` and the script crashed before syncing
> anything. Fixed by querying only the location id and hardening all
> GraphQL null-data handling — but grant the scopes above for full
> functionality.

## What gets written to each product

- **Core**: title, product type (37-category taxonomy from
  `category_map_v4.json`), vendor (detected brand), status, tags
  (manual tags preserved; only `confidence:*`, `source:*` and the
  category handle are managed).
- **Standard Product Taxonomy** category (powers Google Merchant Center).
- **Metafields** (`custom.*`): `mpn`, `brand`, `pack_quantity`,
  `material`, `compatible_models`, `size_inches`, `voltage` — extracted
  from the title/type. Google accepts Brand + MPN in place of GTIN.
- **SEO** meta title/description — only when missing (manual SEO kept).
- **Description** — only when the existing one is thin (< 80 chars of
  text). AI-written via Claude when `ANTHROPIC_API_KEY` is set, template
  fallback otherwise. Rich/manual descriptions are never overwritten.
- **Price + inventory** (absolute quantities, batched).
- Out-of-stock products stay ACTIVE ("Sold out") to preserve Google
  indexing; products that leave the feed are archived with a 301 redirect
  to their collection.

## Collections

`create_collections.py` runs on every scheduled sync (idempotent):

- **Category collections** — one smart collection per taxonomy category
  (productType rule), created once it meets the category's
  `min_products` threshold.
- **Brand collections** (`/collections/brand-<name>`) — one per detected
  vendor with ≥ 5 active products (vendor rule).
- Every collection gets a storefront description + SEO title/description.
  Existing collections with missing/thin descriptions are backfilled;
  hand-written descriptions are never touched.

## Reports

Each sync run uploads two artifacts: `needs_review.csv` (products that
couldn't be auto-categorized) and `skipped_products.csv` (placeholder
rows in the feed).
