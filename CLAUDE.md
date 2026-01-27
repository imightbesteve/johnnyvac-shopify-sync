# CLAUDE.md - JohnnyVac Shopify Sync

## Project Overview

This repository contains an automated product synchronization system that imports products from JohnnyVac's CSV feed into a Shopify store. The system includes intelligent product categorization, delta sync capabilities, and automated collection management.

**Target Store**: Kingsway Janitorial (kingsway-janitorial.myshopify.com)

## Architecture

```
johnnyvac-shopify-sync/
├── sync_shopify_bulk.py      # Main sync script (GraphQL bulk operations)
├── categorizer_v3.py         # Product categorization engine
├── category_map_v3.json      # Category rules and keyword mappings
├── create_collections.py     # Shopify automated collection creator
└── .github/workflows/
    └── sync.yml              # GitHub Actions workflow (daily sync)
```

## Core Components

### 1. Product Sync (`sync_shopify_bulk.py`)

The main synchronization script that:
- Fetches product data from JohnnyVac CSV feed (semicolon-delimited)
- Categorizes products using the categorization engine
- Performs delta sync (only creates/updates changed products)
- Uses parallel batch processing for performance

**Key constants:**
- `CSV_URL`: https://www.johnnyvacstock.com/sigm_all_jv_products/JVWebProducts.csv
- `IMAGE_BASE_URL`: https://www.johnnyvacstock.com/photos/web/
- `API_URL`: Shopify GraphQL Admin API (2024-01 version)
- `BATCH_SIZE`: 100 products
- `RATE_LIMIT_PER_SECOND`: 2 requests
- `MAX_CONCURRENT_REQUESTS`: 4 parallel threads

**Sync workflow:**
1. Initialize categorizer with `category_map_v3.json`
2. Fetch CSV data from JohnnyVac
3. Categorize all products (with min_products enforcement)
4. Fetch existing Shopify products (indexed by SKU)
5. Calculate delta (new vs changed vs unchanged)
6. Batch create new products (parallel chunks of 20)
7. Batch update changed products (parallel chunks of 10)

### 2. Product Categorizer (`categorizer_v3.py`)

A priority-driven categorization engine:

**Key features:**
- Global part keywords pre-check (highest priority short-circuit to "Parts > General Parts")
- SKU/model noise stripping before matching
- Priority-based category loop (higher priority categories match first)
- Confidence levels: `high` (title match), `medium` (description match), `low` (other)
- `min_products` enforcement (demotes underpopulated categories to "Needs Review")
- Bilingual support (EN/FR)

**Text normalization:**
- Removes punctuation, collapses whitespace
- Strips SKU patterns like `simpli_B224-0500`, `JV202`, `PN600`
- Supports phrase matching (multi-word keywords) and word boundary matching

### 3. Category Map (`category_map_v3.json`)

JSON configuration with:
- `settings.global_part_keywords`: Keywords that immediately route to General Parts
- `categories[]`: Array of category definitions sorted by priority (100 = highest)

**Category structure:**
```json
{
  "productType": "Parts & Replacement Parts > Motors & Electrical",
  "handle": "parts-motors-electrical",
  "title": "Motors & Electrical Parts",
  "priority": 100,
  "keywords_en": ["motor", "cord", "plug", ...],
  "keywords_fr": ["moteur", "cordon", ...],
  "exclusions_en": [],
  "exclusions_fr": [],
  "min_products": 1
}
```

**Priority tiers:**
- 100: Specific parts categories (Motors, Pumps, Filters, etc.)
- 95: Assemblies, Repair Kits, Wheels
- 90: Latches, Springs, Squeegees
- 80: Consumables (Chemicals, Paper)
- 70-75: Tools & Accessories
- 60: Safety/PPE
- 50: Equipment & Machines (with exclusions to avoid matching parts)
- 10: General Parts (fallback)
- 0: Needs Review (no match)

### 4. Collection Creator (`create_collections.py`)

Creates Shopify automated collections based on `productType` rules:
- Queries current product counts by productType
- Skips categories below `min_products` threshold
- Idempotent (checks if collection exists before creating)
- Optional auto-publish via `AUTO_PUBLISH` environment variable

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `SHOPIFY_STORE` | Store URL (e.g., `kingsway-janitorial.myshopify.com`) | Yes |
| `SHOPIFY_ACCESS_TOKEN` | Shopify Admin API token (`shpat_...`) | Yes |
| `AUTO_PUBLISH` | Set to `true` to auto-publish collections | No |

## GitHub Actions Workflow

**Schedule**: Daily at 7 AM UTC (cron: `0 7 * * *`)

**Manual trigger options:**
- `create_collections`: Run collection creator after sync
- `auto_publish`: Auto-publish new collections

**Steps:**
1. Checkout repository
2. Setup Python 3.11
3. Install dependencies (`requests`)
4. Validate `category_map_v3.json`
5. Run `sync_shopify_bulk.py`
6. Optionally run `create_collections.py`

## Development Guidelines

### Modifying Categories

When updating `category_map_v3.json`:
1. Use appropriate priority (higher = matches first)
2. Add exclusions to prevent parts from matching equipment categories
3. Set realistic `min_products` threshold
4. Include both `keywords_en` and `keywords_fr` for bilingual support
5. Use the `handle` field for collection URL slugs

### Adding Global Part Keywords

Add to `settings.global_part_keywords` when a keyword should always route to General Parts regardless of other category matches.

### Rate Limiting

The sync script uses a token bucket rate limiter:
- Standard Shopify: 2 requests/second
- Shopify Plus: Can increase to 4 requests/second
- Automatic retry with exponential backoff on connection errors

### Testing Changes

1. Run locally with environment variables set:
   ```bash
   export SHOPIFY_STORE="kingsway-janitorial.myshopify.com"
   export SHOPIFY_ACCESS_TOKEN="shpat_..."
   python sync_shopify_bulk.py
   ```

2. Check `needs_review.csv` output for uncategorized products

3. Review categorization stats in console output

## Common Tasks

### Check categorization without syncing

Create a test script that imports `ProductCategorizer` and runs `batch_categorize()` on the CSV data without calling Shopify APIs.

### Debug uncategorized products

1. Check `needs_review.csv` after a sync run
2. Look at the `Reason` column for why products weren't matched
3. Add appropriate keywords to `category_map_v3.json`

### Force full re-sync

Currently, delta sync only updates products where title, price, inventory, or productType changed. To force a full update, you would need to modify the `calculate_delta()` function or clear products from Shopify.

## API Reference

### Shopify GraphQL Queries Used

- `products(first: 250, after: $cursor)` - Fetch existing products
- `productCreate(input: $input)` - Create new product
- `productUpdate(input: $input)` - Update existing product
- `collectionByHandle(handle: $handle)` - Check collection exists
- `CollectionCreate(input: $input)` - Create automated collection

### CSV Field Mapping

| CSV Field | Shopify Field |
|-----------|---------------|
| `SKU` | `variants[].sku` |
| `ProductTitleEN` / `ProductTitleFR` | `title` |
| `ProductDescriptionEN` / `ProductDescriptionFR` | `descriptionHtml` |
| `RegularPrice` | `variants[].price` |
| `Inventory` | Status (ACTIVE if > 0, else DRAFT) |
| `weight` | `variants[].weight` |
| `upc` | `variants[].barcode` |

## Output Files

- `needs_review.csv`: Products that couldn't be automatically categorized (SKU, titles, descriptions, reason)
