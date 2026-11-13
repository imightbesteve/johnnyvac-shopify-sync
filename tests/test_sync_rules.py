"""Rules the sync must keep. No network: Shopify calls are stubbed.

    python -m unittest discover -s tests -v
"""
import json
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.chdir(ROOT)
for var in ('SHOPIFY_CLIENT_ID', 'SHOPIFY_CLIENT_SECRET', 'SHOPIFY_ACCESS_TOKEN', 'DRY_RUN'):
    os.environ.pop(var, None)

import sync_shopify_bulk_v3 as sync          # noqa: E402
import create_collections                    # noqa: E402
from categorizer_v4 import ProductCategorizer  # noqa: E402
from product_content import compute_vendor, extract_brand  # noqa: E402

HANDLES = {'parts-filters', 'parts-brushes-agitators', 'ops-gloves'}


def existing(sku, status='ACTIVE', **over):
    record = {
        'product_id': f'gid://shopify/Product/{sku}', 'variant_id': f'gid://v/{sku}',
        'inventory_item_id': f'gid://i/{sku}', 'title': f'Title {sku}', 'handle': f'h-{sku}'.lower(),
        'vendor': 'JohnnyVac', 'product_type': 'Parts & Replacement Parts > Brushes & Agitators',
        'status': status, 'tags': ['parts-brushes-agitators'], 'description_text': 'x' * 200,
        'category_id': '', 'seo_title': 't', 'seo_description': 'd', 'has_image': True,
        'compatible_models': [], 'compatible_parts': [], 'price': '10.00', 'inventory': 5,
        'sellable_at_zero': False,
    }
    record.update(over)
    return record


def feed_row(sku, title, inventory='5', price='10.00', category='Roller Brushes & Agitators'):
    return {'SKU': sku, 'ProductTitleEN': title, 'ProductTitleFR': '', 'ProductDescriptionEN': '',
            'ProductDescriptionFR': '', 'upc': '', 'RegularPrice': price, 'Inventory': inventory,
            'ProductCategory': category, 'AssociatedSkus': ''}


class Calls:
    """Stands in for graphql_request and records what would have been sent."""

    def __init__(self):
        self.updates, self.redirects = [], []

    def __call__(self, query, variables=None, use_rate_limit=True):
        variables = variables or {}
        if 'productUpdate' in query:
            self.updates.append(variables['product'])
            return {'data': {'productUpdate': {'product': {'id': variables['product']['id']}, 'userErrors': []}}}
        if 'urlRedirectCreate' in query:
            self.redirects.append(variables['urlRedirect'])
            return {'data': {'urlRedirectCreate': {'urlRedirect': {'id': 'r'}, 'userErrors': []}}}
        if 'collectionByHandle' in query:
            return {'data': {'collectionByHandle': {'id': 'c'}}}
        return {'data': {}}


class SyncTestCase(unittest.TestCase):
    def setUp(self):
        self.calls = Calls()
        self._saved = {k: getattr(sync, k) for k in (
            'graphql_request', 'get_metafield_definition_types', 'FEED_MIN_SKUS',
            'MAX_ARCHIVE_PER_RUN', 'DRY_RUN', 'ARCHIVE_MISSING', 'KEEP_OOS_ACTIVE')}
        sync.graphql_request = self.calls
        sync.get_metafield_definition_types = lambda: {'compatible_models': 'list.single_line_text_field'}
        sync.FEED_MIN_SKUS, sync.MAX_ARCHIVE_PER_RUN = 0, 50
        sync.DRY_RUN, sync.ARCHIVE_MISSING, sync.KEEP_OOS_ACTIVE = False, True, True
        sync._collection_handle_cache.clear()
        sync.ARCHIVED_BY_SYNC.clear()
        sync.ARCHIVED_ONLY_SKUS.clear()
        sync.RETIRED_HANDLES.clear()
        sync.COLLECTION_HANDLES.clear()

    def tearDown(self):
        for k, v in self._saved.items():
            setattr(sync, k, v)

    def categorized(self, rows):
        cat = ProductCategorizer('category_map_v4.json')
        done, skipped = cat.batch_categorize(rows)
        return done, skipped


class StockRule(SyncTestCase):
    def test_one_rule(self):
        self.assertEqual(sync.desired_status(3, None), 'ACTIVE')
        self.assertEqual(sync.desired_status(3, 'DRAFT'), 'ACTIVE')
        self.assertEqual(sync.desired_status(0, 'ACTIVE'), 'ACTIVE')     # sold out, page stays
        self.assertEqual(sync.desired_status(0, 'DRAFT'), 'DRAFT')       # never activated at zero
        self.assertEqual(sync.desired_status(0, None), 'DRAFT')          # new product, no stock
        self.assertEqual(sync.desired_status(0, 'ARCHIVED'), 'DRAFT')    # back in the feed, no stock

    def test_strict_mode_drafts_at_zero(self):
        sync.KEEP_OOS_ACTIVE = False
        self.assertEqual(sync.desired_status(0, 'ACTIVE'), 'DRAFT')

    def test_zero_stock_draft_is_not_activated_and_buyable_variant_is_fixed(self):
        rows, _ = self.categorized([feed_row('A1', 'Brush Roll for Hoover', inventory='0'),
                                    feed_row('A2', 'Brush Roll for Eureka', inventory='0')])
        store = {'A1': existing('A1', 'DRAFT', inventory=0),
                 'A2': existing('A2', 'ACTIVE', inventory=0, sellable_at_zero=True)}
        _, to_update, _ = sync.calculate_delta(rows, store, HANDLES)
        by = {p['SKU']: p for p in to_update}
        self.assertEqual(by['A1']['_desired']['status'], 'DRAFT')
        self.assertEqual(by['A2']['_desired']['status'], 'ACTIVE')
        self.assertEqual(sync.variant_update_input(by['A2']),
                         {'id': 'gid://v/A2', 'inventoryPolicy': 'DENY', 'inventoryItem': {'tracked': True}})


class CompatibleModels(SyncTestCase):
    DYSON = ['DC04', 'DC07', 'DC14', 'DC33']

    def delta(self, stored, title='Brush-roller Agitator for Dyson DC04, DC07, DC14, DC33', fits=None):
        rows, _ = self.categorized([feed_row('PNDC04', title)])
        rows[0]['_fits'] = fits or []
        store = {'PNDC04': existing('PNDC04', title=title, vendor='Dyson', compatible_models=list(stored),
                                    tags=['parts-brushes-agitators', 'confidence:high', 'source:jv_category'])}
        _, to_update, unchanged = sync.calculate_delta(rows, store, HANDLES)
        return (to_update + unchanged)[0]

    def models_written(self, product):
        fields = [m for m in sync.build_metafields(product['_desired']) if m['key'] == 'compatible_models']
        return json.loads(fields[0]['value']) if fields else None

    def test_stored_models_survive_untouched(self):
        product = self.delta(self.DYSON)
        self.assertFalse(product.get('_flags', {}).get('fitment'))
        self.assertIsNone(self.models_written(product))      # not written at all

    def test_shorter_found_list_never_replaces_a_longer_stored_one(self):
        product = self.delta(self.DYSON, fits=['DC07'])
        self.assertIsNone(self.models_written(product))

    def test_new_models_are_appended(self):
        product = self.delta(self.DYSON, fits=['DC41', 'dc07'])
        self.assertEqual(self.models_written(product), self.DYSON + ['DC41'])

    def test_own_sku_is_never_a_model(self):
        product = self.delta([], title='Brush Roll', fits=['PNDC04', 'DC07'])
        self.assertEqual(self.models_written(product), ['DC07'])

    def test_rule_backfill_tags_and_writes(self):
        rows, _ = self.categorized([feed_row('B7', 'Agitator fits Dyson DC07 and DC14'),
                                    feed_row('M1', 'Other Brush for Dyson DC07')])
        store = {'B7': existing('B7'), 'M1': existing('M1', compatible_models=['DC07', 'DC14'])}
        stats = sync.backfill_models(rows, store, {})
        self.assertEqual(stats['rule'], 1)
        _, to_update, _ = sync.calculate_delta(rows, store, HANDLES)
        b7 = next(p for p in to_update if p['SKU'] == 'B7')
        self.assertEqual(sync.final_models(b7['_desired']), ['DC07', 'DC14'])
        self.assertIn('models:rule', b7['_final_tags'])


class AiPass(unittest.TestCase):
    """The AI pass against a stand-in for the anthropic SDK."""

    def test_only_models_written_in_the_text_are_accepted(self):
        import types
        import model_backfill

        class APIError(Exception):
            pass

        seen = []

        class Messages:
            def create(self, **kwargs):
                seen.append(kwargs)
                text = kwargs['messages'][0]['content']
                if 'UH70120' in text:
                    body = {"models": ["UH70120", "UH70125", "562289001"], "confidence": "high"}
                elif 'maybe' in text:
                    body = {"models": ["4122"], "confidence": "low"}
                else:
                    body = {"models": [], "confidence": "high"}
                block = types.SimpleNamespace(type='text', text=json.dumps(body))
                return types.SimpleNamespace(stop_reason='end_turn', content=[block])

        fake = types.SimpleNamespace(
            Anthropic=lambda: types.SimpleNamespace(messages=Messages()),
            APIError=APIError, AuthenticationError=type('A', (APIError,), {}),
            PermissionDeniedError=type('P', (APIError,), {}), NotFoundError=type('N', (APIError,), {}),
            BadRequestError=type('B', (APIError,), {}))
        sys.modules['anthropic'] = fake
        os.environ['ANTHROPIC_API_KEY'] = 'test'
        try:
            items = [
                {'sku': 'hoover_562289001', 'title': 'Belt for Hoover UH70120 - 562289001', 'description': '',
                 'own_ids': {'562289001', 'HOOVER562289001'}},
                {'sku': 'X2', 'title': 'Filter maybe Zing 4122', 'description': '', 'own_ids': {'X2'}},
                {'sku': 'X3', 'title': 'Plain Wheel', 'description': '', 'own_ids': {'X3'}},
            ]
            out = model_backfill.ai_models(items, log=lambda m: None)
        finally:
            del sys.modules['anthropic']
            del os.environ['ANTHROPIC_API_KEY']
        self.assertEqual(out['hoover_562289001'], {'models': ['UH70120'], 'confidence': 'high'})
        self.assertEqual(out['X2'], {'models': ['4122'], 'confidence': 'low'})
        self.assertEqual(out['X3'], {'models': [], 'confidence': 'high'})
        self.assertEqual(seen[0]['output_config']['format']['type'], 'json_schema')
        self.assertEqual(seen[0]['model'], 'claude-sonnet-5-5')


class Retire(SyncTestCase):
    def run_retire(self, store, feed, blocked=None):
        return sync.retire_products(set(feed), blocked or {}, store, HANDLES)

    def test_gone_products_are_archived_with_redirect(self):
        store = {'LIVE': existing('LIVE'), 'OLD': existing('OLD', 'DRAFT'), 'KEEP': existing('KEEP')}
        result = self.run_retire(store, ['KEEP'])
        self.assertEqual({u['id']: u['status'] for u in self.calls.updates},
                         {'gid://shopify/Product/LIVE': 'ARCHIVED', 'gid://shopify/Product/OLD': 'ARCHIVED'})
        self.assertTrue(all('sync:discontinued' in u['tags'] for u in self.calls.updates))
        self.assertEqual({r['path']: r['target'] for r in self.calls.redirects},
                         {'/products/h-live': '/collections/parts-brushes-agitators',
                          '/products/h-old': '/collections/parts-brushes-agitators'})
        self.assertEqual(result['backlog'], 1)

    def test_blocked_rows_are_drafted_not_archived(self):
        store = {'ZERO': existing('ZERO'), 'ZD': existing('ZD', 'DRAFT')}
        result = self.run_retire(store, ['ZERO', 'ZD'], {'ZERO': 'Price $0.00', 'ZD': 'Price $0.00'})
        self.assertEqual(self.calls.updates, [{'id': 'gid://shopify/Product/ZERO', 'status': 'DRAFT'}])
        self.assertEqual(self.calls.redirects, [])
        self.assertEqual(len(result['drafted']), 1)

    def test_one_failed_request_does_not_stop_the_pass(self):
        import requests
        calls = self.calls

        def flaky(query, variables=None, use_rate_limit=True):
            if 'productUpdate' in query and variables['product']['id'].endswith('/B'):
                raise requests.exceptions.HTTPError('404 Client Error')
            return calls(query, variables, use_rate_limit)
        sync.graphql_request = flaky
        store = {k: existing(k, 'DRAFT') for k in ('A', 'B', 'C')}
        result = self.run_retire(store, [])
        self.assertEqual([r['sku'] for r in result['archived']], ['A', 'C'])

    def test_existing_redirect_counts_as_done(self):
        sync.graphql_request = lambda *a, **k: {'data': {'urlRedirectCreate': {'userErrors': [
            {'field': ['urlRedirect', 'path'], 'message': 'Path has already been taken'}]}}}
        self.assertTrue(sync.create_url_redirect('/products/x', '/collections/y'))

    def test_transient_http_error_is_retried(self):
        import types
        answers = [types.SimpleNamespace(status_code=404, headers={'Retry-After': '0'}),
                   types.SimpleNamespace(status_code=200, headers={}, raise_for_status=lambda: None,
                                         json=lambda: {'data': {'ok': True}})]
        real_post, sync.requests.post = sync.requests.post, lambda *a, **k: answers.pop(0)
        sync.graphql_request = self._saved['graphql_request']
        try:
            self.assertEqual(sync.graphql_request('query { x }', use_rate_limit=False)['data'], {'ok': True})
        finally:
            sync.requests.post = real_post

    def test_other_suppliers_products_are_left_alone(self):
        store = {'HIB': existing('HIB', tags=['hibbert']), 'MAN': existing('MAN', tags=['sync:ignore'])}
        self.run_retire(store, [])
        self.assertEqual(self.calls.updates, [])

    def test_cap_refuses_everything(self):
        sync.MAX_ARCHIVE_PER_RUN = 1
        store = {'A': existing('A'), 'B': existing('B'), 'D': existing('D', 'DRAFT')}
        result = self.run_retire(store, [])
        self.assertTrue(result['refused'])
        self.assertEqual(self.calls.updates, [])

    def test_short_feed_refuses_everything(self):
        sync.FEED_MIN_SKUS = 5000
        result = self.run_retire({'D': existing('D', 'DRAFT')}, ['X'])
        self.assertTrue(result['refused'])
        self.assertEqual(self.calls.updates, [])

    def test_redirect_uses_the_surviving_collection(self):
        sync.RETIRED_HANDLES.update({'ops-safety-ppe': 'ops-gloves'})
        sync.COLLECTION_HANDLES.update({'parts-filters': 'filters-filtration'})
        self.assertEqual(sync.redirect_target_for({'tags': ['ops-safety-ppe']}, HANDLES), '/collections/ops-gloves')
        self.assertEqual(sync.redirect_target_for({'tags': ['parts-filters']}, HANDLES),
                         '/collections/filters-filtration')

    def test_archived_by_sync_comes_back_when_the_sku_returns(self):
        products = {}
        sync._register_existing(products, 'BACK', existing('BACK', 'ARCHIVED',
                                                           tags=['parts-brushes-agitators', 'sync:discontinued']))
        sync._register_existing(products, 'DUP', existing('DUP', 'ARCHIVED'))
        rows, _ = self.categorized([feed_row('BACK', 'Brush Roll for Hoover'),
                                    feed_row('DUP', 'Brush Roll for Eureka')])
        to_create, to_update, _ = sync.calculate_delta(rows, products, HANDLES)
        self.assertEqual(to_create, [])                       # the manual archive is not recreated
        self.assertEqual([p['SKU'] for p in to_update], ['BACK'])
        self.assertEqual(to_update[0]['_desired']['status'], 'ACTIVE')
        self.assertNotIn('sync:discontinued', to_update[0]['_final_tags'])


class NeverSell(SyncTestCase):
    def test_rows_that_are_never_created_or_activated(self):
        rows = [
            feed_row('FREE', 'Hose Cuff', price='0.00'),
            feed_row('PENNY', 'Hose Cuff', price='0.010000'),
            feed_row('TU601CX', '**SLIGHTLY USED*TELESCOP WAND 1 1/4"X37"  QDC', category='Wand & Handles'),
            feed_row('UPAC30UD', 'USED: CENTRAL VACUUM 30\' HOSE AND ACCESSORIES BASIC KIT',
                     category='Accessories Kits for Central Vacuums'),
            feed_row('B1NPNL', 'BOX 18 X18 X 9 NO PICTURE NO LOGO', category='All Parts'),
            feed_row('B2BP', 'Box for Central Vacuum Kit - 20" (L) x 16" (W) x 16" (H)', category='All Parts'),
            feed_row('CAP1SM', 'Johnny Vac Trucker Mesh Cap', category='Promotional products'),
            feed_row('OK1', 'Motor Brush Used With Motor 27212081', category='All Parts'),
            feed_row('OK2', 'Battery Box Cover JVC43B', category='All Parts'),
            feed_row('OK3', 'Battery Connector - Can be used for other purposes', category='All Parts'),
        ]
        done, skipped = self.categorized(rows)
        self.assertEqual({p['SKU'] for p in skipped},
                         {'FREE', 'PENNY', 'TU601CX', 'UPAC30UD', 'B1NPNL', 'B2BP', 'CAP1SM'})
        self.assertEqual({p['SKU'] for p in done}, {'OK1', 'OK2', 'OK3'})


class Brands(unittest.TestCase):
    CASES = [
        ('Microfilter Bag for Bissell Zing 4122 Series Canister Vacuum - Pack of 3 Bags - Envirocare 820', 'Bissell'),
        ('HEPA Microfilter Vacuum Bag for Johnny Vac Hydrogen - Pack of 3 Bags', 'JohnnyVac'),
        ('Paper Bag for Johnny Vac Commercial Vacuum JV5 and Ghibli AS5 - Pack of 5 Bags', 'JohnnyVac'),
        ('Console - Ghibli/ Johnny Vac AS6', 'Ghibli'),
        ('Crevice Tool - Bissell 2036655', 'Bissell'),
        ('Float System - EDIC Galaxy', 'EDIC'),
        ('Tangential Vacuum Motor - 7.2" dia - 2 Fans - 120 V - Lamb / Ametek 117465-00 (S)', 'Ametek'),
        ('Microfilter Bag for Riccar 1500 and Simplicity Type H - Pack of 6 Bags', 'Simplicity'),
        ('Axle - Johnny Vac JV400 JV58', 'JohnnyVac'),
        ('Nut M8 - for JVC50BC Autoscrubber', 'JohnnyVac'),
        ('Microfiber Cloth - perfect for windows', None),
        ('Dust Mop for Dry Floors - with 60" Handle', None),
    ]

    def test_brand_is_the_machine_brand(self):
        for title, brand in self.CASES:
            self.assertEqual(extract_brand(title), brand, title)

    def test_unbranded_falls_back(self):
        self.assertEqual(compute_vendor('Dust Mop for Dry Floors'), 'JohnnyVac')


class CategoryMap(SyncTestCase):
    def setUp(self):
        super().setUp()
        self.map = json.load(open('category_map_v4.json', encoding='utf-8'))
        self.handles = {c['handle'] for c in self.map['categories']}

    def test_merged_categories_are_gone(self):
        self.assertNotIn('ops-safety-ppe', self.handles)
        self.assertNotIn('equipment-hepa-vacuums', self.handles)
        mapped = {v['handle'] for v in self.map['jv_category_mappings'].values() if isinstance(v, dict)}
        self.assertLessEqual(mapped, self.handles)
        self.assertEqual(self.map['jv_category_mappings']['HEPA Specialized Vacuums']['handle'],
                         'equipment-commercial-vacuums')

    def test_retired_tag_is_replaced(self):
        sync.RETIRED_HANDLES.update(self.map['settings']['collections']['retired_handles'])
        tags = sync.merge_tags(['ops-safety-ppe', 'hand-added'], ['ops-gloves'], self.handles)
        self.assertEqual(tags, ['hand-added', 'ops-gloves'])

    def test_collection_specs(self):
        create_collections.NEVER_PUBLISH.clear()
        create_collections.MERGED_BRANDS.clear()
        categories = create_collections.load_category_map()
        specs = create_collections.build_specs(categories, {}, {'Riccar': 9, 'Simplicity': 40, 'Rainbow': 5})
        by_handle = {s['handle']: s for s in specs}
        self.assertEqual(by_handle['ops-gloves']['title'], 'Gloves & Safety')
        self.assertTrue(by_handle['ops-gloves']['rename'])
        self.assertIn('filters-filtration', by_handle)          # Filters uses the live collection
        self.assertNotIn('parts-filters', by_handle)            # "UNUSED - Filters & Filtration"
        self.assertNotIn('ops-safety-ppe', by_handle)
        self.assertNotIn('equipment-hepa-vacuums', by_handle)
        self.assertNotIn('brand-riccar', by_handle)
        self.assertIn('brand-simplicity', by_handle)
        self.assertIn('brand-rainbow', by_handle)
        self.assertIn('parts-filters', create_collections.NEVER_PUBLISH)


if __name__ == '__main__':
    unittest.main()
