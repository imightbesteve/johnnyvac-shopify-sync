#!/usr/bin/env python3
"""Put what a shopper can buy at the top of every collection.

About 45% of live products are sold out (they stay ACTIVE so their pages keep
their search ranking), and "Best selling" -- the sort every collection used --
knows nothing about stock or photos, so collection pages opened on sold-out,
image-less items. Shopify has no sort that demotes them, so this keeps each
collection in MANUAL order and re-ranks it daily:

    1. in stock, with a photo
    2. in stock, no photo
    3. sold out, with a photo
    4. sold out, no photo

best-sellers first within each group. Only ACTIVE products are ranked --
archived and draft ones never show, so they are left wherever they sit. Only
the products that are out of place are moved, so after the first run a day's
moves are the handful whose stock, photo or sales changed.

A collection sorted Best selling is switched to Manual and managed from then
on. To opt one out, give it any other sort order in the admin (the one
price-sorted collection is left alone for that reason).

Environment: SHOPIFY_STORE, SHOPIFY_CLIENT_ID/SECRET (or SHOPIFY_ACCESS_TOKEN),
DRY_RUN=true to report moves without making them, ONLY=<handle> to do one.
"""
import os
import sys
import time
from typing import Dict, List, Optional, Tuple

import requests

from shopify_auth import get_access_token

SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', 'kingsway-janitorial.myshopify.com')
API_VERSION = '2026-01'
GRAPHQL_URL = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/graphql.json"
DRY_RUN = os.environ.get('DRY_RUN', 'false').lower() == 'true'
ONLY = os.environ.get('ONLY', '').strip()
MANAGED = ('BEST_SELLING', 'MANUAL')
MOVES_PER_CALL = 250  # collectionReorderProducts limit

TOKEN = ''  # resolved in main()

GROUP_LABELS = ('in stock + photo', 'in stock, no photo', 'sold out + photo', 'sold out, no photo')

Q_COLLECTIONS = """query($cursor: String) {
  collections(first: 100, after: $cursor) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id handle sortOrder
      productsCount { count }
      resourcePublications(first: 10) { nodes { isPublished publication { name } } }
    }
  }
}"""

Q_PRODUCTS = """query($id: ID!, $cursor: String, $sort: ProductCollectionSortKeys!) {
  collection(id: $id) {
    products(first: 250, after: $cursor, sortKey: $sort) {
      pageInfo { hasNextPage endCursor }
      nodes { id status totalInventory tracksInventory featuredMedia { id } }
    }
  }
}"""

M_MANUAL = """mutation($input: CollectionInput!) {
  collectionUpdate(input: $input) { collection { id sortOrder } userErrors { field message } }
}"""

M_REORDER = """mutation($id: ID!, $moves: [MoveInput!]!) {
  collectionReorderProducts(id: $id, moves: $moves) { job { id done } userErrors { field message } }
}"""

Q_JOB = "query($id: ID!) { job(id: $id) { done } }"


def log(msg: str) -> None:
    print(msg, flush=True)


_session = requests.Session()


def gql(query: str, variables: Optional[Dict] = None) -> Dict:
    """POST one GraphQL request, waiting out throttling. Raises on errors."""
    for attempt in range(8):
        r = _session.post(GRAPHQL_URL, json={'query': query, 'variables': variables or {}},
                          headers={'X-Shopify-Access-Token': TOKEN}, timeout=60)
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(min(2 ** attempt, 30))
            continue
        r.raise_for_status()
        body = r.json()
        errors = body.get('errors') or []
        if any((e.get('extensions') or {}).get('code') == 'THROTTLED' for e in errors):
            time.sleep(min(2 ** attempt, 30))
            continue
        if errors:
            raise RuntimeError(str(errors)[:400])
        for op in (body.get('data') or {}).values():
            if isinstance(op, dict) and op.get('userErrors'):
                raise RuntimeError(f"userErrors: {op['userErrors']}")
        return body['data']
    raise RuntimeError('throttled out')


def collection_products(cid: str, sort: str) -> List[Dict]:
    out, cursor = [], None
    while True:
        page = gql(Q_PRODUCTS, {'id': cid, 'cursor': cursor, 'sort': sort})['collection']['products']
        out += page['nodes']
        if not page['pageInfo']['hasNextPage']:
            return out
        cursor = page['pageInfo']['endCursor']


def group_of(p: Dict) -> int:
    in_stock = (p.get('totalInventory') or 0) > 0 if p.get('tracksInventory') else True
    has_photo = bool(p.get('featuredMedia'))
    return (0 if has_photo else 1) if in_stock else (2 if has_photo else 3)


def plan_moves(current: List[str], active_order: List[str]) -> List[Tuple[str, int]]:
    """Moves that bring the ACTIVE products into active_order, applied in
    sequence as collectionReorderProducts applies them. Others stay put."""
    active = set(active_order)
    sim, moves, k = list(current), [], 0
    for want in active_order:
        while sim[k] not in active:
            k += 1
        if sim[k] != want:
            j = sim.index(want, k + 1)
            sim.insert(k, sim.pop(j))
            moves.append((want, k))
        k += 1
    return moves


def apply_moves(cid: str, moves: List[Tuple[str, int]]) -> None:
    for start in range(0, len(moves), MOVES_PER_CALL):
        chunk = moves[start:start + MOVES_PER_CALL]
        job = gql(M_REORDER, {'id': cid, 'moves': [{'id': pid, 'newPosition': str(pos)} for pid, pos in chunk]})
        job_id = job['collectionReorderProducts']['job']['id']
        # Moves are positional, so the next chunk must see this one applied.
        for _ in range(120):
            if gql(Q_JOB, {'id': job_id})['job']['done']:
                break
            time.sleep(1)
        else:
            raise RuntimeError(f"reorder job {job_id} did not finish")


def sort_collection(c: Dict) -> Tuple[int, List[int]]:
    """Returns (moves made or planned, active products per group)."""
    if c['sortOrder'] == 'BEST_SELLING' and not DRY_RUN:
        gql(M_MANUAL, {'input': {'id': c['id'], 'sortOrder': 'MANUAL'}})
    current = [p['id'] for p in collection_products(c['id'], 'COLLECTION_DEFAULT')]
    ranked = [p for p in collection_products(c['id'], 'BEST_SELLING') if p['status'] == 'ACTIVE']
    ranked.sort(key=group_of)  # stable: best-sellers stay first within a group
    groups = [0, 0, 0, 0]
    for p in ranked:
        groups[group_of(p)] += 1
    moves = plan_moves(current, [p['id'] for p in ranked])
    if moves and not DRY_RUN:
        apply_moves(c['id'], moves)
    return len(moves), groups


def main() -> None:
    global TOKEN
    TOKEN = get_access_token() or ''
    if not TOKEN:
        sys.exit('no Shopify token (set SHOPIFY_CLIENT_ID/SECRET or SHOPIFY_ACCESS_TOKEN)')
    log(f"Sorting collections: in stock + photo first{' (DRY RUN)' if DRY_RUN else ''}")

    colls, cursor = [], None
    while True:
        page = gql(Q_COLLECTIONS, {'cursor': cursor})['collections']
        colls += page['nodes']
        if not page['pageInfo']['hasNextPage']:
            break
        cursor = page['pageInfo']['endCursor']

    todo = [c for c in colls
            if c['sortOrder'] in MANAGED and c['productsCount']['count']
            and any(n['isPublished'] and n['publication']['name'] == 'Online Store'
                    for n in c['resourcePublications']['nodes'])
            and (not ONLY or c['handle'] == ONLY)]
    skipped = [c['handle'] for c in colls if c['sortOrder'] not in MANAGED]
    log(f"{len(todo)} collections to rank; left alone (own sort order): {', '.join(skipped) or 'none'}")

    total_moves, failed = 0, []
    for c in todo:
        try:
            n, groups = sort_collection(c)
        except (RuntimeError, requests.exceptions.RequestException) as e:
            failed.append(c['handle'])
            log(f"  ⚠️  {c['handle']}: {e}")
            continue
        total_moves += n
        log(f"  {c['handle']:<40} {n:>5} moves   " + ' | '.join(f"{g} {l}" for g, l in zip(groups, GROUP_LABELS)))

    log(f"✓ {'Would move' if DRY_RUN else 'Moved'} {total_moves} products across {len(todo) - len(failed)} collections"
        + (f"; {len(failed)} failed: {', '.join(failed)}" if failed else ''))
    if failed:
        sys.exit(1)


if __name__ == '__main__':
    main()
