/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["all-ro"]

[test]
reason = "A full-text OR-of-matches nested inside an AND is driven from the multi-index FT union (UnionIndexScan over two FullTextScan) instead of driving from the non-FT equality index and evaluating the OR as a per-row filter. Regression guard for the nested-OR-in-AND fix (the full-text cousin of issue #4996). Covers: (A) top-level OR unions; (B) OR nested in an AND with an indexed sibling unions; (C) a De Morgan rewrite unions; (D) WITH INDEX leaves the pinned plan alone; (E) the nested OR is the only indexable part (unindexed sibling) and still unions rather than falling back to a table scan."
issue = 7402

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: asset:1, search_body: 'the quick brown fox', search_title: 'hello world', tag: 'x', type: 'doc' }]"

[[test.results]]
value = "[{ id: asset:2, search_body: 'lorem ipsum hello', search_title: 'goodbye moon', tag: 'x', type: 'doc' }]"

[[test.results]]
value = "[{ id: asset:3, search_body: 'nothing here', search_title: 'hello there', tag: 'y', type: 'img' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id] {rows: 3}\n    Filter [ctx: Db] [predicate: search_title @1@ 'hello' OR search_body @2@ 'hello'] {rows: 3}\n        UnionIndexScan [ctx: Db] [table: asset, branches: 2] {rows: 3}\n            FullTextScan [ctx: Db] [index: asset_search_title_ft, query: hello] {rows: 2}\n            FullTextScan [ctx: Db] [index: asset_search_body_ft, query: hello] {rows: 1}\n\nTotal rows: 3"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id] {rows: 2}\n    Filter [ctx: Db] [predicate: type = 'doc' AND search_title @1@ 'hello' OR search_body @2@ 'hello'] {rows: 2}\n        UnionIndexScan [ctx: Db] [table: asset, branches: 2] {rows: 3}\n            FullTextScan [ctx: Db] [index: asset_search_title_ft, query: hello] {rows: 2}\n            FullTextScan [ctx: Db] [index: asset_search_body_ft, query: hello] {rows: 1}\n\nTotal rows: 2"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id] {rows: 2}\n    Filter [ctx: Db] [predicate: type = 'doc' AND search_title @1@ 'hello' OR type = 'doc' AND search_body @2@ 'hello'] {rows: 2}\n        UnionIndexScan [ctx: Db] [table: asset, branches: 2] {rows: 3}\n            FullTextScan [ctx: Db] [index: asset_search_title_ft, query: hello] {rows: 2}\n            FullTextScan [ctx: Db] [index: asset_search_body_ft, query: hello] {rows: 1}\n\nTotal rows: 2"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id] {rows: 2}\n    TableScan [ctx: Db] [table: asset, direction: Forward, predicate: type = 'doc' AND search_title @1@ 'hello' OR search_body @2@ 'hello', pre_decode_filter: no (unsupported predicate)] {rows: 2}\n\nTotal rows: 2"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id] {rows: 2}\n    Filter [ctx: Db] [predicate: tag = 'x' AND search_title @1@ 'hello' OR search_body @2@ 'hello'] {rows: 2}\n        UnionIndexScan [ctx: Db] [table: asset, branches: 2] {rows: 3}\n            FullTextScan [ctx: Db] [index: asset_search_title_ft, query: hello] {rows: 2}\n            FullTextScan [ctx: Db] [index: asset_search_body_ft, query: hello] {rows: 1}\n\nTotal rows: 2"'''

*/

DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX asset_search_title_ft ON asset FIELDS search_title FULLTEXT ANALYZER simple BM25;
DEFINE INDEX asset_search_body_ft ON asset FIELDS search_body FULLTEXT ANALYZER simple BM25;
DEFINE INDEX asset_type_idx ON asset FIELDS type;

CREATE asset:1 SET type = 'doc', tag = 'x', search_title = 'hello world', search_body = 'the quick brown fox';
CREATE asset:2 SET type = 'doc', tag = 'x', search_title = 'goodbye moon', search_body = 'lorem ipsum hello';
CREATE asset:3 SET type = 'img', tag = 'y', search_title = 'hello there', search_body = 'nothing here';

LET $q = 'hello';
LET $type = 'doc';
LET $tag = 'x';

-- (A) Top-level OR of two FT matches -> UnionIndexScan over two FullTextScan branches.
EXPLAIN ANALYZE SELECT id FROM asset WHERE search_title @1@ $q OR search_body @2@ $q;

-- (B) The SAME OR nested inside an AND chain (indexed sibling `type`). The fix drives from the
-- FT union and applies `type = $type` as a residual Filter, instead of driving from the low-
-- selectivity type equality and re-scoring the FT index for every `type` row.
EXPLAIN ANALYZE SELECT id FROM asset WHERE type = $type AND (search_title @1@ $q OR search_body @2@ $q);

-- (C) De Morgan rewrite: lifting the OR to the top level also unions (unchanged by the fix).
EXPLAIN ANALYZE SELECT id FROM asset
  WHERE (type = $type AND search_title @1@ $q) OR (type = $type AND search_body @2@ $q);

-- (D) WITH INDEX pins the plan: the nested-OR-in-AND optimisation is skipped under explicit
-- hints, so this still falls back to a full TableScan + Filter (the hint cannot form the union
-- because the top-level expression is an AND).
EXPLAIN ANALYZE SELECT id FROM asset WITH INDEX asset_search_title_ft, asset_search_body_ft
  WHERE type = $type AND (search_title @1@ $q OR search_body @2@ $q);

-- (E) The nested OR is the ONLY indexable part: `tag` has no index, so single-index analysis
-- finds no candidate and `candidates.is_empty()`. The nested-OR union must still be used rather
-- than falling back to a full table scan; `tag = $tag` is applied as a residual filter.
EXPLAIN ANALYZE SELECT id FROM asset WHERE tag = $tag AND (search_title @1@ $q OR search_body @2@ $q);
