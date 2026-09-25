/**
[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
EXPLAIN / row-parity matrix for the two index kinds with their own operators:
FULLTEXT (`FullTextScan`, reached by `@@`) and the vector kinds HNSW and
DISKANN (`KnnScan`, reached by `<|k,EF|>`). Held apart from index_kinds.surql
because both need fixtures the B-tree kinds do not — an analyzer and a declared
vector dimension.

Two axes: a single operator against an index that is present, decommissioned
with `ALTER INDEX ... PREPARE REMOVE`, or absent; and two operators of the same
kind conjoined in one WHERE clause, which is where a conjunct can be lost. The
second axis is pinned with each operator asserted alone first, so a collapse is
distinguishable from a column that simply has no index.

Narrowed to the two streaming strategies because a prefix `EXPLAIN` errors
under `compute-only`; the two streaming strategies plan identically by
contract, so pinning both records that and costs nothing.

Neither kind takes `WITH NOINDEX` as an oracle, and the file establishes why
before relying on anything else:

- `@@` is evaluated by an operator that resolves the full-text index itself,
  per row, whichever scan feeds it. For a single `@@` conjunct — the shape this
  is established on — `WITH NOINDEX` removes `FullTextScan` from the plan and
  changes nothing about the answer. The oracle for a `@@` query is therefore a
  predicate that cannot reach an index at all — here `string::contains`.
- `<|k,EF|>` is answerable *only* by a vector index. With no usable index it
  compiles to a plain `Filter` over a `TableScan` and matches nothing, so
  `WITH NOINDEX` turns a KNN query into an empty result rather than into a
  slower true one. The oracle is the brute-force form `<|k,METRIC|>`, which
  `KnnTopK` computes exactly over a scan.

Four wrong behaviours are pinned rather than hidden, each marked at its case:

1. A FULLTEXT index left behind by `ALTER INDEX ... PREPARE REMOVE` is dropped
   from the plan, so the plan reads as a full table scan — but the `@@`
   operator still resolves the unmaintained index per row, so rows written
   after the ALTER are invisible to the search while the plan claims every row
   was examined. Correct behaviour: a `prepare_remove` full-text index is not
   resolved, and `@@` over it either errors as "no suitable index" or is
   answered from the record. The pinned answer omits a matching row.
2. An HNSW index prepared for removal, or absent, makes `<|k,EF|>` answer `[]`
   with no error and no diagnostic in the plan beyond the missing `KnnScan`.
   Correct behaviour would be to answer the k nearest by brute force, as
   `<|k,METRIC|>` does over the same data.
3. Two `@@` conjuncts over the SAME indexed column reduce to one
   `FullTextScan` for the first term. The second conjunct leaves no trace in
   the plan — no second scan, no `BitmapAnd`, no `Filter` re-check — so the
   query returns every row matching the surviving term, which is a SUPERSET of
   the conjunction: rows that satisfy only one of the two conditions come back.
   Correct answer: the intersection, [ft2:1]. Swapping the conjuncts changes
   which term survives (the textually first one) and so which row is wrongly
   returned. Two facts in the same section make this a defect rather than an
   operator limit: the same two terms ORed on the same column DO keep both
   terms (a `BitmapOr` of two `BitmapFullTextScan`s), and the count() path
   plans the ANDed predicate correctly as an `IndexCountScan` over a
   `BitmapAnd` of one scan per term and reports the oracle's count — so one
   predicate yields a right count and wrong rows from one engine, and the
   intersection the row path needs already exists. Two `@@` conjuncts over
   DIFFERENT columns are also planned and answered correctly.
4. Two ANDed KNN operators collapse to a single `KnnScan`, with the second
   constraint absent and no residual `Filter`, so the query answers the
   surviving operator's top-K alone. The vec2 fixture orders records under
   `pa` and reverses that order under `pb`, so the two 2-nearest sets are
   disjoint and the correct answer to every conjunction pinned here is `[]`;
   the recorded answer is two records that satisfy only one constraint. The
   count() path does NOT rescue this one — it aggregates over the same single
   scan and reports 2 against an oracle of 0. It reproduces for two indexes on
   two columns, for one index queried with two vectors, and for the
   brute-force `<|k,METRIC|>` spelling, which uses no vector index at all —
   so the lost conjunct is in predicate planning, not in index selection.
   Each column's operator is pinned alone first, so the conjunction is a
   statement about losing a conjunct and not about an unindexed column.
"""

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    FullTextScan [ctx: Db] [index: doc_body, query: quick]
''''

[[test.results]]
value = "[{ id: doc:1 }, { id: doc:2 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: doc, direction: Forward, predicate: body @@ 'quick', pre_decode_filter: no (unsupported predicate)]
"'''

[[test.results]]
value = "[{ id: doc:1 }, { id: doc:2 }]"

[[test.results]]
value = "[doc:1, doc:2]"

[[test.results]]
value = '''
'Aggregate [ctx: Db] [mode: GROUP ALL]
    FullTextScan [ctx: Db] [index: doc_body, query: quick]
''''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id, sc]
    SortByKey [ctx: Db] [sort_keys: sc DESC]
        Compute [ctx: Db] [fields: sc = search::score(...)]
            FullTextScan [ctx: Db] [index: doc_body, query: quick]
''''

[[test.results]]
value = "[{ id: doc:1, sc: 0f }, { id: doc:2, sc: 0f }]"

[[test.results]]
value = "[{ id: doc:1, sc: 0.4925091564655304f }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: doc, direction: Forward, predicate: kw.* @@ 'alpha', pre_decode_filter: no (unsupported predicate)]
"'''

[[test.results]]
value = "[{ id: doc:1 }, { id: doc:2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[doc:1, doc:2]"

[[test.results]]
error = "There was no suitable index supporting the expression: kw @@ 'alpha'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ body: 'another quick one', id: doc:4, kw: ['delta'] }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: doc, direction: Forward, predicate: body @@ 'quick', pre_decode_filter: no (unsupported predicate)]
"'''

[[test.results]]
value = "[{ id: doc:1 }, { id: doc:2 }]"

[[test.results]]
value = "[doc:1, doc:2, doc:4]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid query: The `<|k|>` KNN operator (KTree / M-Tree) is no longer supported. Use `<|k, EF|>` against an HNSW index (e.g. `DEFINE INDEX … HNSW DIMENSION N`), or `<|k, DISTANCE|>` for a brute-force KNN with an explicit distance metric."

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    KnnTopK [ctx: Db] [field: point, k: 2, distance: Euclidean, dimension: 2]
        TableScan [ctx: Db] [table: pt, direction: Forward]
''''

[[test.results]]
value = "[pt:2, pt:3]"

[[test.results]]
value = '''
'Aggregate [ctx: Db] [mode: GROUP ALL]
    KnnTopK [ctx: Db] [field: point, k: 2, distance: Euclidean, dimension: 2]
        TableScan [ctx: Db] [table: pt, direction: Forward]
''''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: point <|2,40|> [22f, 2f]]
        TableScan [ctx: Db] [table: pt, direction: Forward]
''''

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    KnnScan [ctx: Db] [index: pt_hnsw, k: 2, ef: 40, dimension: 2]
''''

[[test.results]]
value = "[pt:2, pt:3]"

[[test.results]]
value = "[pt:2, pt:3]"

[[test.results]]
value = '''
'Aggregate [ctx: Db] [mode: GROUP ALL]
    KnnScan [ctx: Db] [index: pt_hnsw, k: 2, ef: 40, dimension: 2]
''''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    KnnScan [ctx: Db] [index: pt_hnsw, k: 2, ef: 40, dimension: 2]
''''

[[test.results]]
value = "[pt:2, pt:3]"

[[test.results]]
value = "[pt:2, pt:3]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: point <|2,40|> [22f, 2f]]
        TableScan [ctx: Db] [table: pt, direction: Forward]
''''

[[test.results]]
value = "[]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    KnnTopK [ctx: Db] [field: point, k: 2, distance: Manhattan, dimension: 2]
        TableScan [ctx: Db] [table: pt, direction: Forward]
''''

[[test.results]]
value = "[pt:2, pt:3]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id, d]
    SortByKey [ctx: Db] [sort_keys: d ASC]
        Compute [ctx: Db] [fields: d = vector::distance::knn(...)]
            Filter [ctx: Db] [predicate: grp = 'a']
                KnnScan [ctx: Db] [index: pt_hnsw, k: 2, ef: 40, dimension: 2, predicate: grp = 'a']
"'''

[[test.results]]
value = "[pt:1, pt:3]"

[[test.results]]
value = "[pt:1, pt:3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ grp: 'a', id: pt:5, point: [21f, 2f] }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: point <|2,40|> [22f, 2f]]
        TableScan [ctx: Db] [table: pt, direction: Forward]
''''

[[test.results]]
value = "[]"

[[test.results]]
value = "[pt:2, pt:5]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    KnnScan [ctx: Db] [index: pt_da, k: 2, ef: 40, dimension: 2]
''''

[[test.results]]
value = "[pt:2, pt:5]"

[[test.results]]
value = "[pt:2, pt:5]"

[[test.results]]
value = '''
'Aggregate [ctx: Db] [mode: GROUP ALL]
    KnnScan [ctx: Db] [index: pt_da, k: 2, ef: 40, dimension: 2]
''''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    FullTextScan [ctx: Db] [index: ft2_title, query: alpha]
''''

[[test.results]]
value = "[ft2:1, ft2:2]"

[[test.results]]
value = "[ft2:1]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: ft2, condition: title @1@ 'alpha' AND title @2@ 'beta']
    BitmapAnd [ctx: Db]
        BitmapFullTextScan [ctx: Db] [index: ft2_title, query: alpha]
        BitmapFullTextScan [ctx: Db] [index: ft2_title, query: beta]
"'''

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    FullTextScan [ctx: Db] [index: ft2_title, query: beta]
''''

[[test.results]]
value = "[ft2:1, ft2:3]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: title @1@ 'alpha' OR title @2@ 'beta']
        BitmapResolve [ctx: Db] [table: ft2]
            BitmapOr [ctx: Db]
                BitmapFullTextScan [ctx: Db] [index: ft2_title, query: alpha]
                BitmapFullTextScan [ctx: Db] [index: ft2_title, query: beta]
"'''

[[test.results]]
value = "[ft2:1, ft2:2, ft2:3]"

[[test.results]]
value = "[ft2:1, ft2:2, ft2:3]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: title @1@ 'alpha' AND body @2@ 'lima']
        BitmapResolve [ctx: Db] [table: ft2]
            BitmapAnd [ctx: Db]
                BitmapFullTextScan [ctx: Db] [index: ft2_body, query: lima]
                BitmapFullTextScan [ctx: Db] [index: ft2_title, query: alpha]
"'''

[[test.results]]
value = "[ft2:1]"

[[test.results]]
value = "[ft2:1]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    KnnScan [ctx: Db] [index: vec2_a, k: 2, ef: 40, dimension: 2]
''''

[[test.results]]
value = "[vec2:1, vec2:2]"

[[test.results]]
value = "[vec2:1, vec2:2]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    KnnScan [ctx: Db] [index: vec2_b, k: 2, ef: 40, dimension: 2]
''''

[[test.results]]
value = "[vec2:4, vec2:5]"

[[test.results]]
value = "[vec2:4, vec2:5]"
[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    KnnScan [ctx: Db] [index: vec2_a, k: 2, ef: 40, dimension: 2]
''''

[[test.results]]
value = "[vec2:1, vec2:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
'Aggregate [ctx: Db] [mode: GROUP ALL]
    KnnScan [ctx: Db] [index: vec2_a, k: 2, ef: 40, dimension: 2]
''''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    KnnTopK [ctx: Db] [field: pa, k: 2, distance: Euclidean, dimension: 2]
        TableScan [ctx: Db] [table: vec2, direction: Forward]
''''

[[test.results]]
value = "[vec2:1, vec2:2]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    KnnScan [ctx: Db] [index: vec2_a, k: 2, ef: 40, dimension: 2]
''''

[[test.results]]
value = "[vec2:1, vec2:2]"

[[test.results]]
value = "[vec2:4, vec2:5]"

[[test.results]]
value = "[]"

*/

-- ═══════════════════════════════════════════════════════════════════════
-- FULLTEXT index (`Index::FullText`)
-- ═══════════════════════════════════════════════════════════════════════

{
    DEFINE ANALYZER words TOKENIZERS blank,class FILTERS lowercase;
    DEFINE TABLE doc SCHEMAFULL;
    DEFINE FIELD body ON doc TYPE string;
    DEFINE FIELD kw ON doc TYPE array<string>;
    DEFINE INDEX doc_body ON doc FIELDS body FULLTEXT ANALYZER words BM25 HIGHLIGHTS;
    DEFINE INDEX doc_kw ON doc FIELDS kw.* FULLTEXT ANALYZER words BM25;
    CREATE doc:1 SET body = 'the quick brown fox', kw = ['alpha', 'beta'];
    CREATE doc:2 SET body = 'a quick red hen', kw = ['alpha'];
    CREATE doc:3 SET body = 'slow green turtle', kw = ['gamma'];
    RETURN 'OK';
};

-- The index is the source: `FullTextScan` renders the index name and the
-- analyzed query, and carries no predicate — the postings *are* the match, so
-- there is nothing left to recheck.
EXPLAIN SELECT id FROM doc WHERE body @@ 'quick';
SELECT id FROM doc WHERE body @@ 'quick';

-- `WITH NOINDEX` takes `FullTextScan` out of the plan and leaves the `@@`
-- evaluation to resolve the same index per row, so the answer is unchanged.
-- This is why the oracle below is a string predicate rather than this hint.
EXPLAIN SELECT id FROM doc WITH NOINDEX WHERE body @@ 'quick';
SELECT id FROM doc WITH NOINDEX WHERE body @@ 'quick';
SELECT VALUE id FROM doc WITH NOINDEX WHERE string::contains(body, 'quick');

-- A count over a full-text match has no index-only path: the scan yields the
-- matching rows and the aggregation counts them.
EXPLAIN SELECT count() FROM doc WHERE body @@ 'quick' GROUP ALL;
SELECT count() FROM doc WHERE body @@ 'quick' GROUP ALL;
SELECT count() FROM doc WITH NOINDEX WHERE string::contains(body, 'quick') GROUP ALL;

-- A numbered match reference feeds `search::score`, which needs the scan's
-- per-row scoring state; the plan shows the score computed above the scan
-- rather than inside it. Both terms are asserted because BM25's IDF term is
-- non-positive for a term most of the corpus holds — `quick` is in two of the
-- three documents and scores zero, `fox` in one and scores above zero — so a
-- single common term would pin a score that carries no information.
EXPLAIN SELECT id, search::score(1) AS sc FROM doc WHERE body @1@ 'quick' ORDER BY sc DESC;
SELECT id, search::score(1) AS sc FROM doc WHERE body @1@ 'quick' ORDER BY sc DESC;
SELECT id, search::score(1) AS sc FROM doc WHERE body @1@ 'fox' ORDER BY sc DESC;

-- A FULLTEXT index over a `*` column answers a match on the `*` path, and only
-- on that path: the operator matches the index by the column path, so `kw`
-- names no index even though `kw.*` does, and the match errors instead of
-- falling back to the record's own value.
--
-- The `kw.*` spelling, which DOES name the index, is answered from a bare
-- `TableScan` with the match left as an unsupported per-row predicate, where
-- the plain-column `body @@ 'quick'` above drives a `FullTextScan`. The rows
-- and the count are correct, so this is a plan-level gap and not a wrong
-- answer, but it is recorded as a gap rather than as the intended shape: a
-- `*`-path FULLTEXT index never becomes the source of a scan, and
-- `WITH INDEX doc_kw` does not recover it either. If that is closed, this
-- assertion is the one that moves.
EXPLAIN SELECT id FROM doc WHERE kw.* @@ 'alpha';
SELECT id FROM doc WHERE kw.* @@ 'alpha';
SELECT count() FROM doc WHERE kw.* @@ 'alpha' GROUP ALL;
-- `kw` holds two distinct elements on doc:1, so a per-element fan-out that
-- emitted a row per matching element would show here as a duplicate.
SELECT VALUE id FROM doc WITH NOINDEX WHERE kw CONTAINS 'alpha';
SELECT id FROM doc WHERE kw @@ 'alpha';

-- DEFECT (1). The index is decommissioned and the plan says so — no
-- `FullTextScan`, just a table scan — yet `@@` still answers from the
-- unmaintained postings, so `doc:4` is missing from a plan that claims to have
-- examined every row. Correct answer: `[{ id: doc:1 }, { id: doc:2 },
-- { id: doc:4 }]`, as the string predicate reports.
ALTER INDEX doc_body ON doc PREPARE REMOVE;
CREATE doc:4 SET body = 'another quick one', kw = ['delta'];
EXPLAIN SELECT id FROM doc WHERE body @@ 'quick';
SELECT id FROM doc WHERE body @@ 'quick';
SELECT VALUE id FROM doc WITH NOINDEX WHERE string::contains(body, 'quick');
SELECT count() FROM doc WHERE body @@ 'quick' GROUP ALL;
SELECT count() FROM doc WITH NOINDEX WHERE string::contains(body, 'quick') GROUP ALL;

-- ═══════════════════════════════════════════════════════════════════════
-- Vector indexes: HNSW and DISKANN (`Index::Hnsw` / `Index::DiskAnn`)
--
-- Two-component vectors, so the indexed array holds two distinct elements.
-- ═══════════════════════════════════════════════════════════════════════

{
    DEFINE TABLE pt SCHEMAFULL;
    DEFINE FIELD point ON pt TYPE array<float>;
    DEFINE FIELD grp ON pt TYPE string;
    INSERT INTO pt [
        { id: pt:1, point: [10f, 1f], grp: 'a' },
        { id: pt:2, point: [20f, 2f], grp: 'b' },
        { id: pt:3, point: [30f, 3f], grp: 'a' },
        { id: pt:4, point: [40f, 4f], grp: 'b' }
    ];
    RETURN 'OK';
};
LET $q = [22f, 2f];

-- The single-argument KNN operator names no metric and no index; it is
-- rejected, which is what makes `<|k,METRIC|>` the only index-free form.
SELECT VALUE id FROM pt WHERE point <|2|> $q;

-- `<|k,METRIC|>` is the brute-force form: `KnnTopK` over a `TableScan`,
-- exact, and available whether or not a vector index exists. It is the oracle
-- every ANN assertion below is compared against.
EXPLAIN SELECT id FROM pt WHERE point <|2,EUCLIDEAN|> $q;
SELECT VALUE id FROM pt WHERE point <|2,EUCLIDEAN|> $q ORDER BY id;
EXPLAIN SELECT count() FROM pt WHERE point <|2,EUCLIDEAN|> $q GROUP ALL;
SELECT count() FROM pt WHERE point <|2,EUCLIDEAN|> $q GROUP ALL;

-- DEFECT (2), absent-index half. `<|k,EF|>` asks for an ANN traversal, which
-- only a vector index can perform. With none defined the operator survives as
-- an ordinary `Filter` over the scan and matches nothing — no error, and
-- nothing in the plan says the query has become unanswerable. Correct answer:
-- the two nearest points, as `<|2,EUCLIDEAN|>` reports above.
EXPLAIN SELECT id FROM pt WHERE point <|2,40|> $q;
SELECT VALUE id FROM pt WHERE point <|2,40|> $q;
SELECT count() FROM pt WHERE point <|2,40|> $q GROUP ALL;

DEFINE INDEX pt_hnsw ON pt FIELDS point HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;

-- With the index, the ANN form becomes `KnnScan`, which renders the index, k,
-- ef and the vector's dimension. The rows agree with the brute-force oracle.
EXPLAIN SELECT id FROM pt WHERE point <|2,40|> $q;
SELECT VALUE id FROM pt WHERE point <|2,40|> $q ORDER BY id;
SELECT VALUE id FROM pt WHERE point <|2,EUCLIDEAN|> $q ORDER BY id;
EXPLAIN SELECT count() FROM pt WHERE point <|2,40|> $q GROUP ALL;
SELECT count() FROM pt WHERE point <|2,40|> $q GROUP ALL;
SELECT count() FROM pt WHERE point <|2,EUCLIDEAN|> $q GROUP ALL;

-- A `WITH INDEX` hint naming the vector index is the plan's own choice, and
-- the rows are asserted alongside the plan: an allow-list that narrowed to an
-- index the ANN form cannot use would leave the same operator in the plan.
EXPLAIN SELECT id FROM pt WITH INDEX pt_hnsw WHERE point <|2,40|> $q;
SELECT VALUE id FROM pt WITH INDEX pt_hnsw WHERE point <|2,40|> $q ORDER BY id;
SELECT VALUE id FROM pt WHERE point <|2,EUCLIDEAN|> $q ORDER BY id;

-- `WITH NOINDEX` removes the only operator that can answer the ANN form, so
-- the hint does not produce a slower true answer — it produces no answer.
EXPLAIN SELECT id FROM pt WITH NOINDEX WHERE point <|2,40|> $q;
SELECT VALUE id FROM pt WITH NOINDEX WHERE point <|2,40|> $q;

-- The explicit-metric form is not routed to the index even when the index was
-- built with that same metric: it stays exact and scan-based.
EXPLAIN SELECT id FROM pt WHERE point <|2,MANHATTAN|> $q;
SELECT VALUE id FROM pt WHERE point <|2,MANHATTAN|> $q ORDER BY id;

-- A non-KNN conjunct is pushed into the traversal and rejects candidates as
-- they are visited, so it shows both as the `KnnScan`'s `predicate:` and as
-- the `Filter` above it. The k nearest *among matching rows* is not the k
-- nearest overall, which is why the oracle carries the same conjunct.
EXPLAIN SELECT id, vector::distance::knn() AS d FROM pt WHERE grp = 'a' AND point <|2,40|> $q ORDER BY d;
SELECT VALUE id FROM pt WHERE grp = 'a' AND point <|2,40|> $q ORDER BY id;
SELECT VALUE id FROM pt WHERE grp = 'a' AND point <|2,EUCLIDEAN|> $q ORDER BY id;

-- DEFECT (2), decommissioned half. Preparing the index for removal returns the
-- plan to `Filter` over `TableScan` and the ANN answer to `[]`, while the
-- brute-force form over the same rows still answers. Correct answer: the two
-- nearest points.
ALTER INDEX pt_hnsw ON pt PREPARE REMOVE;
CREATE pt:5 SET point = [21f, 2f], grp = 'a';
EXPLAIN SELECT id FROM pt WHERE point <|2,40|> $q;
SELECT VALUE id FROM pt WHERE point <|2,40|> $q;
SELECT VALUE id FROM pt WHERE point <|2,EUCLIDEAN|> $q ORDER BY id;
REMOVE INDEX pt_hnsw ON pt;

-- DISKANN reaches the same operator and renders the same attributes, so the
-- plan does not say which vector kind is serving the search. The rows, built
-- over all five rows including the one written while no index was maintained,
-- agree with the brute-force oracle.
DEFINE INDEX pt_da ON pt FIELDS point DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32;
EXPLAIN SELECT id FROM pt WHERE point <|2,40|> $q;
SELECT VALUE id FROM pt WHERE point <|2,40|> $q ORDER BY id;
SELECT VALUE id FROM pt WHERE point <|2,EUCLIDEAN|> $q ORDER BY id;
EXPLAIN SELECT count() FROM pt WHERE point <|2,40|> $q GROUP ALL;
SELECT count() FROM pt WHERE point <|2,40|> $q GROUP ALL;
SELECT count() FROM pt WHERE point <|2,EUCLIDEAN|> $q GROUP ALL;

-- ===========================================================================
-- TWO OPERATORS OF THE SAME SPECIALISED KIND, CONJOINED
--
-- Everything above puts at most one `@@` or one KNN operator in a WHERE
-- clause. Conjoining two operators that each need a specialised index is the
-- shape where a conjunct can be lost, so the two fixtures below exist to make
-- a lost conjunct visible in the rows: ft2:2 matches only 'alpha' and ft2:3
-- only 'beta', so a plan that honours one term and drops the other returns a
-- row the conjunction excludes.
-- ===========================================================================
{
    DEFINE ANALYZER w2 TOKENIZERS blank,class FILTERS lowercase;
    DEFINE TABLE ft2 SCHEMAFULL;
    DEFINE FIELD title ON ft2 TYPE string;
    DEFINE FIELD body ON ft2 TYPE string;
    DEFINE INDEX ft2_title ON ft2 FIELDS title FULLTEXT ANALYZER w2 BM25;
    DEFINE INDEX ft2_body ON ft2 FIELDS body FULLTEXT ANALYZER w2 BM25;
    CREATE ft2:1 SET title = 'alpha beta', body = 'kilo lima';
    CREATE ft2:2 SET title = 'alpha gamma', body = 'kilo mike';
    CREATE ft2:3 SET title = 'beta delta', body = 'nono lima';
    RETURN 'OK';
};

-- DEFECT. Two `@@` conjuncts over the SAME indexed column reduce to a single
-- FullTextScan for the first term. The second conjunct is absent from the plan
-- entirely -- no second scan, no BitmapAnd, and no Filter re-check above the
-- scan -- so every row matching 'alpha' comes back, ft2:2 included, which does
-- not match 'beta'. Correct answer: [ft2:1], which the oracle returns. The
-- oracle is `string::contains` rather than WITH NOINDEX, for the reason given
-- at the top of this file.
EXPLAIN SELECT VALUE id FROM ft2 WHERE title @1@ 'alpha' AND title @2@ 'beta';

SELECT VALUE id FROM ft2 WHERE title @1@ 'alpha' AND title @2@ 'beta';

SELECT VALUE id FROM ft2 WHERE string::contains(title, 'alpha') AND string::contains(title, 'beta');

-- The count path plans the SAME predicate correctly -- an IndexCountScan over
-- a BitmapAnd of one BitmapFullTextScan per term -- and reports the oracle's 1.
-- One predicate therefore yields a right count and wrong rows from one engine,
-- which localises the defect to the row path rather than to the analyzer, and
-- shows that the two-scan intersection the row path needs already exists.
EXPLAIN SELECT count() FROM ft2 WHERE title @1@ 'alpha' AND title @2@ 'beta' GROUP ALL;

SELECT count() FROM ft2 WHERE title @1@ 'alpha' AND title @2@ 'beta' GROUP ALL;

SELECT count() FROM ft2 WHERE string::contains(title, 'alpha') AND string::contains(title, 'beta') GROUP ALL;

-- DEFECT, the same shape with the conjuncts swapped: the surviving term is the
-- textually first one, so which row is wrongly returned changes with the
-- spelling. Correct answer is [ft2:1] either way; here ft2:3 comes back, which
-- matches 'beta' but not 'alpha'.
EXPLAIN SELECT VALUE id FROM ft2 WHERE title @1@ 'beta' AND title @2@ 'alpha';

SELECT VALUE id FROM ft2 WHERE title @1@ 'beta' AND title @2@ 'alpha';

-- The first contrast that makes the AND case a defect rather than a limit of
-- the operator: the same two terms on the same column, ORed, keep both terms.
-- The plan unions two BitmapFullTextScans and re-checks the predicate, and the
-- rows match the oracle -- so scanning one column's index twice in one query
-- is buildable.
EXPLAIN SELECT VALUE id FROM ft2 WHERE title @1@ 'alpha' OR title @2@ 'beta';

SELECT VALUE id FROM ft2 WHERE title @1@ 'alpha' OR title @2@ 'beta' ORDER BY id;

SELECT VALUE id FROM ft2 WHERE string::contains(title, 'alpha') OR string::contains(title, 'beta') ORDER BY id;

-- The second contrast: two `@@` conjuncts over DIFFERENT columns, each with
-- its own index, are planned as a BitmapAnd of the two scans under a Filter
-- re-check and answer correctly. Only reusing ONE column's index twice in a
-- conjunction loses a term.
EXPLAIN SELECT VALUE id FROM ft2 WHERE title @1@ 'alpha' AND body @2@ 'lima';

SELECT VALUE id FROM ft2 WHERE title @1@ 'alpha' AND body @2@ 'lima';

SELECT VALUE id FROM ft2 WHERE string::contains(title, 'alpha') AND string::contains(body, 'lima');

-- The vector half of the same question. vec2 orders records under `pa` and
-- reverses that order under `pb`, so the two 2-nearest sets are disjoint and
-- the correct answer to any conjunction of one constraint on each is the empty
-- set. WITH NOINDEX is not an oracle for a KNN operator (see the head of this
-- file), so the oracle is an intersection of two brute-force subqueries.
{
    DEFINE TABLE vec2 SCHEMAFULL;
    DEFINE FIELD pa ON vec2 TYPE array<float>;
    DEFINE FIELD pb ON vec2 TYPE array<float>;
    DEFINE INDEX vec2_a ON vec2 FIELDS pa HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
    DEFINE INDEX vec2_b ON vec2 FIELDS pb HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
    INSERT INTO vec2 [
        { id: vec2:1, pa: [10f, 1f], pb: [50f, 5f] },
        { id: vec2:2, pa: [20f, 2f], pb: [40f, 4f] },
        { id: vec2:3, pa: [30f, 3f], pb: [30f, 3f] },
        { id: vec2:4, pa: [40f, 4f], pb: [20f, 2f] },
        { id: vec2:5, pa: [50f, 5f], pb: [10f, 1f] }
    ];
    RETURN 'OK';
};

LET $near = [12f, 1f];

-- Control: one KNN operator on its own is served by its index and agrees with
-- the brute-force form over the same data.
EXPLAIN SELECT VALUE id FROM vec2 WHERE pa <|2,40|> $near;

SELECT VALUE id FROM vec2 WHERE pa <|2,40|> $near ORDER BY id;

SELECT VALUE id FROM vec2 WHERE pa <|2,EUCLIDEAN|> $near ORDER BY id;

-- The same control for `pb`, which makes the conjunction below a statement
-- about losing a conjunct rather than about an unindexed column: both columns
-- have their own usable HNSW index, and `pb` orders the records opposite to
-- `pa`, so the two 2-nearest sets share no record.
EXPLAIN SELECT VALUE id FROM vec2 WHERE pb <|2,40|> $near;
SELECT VALUE id FROM vec2 WHERE pb <|2,40|> $near ORDER BY id;
SELECT VALUE id FROM vec2 WHERE pb <|2,EUCLIDEAN|> $near ORDER BY id;

-- DEFECT. Two ANDed KNN operators collapse to a single KnnScan. The second
-- constraint is absent with no residual Filter, so the query answers the
-- surviving operator's top-K and returns two records that satisfy only one of
-- the two constraints. Correct answer: [], which the brute-force intersection
-- below returns.
EXPLAIN SELECT VALUE id FROM vec2 WHERE pa <|2,40|> $near AND pb <|2,40|> $near;

SELECT VALUE id FROM vec2 WHERE pa <|2,40|> $near AND pb <|2,40|> $near;

SELECT VALUE id FROM vec2 WHERE id IN (SELECT VALUE id FROM vec2 WHERE pa <|2,EUCLIDEAN|> $near) AND id IN (SELECT VALUE id FROM vec2 WHERE pb <|2,EUCLIDEAN|> $near) ORDER BY id;

-- The count path does NOT rescue this one: unlike the `@@` case above, it
-- aggregates over the same single KnnScan and reports 2 where the oracle
-- reports 0. Correct answer: 0.
EXPLAIN SELECT count() FROM vec2 WHERE pa <|2,40|> $near AND pb <|2,40|> $near GROUP ALL;

SELECT count() FROM vec2 WHERE pa <|2,40|> $near AND pb <|2,40|> $near GROUP ALL;

SELECT count() FROM vec2 WHERE id IN (SELECT VALUE id FROM vec2 WHERE pa <|2,EUCLIDEAN|> $near) AND id IN (SELECT VALUE id FROM vec2 WHERE pb <|2,EUCLIDEAN|> $near) GROUP ALL;

-- DEFECT. The brute-force spelling collapses identically: a single KnnTopK on
-- `pa` over a table scan, with the `pb` constraint gone. Since this form uses
-- no vector index at all, the lost conjunct is in predicate planning and not in
-- index selection. Correct answer: [].
EXPLAIN SELECT VALUE id FROM vec2 WHERE pa <|2,EUCLIDEAN|> $near AND pb <|2,EUCLIDEAN|> $near;

SELECT VALUE id FROM vec2 WHERE pa <|2,EUCLIDEAN|> $near AND pb <|2,EUCLIDEAN|> $near;

-- DEFECT. Two constraints on ONE column against ONE index collapse the same
-- way, so the shape does not need two indexes to lose a term. The two vectors
-- are at opposite ends of `pa`, so their 2-nearest sets are disjoint and the
-- correct answer is again [].
EXPLAIN SELECT VALUE id FROM vec2 WHERE pa <|2,40|> $near AND pa <|2,40|> [48f, 5f];

SELECT VALUE id FROM vec2 WHERE pa <|2,40|> $near AND pa <|2,40|> [48f, 5f];

SELECT VALUE id FROM vec2 WHERE pa <|2,EUCLIDEAN|> [48f, 5f] ORDER BY id;

SELECT VALUE id FROM vec2 WHERE id IN (SELECT VALUE id FROM vec2 WHERE pa <|2,EUCLIDEAN|> $near) AND id IN (SELECT VALUE id FROM vec2 WHERE pa <|2,EUCLIDEAN|> [48f, 5f]) ORDER BY id;
