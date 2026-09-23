/**
[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
Plan-feature matrix: one fixed index set, varying only the query clause, so a
change in how a clause is planned surfaces as a plan diff rather than as
identical rows. Covers limit and offset pushdown into a scan versus a separate
Limit operator, scan direction, sort elimination, aggregates and counts,
ANALYZE row counts and its Total rows footer, the WITH INDEX allow-list, and
index-selection ties.

Every indexed predicate is paired with the same predicate under WITH NOINDEX,
which forces a table scan and is therefore the oracle. A plan that reads
correctly while returning different rows from the oracle is the failure this
file exists to catch, so neither half stands alone.

Three recorded expectations are wrong. They are pinned rather than bent around:

1. An index-selection tie resolves to the index whose NAME sorts first, not to
   the earliest-defined one, which is the rule the selector states. `tiez`
   defines `zz_defined_first` before `aa_defined_second`, and the plan names
   `aa_defined_second`. Ties break on the position an index holds in its
   table's catalog, and that catalog is keyed by name — `INFO FOR TABLE tiez`
   reports `aa_defined_second` first — so definition order is honoured only
   where it agrees with name order. The correct plan names `zz_defined_first`.
   The tie on `tie` is the control: there the two orders agree, so on its own
   it cannot tell the two rules apart; the pair can, because "last defined
   wins" would have to name `tie_second` there and does not.

2. An `IndexScan` whose access is an exact equality or range on the leading
   column still carries `predicate:`, which the scan evaluates on every
   fetched row — the attribute exists to enforce conditions beyond what the
   indexer proved, and here there are none. The same access drops it as soon
   as a residual conjunct puts a `Filter` above the scan, so the attribute
   does not in fact report "the index narrowed but did not prove the
   predicate". Correctly neither shape would carry it. Rows are unaffected;
   what is lost is a per-row evaluation and the ability to read the attribute
   as a signal.

3. `WITH INDEX` naming an index that does not exist plans a table scan and
   reports nothing, so a typo in a hint cannot be distinguished from a hint
   naming a real index the planner could not use. Hinted names are filtered
   out of the candidate set before selection, so an unmatched hint leaves no
   candidates at all and the plan is a table scan before the selector's own
   unmatched-hint diagnostic can fire. Correctly the failure of the hint
   reaches the user — by rejecting a name no index carries, or by surfacing
   the fallback in the plan.

Narrowed to the streaming strategies because a prefix EXPLAIN errors under
compute-only; the two plan identically by contract.
"""

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: ev_cat_pri, access: ['alpha'], direction: Forward, limit: 2, predicate: cat = 'alpha']
"'''

[[test.results]]
value = "[ev:1, ev:2, ev:3]"

[[test.results]]
value = "[ev:1, ev:2, ev:3]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [limit: 2]
        Filter [ctx: Db] [predicate: note = 'keep']
            IndexScan [ctx: Db] [index: ev_cat_pri, access: ['alpha'], direction: Forward]
"'''

[[test.results]]
value = "[ev:1, ev:3]"

[[test.results]]
value = "[ev:1, ev:3]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: ev_score, access: >30, direction: Forward, limit: 2, predicate: score > 30]
''''

[[test.results]]
value = "[40, 50, 60, 70, 80]"

[[test.results]]
value = "[40, 50, 60, 70, 80]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: ev_cat_pri, access: ['alpha'], direction: Forward, limit: 0, predicate: cat = 'alpha']
"'''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: ev_cat_pri, access: ['alpha'], direction: Forward, limit: 2, offset: 1, predicate: cat = 'alpha']
"'''

[[test.results]]
value = "2"

[[test.results]]
value = "2"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: ev_score, access: , direction: Forward, limit: 3, offset: 2]
''''

[[test.results]]
value = "[30, 40, 50]"

[[test.results]]
value = "[30, 40, 50]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: ev_score, access: , direction: Forward, limit: 3]
''''

[[test.results]]
value = "[10, 20, 30]"

[[test.results]]
value = "[10, 20, 30]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: ev_score, access: , direction: Backward, limit: 3]
''''

[[test.results]]
value = "[80, 70, 60]"

[[test.results]]
value = "[80, 70, 60]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: ev_score, access: , direction: Backward]
''''

[[test.results]]
value = "[80, 70, 60, 50, 40, 30, 20, 10]"

[[test.results]]
value = "[80, 70, 60, 50, 40, 30, 20, 10]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: ev_cat_pri, access: , direction: Forward]
''''

[[test.results]]
value = "[['alpha', 1], ['alpha', 2], ['alpha', 3], ['beta', 1], ['beta', 2], ['gamma', 1], ['gamma', 2], ['gamma', 3]]"

[[test.results]]
value = "[['alpha', 1], ['alpha', 2], ['alpha', 3], ['beta', 1], ['beta', 2], ['gamma', 1], ['gamma', 2], ['gamma', 3]]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    SortByKey [ctx: Db] [sort_keys: cat ASC, pri DESC]
        TableScan [ctx: Db] [table: ev, direction: Forward]
''''

[[test.results]]
value = "[['alpha', 3], ['alpha', 2], ['alpha', 1], ['beta', 2], ['beta', 1], ['gamma', 3], ['gamma', 2], ['gamma', 1]]"

[[test.results]]
value = "[['alpha', 3], ['alpha', 2], ['alpha', 1], ['beta', 2], ['beta', 1], ['gamma', 3], ['gamma', 2], ['gamma', 1]]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: ev_cat_pri, access: ['alpha'], direction: Forward, predicate: cat = 'alpha']
"'''

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: ev_cat_pri, access: ['alpha'], direction: Backward, predicate: cat = 'alpha']
"'''

[[test.results]]
value = "[3, 2, 1]"

[[test.results]]
value = "[3, 2, 1]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    SortByKey [ctx: Db] [sort_keys: pri ASC]
        TableScan [ctx: Db] [table: ev, direction: Forward]
''''

[[test.results]]
value = "[1, 1, 1, 2, 2, 2, 3, 3]"

[[test.results]]
value = "[1, 1, 1, 2, 2, 2, 3, 3]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    SortByKey [ctx: Db] [sort_keys: score ASC, cat ASC]
        TableScan [ctx: Db] [table: ev, direction: Forward]
''''

[[test.results]]
value = "[10, 20, 30, 40, 50, 60, 70, 80]"

[[test.results]]
value = "[10, 20, 30, 40, 50, 60, 70, 80]"

[[test.results]]
value = '''
'Aggregate [ctx: Db] [by: cat]
    TableScan [ctx: Db] [table: ev, direction: Forward]
''''

[[test.results]]
value = "[{ cat: 'alpha', count: 3 }, { cat: 'beta', count: 2 }, { cat: 'gamma', count: 3 }]"

[[test.results]]
value = "[{ cat: 'alpha', count: 3 }, { cat: 'beta', count: 2 }, { cat: 'gamma', count: 3 }]"

[[test.results]]
value = '''
'Aggregate [ctx: Db] [mode: GROUP ALL]
    TableScan [ctx: Db] [table: ev, direction: Forward]
''''

[[test.results]]
value = "[{ total: 360 }]"

[[test.results]]
value = "[{ total: 360 }]"

[[test.results]]
value = '''
'CountScan [ctx: Db] [source: ev]
''''

[[test.results]]
value = "[{ count: 8 }]"

[[test.results]]
value = "[{ count: 8 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: ev, condition: cat = 'alpha']
"'''

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: ev_tags, access: = 'y', direction: Forward, predicate: tags CONTAINS 'y']
"'''

[[test.results]]
value = "[ev:1, ev:3, ev:7, ev:8]"

[[test.results]]
value = "[ev:1, ev:3, ev:7, ev:8]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: ev, condition: tags CONTAINS 'y']
"'''

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: ev_tags, access: = 'y', direction: Forward, limit: 2, predicate: tags CONTAINS 'y']
"'''

[[test.results]]
value = "2"

[[test.results]]
value = "2"

[[test.results]]
value = "2"

[[test.results]]
value = "1"

[[test.results]]
value = "1"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *] {rows: 3}
    IndexScan [ctx: Db] [index: ev_cat_pri, access: ['alpha'], direction: Forward, predicate: cat = 'alpha'] {rows: 3}

Total rows: 3"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *] {rows: 3}
    TableScan [ctx: Db] [table: ev, direction: Forward, predicate: cat = 'alpha', pre_decode_filter: yes] {rows: 3}

Total rows: 3"'''

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: ev, condition: cat = 'alpha'] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *] {rows: 4}
    IndexScan [ctx: Db] [index: ev_tags, access: = 'y', direction: Forward, predicate: tags CONTAINS 'y'] {rows: 4}

Total rows: 4"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *] {rows: 4}
    TableScan [ctx: Db] [table: ev, direction: Forward, predicate: tags CONTAINS 'y', pre_decode_filter: yes] {rows: 4}

Total rows: 4"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: ev_cat_pri, access: ['alpha'], direction: Forward, predicate: cat = 'alpha']
"'''

[[test.results]]
value = "[ev:1, ev:2, ev:3]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: ev, direction: Forward, predicate: cat = 'alpha', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[ev:1, ev:2, ev:3]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: ev, direction: Forward, predicate: cat = 'alpha', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[ev:1, ev:2, ev:3]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: ev, direction: Forward, predicate: cat = 'alpha', pre_decode_filter: yes]
"'''

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: tie_first, access: = 'a', direction: Forward, predicate: k = 'a']
"'''

[[test.results]]
value = "[tie:1, tie:3]"

[[test.results]]
value = "[tie:1, tie:3]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: aa_defined_second, access: = 'a', direction: Forward, predicate: k = 'a']
"'''

[[test.results]]
value = "[tiez:1]"

[[test.results]]
value = "[tiez:1]"
*/

-- Fixed index set. Catalog order matters for the tie cases at the end of this
-- file, so the DEFINE order here is load-bearing.
{
    DEFINE TABLE ev SCHEMALESS;
    DEFINE INDEX ev_cat_pri ON ev FIELDS cat, pri;
    DEFINE INDEX ev_score ON ev FIELDS score;
    DEFINE INDEX ev_tags ON ev FIELDS tags[*];
    DEFINE INDEX ev_region ON ev FIELDS region;

    -- `score` is distinct per row, so every ORDER BY score assertion below has
    -- one correct answer rather than a tie the backend may break either way.
    -- `tags` carries rows with two or more distinct elements, and ev:3 repeats
    -- an element, so a fanning scan that emits one row per key rather than one
    -- row per record is visible instead of hidden behind single-element arrays.
    CREATE ev:1 SET cat = 'alpha', pri = 1, score = 30, region = 'eu', tags = ['x', 'y'], note = 'keep';
    CREATE ev:2 SET cat = 'alpha', pri = 2, score = 10, region = 'eu', tags = ['x'], note = 'drop';
    CREATE ev:3 SET cat = 'alpha', pri = 3, score = 50, region = 'us', tags = ['y', 'z', 'y'], note = 'keep';
    CREATE ev:4 SET cat = 'beta', pri = 1, score = 20, region = 'us', tags = ['x', 'z'], note = 'keep';
    CREATE ev:5 SET cat = 'beta', pri = 2, score = 60, region = 'ap', tags = [], note = 'drop';
    CREATE ev:6 SET cat = 'gamma', pri = 1, score = 40, region = 'eu', tags = ['w'], note = 'keep';
    CREATE ev:7 SET cat = 'gamma', pri = 2, score = 70, region = 'ap', tags = ['x', 'y', 'z'], note = 'keep';
    CREATE ev:8 SET cat = 'gamma', pri = 3, score = 80, region = 'us', tags = ['y'], note = 'keep';
    RETURN 'OK';
};

-- === LIMIT pushed into the scan, versus a separate Limit operator ===

-- The predicate is the index prefix, so the scan can stop at the limit itself
-- and the limit rides along as a scan attribute.
EXPLAIN SELECT * FROM ev WHERE cat = 'alpha' LIMIT 2;

-- Oracle for the `cat = 'alpha'` predicate. Asserted on the whole matching set
-- rather than on the LIMIT 2 query: index order and table order differ, so the
-- first two rows of each are legitimately different rows and comparing them
-- would assert a scan order nothing promises.
array::sort((SELECT VALUE id FROM ev WHERE cat = 'alpha'));
array::sort((SELECT VALUE id FROM ev WITH NOINDEX WHERE cat = 'alpha'));

-- A residual on an unindexed column: rows the scan yields may still be
-- rejected, so the scan cannot count towards the limit and the limit stays in
-- its own operator above the Filter.
--
-- This plan also pins the inconsistency in `predicate:`. The access here is
-- the same exact equality prefix as the case above, and here the scan does
-- NOT re-check it; above, where the equality is the whole WHERE, it does. An
-- exact prefix proves the equality either way, so the correct plan carries no
-- `predicate:` in either shape.
EXPLAIN SELECT * FROM ev WHERE cat = 'alpha' AND note = 'keep' LIMIT 2;

array::sort((SELECT VALUE id FROM ev WHERE cat = 'alpha' AND note = 'keep'));
array::sort((SELECT VALUE id FROM ev WITH NOINDEX WHERE cat = 'alpha' AND note = 'keep'));

-- A range access with a limit: the scan is narrowed at the storage level and
-- still carries the limit.
EXPLAIN SELECT * FROM ev WHERE score > 30 LIMIT 2;

array::sort((SELECT VALUE score FROM ev WHERE score > 30));
array::sort((SELECT VALUE score FROM ev WITH NOINDEX WHERE score > 30));

-- LIMIT 0 asks for no rows at all; the scan must not be dropped in a way that
-- changes the answer from the empty set.
EXPLAIN SELECT * FROM ev WHERE cat = 'alpha' LIMIT 0;
SELECT VALUE id FROM ev WHERE cat = 'alpha' LIMIT 0;
SELECT VALUE id FROM ev WITH NOINDEX WHERE cat = 'alpha' LIMIT 0;

-- === START / offset pushdown ===

-- An offset on a narrowed scan: the skipped rows still have to be produced, so
-- the scan's own budget has to cover offset + limit.
EXPLAIN SELECT * FROM ev WHERE cat = 'alpha' LIMIT 2 START 1;

-- Which two of the three matches come back depends on scan order, which
-- nothing promises, so the oracle is how many: an offset counted twice, or
-- applied to the scan and again above it, leaves one row instead of two.
array::len((SELECT VALUE id FROM ev WHERE cat = 'alpha' LIMIT 2 START 1));
array::len((SELECT VALUE id FROM ev WITH NOINDEX WHERE cat = 'alpha' LIMIT 2 START 1));

-- With ORDER BY covered by the index the offset is deterministic, so the rows
-- can be asserted directly against the oracle.
EXPLAIN SELECT * FROM ev ORDER BY score ASC LIMIT 3 START 2;
SELECT VALUE score FROM ev ORDER BY score ASC LIMIT 3 START 2;
SELECT VALUE score FROM ev WITH NOINDEX ORDER BY score ASC LIMIT 3 START 2;

-- === Scan direction taken from ORDER BY ===

EXPLAIN SELECT * FROM ev ORDER BY score ASC LIMIT 3;
SELECT VALUE score FROM ev ORDER BY score ASC LIMIT 3;
SELECT VALUE score FROM ev WITH NOINDEX ORDER BY score ASC LIMIT 3;

EXPLAIN SELECT * FROM ev ORDER BY score DESC LIMIT 3;
SELECT VALUE score FROM ev ORDER BY score DESC LIMIT 3;
SELECT VALUE score FROM ev WITH NOINDEX ORDER BY score DESC LIMIT 3;

-- Direction is decided by ORDER BY alone, with no limit to push.
EXPLAIN SELECT * FROM ev ORDER BY score DESC;
SELECT VALUE score FROM ev ORDER BY score DESC;
SELECT VALUE score FROM ev WITH NOINDEX ORDER BY score DESC;

-- === ORDER BY coverage: sort eliminated versus sort retained ===

-- Both keys, in index order and index direction: the scan already emits them
-- sorted, so no sort operator survives.
EXPLAIN SELECT * FROM ev ORDER BY cat ASC, pri ASC;
SELECT VALUE [cat, pri] FROM ev ORDER BY cat ASC, pri ASC;
SELECT VALUE [cat, pri] FROM ev WITH NOINDEX ORDER BY cat ASC, pri ASC;

-- Mixed directions cannot be walked by one directional scan of one index.
EXPLAIN SELECT * FROM ev ORDER BY cat ASC, pri DESC;
SELECT VALUE [cat, pri] FROM ev ORDER BY cat ASC, pri DESC;
SELECT VALUE [cat, pri] FROM ev WITH NOINDEX ORDER BY cat ASC, pri DESC;

-- Leading column pinned by the predicate, so the trailing column is ordered
-- within the pinned prefix and the sort can go.
EXPLAIN SELECT * FROM ev WHERE cat = 'alpha' ORDER BY pri ASC;
SELECT VALUE pri FROM ev WHERE cat = 'alpha' ORDER BY pri ASC;
SELECT VALUE pri FROM ev WITH NOINDEX WHERE cat = 'alpha' ORDER BY pri ASC;

-- Same, walked the other way.
EXPLAIN SELECT * FROM ev WHERE cat = 'alpha' ORDER BY pri DESC;
SELECT VALUE pri FROM ev WHERE cat = 'alpha' ORDER BY pri DESC;
SELECT VALUE pri FROM ev WITH NOINDEX WHERE cat = 'alpha' ORDER BY pri DESC;

-- ORDER BY a column past the prefix with nothing pinning the prefix: the index
-- does not order by it, so the sort has to stay.
EXPLAIN SELECT * FROM ev ORDER BY pri ASC;
SELECT VALUE pri FROM ev ORDER BY pri ASC;
SELECT VALUE pri FROM ev WITH NOINDEX ORDER BY pri ASC;

-- Only the first sort key is covered; the second is not in that index at all.
EXPLAIN SELECT * FROM ev ORDER BY score ASC, cat ASC;
SELECT VALUE score FROM ev ORDER BY score ASC, cat ASC;
SELECT VALUE score FROM ev WITH NOINDEX ORDER BY score ASC, cat ASC;

-- === GROUP BY and aggregates ===

EXPLAIN SELECT cat, count() FROM ev GROUP BY cat;
SELECT cat, count() FROM ev GROUP BY cat ORDER BY cat;
SELECT cat, count() FROM ev WITH NOINDEX GROUP BY cat ORDER BY cat;

EXPLAIN SELECT math::sum(score) AS total FROM ev GROUP ALL;
SELECT math::sum(score) AS total FROM ev GROUP ALL;
SELECT math::sum(score) AS total FROM ev WITH NOINDEX GROUP ALL;

-- An unfiltered count has its own source-counting path, with no index and no
-- record fetch.
EXPLAIN SELECT count() FROM ev GROUP ALL;
SELECT count() FROM ev GROUP ALL;
SELECT count() FROM ev WITH NOINDEX GROUP ALL;

-- A counted predicate an index proves exactly: counted from index keys.
EXPLAIN SELECT count() FROM ev WHERE cat = 'alpha' GROUP ALL;
SELECT count() FROM ev WHERE cat = 'alpha' GROUP ALL;
SELECT count() FROM ev WITH NOINDEX WHERE cat = 'alpha' GROUP ALL;

-- === A fanning indexed column ===

-- `tags[*]` holds one key per element, so ev:3 has two keys carrying 'y'. The
-- row set and the count reach the answer by different paths, either of which
-- can disagree with the table scan, so both are asserted.
EXPLAIN SELECT * FROM ev WHERE tags CONTAINS 'y';
array::sort((SELECT VALUE id FROM ev WHERE tags CONTAINS 'y'));
array::sort((SELECT VALUE id FROM ev WITH NOINDEX WHERE tags CONTAINS 'y'));

EXPLAIN SELECT count() FROM ev WHERE tags CONTAINS 'y' GROUP ALL;
SELECT count() FROM ev WHERE tags CONTAINS 'y' GROUP ALL;
SELECT count() FROM ev WITH NOINDEX WHERE tags CONTAINS 'y' GROUP ALL;

-- A limit over a fanning scan. The row identities depend on key order, which
-- nothing promises, so the assertions are on how many rows come back and on
-- whether they are distinct records — which is what a limit applied to keys
-- rather than to records would get wrong.
EXPLAIN SELECT * FROM ev WHERE tags CONTAINS 'y' LIMIT 2;
array::len((SELECT VALUE id FROM ev WHERE tags CONTAINS 'y' LIMIT 2));
array::len(array::distinct((SELECT VALUE id FROM ev WHERE tags CONTAINS 'y' LIMIT 2)));
array::len((SELECT VALUE id FROM ev WITH NOINDEX WHERE tags CONTAINS 'y' LIMIT 2));

-- An offset over a fanning scan: four records match, so skipping three leaves
-- one. An offset counted in keys rather than in records would skip too far.
array::len((SELECT VALUE id FROM ev WHERE tags CONTAINS 'y' LIMIT 10 START 3));
array::len((SELECT VALUE id FROM ev WITH NOINDEX WHERE tags CONTAINS 'y' LIMIT 10 START 3));

-- === EXPLAIN ANALYZE row counts and the Total rows footer ===

-- No outer limit, so nothing cancels a scan mid-stream and the per-operator
-- counts are a property of the data rather than of the backend's timing.
EXPLAIN ANALYZE SELECT * FROM ev WHERE cat = 'alpha';
EXPLAIN ANALYZE SELECT * FROM ev WITH NOINDEX WHERE cat = 'alpha';
EXPLAIN ANALYZE SELECT count() FROM ev WHERE cat = 'alpha' GROUP ALL;

-- The scan's own row count is where a fanning index emitting one row per key
-- rather than one per record would show up, ahead of any dedup above it.
EXPLAIN ANALYZE SELECT * FROM ev WHERE tags CONTAINS 'y';
EXPLAIN ANALYZE SELECT * FROM ev WITH NOINDEX WHERE tags CONTAINS 'y';

-- === WITH INDEX allow-list, and WITH NOINDEX ===

-- Names the index the planner would have chosen anyway.
EXPLAIN SELECT * FROM ev WITH INDEX ev_cat_pri WHERE cat = 'alpha';
array::sort((SELECT VALUE id FROM ev WITH INDEX ev_cat_pri WHERE cat = 'alpha'));

-- Names an index that exists but whose leading column is not in the WHERE, so
-- the allow-list leaves no usable candidate. WITH INDEX is an allow-list and
-- not a force, so the answer must still be the oracle's.
EXPLAIN SELECT * FROM ev WITH INDEX ev_score WHERE cat = 'alpha';
array::sort((SELECT VALUE id FROM ev WITH INDEX ev_score WHERE cat = 'alpha'));

-- Names an index that does not exist at all. The recorded plan is wrong: an
-- unresolvable hint is silently discarded and the query falls back to a table
-- scan, byte-identical to the plan for the usable-but-unselected hint above,
-- so a mistyped index name looks exactly like a correct hint the planner
-- could not use. Correctly the hint's failure reaches the user. The rows stay
-- the oracle's either way, which is why nothing else notices.
EXPLAIN SELECT * FROM ev WITH INDEX ev_nonexistent WHERE cat = 'alpha';
array::sort((SELECT VALUE id FROM ev WITH INDEX ev_nonexistent WHERE cat = 'alpha'));

EXPLAIN SELECT * FROM ev WITH NOINDEX WHERE cat = 'alpha';

-- === Two indexes tied on score ===

-- Equal-scoring candidates have to resolve to the same plan on every run, so
-- the tie breaks on the table's index catalog rather than on the order the
-- analysis happened to push candidates in.
{
    DEFINE TABLE tie SCHEMALESS;
    DEFINE INDEX tie_first ON tie FIELDS k;
    DEFINE INDEX tie_second ON tie FIELDS k;
    CREATE tie:1 SET k = 'a';
    CREATE tie:2 SET k = 'b';
    CREATE tie:3 SET k = 'a';
    RETURN 'OK';
};

EXPLAIN SELECT * FROM tie WHERE k = 'a';
array::sort((SELECT VALUE id FROM tie WHERE k = 'a'));
array::sort((SELECT VALUE id FROM tie WITH NOINDEX WHERE k = 'a'));

-- The same tie with definition order and name order disagreeing, which is
-- what separates "earliest defined" from "first by name". The recorded plan is
-- wrong: it names the index defined SECOND, because the catalog ties are
-- broken on is keyed by name. Correctly it names `zz_defined_first`. The
-- assertion is kept in the wrong form so that a fix has to come here and say
-- so; the `tie` case above cannot show this, since there both rules agree.
{
    DEFINE TABLE tiez SCHEMALESS;
    DEFINE INDEX zz_defined_first ON tiez FIELDS k;
    DEFINE INDEX aa_defined_second ON tiez FIELDS k;
    CREATE tiez:1 SET k = 'a';
    CREATE tiez:2 SET k = 'b';
    RETURN 'OK';
};

EXPLAIN SELECT * FROM tiez WHERE k = 'a';
array::sort((SELECT VALUE id FROM tiez WHERE k = 'a'));
array::sort((SELECT VALUE id FROM tiez WITH NOINDEX WHERE k = 'a'));
