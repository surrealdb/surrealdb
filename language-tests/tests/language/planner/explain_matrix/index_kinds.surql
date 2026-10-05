/**
[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
EXPLAIN / row-parity matrix over DEFINE INDEX *kind*: standard, UNIQUE,
unconditional COUNT, guarded COUNT (`COUNT WHERE <cond>`), and any of those
decommissioned with `ALTER INDEX ... PREPARE REMOVE`. Every plan assertion is
paired with the same query under `WITH NOINDEX` (or, where `WITH NOINDEX` is
not an oracle, with a formulation that cannot reach the index), because a plan
naming an operator says nothing about whether that operator answers correctly.

The file is narrowed to the two streaming strategies because a prefix `EXPLAIN`
errors under `compute-only`; the two streaming strategies plan identically by
contract, so pinning both records that and costs nothing.

Column SPELLING is not an index kind: whole-array equality against a plain
(non-`*`) column whose value is an array returns the wrong answer, but that is
a property of the fan-out key model and is pinned once, in
array_fanout_oracle.surql. What this file keeps of that shape is the
kind-to-kind comparison over identical fanning data -- standard, UNIQUE and
non-unique -- and the `CONTAINS` half of the plain-versus-`*` mirror.

Three wrong behaviours are pinned here rather than hidden, each marked at the
case that produces it:

1. A COUNT index left behind by `ALTER INDEX ... PREPARE REMOVE` is still read
   by both `CountScan` and `IndexCountScan`, while writes no longer maintain
   it, so the count silently goes stale — 3 where the table holds 4 rows,
   2 where the guard admits 3. The correct answer is the true count; the
   correct behaviour is for the count read paths to skip a `prepare_remove`
   index the way the B-tree access-path analysis already does.
2. `WITH NOINDEX` does not suppress the unconditional count fast path, because
   the matcher behind `CountScan` never consults the source's index hint. The
   hint returns the same stale number the unhinted query does, where it should
   force the key-range count. The guarded path does honour the hint, which is
   what makes the unconditional one's silence a defect rather than a policy.
3. A UNIQUE index over a fanning-out column rejects a row whose own array
   repeats an element, reporting the record being written as the conflicting
   holder. One row holding `['s','s']` has one distinct element and violates
   nothing; the write should succeed. The non-unique index over the same shape
   does collapse a row's repeated entries — pinned in the `dupe` section — so
   this is an inconsistency between the two kinds, not a uniform policy.
"""

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: std_zone, access: = 'n', direction: Forward, predicate: zone = 'n']
"'''

[[test.results]]
value = "[{ id: std:1 }, { id: std:2 }]"

[[test.results]]
value = "[{ id: std:1 }, { id: std:2 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: std, direction: Forward, predicate: zone != 'n', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ id: std:3 }]"

[[test.results]]
value = "[{ id: std:3 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: std_zone, access: >'n', direction: Forward, predicate: zone > 'n']
"'''

[[test.results]]
value = "[{ id: std:3 }]"

[[test.results]]
value = "[{ id: std:3 }]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: uq_email, access: = 'a@x', direction: Forward, predicate: email = 'a@x']
"'''

[[test.results]]
value = "[{ id: uq:2 }]"

[[test.results]]
value = "[{ id: uq:2 }]"

[[test.results]]
error = "Database index `uq_email` already contains 'a@x', with record `uq:2`"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: uq_email, access: , direction: Forward]
''''

[[test.results]]
value = "[{ id: uq:2 }, { id: uq:3 }, { id: uq:1 }]"

[[test.results]]
value = "[{ id: uq:2 }, { id: uq:3 }, { id: uq:1 }]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: fan_tags, access: = 'x', direction: Forward, predicate: tags CONTAINS 'x']
"'''

[[test.results]]
value = "[{ id: fan:1 }, { id: fan:2 }]"

[[test.results]]
value = "[{ id: fan:1 }, { id: fan:2 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: fan, condition: tags CONTAINS 'x']
"'''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: fan, direction: Forward, predicate: tags = ['x', 'y'], pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ id: fan:1 }]"

[[test.results]]
value = "[{ id: fan:1 }]"

[[test.results]]
value = '''
"Aggregate [ctx: Db] [mode: GROUP ALL]
    TableScan [ctx: Db] [table: fan, direction: Forward, predicate: tags = ['x', 'y'], pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: fanp, direction: Forward, predicate: tags CONTAINS 'x', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ id: fanp:1 }, { id: fanp:2 }]"

[[test.results]]
value = "[{ id: fanp:1 }, { id: fanp:2 }]"

[[test.results]]
value = '''
"Aggregate [ctx: Db] [mode: GROUP ALL]
    TableScan [ctx: Db] [table: fanp, direction: Forward, predicate: tags CONTAINS 'x', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "'OK'"

[[test.results]]
error = "Database index `fanu_tags` already contains 'p', with record `fanu:1`"

[[test.results]]
error = "Database index `fanu_tags` already contains 's', with record `fanu:4`"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: fanu_tags, access: = 'p', direction: Forward, predicate: tags CONTAINS 'p']
"'''

[[test.results]]
value = "[{ id: fanu:1 }]"

[[test.results]]
value = "[{ id: fanu:1 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: fanu, condition: tags CONTAINS 'p']
"'''

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
'CountScan [ctx: Db] [source: ctr]
''''

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'CountScan [ctx: Db] [source: ctr]
''''

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "3"

[[test.results]]
value = '''
'CountScan [ctx: Db] [source: ctr]
''''

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "2"

[[test.results]]
value = "'OK'"

[[test.results]]
value = """{ events: {  }, fields: { state: 'DEFINE FIELD state ON gc TYPE string PERMISSIONS FULL' }, indexes: { gc_count: "DEFINE INDEX gc_count ON gc COUNT WHERE state = 'on'" }, lives: {  }, tables: {  } }"""

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: gc, condition: state = 'on']
"'''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"Aggregate [ctx: Db] [mode: GROUP ALL]
    TableScan [ctx: Db] [table: gc, direction: Forward, predicate: state = 'on', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"Aggregate [ctx: Db] [mode: GROUP ALL]
    TableScan [ctx: Db] [table: gc, direction: Forward, predicate: 'on' = state, pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"Aggregate [ctx: Db] [mode: GROUP ALL]
    TableScan [ctx: Db] [table: gc, direction: Forward, predicate: state = 'off', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
'CountScan [ctx: Db] [source: gc]
''''

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "3"

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: var, condition: zone = 'guarded']
"'''

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: var, condition: zone = 'n']
"'''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: var, condition: zone = 'n' AND email = 'a@x']
    BitmapAnd [ctx: Db]
        BitmapIndexScan [ctx: Db] [index: var_zone, access: = 'n']
        BitmapIndexScan [ctx: Db] [index: var_email, access: = 'a@x']
"'''

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
'Aggregate [ctx: Db] [mode: GROUP ALL]
    TableScan [ctx: Db] [table: var, direction: Forward, predicate: string::len(...) = 1, pre_decode_filter: no (unsupported predicate)]
''''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: pr_zone, access: = 'n', direction: Forward, predicate: zone = 'n']
"'''

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: pr, direction: Forward, predicate: zone = 'n', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ id: pr:1 }, { id: pr:2 }]"

[[test.results]]
value = "[{ id: pr:1 }, { id: pr:2 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: pr, direction: Forward, predicate: zone = 'n', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ id: pr:1 }, { id: pr:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ email: 'a@x', id: pr:4, zone: 'n' }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: pr, direction: Forward, predicate: email = 'a@x', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ id: pr:1 }, { id: pr:4 }]"

[[test.results]]
value = "[{ id: pr:1 }, { id: pr:4 }]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'CountScan [ctx: Db] [source: prc]
''''

[[test.results]]
value = "[{ id: prc:4 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "4"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: prg, condition: state = 'on']
"'''

[[test.results]]
value = "[{ id: prg:4, state: 'on' }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "3"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"Aggregate [ctx: Db] [mode: GROUP ALL]
    TableScan [ctx: Db] [table: prg, direction: Forward, predicate: state = 'on', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "3"

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: dupe_tags, access: = 'x', direction: Forward, predicate: tags CONTAINS 'x']
"'''

[[test.results]]
value = "[{ id: dupe:1 }, { id: dupe:2 }]"

[[test.results]]
value = "[{ id: dupe:1 }, { id: dupe:2 }]"

[[test.results]]
value = '''
"IndexCountScan [ctx: Db] [source: dupe, condition: tags CONTAINS 'x']
"'''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ id: dupe:4, tags: ['s', 's'] }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"
*/

-- ═══════════════════════════════════════════════════════════════════════
-- Standard index (`Index::Idx`)
-- ═══════════════════════════════════════════════════════════════════════

{
    DEFINE TABLE std SCHEMAFULL;
    DEFINE FIELD zone ON std TYPE string;
    DEFINE INDEX std_zone ON std FIELDS zone;
    CREATE std:1 SET zone = 'n';
    CREATE std:2 SET zone = 'n';
    CREATE std:3 SET zone = 's';
    RETURN 'OK';
};

-- A standard index serves equality as a seek. `predicate:` on the IndexScan
-- line is the WHERE threaded into the scan for a per-row recheck: the access
-- consumed the whole predicate, so no `Filter` sits above the scan.
EXPLAIN SELECT id FROM std WHERE zone = 'n';
SELECT id FROM std WHERE zone = 'n';
SELECT id FROM std WITH NOINDEX WHERE zone = 'n';

-- `!=` is not an access the B-tree offers, so the index is not chosen at all
-- and the predicate is evaluated over the scan.
EXPLAIN SELECT id FROM std WHERE zone != 'n';
SELECT id FROM std WHERE zone != 'n';
SELECT id FROM std WITH NOINDEX WHERE zone != 'n';

-- A range narrows to a one-sided access; the predicate is still rechecked.
EXPLAIN SELECT id FROM std WHERE zone > 'n';
SELECT id FROM std WHERE zone > 'n';
SELECT id FROM std WITH NOINDEX WHERE zone > 'n';

-- ═══════════════════════════════════════════════════════════════════════
-- UNIQUE index (`Index::Uniq`)
-- ═══════════════════════════════════════════════════════════════════════

{
    DEFINE TABLE uq SCHEMAFULL;
    DEFINE FIELD email ON uq TYPE string;
    DEFINE INDEX uq_email ON uq FIELDS email UNIQUE;
    CREATE uq:1 SET email = 'c@x';
    CREATE uq:2 SET email = 'a@x';
    CREATE uq:3 SET email = 'b@x';
    RETURN 'OK';
};

-- UNIQUE is invisible in the plan: the operator, the access rendering and the
-- attribute list are the ones a standard index produces. Nothing in an
-- `IndexScan` line distinguishes a unique index from a non-unique one, so a
-- plan cannot be read as evidence that at most one row can match.
EXPLAIN SELECT id FROM uq WHERE email = 'a@x';
SELECT id FROM uq WHERE email = 'a@x';
SELECT id FROM uq WITH NOINDEX WHERE email = 'a@x';

-- The constraint is enforced on write, which is the only place the kind shows.
CREATE uq:4 SET email = 'a@x';

-- A full index walk for sort elimination renders an empty `access:` — the same
-- for both kinds, so this too carries no kind information. The fixture's emails
-- ascend in an order the record ids do not, so the asserted row order is
-- evidence the walk ordered by `email`: an index walk that yielded key order
-- wrongly, or fell back to id order, would not produce this sequence.
EXPLAIN SELECT id FROM uq ORDER BY email;
SELECT id FROM uq ORDER BY email;
SELECT id FROM uq WITH NOINDEX ORDER BY email;

-- ═══════════════════════════════════════════════════════════════════════
-- A fanning-out column under each B-tree kind
--
-- A `*` column indexes one entry per element, so a row holding two distinct
-- elements is what separates an element access from a whole-array one: with a
-- single-element fixture `tags CONTAINS 'x'` and `tags = ['x']` select the same
-- row and neither access path can be told from the other. The two elements are
-- also what would expose a row emitted once per matching entry, if the read
-- paths did not collapse a row's entries — the `dupe` section at the end of
-- this file pins that they do.
-- ═══════════════════════════════════════════════════════════════════════

{
    DEFINE TABLE fan SCHEMAFULL;
    DEFINE FIELD tags ON fan TYPE array<string>;
    DEFINE INDEX fan_tags ON fan FIELDS tags.*;
    CREATE fan:1 SET tags = ['x', 'y'];
    CREATE fan:2 SET tags = ['x'];
    CREATE fan:3 SET tags = ['z'];
    RETURN 'OK';
};

-- `CONTAINS` against a `*` column is an element seek. Two rows hold 'x' and
-- each is emitted once: a row is reached through one entry per matching
-- element, and only one of `fan:1`'s two elements matches.
EXPLAIN SELECT id FROM fan WHERE tags CONTAINS 'x';
SELECT id FROM fan WHERE tags CONTAINS 'x';
SELECT id FROM fan WITH NOINDEX WHERE tags CONTAINS 'x';

-- The count goes through the key-only path rather than the scan, so it is
-- asserted separately from the rows above: it counts index entries, which is
-- the number of rows only while no row matches twice.
EXPLAIN SELECT count() FROM fan WHERE tags CONTAINS 'x' GROUP ALL;
SELECT count() FROM fan WHERE tags CONTAINS 'x' GROUP ALL;
SELECT count() FROM fan WITH NOINDEX WHERE tags CONTAINS 'x' GROUP ALL;

-- Whole-array equality is not an access a `*` column offers — its entries are
-- elements — so the index is left out of the plan and the scan answers.
EXPLAIN SELECT id FROM fan WHERE tags = ['x', 'y'];
SELECT id FROM fan WHERE tags = ['x', 'y'];
SELECT id FROM fan WITH NOINDEX WHERE tags = ['x', 'y'];
EXPLAIN SELECT count() FROM fan WHERE tags = ['x', 'y'] GROUP ALL;
SELECT count() FROM fan WHERE tags = ['x', 'y'] GROUP ALL;
SELECT count() FROM fan WITH NOINDEX WHERE tags = ['x', 'y'] GROUP ALL;

{
    DEFINE TABLE fanp SCHEMAFULL;
    DEFINE FIELD tags ON fanp TYPE array<string>;
    DEFINE INDEX fanp_tags ON fanp FIELDS tags;
    CREATE fanp:1 SET tags = ['x', 'y'];
    CREATE fanp:2 SET tags = ['x'];
    CREATE fanp:3 SET tags = ['z'];
    RETURN 'OK';
};

-- A plain column holding an array fans out on write exactly as a `*` column
-- does. Its field is declared `array<string>`, so it is planned as the `*`
-- column it behaves as, but `CONTAINS` is not an element seek on it and the
-- scan answers: a declared kind does not show that every stored value is an
-- array, and a string stored whole holds substrings no seek reaches. The `*`
-- spelling above is what serves containment from the index. Whole-array
-- comparisons over the plain column are pinned in array_fanout_oracle.surql.
EXPLAIN SELECT id FROM fanp WHERE tags CONTAINS 'x';
SELECT id FROM fanp WHERE tags CONTAINS 'x';
SELECT id FROM fanp WITH NOINDEX WHERE tags CONTAINS 'x';
EXPLAIN SELECT count() FROM fanp WHERE tags CONTAINS 'x' GROUP ALL;
SELECT count() FROM fanp WHERE tags CONTAINS 'x' GROUP ALL;
SELECT count() FROM fanp WITH NOINDEX WHERE tags CONTAINS 'x' GROUP ALL;

{
    DEFINE TABLE fanu SCHEMAFULL;
    DEFINE FIELD tags ON fanu TYPE array<string>;
    DEFINE INDEX fanu_tags ON fanu FIELDS tags.* UNIQUE;
    CREATE fanu:1 SET tags = ['p', 'q'];
    CREATE fanu:2 SET tags = ['r'];
    RETURN 'OK';
};

-- UNIQUE over a fanning-out column constrains elements, not whole arrays: a
-- second row sharing one element is rejected even though its array differs.
CREATE fanu:3 SET tags = ['p'];

-- DEFECT (4). The element uniqueness check does not deduplicate within the row
-- being written, so an array repeating an element collides with itself and the
-- error names the record being created as the existing holder. One row holding
-- `['s','s']` has one distinct element and violates nothing; the write should
-- succeed.
CREATE fanu:4 SET tags = ['s', 's'];
-- The rejected write left nothing behind, on either path.
SELECT count() FROM fanu WHERE tags CONTAINS 's' GROUP ALL;
SELECT count() FROM fanu WITH NOINDEX WHERE tags CONTAINS 's' GROUP ALL;

EXPLAIN SELECT id FROM fanu WHERE tags CONTAINS 'p';
SELECT id FROM fanu WHERE tags CONTAINS 'p';
SELECT id FROM fanu WITH NOINDEX WHERE tags CONTAINS 'p';
EXPLAIN SELECT count() FROM fanu WHERE tags CONTAINS 'p' GROUP ALL;
SELECT count() FROM fanu WHERE tags CONTAINS 'p' GROUP ALL;
SELECT count() FROM fanu WITH NOINDEX WHERE tags CONTAINS 'p' GROUP ALL;

-- ═══════════════════════════════════════════════════════════════════════
-- Unconditional COUNT index (`Index::Count(None)`)
--
-- `CountScan` resolves the index when it runs, not when it is planned, so the
-- plan is the same line whether the index exists or not. The pair below is
-- asserted before and after `DEFINE INDEX ... COUNT` to document that the plan
-- cannot tell you which of the two is happening: reading `CountScan` as
-- "the COUNT index is being used" is unfounded, and so is the opposite.
-- ═══════════════════════════════════════════════════════════════════════

{
    DEFINE TABLE ctr;
    CREATE ctr:1;
    CREATE ctr:2;
    CREATE ctr:3;
    RETURN 'OK';
};

-- No COUNT index: the key range is counted.
EXPLAIN SELECT count() FROM ctr GROUP ALL;
SELECT count() FROM ctr GROUP ALL;

DEFINE INDEX ctr_count ON ctr COUNT;

-- Same line, different mechanism: the index's deltas are summed. `WITH NOINDEX`
-- is not an oracle for this path (defect 2, below), so the row count is taken
-- through a projection, which reaches no count fast path at all.
EXPLAIN SELECT count() FROM ctr GROUP ALL;
SELECT count() FROM ctr GROUP ALL;
RETURN (SELECT id FROM ctr WITH NOINDEX).len();

-- DEFECT (2), first half. `WITH NOINDEX` does not change the plan either,
-- because the matcher behind `CountScan` never looks at the source's index
-- hint. The number is right here only because the index is still maintained;
-- the same hint is asserted below against an unmaintained one.
EXPLAIN SELECT count() FROM ctr WITH NOINDEX GROUP ALL;
SELECT count() FROM ctr WITH NOINDEX GROUP ALL;

-- The delta sum tracks deletes as well as inserts.
DELETE ctr:2;
SELECT count() FROM ctr GROUP ALL;
RETURN (SELECT id FROM ctr WITH NOINDEX).len();

-- ═══════════════════════════════════════════════════════════════════════
-- Guarded COUNT index (`DEFINE INDEX ... COUNT WHERE <cond>`)
-- ═══════════════════════════════════════════════════════════════════════

{
    DEFINE TABLE gc SCHEMAFULL;
    DEFINE FIELD state ON gc TYPE string;
    DEFINE INDEX gc_count ON gc COUNT WHERE state = 'on';
    CREATE gc:1 SET state = 'on';
    CREATE gc:2 SET state = 'on';
    CREATE gc:3 SET state = 'off';
    RETURN 'OK';
};

-- The guard is spelled back by INFO, which is where the kind is legible.
INFO FOR TABLE gc;

-- A WHERE the guard covers compiles to `IndexCountScan`, whose `condition:`
-- attribute is the query's predicate — not the index's guard, and not the name
-- of the index serving it. The plan therefore does not say *which* index (or
-- whether any) answers the count.
EXPLAIN SELECT count() FROM gc WHERE state = 'on' GROUP ALL;
SELECT count() FROM gc WHERE state = 'on' GROUP ALL;

-- Unlike the unconditional path, this one honours the index hint: the hint
-- takes the count off the fast path entirely, which is visible in the plan.
EXPLAIN SELECT count() FROM gc WITH NOINDEX WHERE state = 'on' GROUP ALL;
SELECT count() FROM gc WITH NOINDEX WHERE state = 'on' GROUP ALL;

-- The guard is matched by its SurrealQL rendering, so an equality written the
-- other way round is a different guard and the fast path is lost. The answer is
-- unaffected — this is a missed optimisation, legible only in the plan, which
-- is the reason to pin it.
EXPLAIN SELECT count() FROM gc WHERE 'on' = state GROUP ALL;
SELECT count() FROM gc WHERE 'on' = state GROUP ALL;

-- A guard that does not cover the WHERE contributes nothing, and with no
-- B-tree index on `state` there is no key-only path either.
EXPLAIN SELECT count() FROM gc WHERE state = 'off' GROUP ALL;
SELECT count() FROM gc WHERE state = 'off' GROUP ALL;

-- A guarded index does not serve an unguarded count: `CountScan` looks for an
-- unconditional COUNT index only, so this counts the key range.
EXPLAIN SELECT count() FROM gc GROUP ALL;
SELECT count() FROM gc GROUP ALL;
RETURN (SELECT id FROM gc WITH NOINDEX).len();

-- ═══════════════════════════════════════════════════════════════════════
-- Which `IndexCountScan` is which
--
-- The operator has four ways to produce its number — a guarded COUNT index's
-- deltas, a covering B-tree index's keys, a fused bitmap's cardinality, and a
-- scan-and-filter fallback — and renders only `source:` and `condition:`. The
-- three reachable by planning are asserted here so the file records exactly
-- how far the plan distinguishes them.
-- ═══════════════════════════════════════════════════════════════════════

{
    DEFINE TABLE var SCHEMAFULL;
    DEFINE FIELD zone ON var TYPE string;
    DEFINE FIELD email ON var TYPE string;
    DEFINE INDEX var_zone ON var FIELDS zone;
    DEFINE INDEX var_email ON var FIELDS email UNIQUE;
    DEFINE INDEX var_count ON var COUNT WHERE zone = 'guarded';
    CREATE var:1 SET zone = 'n', email = 'a@x';
    CREATE var:2 SET zone = 'n', email = 'b@x';
    CREATE var:3 SET zone = 'guarded', email = 'c@x';
    RETURN 'OK';
};

-- (a) The guarded COUNT index's deltas.
EXPLAIN SELECT count() FROM var WHERE zone = 'guarded' GROUP ALL;
SELECT count() FROM var WHERE zone = 'guarded' GROUP ALL;
SELECT count() FROM var WITH NOINDEX WHERE zone = 'guarded' GROUP ALL;

-- (b) A covering B-tree index's keys, counted without reading records. The
-- rendering is identical to (a): same operator, same two attributes.
EXPLAIN SELECT count() FROM var WHERE zone = 'n' GROUP ALL;
SELECT count() FROM var WHERE zone = 'n' GROUP ALL;
SELECT count() FROM var WITH NOINDEX WHERE zone = 'n' GROUP ALL;

-- (c) A fused bitmap over two indexes. This one *is* distinguishable, by the
-- child subtree the operator exposes.
EXPLAIN SELECT count() FROM var WHERE zone = 'n' AND email = 'a@x' GROUP ALL;
SELECT count() FROM var WHERE zone = 'n' AND email = 'a@x' GROUP ALL;
SELECT count() FROM var WITH NOINDEX WHERE zone = 'n' AND email = 'a@x' GROUP ALL;

-- (d) No index can answer the predicate, so the planner does not reach for
-- `IndexCountScan` at all. The operator's own scan-and-filter fallback is
-- therefore not plan-reachable: it is the runtime answer to an index that
-- disappeared between planning and execution, or to a conditional permission.
EXPLAIN SELECT count() FROM var WHERE string::len(zone) = 1 GROUP ALL;
SELECT count() FROM var WHERE string::len(zone) = 1 GROUP ALL;
SELECT count() FROM var WITH NOINDEX WHERE string::len(zone) = 1 GROUP ALL;

-- ═══════════════════════════════════════════════════════════════════════
-- `ALTER INDEX ... PREPARE REMOVE`
--
-- The flag means "no longer maintained": writes stop updating the structure
-- while it is still on disk. Every read path must therefore stop using it. The
-- B-tree access-path analysis does; the count paths do not.
-- ═══════════════════════════════════════════════════════════════════════

{
    DEFINE TABLE pr SCHEMAFULL;
    DEFINE FIELD zone ON pr TYPE string;
    DEFINE FIELD email ON pr TYPE string;
    DEFINE INDEX pr_zone ON pr FIELDS zone;
    DEFINE INDEX pr_email ON pr FIELDS email UNIQUE;
    CREATE pr:1 SET zone = 'n', email = 'a@x';
    CREATE pr:2 SET zone = 'n', email = 'b@x';
    CREATE pr:3 SET zone = 's', email = 'c@x';
    RETURN 'OK';
};

EXPLAIN SELECT id FROM pr WHERE zone = 'n';
ALTER INDEX pr_zone ON pr PREPARE REMOVE;

-- A decommissioned B-tree index leaves the plan: `IndexScan` becomes
-- `TableScan`, and the rows are unchanged. This is the behaviour every other
-- read path below is measured against.
EXPLAIN SELECT id FROM pr WHERE zone = 'n';
SELECT id FROM pr WHERE zone = 'n';
SELECT id FROM pr WITH NOINDEX WHERE zone = 'n';

-- A `WITH INDEX` hint naming it is not an error and does not resurrect it.
EXPLAIN SELECT id FROM pr WITH INDEX pr_zone WHERE zone = 'n';
SELECT id FROM pr WITH INDEX pr_zone WHERE zone = 'n';

-- A decommissioned UNIQUE index also stops constraining writes, which is what
-- "no longer maintained" means for that kind — and why a read path that keeps
-- trusting such an index is unsound.
ALTER INDEX pr_email ON pr PREPARE REMOVE;
CREATE pr:4 SET zone = 'n', email = 'a@x';
EXPLAIN SELECT id FROM pr WHERE email = 'a@x';
SELECT id FROM pr WHERE email = 'a@x';
SELECT id FROM pr WITH NOINDEX WHERE email = 'a@x';

{
    DEFINE TABLE prc;
    DEFINE INDEX prc_count ON prc COUNT;
    CREATE prc:1;
    CREATE prc:2;
    CREATE prc:3;
    RETURN 'OK';
};

ALTER INDEX prc_count ON prc PREPARE REMOVE;

-- DEFECT (1), unconditional half, and DEFECT (2), second half. The plan is
-- unchanged — as it is for every state of a COUNT index — and `CountScan`
-- still sums the deltas of an index writes no longer maintain, so the count is
-- right until the next write and wrong afterwards. `WITH NOINDEX` reports the
-- same stale number. Correct answer for all three reads below: 4, which only a
-- formulation that cannot reach the count fast path reports.
EXPLAIN SELECT count() FROM prc GROUP ALL;
CREATE prc:4;
SELECT count() FROM prc GROUP ALL;
SELECT count() FROM prc WITH NOINDEX GROUP ALL;
RETURN (SELECT id FROM prc WITH NOINDEX).len();

{
    DEFINE TABLE prg SCHEMAFULL;
    DEFINE FIELD state ON prg TYPE string;
    DEFINE INDEX prg_count ON prg COUNT WHERE state = 'on';
    CREATE prg:1 SET state = 'on';
    CREATE prg:2 SET state = 'on';
    CREATE prg:3 SET state = 'off';
    RETURN 'OK';
};

ALTER INDEX prg_count ON prg PREPARE REMOVE;

-- DEFECT (1), guarded half. `IndexCountScan` matches the guard without
-- consulting `prepare_remove`, so the same staleness reaches the guarded path:
-- 2 where the guard admits 3. Correct answer: 3. Here the index hint does
-- reach the decision, so `WITH NOINDEX` recovers the true count — the guarded
-- path has the escape hatch the unconditional one lacks.
EXPLAIN SELECT count() FROM prg WHERE state = 'on' GROUP ALL;
CREATE prg:4 SET state = 'on';
SELECT count() FROM prg WHERE state = 'on' GROUP ALL;
SELECT count() FROM prg WITH NOINDEX WHERE state = 'on' GROUP ALL;
RETURN (SELECT id FROM prg WITH NOINDEX WHERE state = 'on').len();

-- The structure can then be dropped, which is the point of the flag.
REMOVE INDEX prg_count ON prg;
EXPLAIN SELECT count() FROM prg WHERE state = 'on' GROUP ALL;
SELECT count() FROM prg WHERE state = 'on' GROUP ALL;
RETURN (SELECT id FROM prg WITH NOINDEX WHERE state = 'on').len();

-- ═══════════════════════════════════════════════════════════════════════
-- A row whose own array repeats an element
--
-- The fan-out writes one entry per element, so a repeated element is the case
-- where a row could be reached twice through one predicate. Both read paths
-- collapse a row's entries: the rows do not duplicate and the count does not
-- inflate. Pinned because the UNIQUE variant of the same shape does *not*
-- collapse them — that is defect (4) above, and this section is what makes it
-- an inconsistency between the two kinds rather than a uniform policy.
-- ═══════════════════════════════════════════════════════════════════════

{
    DEFINE TABLE dupe SCHEMAFULL;
    DEFINE FIELD tags ON dupe TYPE array<string>;
    DEFINE INDEX dupe_tags ON dupe FIELDS tags.*;
    CREATE dupe:1 SET tags = ['x', 'x'];
    CREATE dupe:2 SET tags = ['x', 'y'];
    CREATE dupe:3 SET tags = ['y'];
    RETURN 'OK';
};

-- `dupe:1` owns two entries keyed 'x' and is emitted once.
EXPLAIN SELECT id FROM dupe WHERE tags CONTAINS 'x';
SELECT id FROM dupe WHERE tags CONTAINS 'x';
SELECT id FROM dupe WITH NOINDEX WHERE tags CONTAINS 'x';
EXPLAIN SELECT count() FROM dupe WHERE tags CONTAINS 'x' GROUP ALL;
SELECT count() FROM dupe WHERE tags CONTAINS 'x' GROUP ALL;
SELECT count() FROM dupe WITH NOINDEX WHERE tags CONTAINS 'x' GROUP ALL;

-- The same non-unique index accepts the row defect (4) rejects under UNIQUE.
CREATE dupe:4 SET tags = ['s', 's'];
SELECT count() FROM dupe WHERE tags CONTAINS 's' GROUP ALL;
SELECT count() FROM dupe WITH NOINDEX WHERE tags CONTAINS 's' GROUP ALL;