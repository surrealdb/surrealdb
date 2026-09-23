/**
[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
Residual stripping: which predicate leaves survive re-checking once the scan
has proved them, and which must not. A leaf an index access proves leaves no
`Filter` node above the scan; a leaf the access merely over-approximates, or
does not address at all, has to remain, because a wrongly stripped leaf returns
rows the predicate excludes. Both directions are asserted here, and every plan
assertion is paired with the same query under WITH NOINDEX -- a forced table
scan, and therefore the oracle for the rows the predicate selects. A plan that
reads correctly while returning rows the oracle does not is the failure this
file exists to catch, so neither half stands alone; every shape over an array
column carries the `count() ... GROUP ALL` pair as well, because a fanning
column reaches one record through several index entries and a miscount survives
a row comparison that sorts and dedupes. Where a case carries a LIMIT the
oracle pair is taken on the whole matching set instead, because index order and
table order differ and which rows a limit keeps is not promised: what the pair
has to prove is that the two paths select the same records, and the plan is
what the limit is asserted on.

What each group owns:

  * A -- a union whose branches are exactly the leaf's literals, and which
    still does not prove it. A multi-value `IN` / `INSIDE` on an indexed
    column, and the `OR` of equalities it is equivalent to, expand to one
    branch per literal; each branch seeks entries carrying its literal, while
    the leaf names the column's whole value. A column holding an array has one
    entry per element, so the two readings part company there, and the
    `Filter` above the `UnionIndexScan` is what keeps them apart.
  * B -- the union is wider than the leaf, so the leaf stays. `CONTAINSALL`
    over a fanning column unions the same branches as `CONTAINSANY` and asks
    for more than any one of them proves; a second `IN` whose literal list is a
    strict superset of the branches likewise; an `OR` reaching a leaf on
    another column is proved by neither side alone. In each case the residual
    names every leaf the access did not prove.
  * C -- `IN []` and `CONTAINSANY []` are false for every record. Neither may
    be stripped by an access that cannot express it: the empty `IN` is answered
    by an `EmptyScan` that reads nothing, and the empty `CONTAINSANY`, which
    has no branch to expand to, is left to a scan that carries it as a
    predicate.
  * D -- a containment leaf pinned at a NON-leading position of a compound
    prefix. On `FIELDS b, tags[*]` an equality on `b` and a single-value
    `CONTAINS` on `tags` pin both columns, so the access is exact, the scan
    needs no `Distinct`, and neither leaf survives as a `Filter`. The
    observable consequence is LIMIT placement: a predicate the scan has fully
    consumed lets the limit be pushed into the scan itself (`limit:` on the
    `IndexScan`), while one unproved leaf puts a `Filter` between the scan and
    the limit and the limit becomes a separate `Limit` operator. The pair D2 /
    D3 is the sharpest evidence in the file, and D6 is the third case: a
    `Distinct` collapsing a partially pinned access also holds the limit out of
    the scan, because the rows the limit counts are the ones the `Distinct`
    emits.
  * E -- the mirror index `(tags[*], b)`, where the fanning column leads. The
    same two leaves pin the same two columns and the same limit is pushed into
    the scan, so the position of the fanning column within the index decides
    nothing about stripping. What it does decide is which single-leaf query the
    index can serve at all: E3 seeks on the containment alone, and E4, the
    equality on `b` alone, reaches no prefix and falls back to a table scan.
  * F -- a conjunction only some of whose leaves are proved. The residual names
    exactly the survivors, and an `IndexScan` carrying a residual `Filter`
    drops its own `predicate:` attribute, so the two places a leaf can be
    re-checked never both hold it.

An `IndexScan` whose access proves the whole predicate still carries that
predicate as a `predicate:` attribute, which the scan evaluates on each fetched
row; plan_features.surql owns that as a recorded wart. It is asserted here
because the attribute is where a stripped leaf would reappear, and because F is
the shape that shows the attribute and the `Filter` node are alternatives
rather than a pair.

Every fixture carries the neighbours that turn a wrong bound into a row
difference. Each seeked value has a record immediately below it and one
immediately above that the access must exclude: `rsc` holds `b` and `d` around
the `a` / `c` branches, `rsa` holds a record whose only tag is the highest
branch value, and `rsf` / `rsm` hold, under the same `b`, one record tagged
below the pinned value and one above it -- so an access that reads one key past
its prefix leaks a record rather than re-deriving the same answer. `rsc` also
carries two further records under `code = 'a'`, each failing exactly one of F1's
two residual leaves, so dropping either leaf alone is visible in the rows; and
an equality seek on `code = 'a'` matches more than one record, so a seek that
stopped at the first entry would be visible too.

Not every residual can be falsified by rows. Where an access is exactly the
predicate's extension -- B3 and B5, where exact bitmaps are unioned or
intersected -- the residual changes no answer and only the plan records whether
it is there. Both are marked where they occur, so the file does not claim an
oracle is doing work it cannot do. Everywhere else the access is strictly wider
than the answer, and dropping the residual is a row difference the NOINDEX half
catches.

Narrowed to the streaming strategies because a prefix `EXPLAIN` errors under
compute-only; the two plan identically by contract, so pinning both costs
nothing and states the intent.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: code INSIDE ['a', 'c']]
        UnionIndexScan [ctx: Db] [table: rsc, branches: 2]
            IndexScan [ctx: Db] [index: rsc_code, access: = 'a', direction: Forward]
            IndexScan [ctx: Db] [index: rsc_code, access: = 'c', direction: Forward]
"'''

[[test.results]]
value = "[rsc:1, rsc:3, rsc:5, rsc:6]"

[[test.results]]
value = "[rsc:1, rsc:3, rsc:5, rsc:6]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: code INSIDE ['a', 'c']]
        UnionIndexScan [ctx: Db] [table: rsc, branches: 2]
            IndexScan [ctx: Db] [index: rsc_code, access: = 'a', direction: Forward]
            IndexScan [ctx: Db] [index: rsc_code, access: = 'c', direction: Forward]
"'''

[[test.results]]
value = "[rsc:1, rsc:3, rsc:5, rsc:6]"

[[test.results]]
value = "[rsc:1, rsc:3, rsc:5, rsc:6]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: code = 'a' OR code = 'c']
        UnionIndexScan [ctx: Db] [table: rsc, branches: 2]
            IndexScan [ctx: Db] [index: rsc_code, access: = 'a', direction: Forward]
            IndexScan [ctx: Db] [index: rsc_code, access: = 'c', direction: Forward]
"'''

[[test.results]]
value = "[rsc:1, rsc:3, rsc:5, rsc:6]"

[[test.results]]
value = "[rsc:1, rsc:3, rsc:5, rsc:6]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: rsc_code, access: = 'a', direction: Forward, predicate: code INSIDE ['a']]
"'''

[[test.results]]
value = "[rsc:1, rsc:5, rsc:6]"

[[test.results]]
value = "[rsc:1, rsc:5, rsc:6]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: country INSIDE ['fr', 'de']]
        UnionIndexScan [ctx: Db] [table: rsv, branches: 2]
            IndexScan [ctx: Db] [index: rsv_country, access: = 'fr', direction: Forward]
            IndexScan [ctx: Db] [index: rsv_country, access: = 'de', direction: Forward]
"'''

[[test.results]]
value = "[rsv:2]"

[[test.results]]
value = "[rsv:2]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: country = 'fr' OR country = 'be']
        UnionIndexScan [ctx: Db] [table: rsv, branches: 2]
            IndexScan [ctx: Db] [index: rsv_country, access: = 'fr', direction: Forward]
            IndexScan [ctx: Db] [index: rsv_country, access: = 'be', direction: Forward]
"'''

[[test.results]]
value = "[rsv:2]"

[[test.results]]
value = "[rsv:2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: tags CONTAINSALL ['x', 'y']]
        UnionIndexScan [ctx: Db] [table: rsa, branches: 2]
            IndexScan [ctx: Db] [index: rsa_tags, access: = 'x', direction: Forward]
            IndexScan [ctx: Db] [index: rsa_tags, access: = 'y', direction: Forward]
"'''

[[test.results]]
value = "[rsa:1]"

[[test.results]]
value = "[rsa:1]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    UnionIndexScan [ctx: Db] [table: rsa, branches: 2]
        IndexScan [ctx: Db] [index: rsa_tags, access: = 'x', direction: Forward]
        IndexScan [ctx: Db] [index: rsa_tags, access: = 'y', direction: Forward]
"'''

[[test.results]]
value = "[rsa:1, rsa:2, rsa:3]"

[[test.results]]
value = "[rsa:1, rsa:2, rsa:3]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: n = 3 OR tags CONTAINS 'z']
        BitmapResolve [ctx: Db] [table: rsa]
            BitmapOr [ctx: Db]
                BitmapIndexScan [ctx: Db] [index: rsa_n, access: = 3]
                BitmapIndexScan [ctx: Db] [index: rsa_tags, access: = 'z']
"'''

[[test.results]]
value = "[rsa:2, rsa:3, rsa:5]"

[[test.results]]
value = "[rsa:2, rsa:3, rsa:5]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: rsc, direction: Forward, predicate: code = 'a' OR note = 'keep', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[rsc:1, rsc:3, rsc:4, rsc:5, rsc:6]"

[[test.results]]
value = "[rsc:1, rsc:3, rsc:4, rsc:5, rsc:6]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: code INSIDE ['a', 'c'] AND code INSIDE ['a', 'c', 'd']]
        BitmapResolve [ctx: Db] [table: rsc]
            BitmapAnd [ctx: Db]
                BitmapOr [ctx: Db]
                    BitmapIndexScan [ctx: Db] [index: rsc_code, access: = 'a']
                    BitmapIndexScan [ctx: Db] [index: rsc_code, access: = 'c']
                BitmapOr [ctx: Db]
                    BitmapIndexScan [ctx: Db] [index: rsc_code, access: = 'a']
                    BitmapIndexScan [ctx: Db] [index: rsc_code, access: = 'c']
                    BitmapIndexScan [ctx: Db] [index: rsc_code, access: = 'd']
"'''

[[test.results]]
value = "[rsc:1, rsc:3, rsc:5, rsc:6]"

[[test.results]]
value = "[rsc:1, rsc:3, rsc:5, rsc:6]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    EmptyScan [ctx: Rt]
''''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: rsa, direction: Forward, predicate: tags CONTAINSANY [], pre_decode_filter: yes]
''''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: rsf_b_tags, access: [1, 'x'], direction: Forward, predicate: b = 1 AND tags CONTAINS 'x']
"'''

[[test.results]]
value = "[rsf:1, rsf:2]"

[[test.results]]
value = "[rsf:1, rsf:2]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: rsf_b_tags, access: [1, 'x'], direction: Forward, limit: 2, predicate: b = 1 AND tags CONTAINS 'x']
"'''

[[test.results]]
value = "[rsf:1, rsf:2]"

[[test.results]]
value = "[rsf:1, rsf:2]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Limit [ctx: Db] [limit: 2]
        Filter [ctx: Db] [predicate: note = 'keep']
            IndexScan [ctx: Db] [index: rsf_b_tags, access: [1, 'x'], direction: Forward]
"'''

[[test.results]]
value = "[rsf:1]"

[[test.results]]
value = "[rsf:1]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: rsf_b_tags, access: [1, 'x'], direction: Forward, predicate: b = 1 AND 'x' INSIDE tags]
"'''

[[test.results]]
value = "[rsf:1, rsf:2]"

[[test.results]]
value = "[rsf:1, rsf:2]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: tags CONTAINSANY []]
        DistinctRecords [ctx: Db]
            IndexScan [ctx: Db] [index: rsf_b_tags, access: [1], direction: Forward]
''''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    Limit [ctx: Db] [limit: 2]
        DistinctRecords [ctx: Db]
            IndexScan [ctx: Db] [index: rsf_b_tags, access: [1], direction: Forward, predicate: b = 1]
''''

[[test.results]]
value = "[rsf:1, rsf:2, rsf:3, rsf:5]"

[[test.results]]
value = "[rsf:1, rsf:2, rsf:3, rsf:5]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: rsm_tags_b, access: ['x', 1], direction: Forward, predicate: tags CONTAINS 'x' AND b = 1]
"'''

[[test.results]]
value = "[rsm:1, rsm:2]"

[[test.results]]
value = "[rsm:1, rsm:2]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: rsm_tags_b, access: ['x', 1], direction: Forward, limit: 2, predicate: tags CONTAINS 'x' AND b = 1]
"'''

[[test.results]]
value = "[rsm:1, rsm:2]"

[[test.results]]
value = "[rsm:1, rsm:2]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [index: rsm_tags_b, access: ['x'], direction: Forward, predicate: tags CONTAINS 'x']
"'''

[[test.results]]
value = "[rsm:1, rsm:2, rsm:3]"

[[test.results]]
value = "[rsm:1, rsm:2, rsm:3]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: rsm, direction: Forward, predicate: b = 1, pre_decode_filter: yes]
''''

[[test.results]]
value = "[rsm:1, rsm:2, rsm:4, rsm:5]"

[[test.results]]
value = "[rsm:1, rsm:2, rsm:4, rsm:5]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: note = 'keep' AND n = 1]
        IndexScan [ctx: Db] [index: rsc_code, access: = 'a', direction: Forward]
"'''

[[test.results]]
value = "[rsc:1]"

[[test.results]]
value = "[rsc:1]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: code INSIDE ['a', 'c'] AND note = 'keep']
        UnionIndexScan [ctx: Db] [table: rsc, branches: 2]
            IndexScan [ctx: Db] [index: rsc_code, access: = 'a', direction: Forward]
            IndexScan [ctx: Db] [index: rsc_code, access: = 'c', direction: Forward]
"'''

[[test.results]]
value = "[rsc:1, rsc:3, rsc:6]"

[[test.results]]
value = "[rsc:1, rsc:3, rsc:6]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: code INSIDE ['a', 'c'] AND code != 'c']
        UnionIndexScan [ctx: Db] [table: rsc, branches: 2]
            IndexScan [ctx: Db] [index: rsc_code, access: = 'a', direction: Forward]
            IndexScan [ctx: Db] [index: rsc_code, access: = 'c', direction: Forward]
"'''

[[test.results]]
value = "[rsc:1, rsc:5, rsc:6]"

[[test.results]]
value = "[rsc:1, rsc:5, rsc:6]"

*/

-- ===========================================================================
-- Group A: a union whose branches are exactly the leaf's literals. The
-- branches partition the leaf's truth set, yet the leaf stays in a `Filter`:
-- a branch seeks an ENTRY carrying its literal, and a column holding an array
-- has one entry per element, so a row can satisfy a branch while the leaf --
-- which names the column's whole value -- rejects it. The union runs its
-- branches without a predicate of their own, so the `Filter` above is the
-- only thing standing between the two readings.
-- ===========================================================================

{
    DEFINE TABLE OVERWRITE rsc SCHEMALESS;
    DEFINE INDEX OVERWRITE rsc_code ON rsc FIELDS code;
    CREATE rsc:1 SET code = 'a', note = 'keep', n = 1;
    CREATE rsc:2 SET code = 'b', note = 'drop', n = 2;
    CREATE rsc:3 SET code = 'c', note = 'keep', n = 3;
    CREATE rsc:4 SET code = 'd', note = 'keep', n = 4;
    CREATE rsc:5 SET code = 'a', note = 'drop', n = 1;
    CREATE rsc:6 SET code = 'a', note = 'keep', n = 9;
    RETURN NONE;
};

-- A1: a multi-value `IN` on a scalar indexed column. Two branches, two
-- literals, and the leaf retained above them.
EXPLAIN SELECT VALUE id FROM rsc WHERE code IN ['a', 'c'];
SELECT VALUE id FROM rsc WHERE code IN ['a', 'c'] ORDER BY id;
SELECT VALUE id FROM rsc WITH NOINDEX WHERE code IN ['a', 'c'] ORDER BY id;

-- A2: `INSIDE` is the same assertion spelled the other way round and plans
-- identically to A1, residual included.
EXPLAIN SELECT VALUE id FROM rsc WHERE code INSIDE ['a', 'c'];
SELECT VALUE id FROM rsc WHERE code INSIDE ['a', 'c'] ORDER BY id;
SELECT VALUE id FROM rsc WITH NOINDEX WHERE code INSIDE ['a', 'c'] ORDER BY id;

-- A3: the `OR` of equalities the list expands to, written out. The union is
-- built from the disjunction itself, so this is the shape A1 is reduced to
-- and carries no residual either.
EXPLAIN SELECT VALUE id FROM rsc WHERE code = 'a' OR code = 'c';
SELECT VALUE id FROM rsc WHERE code = 'a' OR code = 'c' ORDER BY id;
SELECT VALUE id FROM rsc WITH NOINDEX WHERE code = 'a' OR code = 'c' ORDER BY id;

-- A4: a one-literal list. A union of one branch is an ordinary equality seek,
-- and the leaf it proves is carried by the scan's `predicate:` attribute
-- rather than by a `Filter` -- the single-branch boundary of the group.
EXPLAIN SELECT VALUE id FROM rsc WHERE code IN ['a'];
SELECT VALUE id FROM rsc WHERE code IN ['a'] ORDER BY id;
SELECT VALUE id FROM rsc WITH NOINDEX WHERE code IN ['a'] ORDER BY id;

-- A6: the reading the `Filter` above Group A's union keeps apart. `country`
-- carries no `*`, yet its value is an array, so the write path gives it one
-- entry per element and a branch seeking 'fr' reaches this record. The leaf
-- compares the column's whole value, which is not 'fr', so the record is not
-- in the predicate's truth set and the oracle is empty. Stripping the leaf
-- would return it.
{
    DEFINE TABLE OVERWRITE rsv SCHEMALESS;
    DEFINE INDEX OVERWRITE rsv_country ON rsv FIELDS country;
    CREATE rsv:1 SET country = ['fr', 'be'];
    CREATE rsv:2 SET country = 'fr';
    RETURN NONE;
};
EXPLAIN SELECT VALUE id FROM rsv WHERE country IN ['fr', 'de'];
SELECT VALUE id FROM rsv WHERE country IN ['fr', 'de'] ORDER BY id;
SELECT VALUE id FROM rsv WITH NOINDEX WHERE country IN ['fr', 'de'] ORDER BY id;
SELECT count() FROM rsv WHERE country IN ['fr', 'de'] GROUP ALL;
SELECT count() FROM rsv WITH NOINDEX WHERE country IN ['fr', 'de'] GROUP ALL;

-- A7: the same record through the `OR` spelling the union is also built from.
EXPLAIN SELECT VALUE id FROM rsv WHERE country = 'fr' OR country = 'be';
SELECT VALUE id FROM rsv WHERE country = 'fr' OR country = 'be' ORDER BY id;
SELECT VALUE id FROM rsv WITH NOINDEX WHERE country = 'fr' OR country = 'be' ORDER BY id;

-- ===========================================================================
-- Group B: the access is wider than the leaf, so the leaf must remain. B1 and
-- B4 are the two the rows can settle: dropping the residual there returns
-- records the oracle excludes, which is what the NOINDEX half catches. B3 and
-- B5 cannot be settled by rows at all -- a union or intersection of exact
-- bitmaps has precisely the predicate's extension, so their residual is a
-- re-check that changes no answer, and its presence is a plan assertion only.
-- ===========================================================================

{
    DEFINE TABLE OVERWRITE rsa SCHEMALESS;
    DEFINE INDEX OVERWRITE rsa_tags ON rsa FIELDS tags[*];
    DEFINE INDEX OVERWRITE rsa_n ON rsa FIELDS n;
    CREATE rsa:1 SET tags = ['x', 'y'], n = 1, note = 'keep';
    CREATE rsa:2 SET tags = ['x', 'z'], n = 2, note = 'drop';
    CREATE rsa:3 SET tags = ['y'], n = 3, note = 'keep';
    CREATE rsa:4 SET tags = [], n = 4, note = 'keep';
    CREATE rsa:5 SET tags = ['z'], n = 5, note = 'drop';
    RETURN NONE;
};

-- B1: `CONTAINSALL` unions the same branches `CONTAINSANY` would -- one per
-- element -- and a record reaching the union carries at least one of them,
-- which is strictly weaker than carrying all of them. The leaf survives, and
-- the rows prove why: rsa:2 and rsa:3 are in the union and not in the answer.
EXPLAIN SELECT VALUE id FROM rsa WHERE tags CONTAINSALL ['x', 'y'];
SELECT VALUE id FROM rsa WHERE tags CONTAINSALL ['x', 'y'] ORDER BY id;
SELECT VALUE id FROM rsa WITH NOINDEX WHERE tags CONTAINSALL ['x', 'y'] ORDER BY id;
SELECT count() FROM rsa WHERE tags CONTAINSALL ['x', 'y'] GROUP ALL;
SELECT count() FROM rsa WITH NOINDEX WHERE tags CONTAINSALL ['x', 'y'] GROUP ALL;

-- B2: `CONTAINSANY` over the same two elements is the leaf the union does
-- prove -- the control B1 is read against. Same branches, no residual.
EXPLAIN SELECT VALUE id FROM rsa WHERE tags CONTAINSANY ['x', 'y'];
SELECT VALUE id FROM rsa WHERE tags CONTAINSANY ['x', 'y'] ORDER BY id;
SELECT VALUE id FROM rsa WITH NOINDEX WHERE tags CONTAINSANY ['x', 'y'] ORDER BY id;
SELECT count() FROM rsa WHERE tags CONTAINSANY ['x', 'y'] GROUP ALL;
SELECT count() FROM rsa WITH NOINDEX WHERE tags CONTAINSANY ['x', 'y'] GROUP ALL;

-- B3: an `OR` whose two sides are leaves on DIFFERENT indexed columns. The
-- sides are resolved as bitmaps and unioned, and neither side proves the
-- disjunction, so the whole predicate is re-checked above the resolve. The
-- union of two exact bitmaps is the disjunction, so this re-check rejects
-- nothing and the rows cannot tell it apart from its absence; the plan is the
-- whole assertion here.
EXPLAIN SELECT VALUE id FROM rsa WHERE n = 3 OR tags CONTAINS 'z';
SELECT VALUE id FROM rsa WHERE n = 3 OR tags CONTAINS 'z' ORDER BY id;
SELECT VALUE id FROM rsa WITH NOINDEX WHERE n = 3 OR tags CONTAINS 'z' ORDER BY id;
SELECT count() FROM rsa WHERE n = 3 OR tags CONTAINS 'z' GROUP ALL;
SELECT count() FROM rsa WITH NOINDEX WHERE n = 3 OR tags CONTAINS 'z' GROUP ALL;

-- B4: an `OR` mixing an indexed leaf with a leaf on a column no index
-- covers. There is no access that could prove either side of it, so the
-- statement falls back to a table scan and the predicate is carried whole.
EXPLAIN SELECT VALUE id FROM rsc WHERE code = 'a' OR note = 'keep';
SELECT VALUE id FROM rsc WHERE code = 'a' OR note = 'keep' ORDER BY id;
SELECT VALUE id FROM rsc WITH NOINDEX WHERE code = 'a' OR note = 'keep' ORDER BY id;

-- B5: two `IN` leaves on one indexed column whose literal lists differ, the
-- second a strict superset of the first. Each is expanded to its own union of
-- branches and the two are intersected; a record in the intersection is
-- proved to satisfy neither leaf by the intersection alone, so BOTH survive
-- in the residual. As in B3 the intersection is exact, so the rows agree
-- either way and the plan carries the assertion.
EXPLAIN SELECT VALUE id FROM rsc WHERE code IN ['a', 'c'] AND code IN ['a', 'c', 'd'];
SELECT VALUE id FROM rsc WHERE code IN ['a', 'c'] AND code IN ['a', 'c', 'd'] ORDER BY id;
SELECT VALUE id FROM rsc WITH NOINDEX WHERE code IN ['a', 'c'] AND code IN ['a', 'c', 'd'] ORDER BY id;

-- ===========================================================================
-- Group C: an empty containment list is false for every record. A leaf that
-- no access can express must not be discharged by one, and the answer is
-- empty either way -- so the rows alone cannot tell a correct plan from a
-- stripped leaf over a non-empty access, which is what the plans pin.
-- ===========================================================================

-- C1: `IN []` has no branch to expand to and no record can satisfy it, so it
-- is answered without reaching the table at all.
EXPLAIN SELECT VALUE id FROM rsc WHERE code IN [];
SELECT VALUE id FROM rsc WHERE code IN [] ORDER BY id;
SELECT VALUE id FROM rsc WITH NOINDEX WHERE code IN [] ORDER BY id;

-- C2: `CONTAINSANY []` over a fanning column. A union of zero branches is not
-- the same statement as a scan that reads nothing, and the leaf is kept on
-- the scan rather than expanded.
EXPLAIN SELECT VALUE id FROM rsa WHERE tags CONTAINSANY [];
SELECT VALUE id FROM rsa WHERE tags CONTAINSANY [] ORDER BY id;
SELECT VALUE id FROM rsa WITH NOINDEX WHERE tags CONTAINSANY [] ORDER BY id;
SELECT count() FROM rsa WHERE tags CONTAINSANY [] GROUP ALL;
SELECT count() FROM rsa WITH NOINDEX WHERE tags CONTAINSANY [] GROUP ALL;

-- ===========================================================================
-- Group D: a containment leaf pinned at a non-leading position of a compound
-- prefix, and the LIMIT placement that follows from whether the predicate is
-- fully consumed. `FIELDS b, tags[*]` stores one entry per element of `tags`
-- under each `b`, so an equality on `b` and a single-value containment on
-- `tags` together address one exact key range. Under `b = 1` the fixture holds
-- a record tagged only below the pinned value and one tagged only above it, so
-- an access that runs a key past either end of its prefix returns a record the
-- oracle excludes rather than re-deriving the same answer.
-- ===========================================================================

{
    DEFINE TABLE OVERWRITE rsf SCHEMALESS;
    DEFINE INDEX OVERWRITE rsf_b_tags ON rsf FIELDS b, tags[*];
    CREATE rsf:1 SET b = 1, tags = ['x', 'y'], note = 'keep';
    CREATE rsf:2 SET b = 1, tags = ['x', 'z'], note = 'drop';
    CREATE rsf:3 SET b = 1, tags = ['w'], note = 'keep';
    CREATE rsf:4 SET b = 2, tags = ['x', 'y'], note = 'keep';
    CREATE rsf:5 SET b = 1, tags = ['y'], note = 'keep';
    RETURN NONE;
};

-- D1: both columns pinned. The containment at the non-leading position is
-- proved by the access exactly as the leading equality is, so no `Filter`
-- stands above the scan -- and no `Distinct` either, because a pinned fanning
-- column reaches each matching record through one entry.
EXPLAIN SELECT VALUE id FROM rsf WHERE b = 1 AND tags CONTAINS 'x';
SELECT VALUE id FROM rsf WHERE b = 1 AND tags CONTAINS 'x' ORDER BY id;
SELECT VALUE id FROM rsf WITH NOINDEX WHERE b = 1 AND tags CONTAINS 'x' ORDER BY id;
SELECT count() FROM rsf WHERE b = 1 AND tags CONTAINS 'x' GROUP ALL;
SELECT count() FROM rsf WITH NOINDEX WHERE b = 1 AND tags CONTAINS 'x' GROUP ALL;

-- D2: D1 under a LIMIT. Every leaf is consumed by the access, so nothing
-- between the scan and the statement can reject a row and the limit is
-- pushed into the scan as `limit:`.
--
-- The oracle pair is taken on the whole matching set rather than on the
-- limited query: index order and table order differ, so the first two rows of
-- each are legitimately different rows and comparing them would assert a scan
-- order nothing promises.
EXPLAIN SELECT VALUE id FROM rsf WHERE b = 1 AND tags CONTAINS 'x' LIMIT 2;
array::sort((SELECT VALUE id FROM rsf WHERE b = 1 AND tags CONTAINS 'x'));
array::sort((SELECT VALUE id FROM rsf WITH NOINDEX WHERE b = 1 AND tags CONTAINS 'x'));

-- D3: the contrast D2 exists against -- one further leaf on a column the
-- index does not carry. The scan feeds a `Filter`, which may reject rows,
-- so the limit cannot be pushed into the scan and stands as its own `Limit`
-- above the filter. The scan drops its `predicate:` attribute: the residual
-- has moved to the `Filter`, which names the survivor alone.
EXPLAIN SELECT VALUE id FROM rsf WHERE b = 1 AND tags CONTAINS 'x' AND note = 'keep' LIMIT 2;
array::sort((SELECT VALUE id FROM rsf WHERE b = 1 AND tags CONTAINS 'x' AND note = 'keep'));
array::sort((SELECT VALUE id FROM rsf WITH NOINDEX WHERE b = 1 AND tags CONTAINS 'x' AND note = 'keep'));
SELECT count() FROM rsf WHERE b = 1 AND tags CONTAINS 'x' AND note = 'keep' GROUP ALL;
SELECT count() FROM rsf WITH NOINDEX WHERE b = 1 AND tags CONTAINS 'x' AND note = 'keep' GROUP ALL;

-- D4: the containment spelled with the value on the left. Same prefix, same
-- absence of a residual.
EXPLAIN SELECT VALUE id FROM rsf WHERE b = 1 AND 'x' INSIDE tags;
SELECT VALUE id FROM rsf WHERE b = 1 AND 'x' INSIDE tags ORDER BY id;
SELECT VALUE id FROM rsf WITH NOINDEX WHERE b = 1 AND 'x' INSIDE tags ORDER BY id;
SELECT count() FROM rsf WHERE b = 1 AND 'x' INSIDE tags GROUP ALL;
SELECT count() FROM rsf WITH NOINDEX WHERE b = 1 AND 'x' INSIDE tags GROUP ALL;

-- D5: the empty containment in the non-leading position. It pins nothing, so
-- the prefix stops at `b`, the unpinned fanning column puts a `Distinct` over
-- the scan, and the always-false leaf is re-checked above it.
EXPLAIN SELECT VALUE id FROM rsf WHERE b = 1 AND tags CONTAINSANY [];
SELECT VALUE id FROM rsf WHERE b = 1 AND tags CONTAINSANY [] ORDER BY id;
SELECT VALUE id FROM rsf WITH NOINDEX WHERE b = 1 AND tags CONTAINSANY [] ORDER BY id;
SELECT count() FROM rsf WHERE b = 1 AND tags CONTAINSANY [] GROUP ALL;
SELECT count() FROM rsf WITH NOINDEX WHERE b = 1 AND tags CONTAINSANY [] GROUP ALL;

-- D6: the leading equality alone, under a LIMIT. The single leaf IS proved by
-- the access and stays on the scan as `predicate:`, but the unpinned fanning
-- column makes one record reachable through several entries, so a `Distinct`
-- collapses the scan and the limit counts what the `Distinct` emits rather
-- than what the scan reads. A proved predicate is therefore not on its own
-- enough to push a limit into the scan.
EXPLAIN SELECT VALUE id FROM rsf WHERE b = 1 LIMIT 2;
array::sort((SELECT VALUE id FROM rsf WHERE b = 1));
array::sort((SELECT VALUE id FROM rsf WITH NOINDEX WHERE b = 1));
SELECT count() FROM rsf WHERE b = 1 GROUP ALL;
SELECT count() FROM rsf WITH NOINDEX WHERE b = 1 GROUP ALL;

-- ===========================================================================
-- Group E: the mirror index, `FIELDS tags[*], b`. The same two leaves, the
-- same stripping and the same limit pushdown, from the other column order --
-- and the single-leaf queries, where the order is what decides whether an
-- index can be used at all. The same pair of out-of-range neighbours sits under
-- `b = 1` here, guarding both ends of the leading prefix.
-- ===========================================================================

{
    DEFINE TABLE OVERWRITE rsm SCHEMALESS;
    DEFINE INDEX OVERWRITE rsm_tags_b ON rsm FIELDS tags[*], b;
    CREATE rsm:1 SET b = 1, tags = ['x', 'y'], note = 'keep';
    CREATE rsm:2 SET b = 1, tags = ['x', 'z'], note = 'drop';
    CREATE rsm:3 SET b = 2, tags = ['x'], note = 'keep';
    CREATE rsm:4 SET b = 1, tags = ['w'], note = 'keep';
    CREATE rsm:5 SET b = 1, tags = ['y'], note = 'keep';
    RETURN NONE;
};

-- E1: both columns pinned from the mirror order. D1's assertion, restated
-- where the fanning column leads.
EXPLAIN SELECT VALUE id FROM rsm WHERE tags CONTAINS 'x' AND b = 1;
SELECT VALUE id FROM rsm WHERE tags CONTAINS 'x' AND b = 1 ORDER BY id;
SELECT VALUE id FROM rsm WITH NOINDEX WHERE tags CONTAINS 'x' AND b = 1 ORDER BY id;
SELECT count() FROM rsm WHERE tags CONTAINS 'x' AND b = 1 GROUP ALL;
SELECT count() FROM rsm WITH NOINDEX WHERE tags CONTAINS 'x' AND b = 1 GROUP ALL;

-- E2: E1 under a LIMIT -- D2 in the mirror order, so the pushdown depends on
-- the predicate being consumed and not on where the fanning column sits.
EXPLAIN SELECT VALUE id FROM rsm WHERE tags CONTAINS 'x' AND b = 1 LIMIT 2;
array::sort((SELECT VALUE id FROM rsm WHERE tags CONTAINS 'x' AND b = 1));
array::sort((SELECT VALUE id FROM rsm WITH NOINDEX WHERE tags CONTAINS 'x' AND b = 1));

-- E3: the containment alone. It pins the leading column, so the access is a
-- one-value prefix, the leaf is proved, and the pinned fanning column again
-- costs no `Distinct` even though the trailing scalar varies under it.
EXPLAIN SELECT VALUE id FROM rsm WHERE tags CONTAINS 'x';
SELECT VALUE id FROM rsm WHERE tags CONTAINS 'x' ORDER BY id;
SELECT VALUE id FROM rsm WITH NOINDEX WHERE tags CONTAINS 'x' ORDER BY id;
SELECT count() FROM rsm WHERE tags CONTAINS 'x' GROUP ALL;
SELECT count() FROM rsm WITH NOINDEX WHERE tags CONTAINS 'x' GROUP ALL;

-- E4: the equality on the trailing column alone. Nothing pins the leading
-- column, so this index offers no prefix and the statement falls back to a
-- table scan carrying the leaf -- the shape D6 answers from the index.
EXPLAIN SELECT VALUE id FROM rsm WHERE b = 1;
SELECT VALUE id FROM rsm WHERE b = 1 ORDER BY id;
SELECT VALUE id FROM rsm WITH NOINDEX WHERE b = 1 ORDER BY id;
SELECT count() FROM rsm WHERE b = 1 GROUP ALL;
SELECT count() FROM rsm WITH NOINDEX WHERE b = 1 GROUP ALL;

-- ===========================================================================
-- Group F: conjunctions only some of whose leaves are proved. The residual
-- must name exactly the survivors -- naming fewer returns extra rows, naming
-- more costs a re-check the access already settled.
-- ===========================================================================

-- F1: one proved leaf, two survivors. The `Filter` names both unproved leaves
-- and the scan, feeding a filter, carries no `predicate:` of its own. The seek
-- on `code = 'a'` reaches three records: rsc:5 fails `note` alone and rsc:6
-- fails `n` alone, so either survivor dropped on its own shows up as a row the
-- oracle excludes.
EXPLAIN SELECT VALUE id FROM rsc WHERE code = 'a' AND note = 'keep' AND n = 1;
SELECT VALUE id FROM rsc WHERE code = 'a' AND note = 'keep' AND n = 1 ORDER BY id;
SELECT VALUE id FROM rsc WITH NOINDEX WHERE code = 'a' AND note = 'keep' AND n = 1 ORDER BY id;

-- F2: a union of literals with one survivor beside it. The `IN` is proved by
-- the branches and stripped; the leaf on the uncovered column remains alone.
EXPLAIN SELECT VALUE id FROM rsc WHERE code IN ['a', 'c'] AND note = 'keep';
SELECT VALUE id FROM rsc WHERE code IN ['a', 'c'] AND note = 'keep' ORDER BY id;
SELECT VALUE id FROM rsc WITH NOINDEX WHERE code IN ['a', 'c'] AND note = 'keep' ORDER BY id;

-- F3: two leaves on the SAME indexed column, one of which the union proves.
-- The exclusion is not implied by membership in the union, so it survives
-- while the `IN` beside it does not -- stripping is decided per leaf, not per
-- column.
EXPLAIN SELECT VALUE id FROM rsc WHERE code IN ['a', 'c'] AND code != 'c';
SELECT VALUE id FROM rsc WHERE code IN ['a', 'c'] AND code != 'c' ORDER BY id;
SELECT VALUE id FROM rsc WITH NOINDEX WHERE code IN ['a', 'c'] AND code != 'c' ORDER BY id;
