/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "`tags CONTAINS v` through an index on `tags.*` must not miss a stored scalar that satisfies it (a string holding `v`, a range spanning it, a geometry containing it), matching NOINDEX. A declared array kind is not proof that no scalar is stored. A flattening query path (`tags.* CONTAINS v`) seeks the index for `v` alone; the plain path seeks `v`, `NONE` and `NULL`, under which the column lists a stored scalar, with the containment judged again over every record, and reads the table once the index has stored a value whole. The flattening spelling still keeps a stored object whole and follows a stored record link, so section (6) pins the rows the indexed read misses for them as a known gap."
issue = 1375

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: c:flat, tags: ['x', 'y'] }]"

[[test.results]]
value = "[{ id: c:scalar, tags: 'x' }]"

[[test.results]]
value = "[{ id: c:longer, tags: 'xyz' }]"

[[test.results]]
value = "[{ id: c:other, tags: ['z'] }]"

[[test.results]]
value = "[c:flat, c:longer, c:scalar]"

[[test.results]]
value = "[c:flat, c:longer, c:scalar]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[c:flat, c:longer, c:scalar]"

[[test.results]]
value = "[c:flat, c:longer, c:scalar]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: tags CONTAINS 'x']
        UnionIndexScan [ctx: Db] [table: c, branches: 3]
            IndexScan [ctx: Db] [table: c, index: by_tag, access: = 'x', direction: Forward]
            IndexScan [ctx: Db] [table: c, index: by_tag, access: = NONE, direction: Forward]
            IndexScan [ctx: Db] [table: c, index: by_tag, access: = NULL, direction: Forward]
"'''

[[test.results]]
value = "[c:flat]"

[[test.results]]
value = "[c:flat]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: c, index: by_tag, access: = 'x', direction: Forward]
"'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: ct:flat, tags: ['x', 'y'] }]"

[[test.results]]
value = "[{ id: ct:other, tags: ['z'] }]"

[[test.results]]
value = "[ct:flat]"

[[test.results]]
value = "[ct:flat]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: tags CONTAINS 'x']
        UnionIndexScan [ctx: Db] [table: ct, branches: 3]
            IndexScan [ctx: Db] [table: ct, index: by_tag, access: = 'x', direction: Forward]
            IndexScan [ctx: Db] [table: ct, index: by_tag, access: = NONE, direction: Forward]
            IndexScan [ctx: Db] [table: ct, index: by_tag, access: = NULL, direction: Forward]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: ct, index: by_tag, access: = 'x', direction: Forward]
"'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: cp:scalar, tags: 'x' }]"

[[test.results]]
value = "[{ id: cp:flat, tags: ['x', 'y'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[cp:flat, cp:scalar]"

[[test.results]]
value = "[cp:flat, cp:scalar]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: cr:range, tags: 1..5 }]"

[[test.results]]
value = "[{ id: cr:arr, tags: [3] }]"

[[test.results]]
value = "[cr:arr, cr:range]"

[[test.results]]
value = "[cr:arr, cr:range]"

[[test.results]]
value = "[cr:arr, cr:range]"

[[test.results]]
value = "[cr:arr, cr:range]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: cg:point, tags: (1f, 2f) }]"

[[test.results]]
value = "[cg:point]"

[[test.results]]
value = "[cg:point]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: o:arr, tags: ['x'] }]"

[[test.results]]
value = "[{ id: o:obj, tags: { x: 1 } }]"

[[test.results]]
value = "[{ id: o:set, tags: {'x',} }]"

[[test.results]]
value = "[{ id: ol:1, x: 1 }]"

[[test.results]]
value = "[{ id: o:link, tags: ol:1 }]"

[[test.results]]
value = "[o:arr]"

[[test.results]]
value = "[o:arr, o:link, o:obj, o:set]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: cf:scalar, tags: 'xyz' }]"

[[test.results]]
value = "[{ id: cf:flat, tags: ['x'] }]"

[[test.results]]
value = "[cf:flat, cf:scalar]"

[[test.results]]
value = "[cf:flat, cf:scalar]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: tags… CONTAINS 'x']
        TableScan [ctx: Db] [table: cf, direction: Forward]
"'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[tc:1, tc:2]"

[[test.results]]
value = "[tc:1, tc:2]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: status = 'active' AND tags CONTAINS 'x']
        DistinctRecords [ctx: Db]
            IndexScan [ctx: Db] [table: tc, index: ix, access: ['active'], direction: Forward]
"'''

[[test.results]]
value = "[tc:1]"

[[test.results]]
value = "[tc:1]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: tc, index: ix, access: ['active', 'x'], direction: Forward]
"'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[tm:1]"

[[test.results]]
value = "[tm:1]"

[[test.results]]
value = "[tm:1, tm:2]"

[[test.results]]
value = "[tm:1, tm:2]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: tags.* CONTAINSANY ['x', 'y']]
        UnionIndexScan [ctx: Db] [table: tm, branches: 2]
            IndexScan [ctx: Db] [table: tm, index: ix, access: = 'x', direction: Forward]
            IndexScan [ctx: Db] [table: tm, index: ix, access: = 'y', direction: Forward]
"'''

[[test.results]]
value = "[tm:1]"

[[test.results]]
value = "[tm:1]"

[[test.results]]
value = "[tm:1, tm:2]"

[[test.results]]
value = "[tm:1, tm:2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[wa:1, wa:2]"

[[test.results]]
value = "[wa:1, wa:2]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: wa, index: ix, access: = 'x', direction: Forward]
"'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[wm:1]"

[[test.results]]
value = "[wm:1]"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: wm, index: ix, access: = ['x'], direction: Forward]
"'''
*/

-- (1) Schemaless table with wildcard index: string scalar records must not be missed
DEFINE TABLE c SCHEMALESS;
DEFINE INDEX by_tag ON c FIELDS tags.*;
CREATE c:flat SET tags = ['x', 'y'];
CREATE c:scalar SET tags = 'x';
CREATE c:longer SET tags = 'xyz';
CREATE c:other SET tags = ['z'];

-- Rows must agree with NOINDEX (all three matching records returned)
SELECT VALUE id FROM c WHERE tags CONTAINS 'x' ORDER BY id;
SELECT VALUE id FROM c WITH NOINDEX WHERE tags CONTAINS 'x' ORDER BY id;

-- Count must agree with NOINDEX
SELECT count() FROM c WHERE tags CONTAINS 'x' GROUP ALL;
SELECT count() FROM c WITH NOINDEX WHERE tags CONTAINS 'x' GROUP ALL;

-- Reverse spelling 'x' INSIDE tags
SELECT VALUE id FROM c WHERE 'x' INSIDE tags ORDER BY id;
SELECT VALUE id FROM c WITH NOINDEX WHERE 'x' INSIDE tags ORDER BY id;

-- tags CONTAINS 'x' seeks 'x', NONE and NULL, and filters
EXPLAIN SELECT VALUE id FROM c WHERE tags CONTAINS 'x';

-- A flattened query path excludes non-array scalars, so it uses the index
SELECT VALUE id FROM c WHERE tags.* CONTAINS 'x' ORDER BY id;
SELECT VALUE id FROM c WITH NOINDEX WHERE tags.* CONTAINS 'x' ORDER BY id;
EXPLAIN SELECT VALUE id FROM c WHERE tags.* CONTAINS 'x';

-- (2) Typed array column: the declared kind is not trusted, so the seek still
-- reads the nullish keys and filters
DEFINE TABLE ct SCHEMALESS;
DEFINE FIELD tags ON ct TYPE array<string>;
DEFINE INDEX by_tag ON ct FIELDS tags.*;
CREATE ct:flat SET tags = ['x', 'y'];
CREATE ct:other SET tags = ['z'];

SELECT VALUE id FROM ct WHERE tags CONTAINS 'x' ORDER BY id;
SELECT VALUE id FROM ct WITH NOINDEX WHERE tags CONTAINS 'x' ORDER BY id;
EXPLAIN SELECT VALUE id FROM ct WHERE tags CONTAINS 'x';
EXPLAIN SELECT VALUE id FROM ct WHERE tags.* CONTAINS 'x';

-- (3) A string stored before the field is declared an array stays a string
DEFINE TABLE cp SCHEMALESS;
CREATE cp:scalar SET tags = 'x';
CREATE cp:flat SET tags = ['x', 'y'];
DEFINE FIELD tags ON cp TYPE array<string>;
DEFINE INDEX by_tag ON cp FIELDS tags.*;

SELECT VALUE id FROM cp WHERE tags CONTAINS 'x' ORDER BY id;
SELECT VALUE id FROM cp WITH NOINDEX WHERE tags CONTAINS 'x' ORDER BY id;

-- (4) A stored range spans a number without holding it as an element
DEFINE TABLE cr SCHEMALESS;
DEFINE INDEX by_tag ON cr FIELDS tags.*;
CREATE cr:range SET tags = 1..5;
CREATE cr:arr SET tags = [3];

SELECT VALUE id FROM cr WHERE tags CONTAINS 3 ORDER BY id;
SELECT VALUE id FROM cr WITH NOINDEX WHERE tags CONTAINS 3 ORDER BY id;
SELECT VALUE id FROM cr WHERE 3 INSIDE tags ORDER BY id;
SELECT VALUE id FROM cr WITH NOINDEX WHERE 3 INSIDE tags ORDER BY id;

-- (5) A stored geometry contains a geometry without holding it as an element
DEFINE TABLE cg SCHEMALESS;
DEFINE INDEX by_tag ON cg FIELDS tags.*;
CREATE cg:point SET tags = (1.0, 2.0);

SELECT VALUE id FROM cg WHERE tags CONTAINS (1.0, 2.0) ORDER BY id;
SELECT VALUE id FROM cg WITH NOINDEX WHERE tags CONTAINS (1.0, 2.0) ORDER BY id;

-- (6) The flattened spelling maps a stored scalar to NONE, but reads a set per
-- element, keeps an object whole and follows a record link, and each of those
-- matches 'x' with no element entry for it in an index over a field not
-- declared a set. The indexed read misses o:obj, o:link and o:set that the scan
-- returns; these two rows pin that known gap until the index records which
-- columns it stored a value whole in, which rules such values out.
DEFINE TABLE o SCHEMALESS;
DEFINE INDEX by_tag ON o FIELDS tags.*;
CREATE o:arr SET tags = ['x'];
CREATE o:obj SET tags = { x: 1 };
CREATE o:set SET tags = <set> ['x'];
CREATE ol:1 SET x = 1;
CREATE o:link SET tags = ol:1;

SELECT VALUE id FROM o WHERE tags.* CONTAINS 'x' ORDER BY id;
SELECT VALUE id FROM o WITH NOINDEX WHERE tags.* CONTAINS 'x' ORDER BY id;

-- (7) The flatten step (`tags...`) returns a stored scalar unchanged, so it
-- can still match by substring and does not seek the element index.
DEFINE TABLE cf SCHEMALESS;
DEFINE INDEX by_tag ON cf FIELDS tags.*...;
CREATE cf:scalar SET tags = 'xyz';
CREATE cf:flat SET tags = ['x'];

SELECT VALUE id FROM cf WHERE tags... CONTAINS 'x' ORDER BY id;
SELECT VALUE id FROM cf WITH NOINDEX WHERE tags... CONTAINS 'x' ORDER BY id;
EXPLAIN SELECT VALUE id FROM cf WHERE tags... CONTAINS 'x';

-- (8) A compound index: the plain spelling seeks the leading `status` prefix and
-- keeps the containment as a residual; the flattened spelling extends the
-- prefix with the element and leaves nothing to filter.
DEFINE TABLE tc SCHEMALESS;
DEFINE INDEX ix ON tc FIELDS status, tags.*;
CREATE tc:1 SET status = 'active', tags = ['x', 'y'] RETURN NONE;
CREATE tc:2 SET status = 'active', tags = 'xyz' RETURN NONE;
CREATE tc:3 SET status = 'idle', tags = ['x'] RETURN NONE;
SELECT VALUE id FROM tc WHERE status = 'active' AND tags CONTAINS 'x' ORDER BY id;
SELECT VALUE id FROM tc WITH NOINDEX WHERE status = 'active' AND tags CONTAINS 'x' ORDER BY id;
EXPLAIN SELECT VALUE id FROM tc WHERE status = 'active' AND tags CONTAINS 'x';
SELECT VALUE id FROM tc WHERE status = 'active' AND tags.* CONTAINS 'x' ORDER BY id;
SELECT VALUE id FROM tc WITH NOINDEX WHERE status = 'active' AND tags.* CONTAINS 'x' ORDER BY id;
EXPLAIN SELECT VALUE id FROM tc WHERE status = 'active' AND tags.* CONTAINS 'x';

-- (9) The multi-value forms over the flattened spelling expand to element seeks
-- and agree with the scan.
DEFINE TABLE tm SCHEMALESS;
DEFINE INDEX ix ON tm FIELDS tags.*;
CREATE tm:1 SET tags = ['x', 'y'] RETURN NONE;
CREATE tm:2 SET tags = ['x'] RETURN NONE;
CREATE tm:3 SET tags = 'xy' RETURN NONE;
SELECT VALUE id FROM tm WHERE tags.* CONTAINSALL ['x', 'y'] ORDER BY id;
SELECT VALUE id FROM tm WITH NOINDEX WHERE tags.* CONTAINSALL ['x', 'y'] ORDER BY id;
SELECT VALUE id FROM tm WHERE tags.* CONTAINSANY ['x', 'y'] ORDER BY id;
SELECT VALUE id FROM tm WITH NOINDEX WHERE tags.* CONTAINSANY ['x', 'y'] ORDER BY id;
EXPLAIN SELECT VALUE id FROM tm WHERE tags.* CONTAINSANY ['x', 'y'];
SELECT VALUE id FROM tm WHERE ['x', 'y'] ALLINSIDE tags.* ORDER BY id;
SELECT VALUE id FROM tm WITH NOINDEX WHERE ['x', 'y'] ALLINSIDE tags.* ORDER BY id;
SELECT VALUE id FROM tm WHERE ['x', 'y'] ANYINSIDE tags.* ORDER BY id;
SELECT VALUE id FROM tm WITH NOINDEX WHERE ['x', 'y'] ANYINSIDE tags.* ORDER BY id;

-- (10) A path may place its wildcards differently from the column and still
-- read the same values: a field read maps over an array, so `a.b.*` reaches
-- the `b` values an `a.*.b` column holds, and `.*` on an array yields its
-- elements, so an `m.*.*` column holds the inner arrays `m.*` contains. Both
-- seek the index and agree with the scan.
DEFINE TABLE wa SCHEMALESS;
DEFINE INDEX ix ON wa FIELDS a.*.b;
CREATE wa:1 SET a = { b: ['x'] } RETURN NONE;
CREATE wa:2 SET a = [{ b: 'x' }] RETURN NONE;
SELECT VALUE id FROM wa WHERE a.b.* CONTAINS 'x' ORDER BY id;
SELECT VALUE id FROM wa WITH NOINDEX WHERE a.b.* CONTAINS 'x' ORDER BY id;
EXPLAIN SELECT VALUE id FROM wa WHERE a.b.* CONTAINS 'x';
DEFINE TABLE wm SCHEMALESS;
DEFINE INDEX ix ON wm FIELDS m.*.*;
CREATE wm:1 SET m = [['x'], ['y']] RETURN NONE;
SELECT VALUE id FROM wm WHERE m.* CONTAINS ['x'] ORDER BY id;
SELECT VALUE id FROM wm WITH NOINDEX WHERE m.* CONTAINS ['x'] ORDER BY id;
EXPLAIN SELECT VALUE id FROM wm WHERE m.* CONTAINS ['x'];
