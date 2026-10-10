/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
issue = 7460
reason = """
A set can be declared inside another field's type: as a member of an object
literal (`{ tags: set<string> }`) or of a tuple (`[set<int>, int]`), in any
alternative of a union of such types. It can also be reached through a record
link, however many links away, or projected into a view from a set field. An
index that flattens such a set holds one entry per stored set, keyed by the
whole set, so it cannot answer an element lookup. Containment over the set, in
every spelling and in an UPDATE's condition, returns the same records
`WITH NOINDEX` returns.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, meta: { tags: {'a', 'b'} } }]"

[[test.results]]
value = "[{ id: t:2, meta: { tags: {'b', 'c'} } }]"

[[test.results]]
value = "[t:1, t:2]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t:1, t:2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: u:1, pair: [{1, 2}, 0] }]"

[[test.results]]
value = "[{ id: u:2, pair: [{2, 3}, 0] }]"

[[test.results]]
value = "[u:1, u:2]"

[[test.results]]
value = "[u:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: o:1, tags: {'a', 'b'} }]"

[[test.results]]
value = "[{ id: o:2, tags: {'b', 'c'} }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: l:1, r: o:1 }]"

[[test.results]]
value = "[{ id: l:2, r: o:2 }]"

[[test.results]]
value = "[l:1, l:2]"

[[test.results]]
value = "[l:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: src:1, s: {1, 2, 3} }]"

[[test.results]]
value = "[{ id: src:2, s: {3, 4} }]"

[[test.results]]
value = "[v:1, v:2]"

[[test.results]]
value = "[v:2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[w:1, w:2]"

[[test.results]]
value = "[w:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[c0:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[va:1]"


[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[dv:1]"

[[test.results]]
value = "[dv:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
match = '''string::contains($result, "IndexScan") AND string::contains($result, "ix_da")'''
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[uh:1]"

[[test.results]]
value = "[uh:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[um:1]"

[[test.results]]
value = "[um:1]"
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[sf:1]"

[[test.results]]
value = "[sf:1]"

[[test.results]]
value = "[sf:1]"

[[test.results]]
value = "[sf:1]"

[[test.results]]
value = "[sf:1]"

[[test.results]]
value = "[sf:1]"

[[test.results]]
match = '''string::contains($result, "TableScan") AND !string::contains($result, "IndexScan")'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[ac:1]"

[[test.results]]
value = "[ac:1]"

[[test.results]]
match = '''string::contains($result, "IndexScan") AND string::contains($result, "table: ac")'''
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[vw:1]"

[[test.results]]
value = "[vw:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[pa:1]"

[[test.results]]
value = "[pa:1]"
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[pf:1]"

[[test.results]]
value = "[pf:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[pt:1]"

[[test.results]]
value = "[pt:1]"
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[ma:1]"

[[test.results]]
value = "[ma:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[lr:1]"

[[test.results]]
value = "[lr:1]"
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[vv8:1]"

[[test.results]]
value = "[vv8:1]"

[[test.results]]
value = "[vv8:1]"

[[test.results]]
value = "[vv8:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[om:1]"

[[test.results]]
value = "[om:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[on8:1]"

[[test.results]]
value = "[on8:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[lm8:1]"

[[test.results]]
value = "[lm8:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[vv9:1]"

[[test.results]]
value = "[vv9:1]"
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[drr:1]"

[[test.results]]
value = "[drr:1]"

[[test.results]]
value = "[drr:1]"

[[test.results]]
value = "[drr:1]"
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = '''string::contains($result, "IndexScan") AND string::contains($result, "table: sp")'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = '''string::contains($result, "TableScan") AND !string::contains($result, "IndexScan")'''
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[ms:1]"

[[test.results]]
value = "[ms:1]"

[[test.results]]
value = "[ms:1]"

[[test.results]]
value = "[ms:1]"
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[ostar:1]"

[[test.results]]
value = "[ostar:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[cv:1]"

[[test.results]]
value = "[cv:1]"
*/
-- A set declared as a member of an object literal type.
DEFINE TABLE t SCHEMALESS;
DEFINE FIELD meta ON t TYPE { tags: set<string> };
DEFINE INDEX ix ON t FIELDS meta.tags.*;
CREATE t:1 SET meta = { tags: <set>['a', 'b'] };
CREATE t:2 SET meta = { tags: <set>['b', 'c'] };
SELECT VALUE id FROM t WHERE meta.tags CONTAINS 'b' ORDER BY id;
SELECT VALUE id FROM t WHERE meta.tags.* CONTAINS 'a' ORDER BY id;
UPDATE t SET hit = true WHERE meta.tags.* CONTAINS 'b' RETURN NONE;
SELECT VALUE id FROM t WHERE hit = true ORDER BY id;

-- A set declared as a member of a tuple type.
DEFINE TABLE u SCHEMALESS;
DEFINE FIELD pair ON u TYPE [set<int>, int];
DEFINE INDEX ix ON u FIELDS pair[0].*;
CREATE u:1 SET pair = [<set>[1, 2], 0];
CREATE u:2 SET pair = [<set>[2, 3], 0];
SELECT VALUE id FROM u WHERE pair[0] CONTAINS 2 ORDER BY id;
SELECT VALUE id FROM u WHERE pair[0].* CONTAINS 1 ORDER BY id;

-- A set on the far side of a record link.
DEFINE TABLE o SCHEMALESS;
DEFINE FIELD tags ON o TYPE set<string>;
CREATE o:1 SET tags = <set>['a', 'b'];
CREATE o:2 SET tags = <set>['b', 'c'];
DEFINE TABLE l SCHEMALESS;
DEFINE FIELD r ON l TYPE record<o>;
DEFINE INDEX ix ON l FIELDS r.tags.*;
CREATE l:1 SET r = o:1;
CREATE l:2 SET r = o:2;
SELECT VALUE id FROM l WHERE r.tags CONTAINS 'b' ORDER BY id;
SELECT VALUE id FROM l WHERE r.tags.* CONTAINS 'a' ORDER BY id;

-- A set projected into a view from a set-typed field.
DEFINE TABLE src SCHEMALESS;
DEFINE FIELD s ON src TYPE set<int>;
DEFINE TABLE v AS SELECT s FROM src;
DEFINE INDEX ix ON v FIELDS s.*;
CREATE src:1 SET s = <set>[1, 2, 3];
CREATE src:2 SET s = <set>[3, 4];
SELECT VALUE id FROM v WHERE s CONTAINS 3 ORDER BY id;
SELECT VALUE id FROM v WHERE s.* CONTAINS 4 ORDER BY id;

-- A set declared by one alternative of a union of object literal types.
DEFINE TABLE w SCHEMALESS;
DEFINE FIELD meta ON w TYPE { tags: set<int> } | { tags: array<int>, k: int };
DEFINE INDEX ix ON w FIELDS meta.tags.*;
CREATE w:1 SET meta = { tags: <set>[1, 2] } RETURN NONE;
CREATE w:2 SET meta = { tags: [2, 3], k: 1 } RETURN NONE;
SELECT VALUE id FROM w WHERE meta.tags.* CONTAINS 2 ORDER BY id;
SELECT VALUE id FROM w WHERE meta.tags.* CONTAINS 1 ORDER BY id;

-- A set five record links away, past the links the planner follows.
DEFINE FIELD tags ON c5 TYPE set<int>;
CREATE c5:1 SET tags = <set>[1, 2] RETURN NONE;
DEFINE FIELD r ON c4 TYPE record<c5>;
CREATE c4:1 SET r = c5:1 RETURN NONE;
DEFINE FIELD r ON c3 TYPE record<c4>;
CREATE c3:1 SET r = c4:1 RETURN NONE;
DEFINE FIELD r ON c2 TYPE record<c3>;
CREATE c2:1 SET r = c3:1 RETURN NONE;
DEFINE FIELD r ON c1 TYPE record<c2>;
CREATE c1:1 SET r = c2:1 RETURN NONE;
DEFINE FIELD r ON c0 TYPE record<c1>;
DEFINE INDEX ix ON c0 FIELDS r.r.r.r.r.tags.*;
CREATE c0:1 SET r = c1:1 RETURN NONE;
SELECT VALUE id FROM c0 WHERE r.r.r.r.r.tags.* CONTAINS 1;

-- A view field projected under an alias next to `*`, through a record link.
DEFINE TABLE author SCHEMALESS;
DEFINE FIELD tags ON author TYPE set<string>;
CREATE author:1 SET tags = <set>['a', 'b'] RETURN NONE;
DEFINE TABLE article SCHEMALESS;
DEFINE FIELD author ON article TYPE record<author>;
DEFINE TABLE va AS SELECT *, author.tags AS my_tags FROM article;
DEFINE INDEX ix ON va FIELDS my_tags.*;
CREATE article:1 SET author = author:1 RETURN NONE;
SELECT VALUE id FROM va WHERE my_tags.* CONTAINS 'a';

-- A view naming the same field twice holds the last projection: here the set.
DEFINE TABLE dsrc SCHEMALESS;
DEFINE FIELD arr ON dsrc TYPE array<int>;
DEFINE FIELD tags ON dsrc TYPE set<int>;
CREATE dsrc:1 SET arr = [9], tags = <set>[1, 2] RETURN NONE;
DEFINE TABLE dv AS SELECT arr AS x, tags AS x FROM dsrc;
DEFINE INDEX ix ON dv FIELDS x.*;
DEFINE TABLE da AS SELECT tags AS x, arr AS x FROM dsrc;
SELECT VALUE id FROM dv WHERE x.* CONTAINS 1;
SELECT VALUE id FROM dv WITH NOINDEX WHERE x.* CONTAINS 1;
-- When the array is projected last, the view holds the array and its index serves containment.
DEFINE INDEX ix_da ON da FIELDS x.*;
SELECT VALUE id FROM da WHERE x.* CONTAINS 1;
SELECT VALUE id FROM da WITH NOINDEX WHERE x.* CONTAINS 1;
EXPLAIN SELECT VALUE id FROM da WHERE x.* CONTAINS 1;

-- A link declared without a table may reach a table whose field is a set.
DEFINE TABLE ut SCHEMALESS;
DEFINE FIELD tags ON ut TYPE set<int>;
CREATE ut:1 SET tags = <set>[1, 2] RETURN NONE;
DEFINE TABLE uh SCHEMALESS;
DEFINE FIELD r ON uh TYPE record;
DEFINE INDEX ix ON uh FIELDS r.tags.*;
CREATE uh:1 SET r = ut:1 RETURN NONE;
SELECT VALUE id FROM uh WHERE r.tags.* CONTAINS 1;
SELECT VALUE id FROM uh WITH NOINDEX WHERE r.tags.* CONTAINS 1;

-- A link declared in each alternative of a union of object literals is followed through every alternative.
DEFINE TABLE uo SCHEMALESS;
DEFINE FIELD tags ON uo TYPE set<int>;
CREATE uo:1 SET tags = <set>[1, 2] RETURN NONE;
DEFINE TABLE up SCHEMALESS;
DEFINE FIELD tags ON up TYPE array<int>;
DEFINE TABLE um SCHEMALESS;
DEFINE FIELD meta ON um TYPE { r: record<uo> } | { r: record<up> };
DEFINE INDEX ix ON um FIELDS meta.r.tags.*;
CREATE um:1 SET meta = { r: uo:1 } RETURN NONE;
SELECT VALUE id FROM um WHERE meta.r.tags.* CONTAINS 1;
SELECT VALUE id FROM um WITH NOINDEX WHERE meta.r.tags.* CONTAINS 1;

-- A field read through a set maps over its elements and yields a set, so every spelling reads the documents.
DEFINE TABLE sf SCHEMALESS;
DEFINE FIELD s ON sf TYPE set<{ x: int }>;
DEFINE INDEX ix ON sf FIELDS s.x.*;
CREATE sf:1 SET s = <set>[{ x: 1 }, { x: 2 }] RETURN NONE;
SELECT VALUE id FROM sf WHERE s.x CONTAINS 1;
SELECT VALUE id FROM sf WITH NOINDEX WHERE s.x CONTAINS 1;
SELECT VALUE id FROM sf WHERE s.x.* CONTAINS 1;
SELECT VALUE id FROM sf WITH NOINDEX WHERE s.x.* CONTAINS 1;
SELECT VALUE id FROM sf WHERE s.*.x CONTAINS 1;
SELECT VALUE id FROM sf WITH NOINDEX WHERE s.*.x CONTAINS 1;
EXPLAIN SELECT VALUE id FROM sf WHERE s.x CONTAINS 1;

-- The same field read through an array of objects yields an array, and its index keeps serving containment.
DEFINE TABLE ac SCHEMALESS;
DEFINE FIELD arr ON ac TYPE array<{ x: int }>;
DEFINE INDEX ix ON ac FIELDS arr.x.*;
CREATE ac:1 SET arr = [{ x: 1 }, { x: 2 }] RETURN NONE;
SELECT VALUE id FROM ac WHERE arr.x.* CONTAINS 1;
SELECT VALUE id FROM ac WITH NOINDEX WHERE arr.x.* CONTAINS 1;
EXPLAIN SELECT VALUE id FROM ac WHERE arr.x.* CONTAINS 1;

-- A field the view declares for itself does not stop the set being found on the source table.
DEFINE TABLE vsrc SCHEMALESS;
DEFINE FIELD s ON vsrc TYPE set<int>;
CREATE vsrc:1 SET s = <set>[1, 2] RETURN NONE;
DEFINE TABLE vw AS SELECT s FROM vsrc;
DEFINE FIELD s ON vw TYPE any;
DEFINE INDEX ix ON vw FIELDS s.*;
SELECT VALUE id FROM vw WHERE s.* CONTAINS 1;
SELECT VALUE id FROM vw WITH NOINDEX WHERE s.* CONTAINS 1;

-- A position into an array reads its element type.
DEFINE TABLE pa SCHEMALESS;
DEFINE FIELD items ON pa TYPE array<set<int>>;
DEFINE INDEX ix ON pa FIELDS items[0].*;
CREATE pa:1 SET items = [<set>[1, 2]] RETURN NONE;
SELECT VALUE id FROM pa WHERE items[0].* CONTAINS 1;
SELECT VALUE id FROM pa WITH NOINDEX WHERE items[0].* CONTAINS 1;

-- A position given as a non-integer number reads an element too, in an array and in a tuple.
DEFINE TABLE pf SCHEMALESS;
DEFINE FIELD items ON pf TYPE array<set<int>>;
DEFINE INDEX ix ON pf FIELDS items[0.0].*;
CREATE pf:1 SET items = [<set>[1, 2]] RETURN NONE;
SELECT VALUE id FROM pf WHERE items[0.0].* CONTAINS 1;
SELECT VALUE id FROM pf WITH NOINDEX WHERE items[0.0].* CONTAINS 1;
DEFINE TABLE pt SCHEMALESS;
DEFINE FIELD pair ON pt TYPE [set<int>, int];
DEFINE INDEX ix ON pt FIELDS pair[0.0].*;
CREATE pt:1 SET pair = [<set>[1, 2], 7] RETURN NONE;
SELECT VALUE id FROM pt WHERE pair[0.0].* CONTAINS 1;
SELECT VALUE id FROM pt WITH NOINDEX WHERE pair[0.0].* CONTAINS 1;

-- A field's own `TYPE any` does not override its parent declaring a set at the path.
DEFINE TABLE ma SCHEMALESS;
DEFINE FIELD meta ON ma TYPE { tags: set<int> };
DEFINE FIELD meta.tags ON ma TYPE any;
DEFINE INDEX ix ON ma FIELDS meta.tags.*;
CREATE ma:1 SET meta = { tags: <set>[1, 2] } RETURN NONE;
SELECT VALUE id FROM ma WHERE meta.tags.* CONTAINS 1;
SELECT VALUE id FROM ma WITH NOINDEX WHERE meta.tags.* CONTAINS 1;

-- Nor does it stop a record link being followed to the linked table's set.
DEFINE TABLE lo SCHEMALESS;
DEFINE FIELD tags ON lo TYPE set<int>;
CREATE lo:1 SET tags = <set>[1, 2] RETURN NONE;
DEFINE TABLE lr SCHEMALESS;
DEFINE FIELD r ON lr TYPE record<lo>;
DEFINE FIELD r.tags ON lr TYPE any;
DEFINE INDEX ix ON lr FIELDS r.tags.*;
CREATE lr:1 SET r = lo:1 RETURN NONE;
SELECT VALUE id FROM lr WHERE r.tags.* CONTAINS 1;
SELECT VALUE id FROM lr WITH NOINDEX WHERE r.tags.* CONTAINS 1;

-- An unaliased `tags.*` projection is stored under `tags`, and is traced to its source set under that name.
DEFINE TABLE vs8 SCHEMALESS;
DEFINE FIELD tags ON vs8 TYPE set<int>;
CREATE vs8:1 SET tags = <set>[1, 2] RETURN NONE;
DEFINE TABLE vv8 AS SELECT tags.* FROM vs8;
DEFINE INDEX ix ON vv8 FIELDS tags.*;
SELECT VALUE id FROM vv8 WHERE tags CONTAINS 1;
SELECT VALUE id FROM vv8 WITH NOINDEX WHERE tags CONTAINS 1;
SELECT VALUE id FROM vv8 WHERE tags.* CONTAINS 1;
SELECT VALUE id FROM vv8 WITH NOINDEX WHERE tags.* CONTAINS 1;

-- An optional part reads the value it follows, inside a declared type and in a field's name.
DEFINE TABLE om SCHEMALESS;
DEFINE FIELD meta ON om TYPE { tags: set<int> };
DEFINE INDEX ix ON om FIELDS meta.?.tags.*;
CREATE om:1 SET meta = { tags: <set>[1, 2] } RETURN NONE;
SELECT VALUE id FROM om WHERE meta.?.tags CONTAINS 1;
SELECT VALUE id FROM om WITH NOINDEX WHERE meta.?.tags CONTAINS 1;
DEFINE TABLE on8 SCHEMALESS;
DEFINE FIELD meta ON on8 TYPE object;
DEFINE FIELD meta.tags ON on8 TYPE set<int>;
DEFINE INDEX ix ON on8 FIELDS meta.?.tags.*;
CREATE on8:1 SET meta = { tags: <set>[1, 2] } RETURN NONE;
SELECT VALUE id FROM on8 WHERE meta.?.tags CONTAINS 1;
SELECT VALUE id FROM on8 WITH NOINDEX WHERE meta.?.tags CONTAINS 1;

-- Every declared ancestor's record links are followed, not only the longest one's.
DEFINE TABLE lc8 SCHEMALESS;
DEFINE FIELD tags ON lc8 TYPE set<int>;
CREATE lc8:1 SET tags = <set>[1, 2] RETURN NONE;
DEFINE TABLE lb8 SCHEMALESS;
DEFINE TABLE la8 SCHEMALESS;
DEFINE FIELD child ON la8 TYPE record<lc8>;
CREATE la8:1 SET child = lc8:1 RETURN NONE;
DEFINE TABLE lm8 SCHEMALESS;
DEFINE FIELD r.child ON lm8 TYPE option<record<lb8>>;
DEFINE FIELD r ON lm8 TYPE record<la8>;
DEFINE INDEX ix ON lm8 FIELDS r.child.tags.*;
CREATE lm8:1 SET r = la8:1 RETURN NONE;
SELECT VALUE id FROM lm8 WHERE r.child.tags CONTAINS 1;
SELECT VALUE id FROM lm8 WITH NOINDEX WHERE r.child.tags CONTAINS 1;

-- An optional part in a view's indexed path is matched against the name a projection gives the field.
DEFINE TABLE vs9 SCHEMALESS;
DEFINE FIELD meta ON vs9 TYPE { tags: set<int> };
CREATE vs9:1 SET meta = { tags: <set>[1, 2] } RETURN NONE;
DEFINE TABLE vv9 AS SELECT meta.tags FROM vs9;
DEFINE INDEX ix ON vv9 FIELDS meta.?.tags.*;
SELECT VALUE id FROM vv9 WHERE meta.?.tags CONTAINS 1;
SELECT VALUE id FROM vv9 WITH NOINDEX WHERE meta.?.tags CONTAINS 1;

-- `.*` on a record link fetches the record, so the path after it is read on the linked table.
DEFINE TABLE dro SCHEMALESS;
DEFINE FIELD tags ON dro TYPE set<int>;
CREATE dro:1 SET tags = <set>[1, 2] RETURN NONE;
DEFINE TABLE drr SCHEMALESS;
DEFINE FIELD r ON drr TYPE record<dro>;
DEFINE INDEX ix ON drr FIELDS r.*.tags.*;
CREATE drr:1 SET r = dro:1 RETURN NONE;
SELECT VALUE id FROM drr WHERE r.*.tags.* CONTAINS 1;
SELECT VALUE id FROM drr WITH NOINDEX WHERE r.*.tags.* CONTAINS 1;
SELECT VALUE id FROM drr WHERE r.*.tags CONTAINS 1;
SELECT VALUE id FROM drr WITH NOINDEX WHERE r.*.tags CONTAINS 1;

-- A position on a set selects one element: over a set of arrays the flattened column holds that array's elements and keeps its seek; over a set of sets the element is itself a set and the seek is withheld.
DEFINE TABLE sp SCHEMALESS;
DEFINE FIELD s ON sp TYPE set<array<int>>;
DEFINE INDEX ix ON sp FIELDS s[$].*;
EXPLAIN SELECT VALUE id FROM sp WHERE s[$].* CONTAINS 3;
DEFINE TABLE sq SCHEMALESS;
DEFINE FIELD s ON sq TYPE set<set<int>>;
DEFINE INDEX ix ON sq FIELDS s[$].*;
EXPLAIN SELECT VALUE id FROM sq WHERE s[$].* CONTAINS 3;

-- A computed step such as a method can produce a set whatever the field declares.
DEFINE TABLE ms SCHEMALESS;
DEFINE FIELD a ON ms TYPE any;
DEFINE INDEX ix ON ms FIELDS a.to_set().*;
CREATE ms:1 SET a = [1, 2] RETURN NONE;
SELECT VALUE id FROM ms WHERE a.to_set() CONTAINS 1;
SELECT VALUE id FROM ms WITH NOINDEX WHERE a.to_set() CONTAINS 1;
SELECT VALUE id FROM ms WHERE a.to_set().* CONTAINS 1;
SELECT VALUE id FROM ms WITH NOINDEX WHERE a.to_set().* CONTAINS 1;

-- `.*` leaves an object unchanged, so the path after it is read on the object itself.
DEFINE TABLE ostar SCHEMALESS;
DEFINE FIELD meta ON ostar TYPE { nested: { tags: set<int> } };
DEFINE INDEX ix ON ostar FIELDS meta.*.nested.tags.*;
CREATE ostar:1 SET meta = { nested: { tags: <set>[1, 2] } } RETURN NONE;
SELECT VALUE id FROM ostar WHERE meta.*.nested.tags.* CONTAINS 1;
SELECT VALUE id FROM ostar WITH NOINDEX WHERE meta.*.nested.tags.* CONTAINS 1;

-- A view field a computed projection names is not traced to a source field, so it may hold a set.
DEFINE TABLE cs SCHEMALESS;
CREATE cs:1 SET n = 1 RETURN NONE;
DEFINE TABLE cv AS SELECT <set>[1, 2] AS tags FROM cs;
DEFINE INDEX ix ON cv FIELDS tags.*;
SELECT VALUE id FROM cv WHERE tags.* CONTAINS 1;
SELECT VALUE id FROM cv WITH NOINDEX WHERE tags.* CONTAINS 1;
