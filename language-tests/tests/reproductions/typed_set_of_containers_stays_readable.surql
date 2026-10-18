/**
[test]
reason = """
A field typed as a set whose elements are themselves arrays, sets or objects
carries nested definitions below it — `s[*]`, `s[*][*]` or `s[*].x`, implicit
or declared. Applying one rewrites the set's elements in place, and the field
kernel then rebuilds every set holding them, innermost first. Each record must
store the set it was given — not a set of copies of it — and stay readable
through a table scan, an index scan and a later update. A clause that makes two
elements equal collapses them, at every level that holds them.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: sa:1, k: 1, s: {[1, 2], [3]} }]"

[[test.results]]
value = "[{ id: sa:2, k: 2, s: {[1, 2],} }]"

[[test.results]]
value = "[{ s: {[1, 2], [3]} }, { s: {[1, 2],} }]"

[[test.results]]
value = "[{[1, 2], [3]}]"

[[test.results]]
value = "[{ id: sa:1, k: 1, s: {[1, 2], [3], [4]} }]"

[[test.results]]
value = "[{[1, 2], [3], [4]}]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: ss:1, s: {{1, 2}, {3,}} }]"

[[test.results]]
value = "[{ s: {{1, 2}, {3,}} }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: so:1, s: {{ a: 1 }, { b: 2 }} }]"

[[test.results]]
value = "[{ s: {{ a: 1 }, { b: 2 }} }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: no:1, o: { s: {[1, 2], [3]} } }]"

[[test.results]]
value = "[{ o: { s: {[1, 2], [3]} } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: sv:1, s: {[0, 0],} }]"

[[test.results]]
value = "[{ s: {[0, 0],} }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: sw:1, s: {{0,},} }]"

[[test.results]]
value = "[{ s: {{0,},} }]"
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: sx:1, s: {{ x: 0 },} }]"

[[test.results]]
value = "[{ s: {{ x: 0 },} }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: sl:1, s: {[1], [2], [3], [4], [5], [6], [7], [8], [9]} }]"

[[test.results]]
value = "{[1], [2], [3], [4], [5], [6], [7], [8], [9]}"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: sf:1, s: {{ a: 1 }, { a: 2 }} }]"

[[test.results]]
value = "{{ a: 1 }, { a: 2 }}"

[[test.results]]
value = "[{[1, 2], [3], [4]}]"

[[test.results]]
value = "[{[1, 2], [3], [4]}]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: [{0,}, {0,}], id: aset:1 }]"

[[test.results]]
value = "[{0,}, {0,}]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: sy:1, s: {{ x: 1 }, { x: 2 }} }]"

[[test.results]]
value = "[{{ x: 1 }, { x: 2 }}]"

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
value = "[{ id: sz:1, s: {{ x: 1, y: 20, z: 200 }, { x: 2, y: 10, z: 100 }} }]"

[[test.results]]
value = "[{{ x: 1, y: 10, z: 100 }, { x: 2, y: 20, z: 200 }}]"

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
value = "[{ id: sq:1, s: {{ x: 0 },}, u: 1, z: true }]"

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
value = "{ s: {{ x: 0 },}, zc: 1, zz: 1 }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found { x: 2 } for field `s.*`, with record `sr:1`, but field must conform to: $value.x = 1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: st:1, s: {{ x: 2 },} }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{5, 10, 20, 30, 40}]"
*/

-- set<array<int>>: implicit `s[*]` (array<int>) and `s[*][*]` (int).
DEFINE TABLE sa SCHEMALESS;
DEFINE FIELD s ON sa TYPE set<array<int>>;
DEFINE FIELD k ON sa TYPE int;
DEFINE INDEX sa_k ON sa FIELDS k;
CREATE sa:1 SET k = 1, s = <set>[[1, 2], [3]] RETURN AFTER;
CREATE sa:2 SET k = 2, s = <set>[[1, 2]] RETURN AFTER;
SELECT s FROM sa ORDER BY id;
SELECT VALUE s FROM sa WHERE k = 1;
UPDATE sa:1 SET s = <set>[[4], [1, 2], [3]] RETURN AFTER;
SELECT VALUE s FROM sa WHERE k = 1;

-- set<set<int>>.
DEFINE TABLE ss SCHEMALESS;
DEFINE FIELD s ON ss TYPE set<set<int>>;
CREATE ss:1 SET s = <set>[<set>[1, 2], <set>[3]] RETURN AFTER;
SELECT s FROM ss;

-- set<object> with a declared `s[*].*`.
DEFINE TABLE so SCHEMALESS;
DEFINE FIELD s ON so TYPE set<object>;
DEFINE FIELD s[*].* ON so TYPE int;
CREATE so:1 SET s = <set>[{ a: 1 }, { b: 2 }] RETURN AFTER;
SELECT s FROM so;

-- An optional set<array<int>> nested in an object.
DEFINE TABLE no SCHEMALESS;
DEFINE FIELD o ON no TYPE object;
DEFINE FIELD o.s ON no TYPE option<set<array<int>>>;
CREATE no:1 SET o.s = <set>[[1, 2], [3]] RETURN AFTER;
SELECT o FROM no;

-- A clause that makes elements equal collapses them at every level.
DEFINE TABLE sv SCHEMALESS;
DEFINE FIELD s ON sv TYPE set<array<int>>;
DEFINE FIELD OVERWRITE s[*][*] ON sv TYPE int VALUE 0;
CREATE sv:1 SET s = <set>[[1, 2], [3, 4]] RETURN AFTER;
SELECT s FROM sv;
DEFINE TABLE sw SCHEMALESS;
DEFINE FIELD s ON sw TYPE set<set<int>>;
DEFINE FIELD OVERWRITE s[*][*] ON sw TYPE int VALUE 0;
CREATE sw:1 SET s = <set>[<set>[1, 2], <set>[3]] RETURN AFTER;
SELECT s FROM sw;
DEFINE TABLE sx SCHEMALESS;
DEFINE FIELD s ON sx TYPE set<object>;
DEFINE FIELD s[*].x ON sx TYPE int VALUE 0;
CREATE sx:1 SET s = <set>[{ x: 1 }, { x: 2 }] RETURN AFTER;
SELECT s FROM sx;

-- A set of eight or more containers.
DEFINE TABLE sl SCHEMALESS;
DEFINE FIELD s ON sl TYPE set<array<int>>;
CREATE sl:1 SET s = <set>[[9], [8], [7], [6], [5], [4], [3], [2], [1]] RETURN AFTER;
SELECT VALUE s FROM ONLY sl:1;

-- A SCHEMAFULL table.
DEFINE TABLE sf SCHEMAFULL;
DEFINE FIELD s ON sf TYPE set<object>;
DEFINE FIELD s[*].a ON sf TYPE int;
CREATE sf:1 SET s = <set>[{ a: 2 }, { a: 1 }] RETURN AFTER;
SELECT VALUE s FROM ONLY sf:1;

-- An update to another field leaves the set as it is.
UPDATE sa:1 SET k = 42 RETURN VALUE s;
SELECT VALUE s FROM sa WHERE k = 42;

-- array<set<int>>: a clause on `a[*][*]` collapses each inner set.
DEFINE TABLE aset SCHEMALESS;
DEFINE FIELD a ON aset TYPE array<set<int>>;
DEFINE FIELD OVERWRITE a[*][*] ON aset TYPE int VALUE 0;
CREATE aset:1 SET a = [<set>[1, 2], <set>[3]] RETURN AFTER;
SELECT VALUE a FROM ONLY aset:1;

-- A deferred ASSERT on a place within a set judges the element the field
-- wrote there, although the set is rebuilt in order before it is judged.
DEFINE TABLE sy SCHEMALESS;
DEFINE FIELD s ON sy TYPE set<object>;
DEFINE FIELD s[*].x ON sy TYPE int VALUE 3 - $value ASSERT $before = NONE OR $value = 3 - $before;
CREATE sy:1 SET s = <set>[{ x: 1 }, { x: 2 }] RETURN AFTER;
UPDATE sy:1 SET k = 1 RETURN VALUE s;

-- A field that reorders a set leaves every later field's `$before` naming the
-- element it reads: the sets are rebuilt once every field is produced.
DEFINE TABLE sz SCHEMALESS;
DEFINE FIELD s ON sz TYPE set<object>;
DEFINE FIELD s[*].x ON sz TYPE int VALUE 3 - $value;
DEFINE FIELD s[*].y ON sz TYPE int ASSERT $before = NONE OR $value = $before;
DEFINE FIELD s[*].z ON sz TYPE int VALUE $before ?? $value;
CREATE sz:1 SET s = <set>[{ x: 1, y: 10, z: 100 }, { x: 2, y: 20, z: 200 }] RETURN AFTER;
UPDATE sz:1 SET k = 1 RETURN VALUE s;

-- A later field's clause reads every set as it will be stored, although the
-- sets are rebuilt once every field is produced.
DEFINE TABLE sq SCHEMALESS;
DEFINE FIELD s ON sq TYPE set<object>;
DEFINE FIELD s[*].x ON sq TYPE int VALUE 0;
DEFINE FIELD z ON sq VALUE s = <set>[{ x: 0 }];
DEFINE FIELD u ON sq VALUE s.len();
CREATE sq:1 SET s = <set>[{ x: 1 }, { x: 2 }] RETURN AFTER;

-- A COMPUTED field a later field reads is derived from the sets as they will
-- be stored.
DEFINE TABLE sc SCHEMALESS;
DEFINE FIELD s ON sc TYPE set<object>;
DEFINE FIELD s[*].x ON sc TYPE int VALUE 0;
DEFINE FIELD zc ON sc COMPUTED s.len();
DEFINE FIELD zz ON sc VALUE zc;
CREATE sc:1 SET s = <set>[{ x: 1 }, { x: 2 }] RETURN NONE;
SELECT s, zc, zz FROM ONLY sc:1;

-- A deferred ASSERT on a set element judges the element's finished value,
-- after the nested fields below it rewrote it.
DEFINE TABLE sr SCHEMALESS;
DEFINE FIELD s ON sr TYPE set<object>;
DEFINE FIELD OVERWRITE s[*] ON sr TYPE object ASSERT $value.x = 1;
DEFINE FIELD s[*].x ON sr TYPE int VALUE 2;
CREATE sr:1 SET s = <set>[{ x: 1 }];
DEFINE TABLE st SCHEMALESS;
DEFINE FIELD s ON st TYPE set<object>;
DEFINE FIELD OVERWRITE s[*] ON st TYPE object ASSERT $value.x = 2;
DEFINE FIELD s[*].x ON st TYPE int VALUE 2;
CREATE st:1 SET s = <set>[{ x: 1 }, { x: 3 }] RETURN AFTER;

-- A clause of a field that writes the elements of a set reads the set with the
-- values the field has already landed in it, rebuilt, so a lookup in it finds
-- every element it holds.
DEFINE TABLE sk SCHEMALESS;
DEFINE FIELD s ON sk TYPE set<int>;
DEFINE FIELD OVERWRITE s[*] ON sk TYPE int VALUE IF $this.s CONTAINS 4 { $value * 10 } ELSE { $value };
CREATE sk:1 SET s = <set>[1, 2, 3, 4, 5] RETURN VALUE s;
