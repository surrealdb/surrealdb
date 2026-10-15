/**
[env]
namespace = true
database = true

[test]
reason = """
A field whose type is a set of typed collections (`set<array<int>>`,
`set<set<int>>`) implies a definition for the elements of each element
(`v.*.*`). Once that definition's places are written back, every set holding
one is rebuilt where it stands, so the record stores and reads back the set
that was written: one element or several, on a SCHEMALESS or a SCHEMAFULL
table.

A field reached through a set (`a.*.b` under `set<object>`) rewrites that
set's elements in the same way, and an element-level `VALUE` that makes two
elements equal leaves one of them, in a nested set and in the set holding it.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: sl:1, v: {[1],} }]"

[[test.results]]
value = "[{ id: sl:2, v: {[1], [2]} }]"

[[test.results]]
value = "[{ id: sl:1, v: {[1],} }, { id: sl:2, v: {[1], [2]} }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: sf:1, v: {[1],} }]"

[[test.results]]
value = "[{ id: sf:2, v: {[1], [2]} }]"

[[test.results]]
value = "[{ id: sf:1, v: {[1],} }, { id: sf:2, v: {[1], [2]} }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: ql:1, v: {{1,},} }]"

[[test.results]]
value = "[{ id: ql:2, v: {{1,}, {2,}} }]"

[[test.results]]
value = "[{ id: ql:1, v: {{1,},} }, { id: ql:2, v: {{1,}, {2,}} }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: qf:1, v: {{1,},} }]"

[[test.results]]
value = "[{ id: qf:2, v: {{1,}, {2,}} }]"

[[test.results]]
value = "[{ id: qf:1, v: {{1,},} }, { id: qf:2, v: {{1,}, {2,}} }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: {{ b: 1 },}, id: so:1 }]"

[[test.results]]
value = "[{ a: {{ b: 1 },}, id: so:2 }]"

[[test.results]]
value = "[{ a: {{ b: 1 },}, id: so:1 }, { a: {{ b: 1 },}, id: so:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: ss:1, v: {{0,}, {1,}} }]"

[[test.results]]
value = "[{ id: ss:2, v: {{1,},} }]"

[[test.results]]
value = "[{ id: ss:1, v: {{0,}, {1,}} }, { id: ss:2, v: {{1,},} }]"
*/

DEFINE TABLE sl SCHEMALESS;
DEFINE FIELD v ON sl TYPE set<array<int>>;
CREATE sl:1 SET v = <set>[[1]];
CREATE sl:2 SET v = <set>[[1], [2]];
SELECT * FROM sl ORDER BY id;

DEFINE TABLE sf SCHEMAFULL;
DEFINE FIELD v ON sf TYPE set<array<int>>;
CREATE sf:1 SET v = [[1]];
CREATE sf:2 SET v = [[1], [2]];
SELECT * FROM sf ORDER BY id;

DEFINE TABLE ql SCHEMALESS;
DEFINE FIELD v ON ql TYPE set<set<int>>;
CREATE ql:1 SET v = <set>[<set>[1]];
CREATE ql:2 SET v = <set>[<set>[1], <set>[2]];
SELECT * FROM ql ORDER BY id;

DEFINE TABLE qf SCHEMAFULL;
DEFINE FIELD v ON qf TYPE set<set<int>>;
CREATE qf:1 SET v = [[1]];
CREATE qf:2 SET v = [[1], [2]];
SELECT * FROM qf ORDER BY id;

DEFINE TABLE so SCHEMALESS;
DEFINE FIELD a ON so TYPE set<object>;
DEFINE FIELD a[*].b ON so VALUE 1;
CREATE so:1 SET a = <set>[{ b: 2 }];
CREATE so:2 SET a = <set>[{ b: 2 }, { b: 3 }];
SELECT * FROM so ORDER BY id;

DEFINE TABLE ss SCHEMALESS;
DEFINE FIELD v ON ss TYPE set<set<int>>;
DEFINE FIELD OVERWRITE v[*][*] ON ss TYPE int VALUE $value % 2;
CREATE ss:1 SET v = <set>[<set>[1, 3], <set>[2, 4]];
CREATE ss:2 SET v = <set>[<set>[1], <set>[3]];
SELECT * FROM ss ORDER BY id;
