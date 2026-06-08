/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = "Pre-decode filter: OR no-match; computed field row read; int=float equality; repeated AND key; NOT(AND); type::table scan; three-valued logic with missing fields (AND/OR/NOT)."

[[test.results]]
value = "[{ id: pfor:1, tag: 'a' }]"

[[test.results]]
value = "[{ id: pfor:2, tag: 'b' }]"

[[test.results]]
value = "[{ id: pfor:3, tag: 'c' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: pfcf:1, x: 1 }]"

[[test.results]]
value = "[{ a: 1, id: pfcf:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pfmt:1, n: 1f }]"

[[test.results]]
value = "[{ id: pfmt:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pfrk:1, k: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: 3, id: pfn:1 }]"

[[test.results]]
value = "[{ id: pfn:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: dynpf:1, v: 1 }]"

[[test.results]]
value = "[{ id: dynpf:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: pftvl:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: pftvl:1 }]"

[[test.results]]
value = "[{ id: pftvl:1 }]"

*/

CREATE pfor:1 SET tag = 'a';
CREATE pfor:2 SET tag = 'b';
CREATE pfor:3 SET tag = 'c';
SELECT id FROM pfor WHERE tag = 'x' OR tag = 'y';

DEFINE TABLE pfcf SCHEMALESS;
DEFINE FIELD x ON pfcf VALUE 1;
DEFINE FIELD a ON pfcf TYPE int;
CREATE pfcf:1 SET a = 1;
SELECT id, a FROM pfcf WHERE a = 1;

DEFINE TABLE pfmt SCHEMALESS;
CREATE pfmt:1 SET n = 1.0f;
SELECT id FROM pfmt WHERE n = 1;

DEFINE TABLE pfrk SCHEMALESS;
CREATE pfrk:1 SET k = 2;
SELECT id FROM pfrk WHERE k = 1 AND k = 2;

DEFINE TABLE pfn SCHEMALESS;
CREATE pfn:1 SET a = 1, b = 3;
SELECT id FROM pfn WHERE NOT (a = 1 AND b = 2);

DEFINE TABLE dynpf SCHEMALESS;
CREATE dynpf:1 SET v = 1;
SELECT id FROM type::table('dynpf') WHERE v = 1;

-- Three-valued logic with a missing field. The record has only `a`, so any
-- predicate referencing `missing` reads `Value::None`. The pre-decode filter
-- must return the same rows as full decode in every case below.
DEFINE TABLE pftvl SCHEMALESS;
CREATE pftvl:1 SET a = 1;
-- AND with missing → reject (a=1 true, missing=2 false ⇒ false).
SELECT id FROM pftvl WHERE a = 1 AND missing = 2;
-- OR with missing → keep (a=1 carries it; missing=2 is false but harmless).
SELECT id FROM pftvl WHERE a = 1 OR missing = 2;
-- NOT of an equality against a missing field → keep (NOT false = true).
SELECT id FROM pftvl WHERE NOT (missing = 2);
