/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
issue = 7539
reason = """
A B-tree index column declared without `*` (`FIELDS acl`) still stores one
entry per element when the record's value is an array. The comparison
operators keep whole-value semantics: `['a', 'b'] = 'a'` is false,
`['a', 'b'] = ['a', 'b']` is true, and every array sorts after every string.
The correct answer to every query here is the one the table gives with no
index, and a count equals the number of rows the same WHERE selects.

The streaming planner plans such a column as the element column it behaves
as whenever the field's declared kind holds only arrays (`option<...>`
included), whether declared on the column or through a parent's object
literal type, so on `acl` of `r` and `s` and `metadata.tags` of `o` equality,
`IN`, ranges and `ORDER BY` over the whole value are not answered from the
element entries. Nor is membership (`CONTAINS`, `CONTAINSANY`): a declared
kind does not show that every stored value is an array — records written
before it, or imported, are not coerced — and a string stored whole holds
substrings no element seek reaches, so the scan answers it. An index declared
on `acl.*` is what serves membership from the index. A kind that also admits
a string (`m`, `a`) is planned as written. `_legacy` pins the
compute-only engine's answers to the same queries.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ acl: ['a', 'b'], id: r:1 }]"

[[test.results]]
value = "[{ acl: ['c'], id: r:2 }]"

[[test.results]]
value = "[{ acl: ['a'], id: r:3 }]"

[[test.results]]
value = "[{ acl: ['b', 'c'], id: r:4 }]"

[[test.results]]
value = "[{ acl: ['d'], id: r:5 }]"

[[test.results]]
value = "[{ id: r:6 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[r:1]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[r:3]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[r:1, r:2, r:3, r:4, r:5]"

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = "[r:1, r:2, r:3, r:4, r:5]"

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = "[r:6, r:3, r:1, r:4, r:2, r:5]"

[[test.results]]
value = "[r:3, r:1, r:4]"

[[test.results]]
value = "[r:1, r:4]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[r:1, r:4, r:5]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ acl: ['a', 'b'], id: s:1 }]"

[[test.results]]
value = "[{ acl: ['c'], id: s:2 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[s:1]"

[[test.results]]
value = "[s:1, s:2]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ acl: 'alphabet', id: m:1 }]"

[[test.results]]
value = "[{ acl: ['pha'], id: m:2 }]"

[[test.results]]
value = "[m:1, m:2]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ acl: 'alphabet', id: a:1 }]"

[[test.results]]
value = "[{ acl: ['pha'], id: a:2 }]"

[[test.results]]
value = "[a:1, a:2]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: o:1, metadata: { tags: ['a', 'b'] } }]"

[[test.results]]
value = "[{ id: o:2, metadata: { tags: ['c'] } }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[o:1]"

[[test.results]]
value = "[o:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: ou:1, metadata: { tags: ['a', 'b'] } }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[ou:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ acl: 'alphabet', id: p:1 }]"

[[test.results]]
value = "[{ acl: ['pha'], id: p:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p:1, p:2]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[r:1, r:2, r:3, r:4, r:5]"

[[test.results]]
value = "[r:1]"

[[test.results]]
value = "[s:1]"

[[test.results]]
value = "[m:1, m:2]"
*/

DEFINE TABLE r SCHEMALESS;
DEFINE FIELD acl ON r TYPE option<array<string>>;
DEFINE INDEX r_acl ON r FIELDS acl;
CREATE r:1 SET acl = ['a', 'b'];
CREATE r:2 SET acl = ['c'];
CREATE r:3 SET acl = ['a'];
CREATE r:4 SET acl = ['b', 'c'];
CREATE r:5 SET acl = ['d'];
CREATE r:6;

-- Equality on a string matches no array, never an element.
SELECT VALUE id FROM r WHERE acl = 'a';
SELECT count() FROM r WHERE acl = 'a' GROUP ALL;

-- Equality on an array matches the whole array.
SELECT VALUE id FROM r WHERE acl = ['a', 'b'];
SELECT count() FROM r WHERE acl = ['a', 'b'] GROUP ALL;

-- IN tests each whole value against the list.
SELECT VALUE id FROM r WHERE acl IN ['a', ['a']] ORDER BY id;
SELECT count() FROM r WHERE acl IN ['a', ['a']] GROUP ALL;

-- A range compares whole values, and every array sorts after every string:
-- each matching row comes back once, whether or not one of its elements is
-- in the range.
SELECT VALUE id FROM r WHERE acl > 'a' ORDER BY id;
SELECT count() FROM r WHERE acl > 'a' GROUP ALL;
SELECT VALUE id FROM r WHERE acl > 'z' ORDER BY id;
SELECT count() FROM r WHERE acl > 'z' GROUP ALL;

-- ORDER BY orders whole values, not the index's element entries.
SELECT VALUE id FROM r ORDER BY acl;
SELECT VALUE id FROM r WHERE acl >= ['a'] ORDER BY acl LIMIT 3;

-- Membership.
SELECT VALUE id FROM r WHERE acl CONTAINS 'b' ORDER BY id;
SELECT count() FROM r WHERE acl CONTAINS 'b' GROUP ALL;
SELECT VALUE id FROM r WHERE acl CONTAINSANY ['b', 'd'] ORDER BY id;

-- The shape the issue reported: a SCHEMAFULL array<string> field.
DEFINE TABLE s SCHEMAFULL;
DEFINE FIELD acl ON s TYPE array<string>;
DEFINE INDEX s_acl ON s FIELDS acl;
CREATE s:1 SET acl = ['a', 'b'];
CREATE s:2 SET acl = ['c'];
SELECT count() FROM s WHERE acl = 'a' GROUP ALL;
SELECT VALUE id FROM s WHERE acl = 'a';
SELECT VALUE id FROM s WHERE acl = 'a' OR acl = 'c';
SELECT VALUE id FROM s WHERE acl CONTAINS 'a';
SELECT VALUE id FROM s WHERE acl CONTAINSANY ['a', 'c'] ORDER BY id;
SELECT count() FROM s WHERE acl CONTAINSANY ['a', 'c'] GROUP ALL;

-- A kind that also admits a string holds values an element seek cannot
-- answer `CONTAINS` for: a string is stored whole, and contains a substring.
DEFINE TABLE m SCHEMALESS;
DEFINE FIELD acl ON m TYPE array<string> | string;
DEFINE INDEX m_acl ON m FIELDS acl;
CREATE m:1 SET acl = 'alphabet';
CREATE m:2 SET acl = ['pha'];
SELECT VALUE id FROM m WHERE acl CONTAINS 'pha' ORDER BY id;
SELECT count() FROM m WHERE acl CONTAINS 'pha' GROUP ALL;
DEFINE TABLE a SCHEMALESS;
DEFINE FIELD acl ON a TYPE any;
DEFINE INDEX a_acl ON a FIELDS acl;
CREATE a:1 SET acl = 'alphabet';
CREATE a:2 SET acl = ['pha'];
SELECT VALUE id FROM a WHERE acl CONTAINS 'pha' ORDER BY id;
SELECT count() FROM a WHERE acl CONTAINS 'pha' GROUP ALL;

-- A column declared through its parent's object literal type is planned from
-- that type.
DEFINE TABLE o SCHEMAFULL;
DEFINE FIELD metadata ON o TYPE { tags: array<string> };
DEFINE INDEX o_mt ON o FIELDS metadata.tags;
CREATE o:1 SET metadata = { tags: ['a', 'b'] };
CREATE o:2 SET metadata = { tags: ['c'] };
SELECT count() FROM o WHERE metadata.tags = 'a' GROUP ALL;
SELECT VALUE id FROM o WHERE metadata.tags = ['a', 'b'];
SELECT VALUE id FROM o WHERE metadata.tags CONTAINS 'a';

-- A definition of the column itself that declares no kind leaves its parent's
-- object literal type in force: the value is still coerced to an array.
DEFINE TABLE ou SCHEMAFULL;
DEFINE FIELD metadata ON ou TYPE { tags: array<string> };
DEFINE FIELD metadata.tags ON ou PERMISSIONS FULL;
DEFINE INDEX ou_mt ON ou FIELDS metadata.tags;
CREATE ou:1 SET metadata = { tags: ['a', 'b'] };
SELECT count() FROM ou WHERE metadata.tags = 'a' GROUP ALL;
SELECT VALUE id FROM ou WHERE metadata.tags = ['a', 'b'];

-- A kind declared after a string is stored does not rewrite it: `p:1` stays
-- indexed whole, and holds the substring an element seek would miss.
DEFINE TABLE p SCHEMALESS;
DEFINE INDEX p_acl ON p FIELDS acl;
CREATE p:1 SET acl = 'alphabet';
CREATE p:2 SET acl = ['pha'];
DEFINE FIELD acl ON p TYPE array<string>;
SELECT VALUE id FROM p WHERE acl CONTAINS 'pha' ORDER BY id;
SELECT count() FROM p WHERE acl CONTAINS 'pha' GROUP ALL;

-- The same predicates over a source bound in the statement's own block,
-- whose access path is chosen when the scan runs.
{ LET $t = type::table('r'); SELECT VALUE id FROM $t WHERE acl > 'a' ORDER BY id };
{ LET $t = type::table('r'); SELECT VALUE id FROM $t WHERE acl = ['a', 'b'] };
{ LET $t = type::table('s'); SELECT VALUE id FROM $t WHERE acl CONTAINS 'a' };
{ LET $t = type::table('m'); SELECT VALUE id FROM $t WHERE acl CONTAINS 'pha' ORDER BY id };
