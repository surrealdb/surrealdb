/**
[env]
namespace = true
database = true

[test]
issue = 1336
reason = """
Records whose ids differ only in a nested number's kind (`c:[1]` and `c:[1dec]`)
are two records, and a B-tree index gives each its own entry: an indexed
equality, a union of two indexes and a union over the elements of an array,
merged in record id order or not, answer both, each once, as a scan does, in
the order a scan lists them, and an update or delete of one leaves
the other's entry in place. The id kinds are asserted with `type::is_int`,
since the runner compares record ids across number kinds."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ body: 'hello', id: c:[1], tag: 'x', tags: ['a', 'b'] }]"

[[test.results]]
value = "[{ body: 'hello', id: c:[1dec], tag: 'y', tags: ['a', 'b'] }]"

[[test.results]]
value = "[{ body: 'other', id: c:[2], tag: 'z', tags: ['c'] }]"

[[test.results]]
value = "[{ int: false }, { int: true }]"

[[test.results]]
value = "[{ int: false }, { int: true }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [table: c, index: ix, access: = 'hello', direction: Forward]
"'''

[[test.results]]
value = "[{ int: false }, { int: true }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: body = 'hello' OR tag = 'x']
        BitmapScan [ctx: Db] [table: c]
            BitmapOr [ctx: Db]
                BitmapIndexScan [ctx: Db] [index: ix, access: = 'hello']
                BitmapIndexScan [ctx: Db] [index: iy, access: = 'x']
"'''

[[test.results]]
value = "[{ int: false }, { int: true }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: tags.* CONTAINSANY ['a', 'b']]
        UnionIndexScan [ctx: Db] [table: c, branches: 2]
            IndexScan [ctx: Db] [table: c, index: it, access: = 'a', direction: Forward]
            IndexScan [ctx: Db] [table: c, index: it, access: = 'b', direction: Forward]
"'''

[[test.results]]
value = "[{ int: true }, { int: false }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: tags.* CONTAINSANY ['a', 'b']]
        UnionIndexScan [ctx: Db] [table: c, branches: 2, merge_by_id: Asc]
            IndexScan [ctx: Db] [table: c, index: it, access: = 'a', direction: Forward]
            IndexScan [ctx: Db] [table: c, index: it, access: = 'b', direction: Forward]
"'''

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ body: 'gone', id: c:[1dec], tag: 'y', tags: ['a', 'b'] }]"

[[test.results]]
value = "[{ int: true }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ int: false }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: k:[1, 'b'], tags: ['a', 'b'], v: 5 }]"

[[test.results]]
value = "[{ id: k:[1dec, 'a'], tags: ['a', 'b'], v: 5 }]"

[[test.results]]
value = "[{ id: k:[1f, 'c'], tags: ['a', 'b'], v: 5 }]"

[[test.results]]
value = "['b', 'c', 'a']"

[[test.results]]
value = "['b', 'c', 'a']"

[[test.results]]
value = "['b', 'c', 'a']"

[[test.results]]
value = "['a', 'c', 'b']"

[[test.results]]
value = "['a', 'c', 'b']"

*/
DEFINE INDEX ix ON c FIELDS body;
DEFINE INDEX iy ON c FIELDS tag;
DEFINE INDEX it ON c FIELDS tags.*;
CREATE c:[1] SET body = 'hello', tag = 'x', tags = ['a', 'b'];
CREATE c:[1dec] SET body = 'hello', tag = 'y', tags = ['a', 'b'];
CREATE c:[2] SET body = 'other', tag = 'z', tags = ['c'];
SELECT type::is_int(id[0]) AS int FROM c WITH INDEX ix WHERE body = 'hello' ORDER BY int;
SELECT type::is_int(id[0]) AS int FROM c WITH NOINDEX WHERE body = 'hello' ORDER BY int;
EXPLAIN SELECT id FROM c WITH INDEX ix WHERE body = 'hello';
SELECT type::is_int(id[0]) AS int FROM c WHERE body = 'hello' OR tag = 'x' ORDER BY int;
EXPLAIN SELECT id FROM c WHERE body = 'hello' OR tag = 'x';
SELECT type::is_int(id[0]) AS int FROM c WITH INDEX it WHERE tags.* CONTAINSANY ['a', 'b'] ORDER BY int;
EXPLAIN SELECT id FROM c WITH INDEX it WHERE tags.* CONTAINSANY ['a', 'b'];
SELECT type::is_int(id[0]) AS int FROM c WITH INDEX it WHERE tags.* CONTAINSANY ['a', 'b'] ORDER BY id;
EXPLAIN SELECT id FROM c WITH INDEX it WHERE tags.* CONTAINSANY ['a', 'b'] ORDER BY id;
SELECT count() FROM c WITH INDEX ix WHERE body = 'hello' GROUP ALL;
UPDATE c:[1dec] SET body = 'gone';
SELECT type::is_int(id[0]) AS int FROM c WITH INDEX ix WHERE body = 'hello' ORDER BY int;
DELETE c:[1];
SELECT type::is_int(id[0]) AS int FROM c WITH INDEX ix WHERE body = 'gone' ORDER BY int;
SELECT VALUE id FROM c WITH INDEX ix WHERE body = 'hello';
-- Compound ids whose numbers differ only in kind, listed in record id order:
-- a scan, an index scan and a union merged by id read them alike.
DEFINE INDEX kv ON k FIELDS v;
DEFINE INDEX kt ON k FIELDS tags.*;
CREATE k:[1, 'b'] SET v = 5, tags = ['a', 'b'];
CREATE k:[1dec, 'a'] SET v = 5, tags = ['a', 'b'];
CREATE k:[1f, 'c'] SET v = 5, tags = ['a', 'b'];
SELECT VALUE id[1] FROM k WITH NOINDEX WHERE v = 5 ORDER BY id;
SELECT VALUE id[1] FROM k WITH INDEX kv WHERE v = 5 ORDER BY id;
SELECT VALUE id[1] FROM k WITH INDEX kt WHERE tags.* CONTAINSANY ['a', 'b'] ORDER BY id;
SELECT VALUE id[1] FROM k WITH NOINDEX WHERE v = 5 ORDER BY id DESC;
SELECT VALUE id[1] FROM k WITH INDEX kt WHERE tags.* CONTAINSANY ['a', 'b'] ORDER BY id DESC;
