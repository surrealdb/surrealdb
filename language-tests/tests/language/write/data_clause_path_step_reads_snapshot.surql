/**
[env]
namespace = true
database = true

[test]
reason = """
An index or a method argument of a path step in a data clause is evaluated
against the document root, which is the record's pre-mutation snapshot: in
`SET a = ['x', 'y', 'z'][n % 3]` the `n` is the record's field, not a field of
the array being indexed. The same holds for an object key, a method argument,
a nested index, `$this`, and the clauses of `UPSERT`, `INSERT ... ON DUPLICATE
KEY UPDATE`, `MERGE` and `CONTENT`.

A creating record's snapshot is no record, so on `CREATE` and on an `UPSERT`
that creates, the same field reads `NONE` and indexing with it answers `NONE`.
"""

[[test.results]]
value = "[{ id: t:1, k: 'b', n: 1 }]"

[[test.results]]
value = "[{ id: t:2, k: 'a', n: 2 }]"

[[test.results]]
value = "[{ a: 'y' }, { a: 'z' }]"

[[test.results]]
value = "[{ b: 'B' }, { b: 'A' }]"

[[test.results]]
value = "[{ c: 'bcdef' }, { c: 'cdef' }]"

[[test.results]]
value = "[{ d: 20 }, { d: 30 }]"

[[test.results]]
value = "[{ e: 'y' }, { e: 'z' }]"

[[test.results]]
value = "[{ f: 'y', g: 'z' }]"

[[test.results]]
value = "[{ h: [1, 2, 3] }, { h: [2, 4, 6] }]"

[[test.results]]
value = "[{ i: 1 }, { i: 4 }]"

[[test.results]]
value = "[{ j: 'y' }]"

[[test.results]]
value = "[{ j: NONE }]"

[[test.results]]
value = "[{ l: NONE }]"

[[test.results]]
value = "[{ o: 'z' }]"

[[test.results]]
value = "[{ p: 'y' }]"

[[test.results]]
value = "[{ q: 'y' }]"

[[test.results]]
value = "[{ a: 'y', id: t:1 }]"
*/

CREATE t:1 SET n = 1, k = 'b';
CREATE t:2 SET n = 2, k = 'a';
UPDATE t SET a = ['x', 'y', 'z'][n % 3] RETURN a;
UPDATE t SET b = { a: 'A', b: 'B' }[k] RETURN b;
UPDATE t SET c = 'abcdef'.slice(n) RETURN c;
UPDATE t SET d = [10, 20, 30][$this.n] RETURN d;
UPDATE t SET e = ['x', 'y', 'z'][record::id(id) % 3] RETURN e;
UPDATE t:1 SET f = ['x', 'y', 'z'][n], g = ['x', 'y', 'z'][n + 1] RETURN f, g;
UPDATE t SET h = [1, 2, 3].map(|$v| $v * n) RETURN h;
UPDATE t SET i = [[1, 2], [3, 4]][n - 1][n - 1] RETURN i;
UPSERT t:1 SET j = ['x', 'y', 'z'][n] RETURN j;
UPSERT t:9 SET j = ['x', 'y', 'z'][n] RETURN j;
CREATE t:10 SET l = ['x', 'y', 'z'][n] RETURN l;
INSERT INTO t { id: t:2, n: 5 } ON DUPLICATE KEY UPDATE o = ['x', 'y', 'z'][n] RETURN o;
UPDATE t:1 MERGE { p: ['x', 'y', 'z'][n] } RETURN p;
UPDATE t:1 CONTENT { n: 1, q: ['x', 'y', 'z'][n] } RETURN q;
UPDATE t:1, t:2 SET a = ['x', 'y', 'z'][n % 3] WHERE ['x', 'y', 'z'][n % 3] = 'y' RETURN id, a;
