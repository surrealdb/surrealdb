/**
[env]
namespace = true
database = true

[test]
reason = """
The JSON Patch operations a `PATCH` clause accepts beyond `add`, which is the
only one the rest of the suite exercises on a successful path.

`copy` writes the value at `from` to `path` and leaves the source in place;
`move` does the same and then removes the source. Both resolve a pointer of
more than one segment, so a nested source is reachable.

`change` is not an RFC 6902 operation: its `value` is a unified diff, applied to
the string already at `path`. It is a no-op unless both the operand and the
stored value are strings.

`replace` overwrites and `remove` deletes. `test` compares and produces no
change when it matches; when it does not, it fails the statement.

A patch is applied to a copy that replaces the record only once every operation
in it has succeeded, so a failing `test` discards the operations that ran before
it in the same patch. The last pair pins that: `replace` sets `a` to 77 and the
`test` that follows fails against it — the error reports `got 77`, so the write
did happen to the working copy — and the record still reads 10 afterwards. A
patch whose only operation is the failing `test` cannot show this, because it
has nothing to roll back.
"""

[[test.results]]
value = "[{ a: 'one', b: { c: 'two' }, id: doc:1 }]"

[[test.results]]
value = "[{ a: 'one', a_copy: 'one', b: { c: 'two' }, id: doc:1 }]"

[[test.results]]
value = "[{ a: 'one', a_copy: 'one', b: { c: 'two' }, id: doc:1, nested_copy: 'two' }]"

[[test.results]]
value = "[{ a: 'one', b: { c: 'two' }, id: doc:1, moved: 'one', nested_copy: 'two' }]"

[[test.results]]
value = "[{ id: doc:2, text: 'hello world' }]"

[[test.results]]
value = "[{ id: doc:2, text: 'hello there' }]"

[[test.results]]
value = "[{ a: 1, b: 2, id: doc:3 }]"

[[test.results]]
value = "[{ a: 10, b: 2, id: doc:3 }]"

[[test.results]]
value = "[{ a: 10, id: doc:3 }]"

[[test.results]]
value = "[{ a: 10, id: doc:3 }]"

[[test.results]]
error = "Given test operation failed for JSON Patch. Expected `99`, but got `10` instead."

[[test.results]]
value = "[{ a: 10, id: doc:3 }]"

[[test.results]]
error = "Given test operation failed for JSON Patch. Expected `99`, but got `77` instead."

[[test.results]]
value = "[{ a: 10, id: doc:3 }]"























*/

-- `copy` duplicates a value and leaves the source; `move` removes it.
CREATE doc:1 SET a = 'one', b = { c: 'two' };
UPDATE doc:1 PATCH [{ op: 'copy', from: '/a', path: '/a_copy' }];
UPDATE doc:1 PATCH [{ op: 'copy', from: '/b/c', path: '/nested_copy' }];
UPDATE doc:1 PATCH [{ op: 'move', from: '/a_copy', path: '/moved' }];

-- `change` applies a unified diff to a string in place.
CREATE doc:2 SET text = 'hello world';
UPDATE doc:2 PATCH [{ op: 'change', path: '/text', value: '@@ -1,11 +1,9 @@\n hello \n-world\n+there\n' }];

-- `replace` overwrites, `remove` deletes, and `test` passes silently or fails
-- the whole patch.
CREATE doc:3 SET a = 1, b = 2;
UPDATE doc:3 PATCH [{ op: 'replace', path: '/a', value: 10 }];
UPDATE doc:3 PATCH [{ op: 'remove', path: '/b' }];
UPDATE doc:3 PATCH [{ op: 'test', path: '/a', value: 10 }];
UPDATE doc:3 PATCH [{ op: 'test', path: '/a', value: 99 }];
SELECT * FROM doc:3;

-- A failing `test` after a mutating operation in the same patch discards that
-- operation too: `a` is not left at 77.
UPDATE doc:3 PATCH [{ op: 'replace', path: '/a', value: 77 }, { op: 'test', path: '/a', value: 99 }];
SELECT * FROM doc:3;
