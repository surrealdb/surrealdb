/**
[env]
namespace = true
database = true
planner-strategy = ["compute-only"]

[test]
reason = """
Half of a pair. Same queries as
`projection_indexed_alias_new_planner.surql`, pinned to the legacy compute
path; the two files' expected results are the divergence.

An index part in a projection alias selects one element here. `AS arr[0]`
replaces the first element and leaves the rest, `AS arr[$]` the last, and
`AS arr[1].a` writes a leaf under exactly one element. An index past the end
writes nothing.

`AS out[0]` where `out` does not exist yet is the one case that is odd on this
side too: it produces an object keyed `"0"` rather than a one-element array.

The new planner drops the index instead, so none of these six agrees across
strategies. Read this file next to its pair rather than on its own.
"""

[[test.results]]
value = "[{ arr: [{ a: 1 }, { a: 2 }, { a: 3 }], id: t:1, n: 9 }]"

[[test.results]]
value = "[{ arr: [9, { a: 2 }, { a: 3 }], id: t:1, n: 9 }]"

[[test.results]]
value = "[{ arr: [{ a: 1 }, { a: 2 }, 9], id: t:1, n: 9 }]"

[[test.results]]
value = "[{ arr: [{ a: 1 }, { a: 9 }, { a: 3 }], id: t:1, n: 9 }]"

[[test.results]]
value = "[{ arr: [{ a: 1 }, { a: 2 }, { a: 9 }], id: t:1, n: 9 }]"

[[test.results]]
value = "[{ arr: [{ a: 1 }, { a: 2 }, { a: 3 }], id: t:1, n: 9 }]"

[[test.results]]
value = '[{ out: { "0": 9 } }]'
*/

CREATE t:1 SET n = 9, arr = [{ a: 1 }, { a: 2 }, { a: 3 }];
SELECT *, n AS arr[0] FROM t;
SELECT *, n AS arr[$] FROM t;
SELECT *, n AS arr[1].a FROM t;
SELECT *, n AS arr[$].a FROM t;
SELECT *, n AS arr[9] FROM t;
SELECT n AS out[0] FROM t;
