/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
Half of a pair. Same queries as `projection_indexed_alias_legacy.surql`, pinned
to the planner strategies that use the new execution path; the two files'
expected results are the divergence.

An index part in a projection alias is discarded here rather than selecting an
element. `AS arr[0]` and `AS arr[$]` both replace the whole array with the
value, as though the alias were `AS arr`, and so does an index past the end,
which the legacy path treats as a no-op. `AS arr[1].a` and `AS arr[$].a` write
the leaf into every element, as though the alias were `AS arr.a`.

`set_at_field_path_owned` — the setter this path uses — does handle index,
first and last parts, so the loss is upstream of it, in what the alias idiom is
converted into before the write.

Every one of the six queries disagrees with the legacy result. This is a parity
gap rather than a coverage gap: both sides are recorded so the disagreement is
visible and a change to either side fails a test.
"""

[[test.results]]
value = "[{ arr: [{ a: 1 }, { a: 2 }, { a: 3 }], id: t:1, n: 9 }]"

[[test.results]]
value = "[{ arr: 9, id: t:1, n: 9 }]"

[[test.results]]
value = "[{ arr: 9, id: t:1, n: 9 }]"

[[test.results]]
value = "[{ arr: [{ a: 9 }, { a: 9 }, { a: 9 }], id: t:1, n: 9 }]"

[[test.results]]
value = "[{ arr: [{ a: 9 }, { a: 9 }, { a: 9 }], id: t:1, n: 9 }]"

[[test.results]]
value = "[{ arr: 9, id: t:1, n: 9 }]"

[[test.results]]
value = "[{ out: 9 }]"
*/

CREATE t:1 SET n = 9, arr = [{ a: 1 }, { a: 2 }, { a: 3 }];
SELECT *, n AS arr[0] FROM t;
SELECT *, n AS arr[$] FROM t;
SELECT *, n AS arr[1].a FROM t;
SELECT *, n AS arr[$].a FROM t;
SELECT *, n AS arr[9] FROM t;
SELECT n AS out[0] FROM t;
