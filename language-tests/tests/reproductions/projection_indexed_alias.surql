/**
[env]
namespace = true
database = true

[test]
reason = """
An index part in a projection alias is discarded rather than selecting an
element. `AS arr[0]` and `AS arr[$]` both replace the whole array with the
value, as though the alias were `AS arr`, and so does an index past the end.
`AS arr[1].a` and `AS arr[$].a` write the leaf into every element, as though
the alias were `AS arr.a`.

The intended contract is that the alias selects the element the index names
(an index past the end is a no-op); this file pins the current answer for all
six queries. `set_at_field_path_owned` -- the setter this path uses -- does
handle index, first and last parts, so the loss is upstream of it, in what the
alias idiom is converted into before the write.
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
