/**
[env]
namespace = true
database = true

[test]
reason = """
An `id` in an inserted object supplies the id part of the record id and keeps
its own type: a number stays a number, and an object, a UUID and an array each
survive as a structured id rather than being flattened to a string. These are
the record-id key shapes INSERT can produce, and none of them is otherwise
covered.

INSERT RELATION requires both endpoints to be record ids. `out` holding a
number is rejected, and so is `out` being absent, which arrives as NONE. The
error names the offending property and its value.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1 }]"

[[test.results]]
value = "[{ id: t:2 }]"

[[test.results]]
value = "[{ id: t:7, n: 1 }]"

[[test.results]]
value = "[{ id: t:abc, n: 1 }]"

[[test.results]]
value = "[{ id: t:{ a: 1, b: 2 }, n: 1 }]"

[[test.results]]
value = "[{ id: t:u'018f9a3c-0000-7000-8000-000000000000', n: 1 }]"

[[test.results]]
value = "[{ id: t:[1, 2], n: 1 }]"

[[test.results]]
value = "[{ id: lnk:e1, in: t:1, out: t:2 }]"

[[test.results]]
error = "Cannot execute INSERT statement where property 'out' is: 42"

[[test.results]]
error = "Cannot execute INSERT statement where property 'out' is: NONE"
*/

DEFINE TABLE t SCHEMALESS;
DEFINE TABLE lnk SCHEMALESS;
CREATE t:1;
CREATE t:2;

-- An `id` in the inserted object becomes the id part of the record id, and it
-- keeps its type rather than being stringified.
INSERT INTO t { id: 7, n: 1 };
INSERT INTO t { id: 'abc', n: 1 };
INSERT INTO t { id: { a: 1, b: 2 }, n: 1 };
INSERT INTO t { id: u"018f9a3c-0000-7000-8000-000000000000", n: 1 };
INSERT INTO t { id: [1, 2], n: 1 };

-- INSERT RELATION requires both endpoints to be record ids.
INSERT RELATION INTO lnk { id: lnk:e1, in: t:1, out: t:2 };
INSERT RELATION INTO lnk { id: lnk:e2, in: t:1, out: 42 };
INSERT RELATION INTO lnk { id: lnk:e3, in: t:1 };
