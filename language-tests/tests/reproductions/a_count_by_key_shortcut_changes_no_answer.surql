/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	Counting by key — reading a count from the keys a scan can see, without
	reading a record — is a fast path taken for some statements and not for
	others: whether a `WHERE true` survives folding, and whether one source or
	several are counted, change which plan runs.

	None of that is visible in an answer. `GROUP ALL` opens its group whether
	or not a row reaches it, so a source with nothing to count reports a count
	of zero on every plan, and a source with rows reports the same number
	however the rows were counted. This test is what keeps it that way: a fast
	path that changed an answer would be a bug in the fast path, not a
	divergence to pin.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ id: e:1 }, { id: e:2 }]"

[[test.results]]
value = "[{ id: f:1 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 3 }]"
*/

DEFINE TABLE e SCHEMALESS;
DEFINE TABLE f SCHEMALESS;

-- A predicate that folds away: counted by key on one engine, iterated on the
-- other, and the same answer from both.
SELECT count() FROM e WHERE true GROUP ALL;

-- Several empty sources: counted once per source on one engine, scanned and
-- unioned on the other.
SELECT count() FROM e, f GROUP ALL;

-- With rows to count, the shortcut and the scan still agree.
CREATE e:1, e:2;
CREATE f:1;
SELECT count() FROM e WHERE true GROUP ALL;
SELECT count() FROM e, f GROUP ALL;
