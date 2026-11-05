/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A float in a record id is stored as its exact decimal digits and must decode to
the same `f64` bit for bit, because a scan hands the decoded id back and a read
of that id re-encodes it. That holds for a 17-digit float (`1.6330027767028172`)
and for one below ~1e-6 (`5.6527459748369866e-8`, `1e-20`), whose digits carry a
scientific-notation exponent.

A decode that lands on a neighbouring float is not an error: the table scans
cleanly and returns ids that are not the ones that were created, and a read of
one finds nothing. The expectations are therefore exact SurrealQL comparisons
and counts rather than literal ids, because the runner compares floats within
1e-15, which is wider than the drift.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "12"

[[test.results]]
value = "12"

[[test.results]]
value = "true"
*/

LET $keys = [
	['a', 1.6330027767028172],
	['b', 0.30000000000000004],
	['c', 0.1],
	['d', 0.006974602931802843],
	['e', 5.6527459748369866e-8],
	['f', 1e-7],
	['g', 1e-20],
	['h', 2.2250738585072014e-308],
	['i', 5e-324],
	['j', -5e-324],
	['k', 9007199254740992.0],
	['l', 23.862949262519116],
];
FOR $k IN $keys { CREATE type::record('t', $k); };
-- A scan decodes every stored key.
LET $ids = SELECT VALUE id FROM t ORDER BY id;
$ids.len();
-- The ids a scan returns each address their own record.
(SELECT VALUE id FROM $ids).len();
-- ...because every one decodes to exactly the key it was written under. The
-- comparison is exact where the runner's own float comparison is not.
(SELECT VALUE record::id(id) FROM t ORDER BY id) == $keys;
