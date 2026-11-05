/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A record id's key holds each float component as the exact decimal digits of its
`f64` plus its kind, and every scan decodes that key to hand the id back. The
decode has to return the float at every magnitude, including those a
`rust_decimal::Decimal` cannot hold (above ~7.9e28), so `t:['d', 1e30]` scans
like any other id and addresses its own record.

The expectations are exact SurrealQL comparisons and counts rather than literal
ids, because the runner compares floats within 1e-15.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "7"

[[test.results]]
value = "7"

[[test.results]]
value = "true"
*/

LET $keys = [
	['a', 1e29],
	['b', 1e30],
	['c', -1e30],
	['d', 1e300],
	['e', 1.7976931348623157e308],
	['f', -1.7976931348623157e308],
	['g', 7.9e28],
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
