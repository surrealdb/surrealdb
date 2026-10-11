/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A decimal in a record id is stored as its digits and exponent, and every scan
decodes that key to hand the id back. The decode has to read a decimal at every
scale a `Decimal` holds, including 7 or more fractional digits
(`0.0000001dec`), whose `D128` text is in scientific notation, and it has to
return a decimal with its kind, so the id addresses its own record.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "8"

[[test.results]]
value = "8"

[[test.results]]
value = "true"

[[test.results]]
value = "8"
*/

LET $keys = [
	['a', 0.0000001dec],
	['b', 0.00000000000000000000000001dec],
	['c', -0.0000000121dec],
	['d', 7.9228162514264337593543950335dec],
	['e', 79228162514264337593543950335dec],
	['f', 1.50dec],
	['g', 1234567.1234567dec],
	['h', 0dec],
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
-- -- Every component is still a decimal.
(SELECT VALUE id FROM t WHERE type::is_decimal(record::id(id)[1])).len();
