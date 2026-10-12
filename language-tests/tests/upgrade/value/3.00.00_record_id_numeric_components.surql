/**
[env]
imports = ["upgrade/value/3.00.00_record_id_numeric_components_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
version = ">=3.3.4"
reason = """
A record id with a numeric component is stored under a key holding the number's
decimal digits and its kind, and every scan decodes that key to hand the id
back. Whichever release wrote the key, a float at or above ~1e29, a float below
~1e-6, a 17-digit float and a decimal with 7 or more fractional digits decode to
the value they were created with and address their own record.

The comparison is exact where the runner's own float comparison is not: a
one-ulp drift is not an error and returns the same number of rows.

The upgrade target is the current checkout. A release that predates the decode
fix cannot read these keys itself, so a hop onto one is excluded; the seed axis
sends every released version straight to the current build.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "14"

[[test.results]]
value = "14"

[[test.results]]
value = "true"

[[test.results]]
value = "4"
*/

LET $keys = [
	['a', 1e30],
	['b', -1e30],
	['c', 1.7976931348623157e308],
	['d', 1.6330027767028172],
	['e', 0.30000000000000004],
	['f', 5.6527459748369866e-8],
	['g', 1e-20],
	['h', 5e-324],
	['i', 0.0000001dec],
	['j', 0.00000000000000000000000001dec],
	['k', 79228162514264337593543950335dec],
	['l', 1.5],
	['m', 1000],
	['n', 1.50dec],
];
-- A scan decodes every stored key.
LET $ids = SELECT VALUE id FROM numkey ORDER BY id;
$ids.len();
-- The ids it returns each address their own record.
(SELECT VALUE id FROM $ids).len();
-- Every one decodes to exactly the key it was written under.
(SELECT VALUE record::id(id) FROM numkey ORDER BY id) == $keys;
-- Decimal components keep their kind.
(SELECT VALUE id FROM numkey WHERE type::is_decimal(record::id(id)[1])).len();
