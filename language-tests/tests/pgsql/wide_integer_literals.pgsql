/**
[env]
namespace = true
database = true
imports = ["pgsql/_wide_integers.surql"]

[test]
reason = "A decimal integer literal beyond 64 bits is a float and compares with integer values, the int8 minimum is an integer literal"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }, { id: 5 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }, { id: 3 }, { id: 5 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }, { id: 5 }]"

[[test.results]]
value = "[{ id: 4 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ lowest: -9223372036854775808 }]"

[[test.results]]
value = "[{ wide: 10000000000000000000f }]"

[[test.results]]
value = "[{ remainder: 0 }]"
*/

SELECT id FROM ledger WHERE amount > 10000000000000000000 ORDER BY id;

SELECT id FROM ledger WHERE amount < 10000000000000000000 ORDER BY id;

SELECT id FROM ledger WHERE amount > -9223372036854775808 ORDER BY id;

SELECT id FROM ledger WHERE amount >= -9223372036854775808 ORDER BY id;

SELECT id FROM ledger WHERE amount = -9223372036854775808;

SELECT id FROM ledger WHERE amount < -9223372036854775808;

SELECT -9223372036854775808 AS lowest;

SELECT 10000000000000000000 AS wide;

SELECT -9223372036854775808 % -1 AS remainder;
