/**
[env]
namespace = true
database = true

[test]
reason = "A numeric above 28 digits, or without a precision, is a bigdecimal column: values of 40 and 50 digits round-trip exactly, are rounded half away from zero to the declared scale, and a value beyond the precision is refused"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ amount: 123456789012345678901234567890.1234567891bigdec, free: 1.2300bigdec, id: 1, small: 1.26dec }]"

[[test.results]]
value = "[{ amount: -123456789012345678901234567890.1234567891bigdec, free: 12345678901234567890123456789012345678901234567890.5bigdec, id: 2, small: -1.26dec }]"

[[test.results]]
value = "[{ amount: 0.0000000001bigdec, free: 7bigdec, id: 3, small: 0.00dec }]"

[[test.results]]
value = "[{ amount: 123456789012345678901234567890.1234567891bigdec, free: 1.2300bigdec, id: 1, small: 1.26dec }, { amount: -123456789012345678901234567890.1234567891bigdec, free: 12345678901234567890123456789012345678901234567890.5bigdec, id: 2, small: -1.26dec }, { amount: 0.0000000001bigdec, free: 7bigdec, id: 3, small: 0.00dec }]"

[[test.results]]
value = "[{ id: 1 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 3 }, { id: 2 }]"

[[test.results]]
value = "[{ plus_one: 123456789012345678901234567891.1234567891bigdec }]"

[[test.results]]
error = "numeric field overflow (field `amount` of `ledger:4`, SQL TYPE numeric(40,10))"

[[test.results]]
value = "[{ narrow: 1.5dec, scaled: 1.5bigdec, wide: 99999999999999999999999999999999999.5bigdec }]"

*/

CREATE TABLE ledger (id INT PRIMARY KEY, amount numeric(40,10), free numeric, small numeric(10,2));
INSERT INTO ledger (id, amount, free, small) VALUES (1, CAST('123456789012345678901234567890.1234567891' AS numeric(40,10)), CAST('1.2300' AS numeric), 1.255);
INSERT INTO ledger (id, amount, free, small) VALUES (2, '-123456789012345678901234567890.12345678905'::numeric(40,10), '12345678901234567890123456789012345678901234567890.5'::numeric, -1.255);
INSERT INTO ledger (id, amount, free, small) VALUES (3, 0.00000000005, 7, 0);
SELECT id, amount, free, small FROM ledger ORDER BY id;
SELECT id FROM ledger WHERE amount > CAST('123456789012345678901234567890.1234567890' AS numeric(40,10));
SELECT id FROM ledger ORDER BY amount DESC;
SELECT amount + 1 AS plus_one FROM ledger WHERE id = 1;
INSERT INTO ledger (id, amount) VALUES (4, CAST('1000000000000000000000000000000' AS numeric));
SELECT CAST('99999999999999999999999999999999999.5' AS numeric) AS wide, CAST('1.5' AS numeric(40,10)) AS scaled, CAST('1.5' AS numeric(10,2)) AS narrow;
