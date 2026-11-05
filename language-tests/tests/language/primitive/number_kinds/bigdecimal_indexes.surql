/**
[test]
reason = "An indexed bigdecimal field is read through its index: ranges and equalities over values of 40 and 65 digits, negatives and zero, in numeric order, and a unique index holds 1.5bigdec, 1.50bigdec, 1.50dec and 1.5f as one value."

*/

DEFINE TABLE bd SCHEMAFULL;
DEFINE FIELD v ON bd TYPE bigdecimal;
DEFINE INDEX bd_v ON bd FIELDS v;

CREATE bd:one_and_a_half SET v = 1.5bigdec;
CREATE bd:big_negative SET v = -1234567890123456789012345678901234567890.5bigdec;
CREATE bd:sixty_five SET v = 12345678901234567890123456789012345678901234567890123456789012345bigdec;
CREATE bd:zero SET v = 0bigdec;
CREATE bd:two_and_a_half SET v = 2.50bigdec;
CREATE bd:tiny SET v = 0.0000000000000000000000000000000000000001bigdec;
CREATE bd:minus_tiny SET v = -0.0000000000000000000000000000000000000001bigdec;
CREATE bd:forty SET v = 9999999999999999999999999999999999999999bigdec;

-- Every value in ascending and descending order, read through the index.
SELECT VALUE id FROM bd WHERE v >= -10000000000000000000000000000000000000000000bigdec ORDER BY v ASC;
SELECT VALUE id FROM bd WHERE v >= -10000000000000000000000000000000000000000000bigdec ORDER BY v DESC;

-- Ranges whose bounds are other kinds.
SELECT VALUE id FROM bd WHERE v > 0 ORDER BY v ASC;
SELECT VALUE id FROM bd WHERE v >= 1.5dec AND v <= 2.5f ORDER BY v ASC;
SELECT VALUE id FROM bd WHERE v < 0 ORDER BY v ASC;
SELECT VALUE id FROM bd WHERE v > 9999999999999999999999999999999999999998.9bigdec ORDER BY v ASC;

-- Equality, through the index and without it.
SELECT VALUE id FROM bd WHERE v = 2.5bigdec;
SELECT VALUE id FROM bd WITH NOINDEX WHERE v = 2.5bigdec;
SELECT VALUE id FROM bd WHERE v = 12345678901234567890123456789012345678901234567890123456789012345bigdec;

-- The values keep the scale they were written with.
SELECT VALUE v FROM bd:two_and_a_half;

EXPLAIN SELECT VALUE id FROM bd WHERE v > 0 ORDER BY v ASC;
EXPLAIN SELECT VALUE id FROM bd WHERE v = 2.5bigdec;

-- A unique index holds equal values of every kind as one.
DEFINE TABLE uq SCHEMALESS;
DEFINE INDEX uq_v ON uq FIELDS v UNIQUE;
CREATE uq:a SET v = 1.5bigdec;
CREATE uq:b SET v = 1.50dec;
CREATE uq:c SET v = 1.50bigdec;
CREATE uq:d SET v = 1.5f;
CREATE uq:e SET v = 12345678901234567890123456789012345678901bigdec;
CREATE uq:f SET v = 12345678901234567890123456789012345678901.000bigdec;
CREATE uq:g SET v = 12345678901234567890123456789012345678902bigdec;
SELECT VALUE id FROM uq ORDER BY id;
