-- Claims inside transactions the caller owns. A MIGRATE minted in a transaction, rows written before and after it
-- in the same transaction, statements that fail while the caller commits anyway, and an ASSERT added in the
-- transaction over rows that break it. The commit leaves each claim what the rows hold; a later transaction
-- repairs the rows the ASSERT counted, and the verification and guarantee follow what the rows hold then.
DEFINE TABLE cs SCHEMAFULL;
DEFINE FIELD a ON cs TYPE string;
DEFINE FIELD b ON cs TYPE string;
DEFINE FIELD c ON cs TYPE option<int>;
CREATE cs:1 SET a = '1', b = 'p', c = 1 RETURN NONE;
CREATE cs:2 SET a = 'x', b = 'q', c = 2 RETURN NONE;
-- @upgrade
-- @client
ALTER FIELD a ON cs TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
(INFO FOR TABLE cs).migrations.guaranteed;
CREATE cs:3 SET a = 3, b = 'r', c = 3 RETURN NONE;
CREATE cs:4 SET a = 'four', b = 's' RETURN NONE;
ALTER FIELD b ON cs TYPE int MIGRATE EXISTING ROWS AS b_int USING <int>$value FALLBACK -1;
CREATE cs:5 SET a = 5, b = 'z' RETURN NONE;
CREATE cs:6 SET a = 6, b = 6 RETURN NONE;
ALTER FIELD c ON cs ASSERT $value > 2;
CREATE cs:7 SET a = 7, b = 7, c = 1 RETURN NONE;
UPDATE cs:1 SET c = 1 RETURN NONE;
-- @end
(INFO FOR TABLE cs).migrations.guaranteed;
(INFO FOR TABLE cs).migrations.verification;
SELECT * FROM cs ORDER BY id;
-- @check
-- @drain
SELECT * FROM cs ORDER BY id;
(INFO FOR TABLE cs).migrations.guaranteed;
(INFO FOR TABLE cs).migrations.verification;
-- @check
-- A second transaction repairs the rows the ASSERT counted, and writes one row the TYPE refuses.
-- @client
UPDATE cs:1 SET c = 3 RETURN NONE;
UPDATE cs:2 SET c = 4 RETURN NONE;
UPDATE cs:3 SET b = 'again' RETURN NONE;
UPDATE cs:6 SET b = 60 RETURN NONE;
-- @end
(INFO FOR TABLE cs).migrations.verification;
(INFO FOR TABLE cs).migrations.guaranteed;
SELECT * FROM cs ORDER BY id;
-- @check
-- @drain
(INFO FOR TABLE cs).migrations;
SELECT * FROM cs ORDER BY id;
-- @check
