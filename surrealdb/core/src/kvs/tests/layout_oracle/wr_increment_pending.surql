-- += and -= on a numeric and an array field with pending steps read the value every
-- earlier step produced and store the result once. A row written between two steps
-- is migrated by the later step only, a row created after a step is not migrated
-- by that step, and a SET expression that reads a pending field sees its migrated value.
DEFINE TABLE cnt SCHEMAFULL;
DEFINE FIELD c ON cnt TYPE string;
DEFINE FIELD arr ON cnt TYPE array<string>;
DEFINE FIELD total ON cnt TYPE option<int>;
DEFINE FIELD copy ON cnt TYPE option<int>;
CREATE cnt:1 SET c = '1', arr = ['a'];
CREATE cnt:2 SET c = '2', arr = ['b', 'c'];
CREATE cnt:3 SET c = '3', arr = [];
-- @upgrade
ALTER FIELD c ON cnt TYPE int MIGRATE EXISTING ROWS AS c_x10 USING <int>$value * 10 FALLBACK 0;
ALTER FIELD arr ON cnt TYPE array<string> MIGRATE EXISTING ROWS AS arr_m USING array::append($value, 'm') FALLBACK [];
UPDATE cnt:1 SET total = 5;
UPDATE cnt:2 SET c += 3 RETURN BEFORE;
UPDATE cnt:3 SET c -= 1, arr += 'z' RETURN BEFORE;
UPDATE cnt:1 SET arr -= 'a' RETURN AFTER;
CREATE cnt:4 SET c = 4, arr = ['d'];
ALTER FIELD c ON cnt TYPE int MIGRATE EXISTING ROWS AS c_plus1 USING $value + 1 FALLBACK 0;
UPDATE cnt:1 SET c += 100 RETURN BEFORE;
UPDATE cnt:4 SET c -= 2 RETURN BEFORE;
UPDATE cnt:2 SET copy = c RETURN AFTER;
UPDATE cnt:3 SET copy = c * 2 RETURN AFTER;
CREATE cnt:5 SET c = 50, arr = ['e'];
SELECT VALUE [record::id(id), c, arr, total, copy] FROM cnt ORDER BY id;
-- @check
SELECT VALUE id FROM cnt WHERE c > 20 ORDER BY id;
-- @drain
SELECT VALUE [record::id(id), c, arr, total, copy] FROM cnt ORDER BY id;
