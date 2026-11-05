-- DELETE and CREATE while steps are pending. A deleted row is not brought back when the
-- job reaches it, a record created under a deleted id is migrated only by the steps
-- minted after its creation, and an index defined after the migrations answers DELETE
-- WHERE with the migrated value.
DEFINE TABLE d SCHEMAFULL;
DEFINE FIELD n ON d TYPE string;
DEFINE FIELD tag ON d TYPE string;
CREATE d:1 SET n = '1', tag = 'x';
CREATE d:2 SET n = '2', tag = 'x';
CREATE d:3 SET n = '3', tag = 'y';
CREATE d:4 SET n = '4', tag = 'y';
CREATE d:5 SET n = '5', tag = 'z';
-- @upgrade
CREATE d:6 SET n = '6', tag = 'z';
ALTER FIELD n ON d TYPE int MIGRATE EXISTING ROWS AS n_plus USING <int>$value + 10 FALLBACK -1;
DELETE d:1 RETURN BEFORE;
CREATE d:1 SET n = 1, tag = 'fresh';
DELETE d:2;
DELETE d WHERE n > 13 RETURN BEFORE;
CREATE d:4 SET n = 4, tag = 'again';
CREATE d:7 SET n = 7, tag = 'between';
SELECT VALUE id FROM d WHERE n = 13 ORDER BY id;
-- @check
ALTER FIELD n ON d TYPE int MIGRATE EXISTING ROWS AS n_double USING $value * 2 FALLBACK 0;
DEFINE INDEX by_n ON d FIELDS n;
DELETE d:3 RETURN BEFORE;
CREATE d:3 SET n = 3, tag = 're';
SELECT VALUE id FROM d WHERE n = 14 ORDER BY id;
DELETE d WHERE n = 8 RETURN BEFORE;
-- @drain
SELECT VALUE [record::id(id), n, tag] FROM d ORDER BY id;
SELECT VALUE id FROM d WHERE n > 0 ORDER BY id;
