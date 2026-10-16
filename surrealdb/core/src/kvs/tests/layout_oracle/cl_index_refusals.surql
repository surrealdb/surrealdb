-- Failing schema statements in transactions the caller owns, where each failure is an index refusal:
-- a migration of an indexed field, a removal of one, an index defined beside a migration, and a UNIQUE
-- index over values the rows collide on. A refusal that comes before its statement writes leaves the
-- caller to commit what the other statements wrote. A UNIQUE build refuses the duplicates after it
-- wrote, which refuses the commit of its transaction. The refused statements leave
-- the field, its index and the migration names as they were. A remove, migrate and define sequence
-- in one transaction takes effect as a whole, and the indexes answer as a scan of the rows does.
DEFINE TABLE ix SCHEMAFULL;
DEFINE FIELD i ON ix TYPE string;
DEFINE FIELD u ON ix TYPE string;
DEFINE FIELD n ON ix TYPE string;
DEFINE INDEX by_i ON ix FIELDS i;
CREATE ix:1 SET i = '1', u = 'a', n = '10' RETURN NONE;
CREATE ix:2 SET i = '2', u = 'b', n = '20' RETURN NONE;
CREATE ix:3 SET i = '3', u = 'a', n = '30' RETURN NONE;
-- @upgrade
-- @client
CREATE ix:4 SET i = '4', u = 'c', n = '40' RETURN NONE;
ALTER FIELD i ON ix TYPE int MIGRATE EXISTING ROWS AS i_int USING <int>$value FALLBACK 0;
REMOVE FIELD i ON ix;
ALTER FIELD n ON ix TYPE int MIGRATE EXISTING ROWS AS n_int USING <int>$value FALLBACK 0;
CREATE ix:5 SET i = '5', u = 'd', n = 50 RETURN NONE;
DEFINE INDEX by_u ON ix FIELDS u UNIQUE;
CREATE ix:6 SET i = '6', u = 'e', n = 60 RETURN NONE;
-- @end
SELECT * FROM ix ORDER BY id;
SELECT VALUE id FROM ix WHERE i = '2';
(INFO FOR TABLE ix).fields;
(INFO FOR TABLE ix).indexes;
(INFO FOR TABLE ix).migrations;
-- @check
-- A remove, migrate and define of the indexed field, then a refused migration of the same field under
-- another name, and a UNIQUE index over the migrated field that holds no duplicate.
-- @client
REMOVE INDEX by_i ON ix;
ALTER FIELD i ON ix TYPE int MIGRATE EXISTING ROWS AS i_int USING <int>$value FALLBACK 0;
DEFINE INDEX by_i ON ix FIELDS i;
ALTER FIELD i ON ix TYPE int MIGRATE EXISTING ROWS AS i_bad USING <int>$value FALLBACK 'x';
CREATE ix:7 SET i = 7, u = 'f', n = 70 RETURN NONE;
DEFINE INDEX by_n ON ix FIELDS n UNIQUE;
-- @end
SELECT * FROM ix ORDER BY id;
SELECT VALUE id FROM ix WHERE i = 2;
SELECT VALUE id FROM ix WHERE i > 3 ORDER BY id;
SELECT VALUE id FROM ix WHERE n = 30;
(INFO FOR TABLE ix).indexes;
(INFO FOR TABLE ix).migrations;
-- @drain
SELECT * FROM ix ORDER BY id;
SELECT VALUE id FROM ix WHERE i = 2;
(INFO FOR TABLE ix).migrations;
-- @check
-- A UNIQUE index refused for duplicates is not left behind: the duplicate it refused is still
-- writable in the transaction that refused it, and the refusal fails that transaction's commit, so
-- neither the definition nor the write stays.
-- @client
DEFINE INDEX by_u ON ix FIELDS u UNIQUE;
CREATE ix:8 SET i = 8, u = 'a', n = 80 RETURN NONE;
-- @end
SELECT VALUE id FROM ix WHERE u = 'a' ORDER BY id;
(INFO FOR TABLE ix).indexes;
-- @check
-- The same refused UNIQUE definition in a unit of its own: a failed statement there is rolled back, so
-- the definition must not be listed afterwards.
DEFINE INDEX by_t ON ix FIELDS u UNIQUE;
(INFO FOR TABLE ix).indexes;
SELECT VALUE id FROM ix WHERE u = 'a' ORDER BY id;
-- @check
