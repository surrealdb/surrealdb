-- Restating a migration by name: the same name with the same step is a no-op
-- before the job runs, after it, and inside a transaction its caller commits.
-- The same name with any other transform, FALLBACK, type or field is refused,
-- mints nothing and leaves the field as it was. Names are per table, so a name
-- one table ran is free on another.
DEFINE TABLE r SCHEMAFULL;
DEFINE FIELD n ON r TYPE string;
DEFINE FIELD m ON r TYPE string;
CREATE r:1 SET n = '1', m = 'a' RETURN NONE;
CREATE r:2 SET n = '2', m = 'b' RETURN NONE;
-- @upgrade
ALTER FIELD n ON r TYPE int MIGRATE EXISTING ROWS AS to_int USING <int>$value FALLBACK 0;
(INFO FOR TABLE r).migrations;
-- The same statement again, before the job runs: a no-op.
ALTER FIELD n ON r TYPE int MIGRATE EXISTING ROWS AS to_int USING <int>$value FALLBACK 0;
(INFO FOR TABLE r).migrations;
-- Under the same name: another transform, another FALLBACK, another type, and
-- another field. Each is refused.
ALTER FIELD n ON r TYPE int MIGRATE EXISTING ROWS AS to_int USING <int>$value * 1 FALLBACK 0;
ALTER FIELD n ON r TYPE int MIGRATE EXISTING ROWS AS to_int USING <int>$value FALLBACK 1;
ALTER FIELD n ON r TYPE float MIGRATE EXISTING ROWS AS to_int USING <float>$value FALLBACK 0f;
ALTER FIELD m ON r TYPE int MIGRATE EXISTING ROWS AS to_int USING <int>$value FALLBACK 0;
(INFO FOR TABLE r).fields;
SELECT * FROM r ORDER BY id;
(INFO FOR TABLE r).migrations;
-- A name the table has not run is free for another field.
ALTER FIELD m ON r TYPE int MIGRATE EXISTING ROWS AS m_int USING <int>$value FALLBACK 0;
SELECT * FROM r ORDER BY id;
-- @drain
SELECT * FROM r ORDER BY id;
-- The same step after the job ran: still a no-op.
ALTER FIELD n ON r TYPE int MIGRATE EXISTING ROWS AS to_int USING <int>$value FALLBACK 0;
ALTER FIELD m ON r TYPE int MIGRATE EXISTING ROWS AS m_int USING <int>$value FALLBACK 0;
(INFO FOR TABLE r).migrations;
-- A different transform under a name the job has run: refused.
ALTER FIELD n ON r TYPE int MIGRATE EXISTING ROWS AS to_int USING <int>$value + 0 FALLBACK 0;
SELECT * FROM r ORDER BY id;
-- @check
-- Inside a transaction the caller commits: a restatement is a no-op, a refused
-- restatement fails and the transaction keeps what the statements before it wrote.
-- @client
CREATE r:3 SET n = 3, m = 7 RETURN NONE;
ALTER FIELD n ON r TYPE int MIGRATE EXISTING ROWS AS to_int USING <int>$value FALLBACK 0;
ALTER FIELD n ON r TYPE int MIGRATE EXISTING ROWS AS to_int USING <int>$value FALLBACK 9;
CREATE r:4 SET n = 4, m = 8 RETURN NONE;
ALTER FIELD m ON r TYPE int MIGRATE EXISTING ROWS AS m_wide USING <int>$value FALLBACK 0;
-- @end
SELECT * FROM r ORDER BY id;
(INFO FOR TABLE r).migrations;
-- A restatement of a name the job has run, after the field's definition has
-- since been widened. The statement succeeds, and its TYPE clause is not applied
-- because the step matches the one that ran.
DEFINE FIELD OVERWRITE n ON r TYPE int | string;
ALTER FIELD n ON r TYPE int MIGRATE EXISTING ROWS AS to_int USING <int>$value FALLBACK 0;
(INFO FOR TABLE r).fields;
-- @drain
(INFO FOR TABLE r).fields;
SELECT * FROM r ORDER BY id;
-- @check
-- The same name on another table is a different migration.
DEFINE TABLE r2 SCHEMAFULL;
DEFINE FIELD n ON r2 TYPE string;
CREATE r2:1 SET n = '5' RETURN NONE;
ALTER FIELD n ON r2 TYPE int MIGRATE EXISTING ROWS AS to_int USING <int>$value FALLBACK 0;
SELECT * FROM r2;
-- @drain
SELECT * FROM r ORDER BY id;
SELECT * FROM r2;
(INFO FOR TABLE r).migrations;
(INFO FOR TABLE r2).migrations;
-- @check
