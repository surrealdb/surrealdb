-- Property: a table converted into a view and back with DEFINE TABLE OVERWRITE neither
-- resurrects the rows it held before the conversion nor loses the rows the view holds
-- once it is a table again, and writes to the source of a view that is no longer a view
-- do not reach the table. Part one shares ids with its source and converts a SCHEMAFULL
-- table, which drops the table's field definitions and steps. Part two uses ids disjoint
-- from its source on a SCHEMALESS table, so any row that reappears can only be a stored row.
DEFINE TABLE src SCHEMAFULL;
DEFINE FIELD a ON src TYPE int;
CREATE src:1 SET a = 1 RETURN NONE;
CREATE src:2 SET a = 2 RETURN NONE;
DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD n ON t TYPE string;
CREATE t:1 SET n = '5' RETURN NONE;
CREATE t:2 SET n = 'junk' RETURN NONE;
DEFINE TABLE src2 SCHEMAFULL;
DEFINE FIELD a ON src2 TYPE int;
CREATE src2:1 SET a = 1 RETURN NONE;
DEFINE TABLE t2 SCHEMALESS;
DEFINE FIELD n ON t2 TYPE string;
CREATE t2:10 SET n = '5' RETURN NONE;
CREATE t2:11 SET n = 'junk' RETURN NONE;
-- @upgrade
ALTER FIELD n ON t TYPE int MIGRATE EXISTING ROWS AS n_int USING <int>$value FALLBACK -1;
ALTER FIELD n ON t2 TYPE int MIGRATE EXISTING ROWS AS n_int USING <int>$value FALLBACK -1;
-- @drain
SELECT * FROM t ORDER BY id;
SELECT * FROM t2 ORDER BY id;
DEFINE TABLE OVERWRITE t AS SELECT a AS n FROM src;
DEFINE TABLE OVERWRITE t2 AS SELECT a AS n FROM src2;
SELECT * FROM t ORDER BY id;
SELECT * FROM t2 ORDER BY id;
SELECT * FROM t2:10;
CREATE src:3 SET a = 3 RETURN NONE;
CREATE src2:2 SET a = 2 RETURN NONE;
SELECT * FROM t ORDER BY id;
SELECT * FROM t2 ORDER BY id;
-- @check
DEFINE TABLE OVERWRITE t SCHEMAFULL;
DEFINE TABLE OVERWRITE t2 SCHEMALESS;
SELECT * FROM t ORDER BY id;
SELECT * FROM t2 ORDER BY id;
(INFO FOR TABLE t).migrations;
CREATE src:4 SET a = 4 RETURN NONE;
CREATE src2:3 SET a = 3 RETURN NONE;
SELECT * FROM t ORDER BY id;
SELECT * FROM t2 ORDER BY id;
REMOVE TABLE src;
REMOVE TABLE src2;
-- @check
