-- Property: redefining a view with DEFINE TABLE OVERWRITE moves its source. Afterwards
-- writes to the old source no longer change the view, writes to the new source do, and a
-- migration of the old source is judged against the new definition. A view converted back
-- into a table is no longer fed by writes to its former sources, so a later write there
-- leaves the converted table as it is, and a migration of that source is accepted. The
-- pv part has no migration; the v part repeats the same moves with migrated sources.
DEFINE TABLE src SCHEMAFULL;
DEFINE FIELD a ON src TYPE string;
DEFINE TABLE other SCHEMAFULL;
DEFINE FIELD a ON other TYPE string;
CREATE src:1 SET a = '1' RETURN NONE;
CREATE other:1 SET a = '10' RETURN NONE;
DEFINE TABLE psrc SCHEMAFULL;
DEFINE FIELD a ON psrc TYPE int;
DEFINE TABLE pother SCHEMAFULL;
DEFINE FIELD a ON pother TYPE int;
CREATE psrc:1 SET a = 1 RETURN NONE;
CREATE pother:1 SET a = 10 RETURN NONE;
-- @upgrade
DEFINE TABLE pv AS SELECT a FROM psrc;
DEFINE TABLE OVERWRITE pv AS SELECT a FROM pother;
SELECT * FROM pv ORDER BY id;
UPDATE psrc:1 SET a = 5 RETURN NONE;
SELECT * FROM pv ORDER BY id;
UPDATE pother:1 SET a = 20 RETURN NONE;
SELECT * FROM pv ORDER BY id;
DEFINE TABLE OVERWRITE pv SCHEMAFULL;
DEFINE FIELD OVERWRITE a ON pv TYPE option<int>;
CREATE pother:2 SET a = 30 RETURN NONE;
UPDATE psrc:1 SET a = 6 RETURN NONE;
SELECT * FROM pv ORDER BY id;
ALTER FIELD a ON src TYPE int MIGRATE EXISTING ROWS AS src_int USING <int>$value FALLBACK 0;
ALTER FIELD a ON other TYPE int MIGRATE EXISTING ROWS AS other_int USING <int>$value FALLBACK 0;
DEFINE TABLE v AS SELECT a FROM src;
SELECT * FROM v ORDER BY id;
DEFINE TABLE OVERWRITE v AS SELECT a FROM other;
SELECT * FROM v ORDER BY id;
ALTER FIELD a ON src TYPE int MIGRATE EXISTING ROWS AS src_plus USING $value + 1 FALLBACK 0;
UPDATE other:1 SET a = 20 RETURN NONE;
SELECT * FROM v ORDER BY id;
UPDATE src:1 SET a = 5 RETURN NONE;
SELECT * FROM v ORDER BY id;
-- @check
ALTER FIELD a ON other TYPE int MIGRATE EXISTING ROWS AS other_plus USING $value + 1 FALLBACK 0;
SELECT * FROM v ORDER BY id;
-- @drain
SELECT * FROM v ORDER BY id;
DEFINE TABLE OVERWRITE v SCHEMAFULL;
DEFINE FIELD OVERWRITE a ON v TYPE option<int>;
SELECT * FROM v ORDER BY id;
CREATE other:3 SET a = 30 RETURN NONE;
UPDATE src:1 SET a = 6 RETURN NONE;
SELECT * FROM v ORDER BY id;
ALTER FIELD a ON other TYPE int MIGRATE EXISTING ROWS AS other_x2 USING $value * 2 FALLBACK 0;
SELECT * FROM other ORDER BY id;
-- @check
-- Removing a redefined view: both of the sources it has read are free again, and their
-- migrations are accepted, when the view is removed as a table.
REMOVE TABLE v;
ALTER FIELD a ON other TYPE int MIGRATE EXISTING ROWS AS other_x2 USING $value * 2 FALLBACK 0;
ALTER FIELD a ON src TYPE int MIGRATE EXISTING ROWS AS src_x5 USING $value * 5 FALLBACK 0;
SELECT * FROM other ORDER BY id;
SELECT * FROM src ORDER BY id;
-- @drain
SELECT * FROM other ORDER BY id;
-- @check
-- A view that is still a view when it is removed, after OVERWRITE moved it: both of its
-- sources are free again, and their migrations are accepted.
DEFINE TABLE q1 SCHEMAFULL;
DEFINE FIELD a ON q1 TYPE int;
DEFINE TABLE q2 SCHEMAFULL;
DEFINE FIELD a ON q2 TYPE int;
CREATE q1:1 SET a = 1 RETURN NONE;
CREATE q2:1 SET a = 2 RETURN NONE;
DEFINE TABLE w AS SELECT a FROM q1;
DEFINE TABLE OVERWRITE w AS SELECT a FROM q2;
REMOVE TABLE w;
ALTER FIELD a ON q2 TYPE int MIGRATE EXISTING ROWS AS q2_x7 USING $value * 7 FALLBACK 0;
ALTER FIELD a ON q1 TYPE int MIGRATE EXISTING ROWS AS q1_x7 USING $value * 7 FALLBACK 0;
SELECT * FROM q1 ORDER BY id;
SELECT * FROM q2 ORDER BY id;
-- @drain
SELECT * FROM q1 ORDER BY id;
SELECT * FROM q2 ORDER BY id;
-- @check
