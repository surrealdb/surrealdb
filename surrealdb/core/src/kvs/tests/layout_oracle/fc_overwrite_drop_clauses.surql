-- DEFINE FIELD OVERWRITE and the DROP forms on a populated enforced table: a
-- redefinition replaces every clause the field carried, a dropped REFERENCE
-- purges its backlinks, a dropped ASSERT, READONLY, DEFAULT ALWAYS or VALUE stops
-- applying to later writes while stored values stay, and a Guaranteed field
-- stays Guaranteed only while its TYPE is restated unchanged.
DEFINE TABLE author SCHEMAFULL;
DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD n ON t TYPE string;
DEFINE FIELD by ON t TYPE option<record<author>> REFERENCE ON DELETE CASCADE;
DEFINE FIELD tag ON t TYPE option<string> DEFAULT ALWAYS 'd' READONLY ASSERT $value != 'x' COMMENT 'c';
DEFINE FIELD up ON t TYPE option<string> VALUE $value ?? 'none';
CREATE author:1, author:2 RETURN NONE;
CREATE t:1 SET n = '1', by = author:1, tag = 'a', up = 'u1' RETURN NONE;
CREATE t:2 SET n = '2', by = author:2, tag = 'b' RETURN NONE;
CREATE t:3 SET n = '3' RETURN NONE;
-- @upgrade
ALTER FIELD n ON t TYPE int MIGRATE EXISTING ROWS AS n_int USING <int>$value FALLBACK 0;
-- @drain
(INFO FOR TABLE t).migrations.guaranteed;
SELECT * FROM t ORDER BY id;
-- Overwritten with only TYPE: the backlinks go, the links stay.
DEFINE FIELD OVERWRITE by ON t TYPE option<record<author>>;
SELECT id, <~t AS b FROM author ORDER BY id;
DELETE author:1;
SELECT * FROM t ORDER BY id;
-- The overwrite of tag drops DEFAULT ALWAYS, READONLY, ASSERT and COMMENT.
DEFINE FIELD OVERWRITE tag ON t TYPE option<string>;
(INFO FOR TABLE t).fields.tag;
UPDATE t:2 SET tag = 'x', n = 2 RETURN NONE;
UPDATE t:2 SET tag = 'y' RETURN NONE;
CREATE t:4 SET n = 4 RETURN NONE;
SELECT * FROM t ORDER BY id;
-- The up VALUE is dropped: a write no longer recomputes it.
ALTER FIELD up ON t DROP VALUE;
UPDATE t:2 SET up = NONE RETURN NONE;
SELECT id, up FROM t ORDER BY id;
-- A restated TYPE keeps n Guaranteed; a changed TYPE forfeits it.
DEFINE FIELD OVERWRITE n ON t TYPE int;
(INFO FOR TABLE t).migrations.guaranteed;
DEFINE FIELD OVERWRITE n ON t TYPE option<int>;
(INFO FOR TABLE t).migrations.guaranteed;
(INFO FOR TABLE t).migrations.generation;
SELECT * FROM t ORDER BY id;
-- DROP forms, several in one statement.
ALTER FIELD by ON t DROP REFERENCE;
ALTER FIELD tag ON t DROP COMMENT;
ALTER FIELD n ON t DROP COMMENT;
(INFO FOR TABLE t).fields;
SELECT * FROM t ORDER BY id;
-- Two overwrites whose values cannot take the new TYPE: a MIGRATE whose USING
-- yields a string for an int field, and a TYPE change that the stored strings
-- do not satisfy without a MIGRATE. Neither may leave a step, a generation or a
-- changed definition behind.
DEFINE FIELD OVERWRITE n ON t TYPE int MIGRATE EXISTING ROWS AS n_back USING <string>$value FALLBACK '';
DEFINE FIELD OVERWRITE up ON t TYPE option<int>;
SELECT * FROM t ORDER BY id;
(INFO FOR TABLE t).fields;
(INFO FOR TABLE t).migrations.generation;
(INFO FOR TABLE t).migrations.guaranteed;
-- @check
-- @drain
SELECT * FROM t ORDER BY id;
(INFO FOR TABLE t).migrations.guaranteed;
-- @check
-- Isolated: which statements keep a Guaranteed claim. A new field on the same
-- table, a restated TYPE with nothing else changed, then a widening.
DEFINE TABLE iso SCHEMAFULL;
DEFINE FIELD m ON iso TYPE string;
CREATE iso:1 SET m = '1' RETURN NONE;
CREATE iso:2 SET m = '2' RETURN NONE;
ALTER FIELD m ON iso TYPE int MIGRATE EXISTING ROWS AS m_int USING <int>$value FALLBACK 0;
-- @drain
(INFO FOR TABLE iso).migrations.guaranteed;
DEFINE FIELD o ON iso TYPE option<int>;
(INFO FOR TABLE iso).migrations.guaranteed;
DEFINE FIELD OVERWRITE m ON iso TYPE int;
(INFO FOR TABLE iso).migrations.guaranteed;
DEFINE FIELD OVERWRITE m ON iso TYPE option<int>;
(INFO FOR TABLE iso).migrations.guaranteed;
-- @drain
(INFO FOR TABLE iso).migrations.guaranteed;
-- A kept VALUE and DEFAULT that read or hold a value of the old TYPE: a TYPE
-- change must restate or drop them, whatever the migration does.
DEFINE FIELD v ON iso TYPE option<string> VALUE $value ?? 'x' ASSERT $value != 'bad';
DEFINE FIELD w ON iso TYPE option<string> DEFAULT 'w0';
CREATE iso:3 SET m = 3, v = 'q' RETURN NONE;
ALTER FIELD v ON iso TYPE option<int> MIGRATE EXISTING ROWS AS v_int USING <int>$value FALLBACK 0;
ALTER FIELD w ON iso TYPE option<int> MIGRATE EXISTING ROWS AS w_int USING <int>$value FALLBACK 0;
ALTER FIELD v ON iso TYPE option<int> VALUE $value ?? 0 ASSERT $value != 5 MIGRATE EXISTING ROWS AS v_int2 USING <int>$value FALLBACK 0;
-- @drain
SELECT * FROM iso ORDER BY id;
(INFO FOR TABLE iso).fields;
(INFO FOR TABLE iso).migrations.guaranteed;
-- @check
