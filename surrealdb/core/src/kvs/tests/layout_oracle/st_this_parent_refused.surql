-- A transform reads $value alone. A reference to the row ($this, $doc), to the
-- enclosing object ($parent), to a sibling field, or to a parameter the transform
-- does not declare is refused when the migration is defined. A refused definition
-- leaves the field, the name it gave and the rows as they were, and the name is
-- free for a legal transform afterwards.
DEFINE TABLE pr SCHEMAFULL;
DEFINE FIELD a ON pr TYPE string;
DEFINE FIELD b ON pr TYPE int;
DEFINE FIELD o ON pr TYPE object;
DEFINE FIELD o.n ON pr TYPE string;
DEFINE FIELD o.m ON pr TYPE int;
CREATE pr:1 SET a = '1', b = 2, o = { n: 'x', m: 3 } RETURN NONE;
CREATE pr:2 SET a = '2', b = 5, o = { n: 'y', m: 4 } RETURN NONE;
-- @upgrade
ALTER FIELD a ON pr TYPE int MIGRATE EXISTING ROWS AS r_this USING <int>$value + $this.b FALLBACK 0;
ALTER FIELD a ON pr TYPE int MIGRATE EXISTING ROWS AS r_parent USING <int>$value + $parent.b FALLBACK 0;
ALTER FIELD a ON pr TYPE int MIGRATE EXISTING ROWS AS r_doc USING <int>$doc.b FALLBACK 0;
ALTER FIELD a ON pr TYPE int MIGRATE EXISTING ROWS AS r_sib USING <int>$value + b FALLBACK 0;
ALTER FIELD a ON pr TYPE int MIGRATE EXISTING ROWS AS r_bare USING $this FALLBACK 0;
ALTER FIELD a ON pr TYPE int MIGRATE EXISTING ROWS AS r_param USING <int>$value + $p FALLBACK 0;
ALTER FIELD o.m ON pr TYPE int MIGRATE EXISTING ROWS AS r_nested USING $value + $parent.n FALLBACK 0;
SELECT * FROM pr ORDER BY id;
(INFO FOR TABLE pr).fields;
(INFO FOR TABLE pr).migrations;
-- The refused names are free: the same name with a legal transform.
ALTER FIELD a ON pr TYPE int MIGRATE EXISTING ROWS AS r_this USING <int>$value * 10 FALLBACK 0;
SELECT * FROM pr ORDER BY id;
-- A legal nested transform beside the refused one.
ALTER FIELD o.m ON pr TYPE int MIGRATE EXISTING ROWS AS nested_ok USING $value * 2 FALLBACK 0;
SELECT * FROM pr ORDER BY id;
-- @check
-- @drain
SELECT * FROM pr ORDER BY id;
(INFO FOR TABLE pr).migrations;
-- @check
-- A row written whole after the job ran is stamped with the current generation,
-- so the nested step does not apply to the object it holds.
UPSERT pr:3 SET a = 3, b = 3, o = { n: 'z', m: 5 } RETURN NONE;
SELECT VALUE o.m FROM pr ORDER BY id;
SELECT * FROM pr ORDER BY id;
-- @drain
-- @check
