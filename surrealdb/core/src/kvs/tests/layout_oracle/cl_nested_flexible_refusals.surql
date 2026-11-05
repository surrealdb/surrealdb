-- Definition refusals in transactions the caller owns, against nested fields and the kinds that hold
-- them: a parent whose TYPE cannot hold a child, a child of an undefined parent, FLEXIBLE on a
-- non-object, the id, a REFERENCE and a COMPUTED field, and a DEFINE FIELD OVERWRITE that drops the
-- TYPE rows still hold. A child migration that succeeds in the same transaction stacks with a later
-- one of the same child, and every row reads as the fields its own write declared.
DEFINE TABLE nx SCHEMAFULL;
DEFINE FIELD o ON nx TYPE object;
DEFINE FIELD o.k ON nx TYPE string;
DEFINE FIELD s ON nx TYPE string;
DEFINE FIELD r ON nx TYPE option<record<nx>> REFERENCE;
DEFINE FIELD cc ON nx COMPUTED 1;
CREATE nx:1 SET o = { k: 'v' }, s = 'one' RETURN NONE;
CREATE nx:2 SET o = { k: 'w' }, s = 'two' RETURN NONE;
-- @upgrade
-- @client
CREATE nx:3 SET o = { k: 'z' }, s = 'three' RETURN NONE;
ALTER FIELD o.k ON nx TYPE int MIGRATE EXISTING ROWS AS k_int USING <int>$value FALLBACK 0;
ALTER FIELD o ON nx TYPE string MIGRATE EXISTING ROWS AS o_str USING <string>$value FALLBACK '';
DEFINE FIELD zz.y ON nx TYPE int;
DEFINE FIELD s.x ON nx TYPE int;
ALTER FIELD s ON nx FLEXIBLE TYPE int MIGRATE EXISTING ROWS AS sf USING <int>$value FALLBACK 0;
DEFINE FIELD OVERWRITE id ON nx TYPE int MIGRATE EXISTING ROWS AS idm USING 1 FALLBACK 1;
ALTER FIELD r ON nx MIGRATE EXISTING ROWS AS rm USING NONE;
ALTER FIELD cc ON nx TYPE int MIGRATE EXISTING ROWS AS cm USING <int>$value FALLBACK 0;
DEFINE FIELD OVERWRITE s ON nx TYPE int;
CREATE nx:4 SET o = { k: 4 }, s = 'four' RETURN NONE;
CREATE nx:5 SET o = { k: 'q' }, s = 'five' RETURN NONE;
-- @end
SELECT * FROM nx ORDER BY id;
(INFO FOR TABLE nx).fields;
(INFO FOR TABLE nx).migrations;
-- @check
-- @drain
SELECT * FROM nx ORDER BY id;
(INFO FOR TABLE nx).migrations;
-- @check
-- A second migration of the child, stacked on the first in the same transaction, a refused one of the
-- parent beside it, and a write that reads the child as the stacked steps leave it.
-- @client
ALTER FIELD o.k ON nx TYPE float MIGRATE EXISTING ROWS AS k_float USING <float>$value / 4 FALLBACK 0.5;
ALTER FIELD o ON nx TYPE array MIGRATE EXISTING ROWS AS o_arr USING [$value] FALLBACK [];
CREATE nx:6 SET o = { k: 2.5 }, s = 'six' RETURN NONE;
-- @end
SELECT * FROM nx ORDER BY id;
SELECT VALUE o.k FROM nx ORDER BY id;
(INFO FOR TABLE nx).fields;
(INFO FOR TABLE nx).migrations;
-- @drain
SELECT * FROM nx ORDER BY id;
SELECT VALUE o.k FROM nx ORDER BY id;
-- @check
