-- Removal and redefinition refusals in transactions the caller owns. REMOVE FIELD is refused while a
-- view reads the field, and the refusal leaves the field and its values as they were. Once the view
-- is removed in the same transaction, the removal is minted, and a failing definition after it does
-- not undo it. The removed values go for every reader once the job has run, and a field defined over
-- rows that still hold a removed value is refused until it says what those rows hold.
DEFINE TABLE src SCHEMAFULL;
DEFINE FIELD a ON src TYPE string;
DEFINE FIELD b ON src TYPE string;
DEFINE TABLE v AS SELECT a FROM src;
CREATE src:1 SET a = '1', b = 'x' RETURN NONE;
CREATE src:2 SET a = '2', b = 'y' RETURN NONE;
-- @upgrade
-- @client
REMOVE FIELD a ON src;
CREATE src:3 SET a = '3', b = 'z' RETURN NONE;
UPDATE src:1 SET a = '11' RETURN NONE;
REMOVE FIELD b ON src;
DEFINE FIELD b ON src TYPE string;
CREATE src:4 SET a = '4', b = 'w' RETURN NONE;
-- @end
SELECT * FROM src ORDER BY id;
SELECT VALUE a FROM v ORDER BY id;
(INFO FOR TABLE src).fields;
(INFO FOR TABLE src).migrations;
-- @check
-- The view goes in the transaction that removes its field. A view defined over the removed field
-- after the removal reads the field as absent, and a redefinition of the removed field, over the
-- rows that still hold it, is refused.
-- @client
REMOVE TABLE v;
REMOVE FIELD a ON src;
DEFINE TABLE v2 AS SELECT a, b FROM src;
DEFINE FIELD a ON src TYPE string;
ALTER FIELD b ON src TYPE int MIGRATE EXISTING ROWS AS b_int USING <int>$value FALLBACK 0;
CREATE src:5 SET a = 'five', b = 5 RETURN NONE;
-- @end
SELECT * FROM src ORDER BY id;
SELECT * FROM v2 ORDER BY id;
(INFO FOR TABLE src).fields;
(INFO FOR TABLE src).migrations;
-- @drain
SELECT * FROM src ORDER BY id;
(INFO FOR TABLE src).migrations;
-- @check
-- A removal of a field whose values a stacked migration still rewrites, in a transaction whose later
-- statement fails, and the field redefined afterwards, with the rows behind the removal read as the
-- declared type says they are.
DEFINE FIELD c ON src TYPE option<string>;
UPDATE src:2 SET c = 'cc' RETURN NONE;
-- @client
REMOVE FIELD c ON src;
DEFINE FIELD c ON src TYPE option<int>;
UPDATE src:2 SET c = 2 RETURN NONE;
ALTER FIELD c ON src TYPE int MIGRATE EXISTING ROWS AS c_nn USING <int>$value FALLBACK 0;
UPDATE src:2 SET c = 'nope' RETURN NONE;
-- @end
SELECT * FROM src ORDER BY id;
SELECT VALUE c FROM src ORDER BY id;
(INFO FOR TABLE src).fields;
(INFO FOR TABLE src).migrations;
-- @drain
SELECT * FROM src ORDER BY id;
(INFO FOR TABLE src).migrations;
-- @check
