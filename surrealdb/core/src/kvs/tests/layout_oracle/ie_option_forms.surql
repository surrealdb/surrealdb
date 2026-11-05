-- Which containers carry OPTION IMPORT to the writes after it. A bare OPTION statement is a request of
-- its own and reaches nothing after it. A BEGIN block and a request that holds a LET are one request
-- each, and the option holds for every later statement of that request.
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE int;
OPTION IMPORT;
CREATE p:1 SET a = 'x' RETURN NONE;
OPTION IMPORT = false;
-- @check
BEGIN;
OPTION IMPORT;
CREATE p:2 SET a = 'x' RETURN NONE;
OPTION IMPORT = false;
COMMIT;
-- @check
SELECT * FROM p ORDER BY id;
-- @check
LET $seg = 1;
OPTION IMPORT;
CREATE p:3 SET a = 'x' RETURN NONE;
OPTION IMPORT = false;
SELECT * FROM p ORDER BY id;
(INFO FOR TABLE p).migrations.guaranteed;
