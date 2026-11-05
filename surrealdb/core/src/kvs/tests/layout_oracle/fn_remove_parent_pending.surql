-- Removal while a child's step is still pending on rows the job has not reached. Removing a parent
-- cascades to its child; a parent re-declared under the same path, of another kind, must not show a
-- removed row's value. Removing a child whose step is pending must leave no stepped value behind when
-- the child is re-declared with its own kind. A control table removes its parent with no step pending.
DEFINE TABLE rp SCHEMAFULL;
DEFINE FIELD o ON rp TYPE option<object>;
DEFINE FIELD o.n ON rp TYPE option<string>;
DEFINE FIELD p ON rp TYPE option<string>;
CREATE rp:1 SET o = { n: '1' }, p = 'p1' RETURN NONE;
CREATE rp:2 SET o = { n: '2' }, p = 'p2' RETURN NONE;
CREATE rp:3 SET p = 'p3' RETURN NONE;
DEFINE TABLE rq SCHEMAFULL;
DEFINE FIELD o ON rq TYPE option<object>;
DEFINE FIELD o.n ON rq TYPE option<string>;
CREATE rq:1 SET o = { n: '1' } RETURN NONE;
CREATE rq:2 SET o = {} RETURN NONE;
DEFINE TABLE rs SCHEMAFULL;
DEFINE FIELD o ON rs TYPE option<object>;
DEFINE FIELD o.n ON rs TYPE option<string>;
CREATE rs:1 SET o = { n: 'plain' } RETURN NONE;
-- @upgrade
ALTER FIELD o.n ON rp TYPE option<int> MIGRATE EXISTING ROWS AS n_int USING <int>$value + 1000 FALLBACK -1;
REMOVE FIELD o ON rp;
(INFO FOR TABLE rp).fields;
(INFO FOR TABLE rp).migrations;
SELECT * FROM rp ORDER BY id;
CREATE rp:4 SET o = { n: '4' } RETURN AFTER;
-- A parent re-declared under the same path, of another kind.
DEFINE FIELD o ON rp TYPE option<string>;
SELECT * FROM rp ORDER BY id;
UPDATE rp:1 SET o = 'again' RETURN AFTER;
-- @check
-- The control: the removed parent had no step pending.
REMOVE FIELD o ON rs;
DEFINE FIELD o ON rs TYPE option<string>;
SELECT * FROM rs ORDER BY id;
-- A child removed while its step is pending, under a parent that stays.
ALTER FIELD o.n ON rq TYPE option<int> MIGRATE EXISTING ROWS AS rq_int USING <int>$value + 1000 FALLBACK -1;
REMOVE FIELD o.n ON rq;
(INFO FOR TABLE rq).fields;
(INFO FOR TABLE rq).migrations;
SELECT * FROM rq ORDER BY id;
DEFINE FIELD o.n ON rq TYPE option<string>;
SELECT * FROM rq ORDER BY id;
UPDATE rq:1 SET o.n = 'back' RETURN AFTER;
-- @drain
SELECT * FROM rp ORDER BY id;
SELECT * FROM rq ORDER BY id;
SELECT * FROM rs ORDER BY id;
-- @check
