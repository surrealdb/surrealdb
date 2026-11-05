-- An INLINE field keeps its value in the adjacency payload of each edge. A migration
-- of an INLINE field is refused, while a migration of a field beside it is not. INLINE
-- set or dropped over edges written before it must leave traversals, edge scans and
-- filters reading the same values, both before and after the job runs.
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE TABLE likes TYPE RELATION IN person OUT person SCHEMAFULL;
DEFINE FIELD score ON likes TYPE string;
DEFINE FIELD tag ON likes TYPE option<string>;
DEFINE FIELD note ON likes TYPE string;
CREATE person:1 SET name = 'a' RETURN NONE;
CREATE person:2 SET name = 'b' RETURN NONE;
CREATE person:3 SET name = 'c' RETURN NONE;
RELATE person:1->likes:1->person:2 SET score = '10', tag = 'x', note = 'n1' RETURN NONE;
RELATE person:1->likes:2->person:3 SET score = '3', tag = 'y', note = 'n2' RETURN NONE;
RELATE person:2->likes:3->person:1 SET score = 'z', note = 'n3' RETURN NONE;
ALTER FIELD score ON likes INLINE;
-- @upgrade
RELATE person:3->likes:4->person:1 SET score = '7', tag = 'w', note = 'n4' RETURN NONE;
SELECT * FROM likes ORDER BY id;
person:1->likes.score;
SELECT VALUE <-likes.score FROM person ORDER BY id;
-- A migration of the INLINE field is refused; a migration of the field beside it is not.
ALTER FIELD score ON likes TYPE int MIGRATE EXISTING ROWS AS score_int USING <int>$value FALLBACK -1;
ALTER FIELD note ON likes TYPE int MIGRATE EXISTING ROWS AS note_int USING <int>$value FALLBACK -1;
SELECT VALUE note FROM likes ORDER BY id;
SELECT VALUE id FROM likes WHERE score = '7';
person:1->likes[WHERE score > '5'].id;
-- @check
-- A write to the INLINE field behind the job, then INLINE added over a field the edges hold.
UPDATE likes:1 SET score = '99' RETURN AFTER;
person:1->likes.score;
ALTER FIELD tag ON likes INLINE;
person:1->likes.tag;
SELECT VALUE tag FROM likes ORDER BY id;
person:1->likes[WHERE tag = 'x'].id;
-- @drain
SELECT * FROM likes ORDER BY id;
-- Dropping INLINE: the field may then migrate.
ALTER FIELD score ON likes DROP INLINE;
ALTER FIELD score ON likes TYPE int MIGRATE EXISTING ROWS AS score_int USING <int>$value FALLBACK -1;
SELECT VALUE score FROM likes ORDER BY id;
person:1->likes.score;
person:2->likes[WHERE score > 5].score;
-- INLINE set again while the step still waits for the job.
ALTER FIELD score ON likes INLINE;
person:1->likes.score;
SELECT VALUE score FROM likes ORDER BY id;
-- @check
-- @drain
SELECT * FROM likes ORDER BY id;
person:1->likes.score;
(INFO FOR TABLE likes).fields;
-- @check
