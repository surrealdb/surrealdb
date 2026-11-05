-- A target field migrated twice: each row applies the steps it missed, in order, and graph reads through a
-- relation (->edge->node.field, <-edge<-node.field, an edge filter, and FETCH of an edge end) answer the
-- value the row holds after them, for edges written before and after the steps.
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE TABLE post SCHEMAFULL;
DEFINE FIELD score ON post TYPE string;
DEFINE TABLE likes TYPE RELATION IN person OUT post SCHEMAFULL;
DEFINE FIELD w ON likes TYPE string;
CREATE person:1 SET name = 'Ann' RETURN NONE;
CREATE person:2 SET name = 'Bob' RETURN NONE;
CREATE post:1 SET score = '4' RETURN NONE;
CREATE post:2 SET score = '5' RETURN NONE;
CREATE post:3 SET score = 'x' RETURN NONE;
RELATE person:1->likes:a->post:1 SET w = '2' RETURN NONE;
RELATE person:1->likes:b->post:2 SET w = '1' RETURN NONE;
RELATE person:2->likes:c->post:3 SET w = '3' RETURN NONE;
RELATE person:2->likes:d->post:1 SET w = '9' RETURN NONE;
-- @upgrade
RELATE person:2->likes:e->post:2 SET w = '4' RETURN NONE;
ALTER FIELD score ON post TYPE int MIGRATE EXISTING ROWS AS score_int USING (<int>$value) * 10 FALLBACK -1;
ALTER FIELD score ON post TYPE float MIGRATE EXISTING ROWS AS score_f USING (<float>$value) / 4 FALLBACK 0;
SELECT VALUE score FROM post ORDER BY id;
SELECT VALUE type::is_float(score) FROM post ORDER BY id;
SELECT VALUE ->likes->post.score FROM person ORDER BY id;
person:1->likes->post.score;
SELECT VALUE ->likes[WHERE w > '1']->post.score FROM ONLY person:1;
SELECT VALUE <-likes<-person.name FROM ONLY post:1;
SELECT VALUE <-likes.w FROM ONLY post:2;
SELECT VALUE ->likes[WHERE w > '2']->post.id FROM person ORDER BY id;
SELECT out.score AS s, in.name AS n FROM likes ORDER BY id;
SELECT * FROM likes ORDER BY id FETCH out;
SELECT VALUE id FROM post WHERE score > 10 ORDER BY id;
-- @check
RELATE person:1->likes:f->post:3 SET w = '5' RETURN NONE;
SELECT VALUE ->likes->post.score FROM ONLY person:1;
UPDATE post:1 SET score = 7.5 RETURN AFTER;
SELECT VALUE ->likes->post.score FROM ONLY person:1;
-- @drain
SELECT VALUE score FROM post ORDER BY id;
SELECT VALUE ->likes->post.score FROM person ORDER BY id;
DELETE post:2 RETURN BEFORE;
SELECT VALUE ->likes->post.score FROM person ORDER BY id;
SELECT VALUE ->likes->post.id FROM person ORDER BY id;
-- @check
