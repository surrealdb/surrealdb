-- FLEXIBLE objects with declared children. A child's step keeps the undeclared siblings of that child;
-- a literal shape that would strand a declared child is refused; a FLEXIBLE object that is not
-- rewritten keeps its undeclared keys; a FALLBACK on a parent replaces the whole parent, declared
-- children included; FLEXIBLE added to a strict object admits undeclared keys afterwards, and a later
-- child step leaves them in place; afterwards a write with a key the object does not declare is refused.
DEFINE TABLE fl SCHEMAFULL;
DEFINE FIELD o ON fl TYPE option<object> FLEXIBLE;
DEFINE FIELD o.n ON fl TYPE option<string>;
DEFINE FIELD o.deep ON fl TYPE option<object> FLEXIBLE;
DEFINE TABLE fb SCHEMAFULL;
DEFINE FIELD o ON fb TYPE option<object> FLEXIBLE;
DEFINE FIELD o.n ON fb TYPE option<int>;
DEFINE TABLE fa SCHEMAFULL;
DEFINE FIELD o ON fa TYPE option<object>;
DEFINE FIELD o.n ON fa TYPE option<string>;
CREATE fl:1 SET o = { n: '1', extra: 'keep', deep: { x: 1 } } RETURN NONE;
CREATE fl:2 SET o = { extra: 'only' } RETURN NONE;
CREATE fl:3 SET o = {} RETURN NONE;
CREATE fl:4 RETURN NONE;
CREATE fb:1 SET o = { n: 1, extra: 'keep' } RETURN NONE;
CREATE fb:2 SET o = { n: 2 } RETURN NONE;
CREATE fa:1 SET o = { n: '1' } RETURN NONE;
CREATE fa:2 SET o = {} RETURN NONE;
-- @upgrade
CREATE fl:5 SET o = { n: '5', junk: [1, 2], deep: { y: 'q' } } RETURN NONE;
ALTER FIELD o.n ON fl TYPE option<int> MIGRATE EXISTING ROWS AS n_int USING <int>$value + 1000 FALLBACK -1;
(INFO FOR TABLE fl).migrations;
SELECT * FROM fl ORDER BY id;
-- @check
-- Refused: a literal shape with no room for the declared child `deep`.
DEFINE FIELD OVERWRITE o ON fl TYPE option<{ n: option<int> }> MIGRATE EXISTING ROWS AS o_strict_bad USING $value FALLBACK NONE;
-- Refused: the USING reads `$value.n` through an object, which may hold a record link.
ALTER FIELD o ON fl DROP FLEXIBLE MIGRATE EXISTING ROWS AS o_strict USING { n: $value.n, deep: $value.deep } FALLBACK {};
-- Accepted: `fl.o` is still FLEXIBLE, because the DROP above was refused, so an undeclared key is admitted.
UPDATE fl:2 SET o.zz = 1 RETURN AFTER;
-- Accepted: `deep` is a declared FLEXIBLE child.
CREATE fl:9 SET o = { n: 9, deep: { q: 1 } } RETURN AFTER;
-- Refused: `n` is an int now.
CREATE fl:10 SET o = { n: '10' } RETURN AFTER;
-- A FALLBACK on the parent replaces the whole object: fb:1 has an undeclared key, so it falls back to {}.
ALTER FIELD o ON fb DROP FLEXIBLE MIGRATE EXISTING ROWS AS fb_strict USING $value FALLBACK {};
(INFO FOR TABLE fb).migrations;
SELECT * FROM fb ORDER BY id;
-- FLEXIBLE added to a strict object, then a child step over rows that now carry undeclared keys.
UPDATE fa:1 SET o.extra = 'new' RETURN AFTER;
DEFINE FIELD OVERWRITE o ON fa TYPE option<object> FLEXIBLE MIGRATE EXISTING ROWS AS fa_flex USING $value FALLBACK NONE;
(INFO FOR TABLE fa).migrations;
CREATE fa:3 SET o = { n: 'x', junk: 1 } RETURN AFTER;
ALTER FIELD o.n ON fa TYPE option<int> MIGRATE EXISTING ROWS AS fa_int USING <int>$value FALLBACK -1;
(INFO FOR TABLE fa).migrations;
SELECT * FROM fa ORDER BY id;
-- @drain
SELECT * FROM fl ORDER BY id;
SELECT * FROM fb ORDER BY id;
SELECT * FROM fa ORDER BY id;
-- @check
