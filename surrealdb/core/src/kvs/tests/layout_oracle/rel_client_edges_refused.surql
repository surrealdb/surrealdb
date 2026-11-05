-- A caller-owned transaction writes edges, then runs statements that fail: a migration
-- of an endpoint's field, a migration of an INLINE field, a migration of `in`, and a
-- RELATE to an out record of a table the relation does not admit. The caller commits
-- what was written. Each edge must read its own values through traversal and edge scan
-- alike, the refused migrations must leave no step behind, and the INLINE value must
-- not be read from another edge.
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE TABLE org SCHEMAFULL;
DEFINE FIELD size ON org TYPE string;
DEFINE TABLE works TYPE RELATION IN person OUT org SCHEMAFULL;
DEFINE FIELD since ON works TYPE string;
DEFINE FIELD grade ON works TYPE option<string> INLINE;
CREATE person:1 SET name = 'a' RETURN NONE;
CREATE person:2 SET name = 'b' RETURN NONE;
CREATE org:1 SET size = '5' RETURN NONE;
CREATE org:2 SET size = 'big' RETURN NONE;
RELATE person:1->works:1->org:1 SET since = '1', grade = 'a' RETURN NONE;
RELATE person:2->works:2->org:2 SET since = 'two', grade = 'b' RETURN NONE;
-- @upgrade
-- @client
RELATE person:1->works:3->org:2 SET since = '3', grade = 'c' RETURN NONE;
ALTER FIELD size ON org TYPE int MIGRATE EXISTING ROWS AS size_int USING <int>$value FALLBACK 0;
RELATE person:2->works:4->org:1 SET since = '4', grade = 'd' RETURN NONE;
-- @end
-- @client
ALTER FIELD grade ON works TYPE int MIGRATE EXISTING ROWS AS grade_int USING <int>$value FALLBACK 0;
ALTER FIELD in ON works TYPE record<person> MIGRATE EXISTING ROWS AS m USING $value;
RELATE person:2->works:5->org:1 SET since = '5', grade = 'e' RETURN NONE;
RELATE person:1->works:6->nobody:1 SET since = '6' RETURN NONE;
-- @end
SELECT * FROM works ORDER BY id;
SELECT VALUE grade FROM works ORDER BY id;
person:1->works.since;
person:2->works[WHERE grade = 'd'].since;
SELECT VALUE ->works->org.size FROM person ORDER BY id;
(INFO FOR TABLE works).migrations;
-- @check
-- @drain
SELECT * FROM works ORDER BY id;
SELECT VALUE ->works->org.size FROM person ORDER BY id;
(INFO FOR TABLE org).migrations;
-- @check
