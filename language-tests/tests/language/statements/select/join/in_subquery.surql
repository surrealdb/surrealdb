/**
[env]
namespace = true
database = true

[test]
reason = """
	An `IN` / `NOT IN` / `CONTAINS` / `CONTAINSNOT` predicate whose right
	side is a `SELECT VALUE` over a table is planned as a semi or anti join
	and admits the same rows as the per-row subquery it replaces: a
	correlated `r = $parent.p` conjunct is a join key, another correlated
	conjunct is the join's residual, an uncorrelated conjunct filters the
	right side, and `NONE` and `NULL` keys match their own kind as `IN`
	compares elements. Shapes the pass does not lift — a `LIMIT`, a
	correlation through a function argument, a `$this`, a `MATCHES` in the
	subquery's filter — still answer the same under every planner strategy.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ active: true, id: team:a, name: 'A' }]"

[[test.results]]
value = "[{ active: false, id: team:b, name: 'B' }]"

[[test.results]]
value = "[{ active: true, id: team:c, name: 'C' }]"

[[test.results]]
value = "[{ id: person:1, name: 'p1', team: team:a }]"

[[test.results]]
value = "[{ id: person:2, name: 'p2', team: team:b }]"

[[test.results]]
value = "[{ id: person:3, name: 'p3', team: team:zzz }]"

[[test.results]]
value = "[{ id: person:4, name: 'p4' }]"

[[test.results]]
value = "[{ id: grant:1, n: 2, team: team:a, who: person:1 }]"

[[test.results]]
value = "[{ id: grant:2, n: 0, team: team:a, who: person:2 }]"

[[test.results]]
value = "[{ id: grant:3, n: 5, who: person:4 }]"

[[test.results]]
value = "[{ id: grant:4, n: 1, team: NULL, who: person:3 }]"

[[test.results]]
value = "[person:1]"

[[test.results]]
value = "[person:2, person:3, person:4]"

[[test.results]]
value = "[person:1, person:4]"

[[test.results]]
value = "[person:2, person:3]"

[[test.results]]
value = "[person:4]"

[[test.results]]
value = "[person:1, person:2, person:3, person:4]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[person:2]"

[[test.results]]
value = "[person:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[person:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[person:1]"
*/

DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD team ON person TYPE option<record<team>>;
DEFINE TABLE team SCHEMAFULL;
DEFINE FIELD name ON team TYPE string;
DEFINE FIELD active ON team TYPE bool;
DEFINE TABLE grant SCHEMALESS;
CREATE team:a SET name = 'A', active = true;
CREATE team:b SET name = 'B', active = false;
CREATE team:c SET name = 'C', active = true;
CREATE person:1 SET name = 'p1', team = team:a;
CREATE person:2 SET name = 'p2', team = team:b;
CREATE person:3 SET name = 'p3', team = team:zzz;
CREATE person:4 SET name = 'p4';
CREATE grant:1 SET who = person:1, team = team:a, n = 2;
CREATE grant:2 SET who = person:2, team = team:a, n = 0;
CREATE grant:3 SET who = person:4, team = NONE, n = 5;
CREATE grant:4 SET who = person:3, team = NULL, n = 1;
-- A semi join on the target's id, with the right side filtered once.
SELECT VALUE id FROM person WHERE team IN (SELECT VALUE id FROM team WHERE active = true) ORDER BY id;
-- The anti join of the same shape: a dangling link and a missing link both have no match.
SELECT VALUE id FROM person WHERE team NOT IN (SELECT VALUE id FROM team WHERE active = true) ORDER BY id;
-- CONTAINS with two join keys; `NONE CONTAINS NONE` admits person:4.
SELECT VALUE id FROM person WHERE (SELECT VALUE team FROM grant WHERE who = $parent.id AND n > 1) CONTAINS team ORDER BY id;
-- CONTAINSNOT: person:3's grant holds NULL, which its dangling link does not equal.
SELECT VALUE id FROM person WHERE (SELECT VALUE team FROM grant WHERE who = $parent.id) CONTAINSNOT team ORDER BY id;
-- A correlated conjunct that is not an equality is the join's residual.
SELECT VALUE id FROM person WHERE id IN (SELECT VALUE who FROM grant WHERE n > 1 AND $parent.name > 'p2') ORDER BY id;
-- A constant outer side is a join all the same.
SELECT VALUE id FROM person WHERE 'A' IN (SELECT VALUE name FROM team) ORDER BY id;
SELECT VALUE id FROM person WHERE 'Z' IN (SELECT VALUE name FROM team) ORDER BY id;
-- The lifted conjunct's neighbours keep filtering the rows.
SELECT VALUE id FROM person WHERE name != 'p1' AND team IN (SELECT VALUE id FROM team) AND name != 'p3' ORDER BY id;
-- Not lifted: a LIMIT, a correlation through a function argument, a `$this`.
SELECT VALUE id FROM person WHERE team IN (SELECT VALUE id FROM team WHERE active = true ORDER BY id LIMIT 1) ORDER BY id;
SELECT VALUE id FROM person WHERE team IN (SELECT VALUE id FROM team WHERE name = string::uppercase($parent.name)) ORDER BY id;
SELECT VALUE id FROM person WHERE team IN (SELECT VALUE id FROM team WHERE $this.active = true) ORDER BY id;
-- Not lifted: a MATCHES, which names an index of the subquery's own statement.
DEFINE ANALYZER simple TOKENIZERS blank;
DEFINE INDEX team_name ON team FIELDS name FULLTEXT ANALYZER simple BM25;
SELECT VALUE id FROM person WHERE team IN (SELECT VALUE id FROM team WHERE name @@ 'A') ORDER BY id;
