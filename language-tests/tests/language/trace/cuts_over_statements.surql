/**
[env]
imports = ["language/trace/graph_import.surql"]
trace = ["relations", "links"]

[test]
reason = "A step that cuts values holding subqueries or link reads reports nothing for the values it drops"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[person:b, person:c]]"
trace = "{ partial: true }"

[[test.results]]
value = "{ k: [person:b, person:c], n: 'Bob' }"
trace = "{ partial: true, relations: [[person:a, knows:ab, person:b], [person:a, knows:ac, person:c]] }"

[[test.results]]
value = "[]"
*/

-- 0..1: setup
UPDATE person:a SET best = person:b RETURN NONE;
UPDATE person:c SET best = person:d RETURN NONE;

-- 2: a cut over subqueries: each subquery reports into the statement's trace when it runs, so the
-- one the cut drops would be reported too: partial
RETURN [(SELECT VALUE ->knows->person FROM person:a), (SELECT VALUE ->knows->person FROM person:c)][0];

-- 3: a cut over lookups and link reads reports the lookups the kept element walked; a link
-- read names no record to prune it against, so it marks the trace partial
RETURN [{ k: person:a->knows->person, n: person:a.best.name }, { k: person:c->knows->person, n: person:c.best.name }][0];

-- 4: a cut over a subquery that is not traced: a condition reports nothing
RETURN [1, 2][WHERE $this IN (SELECT VALUE ->knows->person FROM person:a)];
