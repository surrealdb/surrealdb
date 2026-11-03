/**
[env]
imports = ["language/trace/graph_import.surql"]
trace = ["relations"]

[test]
reason = "A step that keeps part of what a lookup answered reports only the edges behind what it keeps"

[[test.results]]
value = "person:b"
trace = "{ relations: [[person:a, knows:ab, person:b]] }"

[[test.results]]
value = "person:c"
trace = "{ relations: [[person:a, knows:ac, person:c]] }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "person:c"
trace = "{ relations: [[person:a, knows:ac, person:c]] }"

[[test.results]]
value = "[person:c]"
trace = "{ relations: [[person:a, knows:ac, person:c]] }"

[[test.results]]
value = "[]"

[[test.results]]
value = "{ friend: [person:b, person:c] }"
trace = "{ relations: [[person:a, knows:ab, person:b], [person:a, knows:ac, person:c]] }"

[[test.results]]
value = "[person:b]"
trace = "{ relations: [[person:a, knows:ab, person:b]] }"

[[test.results]]
value = "[{ id: person:b, name: 'Bob' }]"
trace = "{ partial: true }"

[[test.results]]
value = "[{ friends: [person:b, person:c], id: person:a }]"
trace = "{ relations: [[person:a, knows:ab, person:b], [person:a, knows:ac, person:c]] }"

[[test.results]]
value = "[person:c, person:d]"
trace = "{ relations: [[person:a, knows:ab, person:b], [person:a, knows:ac, person:c], [person:b, knows:bc, person:c], [person:c, knows:cd, person:d]] }"

[[test.results]]
value = "[{ id: person:b, name: 'Bob' }]"
trace = "{ partial: true }"

[[test.results]]
value = "[{ friends: [person:b, person:c], id: person:a }]"
trace = "{ relations: [[person:a, knows:ab, person:b], [person:a, knows:ac, person:c]] }"
*/

-- 0: the first element
RETURN (person:a->knows->person)[0];

-- 1: the last element
RETURN (person:a->knows->person)[$];

-- 2..3: an element by a computed index
LET $i = 1;
RETURN (person:a->knows->person)[$i];

-- 4: a filter that ends the path
RETURN (person:a->knows->person)[WHERE id = person:c];

-- 5: a filter that keeps nothing reports nothing
RETURN (person:a->knows->person)[WHERE id = person:z];

-- 6: a kept object still leads to the lookup held in it
RETURN [{ friend: person:a->knows->person }][0];

-- 7: an index inside a projection
SELECT VALUE ->knows->person[0] FROM person:a;

-- 8: a page over a graph in FROM drops rows the lookup walked: partial
SELECT * FROM person:a->knows->person LIMIT 1;

-- 9: the rows a statement's own LIMIT drops report nothing
SELECT id, ->knows->person AS friends FROM person ORDER BY id LIMIT 1;

-- 10: a cut keeps every walk leading to a record it keeps, by record rather than by path:
-- the first path is [c, d], and c is also reached through b, so knows:ab and knows:bc stay
RETURN person:a.{..3+path}(->knows->person)[0];

-- 11: a WHERE over a graph in FROM drops rows the lookup walked: partial
SELECT * FROM person:a->knows->person WHERE id = person:b;

-- 12: a WHERE beneath a projection drops rows before their lookups run: traced
SELECT id, ->knows->person AS friends FROM person WHERE id = person:a;
