/**
[env]
imports = ["language/trace/graph_import.surql"]
trace = ["relations", "links"]

[test]
reason = "A write's RETURN clause is traced; its data clause is not, and a RETURN that reads stored ids walks no edge"

[[test.results]]
value = "[{ walked: [person:b, person:c] }]"
trace = "{ relations: [[person:a, knows:ab, person:b], [person:a, knows:ac, person:c]] }"

[[test.results]]
value = "[{ friends: [person:b, person:c] }]"

[[test.results]]
value = "[{ names: ['Bob', 'Cat'] }]"
trace = "{ links: [[person:a, 'friends', person:b], [person:a, 'friends', person:c]] }"

[[test.results]]
value = "[{ id: person:b, name: 'Bob', seen: true }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: knows:da, in: person:d, out: person:a }]"

[[test.results]]
value = "[{ walked: [person:a] }]"
trace = "{ relations: [[person:d, knows:da, person:a]] }"

[[test.results]]
value = "[{ walked: [] }]"

[[test.results]]
value = "[{ id: person:e, name: 'Eve' }]"

*/

-- 0: a RETURN that walks the graph
UPDATE person:a SET friends = ->knows->person RETURN ->knows->person AS walked;

-- 1: a RETURN of the stored ids walks no edge and reads through no link
UPDATE person:a SET friends = ->knows->person RETURN friends;

-- 2: a RETURN that reads through the stored ids reports the links
UPDATE person:a SET friends = ->knows->person RETURN friends.name AS names;

-- 3: RETURN AFTER and BEFORE trace nothing
UPDATE person:b SET seen = true RETURN AFTER;

-- 4: RETURN NONE traces nothing
UPDATE person:b SET seen = true RETURN NONE;

-- 5: RELATE reports no edge: its result holds the edge with its ends
RELATE person:d->knows:da->person:a RETURN id, in, out;

-- 6: a RETURN walks the edge an earlier statement wrote
UPDATE person:d SET seen = true RETURN ->knows->person AS walked;

-- 7: a CREATE whose RETURN walks no edge reports nothing
CREATE person:e SET name = 'Eve' RETURN ->knows->person AS walked;

-- 8: a DELETE's RETURN BEFORE traces nothing
DELETE person:e RETURN BEFORE;
