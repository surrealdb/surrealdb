/**
[env]
trace = ["variables", "subscriptions", "relations"]
# The live query registers itself on the node, outside the namespace.
clean = true

[test]
reason = "A top-level LET reports what it bound on its own statement, and a LIVE SELECT the id of the live query it started"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:a }, { id: person:b }]"

[[test.results]]
value = "[{ id: knows:ab, in: person:a, out: person:b }]"

[[test.results]]
value = "NONE"
trace = "{ variables: { n: 1 } }"

[[test.results]]
value = "NONE"
trace = "{ variables: { ids: [person:a, person:b] } }"

[[test.results]]
value = "NONE"
trace = "{ variables: { n: 2 } }"

[[test.results]]
value = "NONE"
trace = "{ relations: [[person:a, knows:ab, person:b]], variables: { friends: [person:b] } }"

[[test.results]]
value = "2"

[[test.results]]
match = "type::is_uuid($result)"
trace = "{ subscriptions: [u'00000000-0000-0000-0000-000000000000'] }"

[[test.results]]
value = "1"
*/

-- 0: setup
DEFINE TABLE person;
CREATE person:a, person:b;
RELATE person:a->knows:ab->person:b;

-- 3: a LET of a literal
LET $n = 1;

-- 4: a LET of a statement's result
LET $ids = SELECT VALUE id FROM person;

-- 5: rebinding reports the new value
LET $n = 2;

-- 6: a LET whose value walks the graph reports both
LET $friends = person:a->knows->person;

-- 7: a statement that binds nothing sends no trace
RETURN $n;

-- 8: a LIVE SELECT reports its subscription, a fresh uuid each run
LIVE SELECT * FROM person;

-- 9: a LET inside a block is not a top-level binding
{ LET $inner = 1; RETURN $inner };
