/**
[env]
namespace = true
database = true

[test]
reason = "A last hop paged by an integer LIMIT and START answers the page of what its origins reach in turn, whatever the page and the degree: with START, with an array of origins, through an edge to its vertex, and past the degree."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:a }, { id: person:b }, { id: person:c }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: likes:[7], in: person:b, out: thing:2 }]"

[[test.results]]
value = "[{ id: likes:[8], in: person:b, out: thing:4 }]"

[[test.results]]
value = "[{ id: likes:[9], in: person:b, out: thing:6 }]"

[[test.results]]
value = "[{ id: likes:[10], in: person:c, out: thing:1 }]"

[[test.results]]
value = "[likes:[1], likes:[2]]"

[[test.results]]
value = "[likes:[2], likes:[3]]"

[[test.results]]
value = "[likes:[5], likes:[6]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[likes:[1], likes:[2], likes:[3], likes:[4], likes:[5], likes:[6]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[thing:1, thing:2, thing:3]"

[[test.results]]
value = "[thing:2, thing:3]"

[[test.results]]
value = "[person:a]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[likes:[7], likes:[8], likes:[9], likes:[1]]"

[[test.results]]
value = "[likes:[9], likes:[1]]"

[[test.results]]
value = "[[thing:2, thing:4, thing:6], [thing:1, thing:2, thing:3], [thing:1]]"

[[test.results]]
value = "[thing:1, thing:2]"

[[test.results]]
value = "[thing:3, thing:4]"

[[test.results]]
value = "[thing:1, thing:2, thing:3, thing:4, thing:5]"

[[test.results]]
value = "[thing:1]"

[[test.results]]
value = "[likes:[3], likes:[4]]"

[[test.results]]
value = "likes:[2]"
*/

DEFINE TABLE person SCHEMALESS;
DEFINE TABLE thing SCHEMALESS;
DEFINE TABLE likes TYPE RELATION IN person OUT thing SCHEMALESS;

CREATE person:a, person:b, person:c;
FOR $i IN 1..=6 {
	CREATE type::record('thing', $i) SET n = $i;
};
FOR $i IN 1..=6 {
	RELATE person:a -> likes:[$i] -> (type::record('thing', $i));
};
RELATE person:b -> likes:[7] -> thing:2;
RELATE person:b -> likes:[8] -> thing:4;
RELATE person:b -> likes:[9] -> thing:6;
RELATE person:c -> likes:[10] -> thing:1;

-- The lone page of one origin.
SELECT VALUE ->(likes LIMIT 2) FROM ONLY person:a;
SELECT VALUE ->(likes LIMIT 2 START 1) FROM ONLY person:a;
SELECT VALUE ->(likes START 4) FROM ONLY person:a;
SELECT VALUE ->(likes LIMIT 0) FROM ONLY person:a;
SELECT VALUE ->(likes LIMIT 100) FROM ONLY person:a;
SELECT VALUE ->(likes LIMIT 2 START 100) FROM ONLY person:a;

-- A page of the vertices an edge reaches.
SELECT VALUE ->likes->(thing LIMIT 3) FROM ONLY person:a;
SELECT VALUE ->likes->(thing LIMIT 2 START 1) FROM ONLY person:a;
SELECT VALUE <-likes<-(person LIMIT 1) FROM ONLY thing:2;

-- A page over several origins is the page of what they reach in turn.
LET $origins = [person:b, person:a, person:c];
RETURN $origins->(likes LIMIT 4);
RETURN $origins->(likes LIMIT 2 START 2);
RETURN $origins->likes->(thing LIMIT 3);

-- The page of the vertices an edge reaches, over several degrees.
SELECT VALUE ->likes->(thing LIMIT 2) FROM ONLY person:a;
SELECT VALUE ->likes->(thing LIMIT 2 START 2) FROM ONLY person:a;
SELECT VALUE ->likes->(thing LIMIT 5) FROM ONLY person:a;
SELECT VALUE ->likes->(thing LIMIT 1) FROM ONLY person:c;

-- A paged hop under a clause that is not a page is the statement it makes.
SELECT VALUE ->(likes WHERE id[0] > 2 LIMIT 2) FROM ONLY person:a;
SELECT VALUE ->(likes LIMIT 2 START 1)[0] FROM ONLY person:a;
