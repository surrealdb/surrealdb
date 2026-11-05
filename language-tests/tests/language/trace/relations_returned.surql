/**
[env]
imports = ["language/trace/graph_import.surql"]
trace = ["relations"]

[test]
reason = "A lookup in a returned position reports the edges behind what reaches the result, cut to what leads there"

[[test.results]]
value = "[{ id: person:a, places: [place:cafe, place:lib] }]"
trace = "{ relations: [[person:a, customer_of:a_cafe, place:cafe], [person:a, customer_of:a_lib, place:lib]] }"

[[test.results]]
value = "[[place:cafe, place:lib]]"
trace = "{ relations: [[person:a, customer_of:a_cafe, place:cafe], [person:a, customer_of:a_lib, place:lib]] }"

[[test.results]]
value = "[[post:p1, post:p2]]"
trace = "{ relations: [[person:a, knows:ab, person:b], [person:a, knows:ac, person:c], [person:b, likes:b_p1, post:p1], [person:c, likes:c_p2, post:p2]] }"

[[test.results]]
value = "[[]]"

[[test.results]]
value = "[[place:cafe]]"
trace = "{ relations: [[person:a, customer_of:a_cafe, place:cafe]] }"

[[test.results]]
value = "[['Cafe', 'Library']]"
trace = "{ relations: [[person:a, customer_of:a_cafe, place:cafe], [person:a, customer_of:a_lib, place:lib]] }"

[[test.results]]
value = "[person:b, person:c]"
trace = "{ relations: [[person:a, knows:ab, person:b], [person:a, knows:ac, person:c]] }"

[[test.results]]
value = "[{ id: person:b, name: 'Bob' }, { id: person:c, name: 'Cat' }]"
trace = "{ relations: [[person:a, knows:ab, person:b], [person:a, knows:ac, person:c]] }"

[[test.results]]
value = "[person:a, person:b]"
trace = "{ relations: [[person:a, customer_of:a_lib, place:lib], [person:b, customer_of:b_lib, place:lib]] }"

[[test.results]]
value = "[knows:ab, knows:bc]"
trace = "{ relations: [[person:a, knows:ab, person:b], [person:b, knows:bc, person:c]] }"

[[test.results]]
value = "[person:b, person:c, person:d]"
trace = "{ relations: [[person:a, knows:ab, person:b], [person:a, knows:ac, person:c], [person:b, knows:bc, person:c], [person:c, knows:cd, person:d]] }"

[[test.results]]
value = "[{ nested: { friends: [person:b, person:c], places: [[place:cafe, place:lib]] } }]"
trace = "{ relations: [[person:a, customer_of:a_cafe, place:cafe], [person:a, customer_of:a_lib, place:lib], [person:a, knows:ab, person:b], [person:a, knows:ac, person:c]] }"

[[test.results]]
value = "[{ liked: [post:p1] }]"
trace = "{ relations: [[person:b, likes:b_p1, post:p1]] }"

[[test.results]]
value = "[[place:lib]]"
trace = "{ relations: [[person:a, works_at:a_lib, place:lib]] }"
*/

-- 0: a projection field
SELECT id, ->customer_of->place AS places FROM person:a;

-- 1: SELECT VALUE
SELECT VALUE ->customer_of->place FROM person:a;

-- 2: two edge tables deep, through the vertices between them
SELECT VALUE ->knows->person->likes->post FROM person:a;

-- 3: a dead end reports nothing: person:d likes no post
SELECT VALUE ->knows->person->likes->post FROM person:c;

-- 4: an edge filter between the hops cuts the edge it drops
SELECT VALUE ->customer_of[WHERE since > 2020]->place FROM person:a;

-- 5: a field read after the lookup keeps its edges
SELECT VALUE ->customer_of->place.name FROM person:a;

-- 6: RETURN
RETURN person:a->knows->person;

-- 7: a graph in FROM
SELECT * FROM person:a->knows->person;

-- 8: walked backwards, an edge still reads [in, edge, out]
RETURN place:lib<-customer_of<-person;

-- 9: both ways
RETURN person:b<->knows;

-- 10: a recursion reports every depth
RETURN person:a.{..3+collect}(->knows->person);

-- 11: a lookup in a nested object and array
SELECT { friends: ->knows->person, places: [->customer_of->place] } AS nested FROM person:a;

-- 12: a subquery in a returned position
SELECT (SELECT VALUE ->likes->post FROM ONLY $parent) AS liked FROM person:b;

-- 13: works_at is reported only by a lookup that walks it (statement 0 walks customer_of alone)
SELECT VALUE ->works_at->place FROM person:a;
