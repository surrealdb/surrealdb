/**
[env]
imports = ["language/trace/graph_import.surql"]
trace = ["relations"]

[test]
reason = "A lookup that does not reach the result is not reported: a condition reports nothing, and one that feeds the result through a change of value marks the trace partial"

[[test.results]]
value = "[{ id: person:a }]"

[[test.results]]
value = "[{ n: 2 }]"
trace = "{ partial: true }"

[[test.results]]
value = "[2]"
trace = "{ partial: true }"

[[test.results]]
value = "[true]"
trace = "{ partial: true }"

[[test.results]]
value = "[{ all: [person:b, person:c, person:d] }]"
trace = "{ partial: true }"

[[test.results]]
value = "[[{ id: knows:ac, in: person:a, out: person:c }]]"
trace = "{ partial: true }"

[[test.results]]
value = "[[person:b, person:c]]"
trace = "{ relations: [[person:a, knows:ab, person:b], [person:a, knows:ac, person:c]] }"

[[test.results]]
value = "[]"

[[test.results]]
value = "[place:cafe, place:lib]"
trace = "{ relations: [[person:a, customer_of:a_cafe, place:cafe], [person:a, customer_of:a_lib, place:lib]] }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['Ann']"

[[test.results]]
value = "[{ friends: [], id: person:d }, { friends: [person:d], id: person:c }, { friends: [person:c], id: person:b }, { friends: [person:b, person:c], id: person:a }]"
trace = "{ relations: [[person:a, knows:ab, person:b], [person:a, knows:ac, person:c], [person:b, knows:bc, person:c], [person:c, knows:cd, person:d]] }"
*/

-- 0: WHERE is a condition: nothing, not partial
SELECT id FROM person WHERE ->knows->person CONTAINS person:b;

-- 1: a function argument feeds the result through a change: partial
SELECT count(->knows) AS n FROM person:a;

-- 2: so does a method
SELECT VALUE (->knows->person).len() FROM person:a;

-- 3: so does an operator
SELECT VALUE ->knows->person CONTAINS person:b FROM person:a;

-- 4: and an aggregate, which spans rows
SELECT array::group(->knows->person) AS all FROM person GROUP ALL;

-- 5: a lookup with clauses of its own is a statement that reshapes what it reaches: partial
SELECT VALUE ->(SELECT * FROM knows ORDER BY id DESC LIMIT 1) FROM person:a;

-- 6: a WHERE inside a filter part is a condition; the lookup it filters is traced
SELECT VALUE ->knows->person[WHERE ->likes->post] FROM person:a;

-- 7: a SET clause writes and returns nothing traced
UPDATE person:a SET friends = ->knows->person RETURN NONE;

-- 8: an IF condition is not traced, its branch is
RETURN IF person:a->knows->person { person:a->customer_of->place } ELSE { [] };

-- 9: a FOR loop answers nothing of its body
FOR $p IN [person:a] { LET $x = $p->knows->person; };

-- 10: a statement with no lookup sends no trace
SELECT VALUE name FROM person:a;

-- 11: an ORDER BY over a projected lookup orders the rows, and each row's lookup is traced
SELECT id, ->knows->person AS friends FROM person ORDER BY id DESC;
