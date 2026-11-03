/**
[env]
trace = ["relations", "variables"]

[test]
reason = "A trace reports only the kinds the request asked for: here relations and variables, never links, references or subscriptions"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ best: person:b, id: person:a, name: 'Ann' }]"

[[test.results]]
value = "[{ id: person:b, name: 'Bob' }]"

[[test.results]]
value = "[{ author: person:a, id: post:1 }]"

[[test.results]]
value = "[{ id: knows:ab, in: person:a, out: person:b }]"

[[test.results]]
value = "['Bob']"

[[test.results]]
value = "[[post:1]]"

[[test.results]]
value = "[{ best: 'Bob', friends: [person:b], posts: [post:1] }]"
trace = "{ relations: [[person:a, knows:ab, person:b]] }"

[[test.results]]
value = "NONE"
trace = "{ relations: [[person:a, knows:ab, person:b]], variables: { all: { best: 'Bob', friends: [person:b] } } }"

[[test.results]]
value = "['BOB']"
*/

-- 0: setup
DEFINE FIELD author ON post TYPE option<record<person>> REFERENCE;
CREATE person:a SET name = 'Ann', best = person:b;
CREATE person:b SET name = 'Bob';
CREATE post:1 SET author = person:a;
RELATE person:a->knows:ab->person:b;

-- 5: a link read is not reported
SELECT VALUE best.name FROM person:a;

-- 6: a reference lookup is not reported
SELECT VALUE <~post FROM person:a;

-- 7: a statement mixing every kind reports only relations
SELECT best.name AS best, <~post AS posts, ->knows->person AS friends FROM person:a;

-- 8: a LET mixing kinds reports its value and its relations
LET $all = SELECT best.name AS best, ->knows->person AS friends FROM ONLY person:a;

-- 9: a link read under a function is a kind the request did not ask for: not partial
SELECT VALUE string::uppercase(best.name) FROM person:a;
