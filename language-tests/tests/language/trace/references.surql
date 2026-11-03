/**
[env]
trace = ["references"]

[test]
reason = "A reference lookup in a returned position reports [referrer, referenced], the record holding the field first"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:a }, { id: person:b }]"

[[test.results]]
value = "[{ author: person:a, id: post:1 }]"

[[test.results]]
value = "[{ author: person:a, id: post:2 }]"

[[test.results]]
value = "[{ author: person:b, id: post:3 }]"

[[test.results]]
value = "[{ author: person:a, id: note:1 }]"

[[test.results]]
value = "[{ id: knows:ab, in: person:a, out: person:b }]"

[[test.results]]
value = "[{ id: person:a, posts: [post:1, post:2] }]"
trace = "{ references: [[post:1, person:a], [post:2, person:a]] }"

[[test.results]]
value = "[post:1, post:2, note:1]"
trace = "{ references: [[note:1, person:a], [post:1, person:a], [post:2, person:a]] }"

[[test.results]]
value = "[post:1, post:2, post:3]"
trace = "{ references: [[post:1, person:a], [post:2, person:a], [post:3, person:b]] }"

[[test.results]]
value = "[{ id: person:a }]"

[[test.results]]
value = "[2]"
trace = "{ partial: true }"

[[test.results]]
value = "[[post:1, post:2]]"
trace = "{ references: [[post:1, person:a], [post:2, person:a]] }"

[[test.results]]
value = "[person:b]"

*/

-- 0: setup
DEFINE FIELD author ON post TYPE option<record<person>> REFERENCE;
DEFINE FIELD author ON note TYPE option<record<person>> REFERENCE;
CREATE person:a, person:b;
CREATE post:1 SET author = person:a;
CREATE post:2 SET author = person:a;
CREATE post:3 SET author = person:b;
CREATE note:1 SET author = person:a;
RELATE person:a->knows:ab->person:b;

-- 8: the records referencing one record
SELECT id, <~post AS posts FROM person:a;

-- 9: several subjects
RETURN person:a<~(post, note);

-- 10: several origins
RETURN [person:a, person:b]<~post;

-- 11: a reference lookup in a condition is not traced
SELECT id FROM person WHERE count(<~post) > 1;

-- 12: a reference lookup through a function marks the trace partial
SELECT VALUE count(<~post) FROM person:a;

-- 13: a reference lookup followed by a field read keeps its references
SELECT VALUE <~post.id FROM person:a;

-- 14: asking for references only, a graph lookup reports nothing
RETURN person:a->knows->person;
