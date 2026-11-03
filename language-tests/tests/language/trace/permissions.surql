/**
[env]
imports = ["language/trace/permissions_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "pal:a" }
trace = ["relations", "references", "links"]

[test]
reason = "SECURITY: a record user is traced only what it may read: no hidden vertex or edge, no edge of a table it cannot read, the ends of an edge as its field permissions show them, and no link through a field it cannot read"

[[test.results]]
value = "[pal:b]"
trace = "{ relations: [[pal:a, knows:ab, pal:b]] }"

[[test.results]]
value = "[knows:ab, knows:ah]"
trace = "{ relations: [[pal:a, knows:ab, pal:b], [pal:a, knows:ah, pal:h]] }"

[[test.results]]
value = "[]"

[[test.results]]
value = "[follows:ab, follows:ah]"
trace = "{ relations: [[pal:a, follows:ab, NONE], [pal:a, follows:ah, NONE]] }"

[[test.results]]
value = "[pal:b]"
trace = "{ relations: [[pal:a, follows:ab, NONE]] }"

[[test.results]]
value = "[{ name: NONE }]"

[[test.results]]
value = "[{ name: NONE }]"
trace = "{ links: [[note:1, 'friend', pal:h]] }"

[[test.results]]
value = "[note:2]"
trace = "{ references: [[note:2, pal:h]] }"

[[test.results]]
value = "[note:1]"
trace = "{ references: [[note:1, pal:a]] }"

[[test.results]]
value = "[]"

[[test.results]]
value = "[note:1]"
trace = "{ references: [[note:1, pal:a]] }"

[[test.results]]
value = "[{ names: ['Bob'] }]"
trace = "{ relations: [[pal:a, knows:ab, pal:b]] }"

[[test.results]]
value = "follows:ab"
trace = "{ relations: [[pal:a, follows:ab, NONE]] }"

[[test.results]]
value = "[follows:ah]"
trace = "{ relations: [[pal:a, follows:ah, NONE]] }"
*/

-- 0: a walk to a hidden vertex reports only the visible one
RETURN pal:a->knows->pal;

-- 1: a walk ending on the edges reports both: the user reads both edges and their out field
RETURN pal:a->knows;

-- 2: an edge table the user cannot read reports nothing
RETURN pal:a->blocks->pal;

-- 3: a walk ending on edges whose out field is hidden reports NONE for that end
RETURN pal:a->follows;

-- 4: walking on to the vertex still reports the edge's ends as the user reads them
RETURN pal:a->follows->pal;

-- 5: a link through a field the user cannot read is not reported
SELECT owner.name AS name FROM secret:1;

-- 6: a link to a record the user cannot read: the field's value is the id, which the user reads
SELECT friend.name AS name FROM note:1;

-- 7: a reference to a record the user cannot read, from one it can, is reported
RETURN pal:h<~note;

-- 8: a reference the user may read is reported
RETURN pal:a<~note;

-- 9: a reference from a record the user cannot read is not reported
RETURN pal:a<~draft;

-- 10: of several subjects, only the readable referrers are reported
RETURN pal:a<~(note, draft);

-- 11: a projection over a hidden vertex reports nothing for it
SELECT ->knows->pal.name AS names FROM pal:a;

-- 12: a step that cuts edges whose out field is hidden still reports NONE for that end
RETURN (pal:a->follows)[0];

-- 13: so does a filter that ends the path
RETURN (pal:a->follows)[WHERE id = follows:ah];
