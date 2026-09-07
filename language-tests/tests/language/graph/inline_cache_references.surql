/**
[test]
reason = "Reverse-reference lookups over a table with an INLINE REFERENCES cap: the cached back-link set stays equal to the reference keys across reference moves, record deletes, and the ON DELETE strategy of the referencing field"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[post:a, post:b]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[post:b]"

[[test.results]]
value = "[post:a]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "{ id: post:a }"
*/

DEFINE TABLE person INLINE REFERENCES 4;
DEFINE TABLE post;
DEFINE FIELD author ON post TYPE option<record<person>> REFERENCE ON DELETE UNSET;
CREATE person:p1, person:p2 RETURN NONE;
CREATE post:a SET author = person:p1 RETURN NONE;
CREATE post:b SET author = person:p1 RETURN NONE;

SELECT VALUE <~post FROM ONLY person:p1;

-- Moving a reference maintains both records' caches in one transaction.
UPDATE post:a SET author = person:p2 RETURN NONE;
SELECT VALUE <~post FROM ONLY person:p1;
SELECT VALUE <~post FROM ONLY person:p2;

-- Deleting a referencing record removes its back-link.
DELETE post:b;
SELECT VALUE <~post FROM ONLY person:p1;

-- Deleting the referenced record applies ON DELETE UNSET to the survivor.
DELETE person:p2;
SELECT * FROM ONLY post:a;
