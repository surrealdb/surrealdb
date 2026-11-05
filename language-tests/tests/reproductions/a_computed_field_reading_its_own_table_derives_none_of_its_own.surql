/**
[test]
reason = "A COMPUTED field whose body reads its own table is derived only where a statement reads it. The body's own read names `id` alone, so it derives no field of the records it reads, and a read of `id` derives none: the derivation ends instead of re-entering itself through every other record."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: ring:1, members: 1 }]"

[[test.results]]
value = "[{ id: ring:2, members: 2 }]"

[[test.results]]
value = "[ring:1, ring:2]"

[[test.results]]
value = "[ring:1]"

[[test.results]]
value = "[{ id: ring:1, members: 2 }, { id: ring:2, members: 2 }]"

[[test.results]]
value = "[2]"

*/

DEFINE TABLE ring SCHEMALESS;
DEFINE FIELD members ON ring COMPUTED count(SELECT VALUE id FROM ring);

CREATE ring:1;
CREATE ring:2;

SELECT VALUE id FROM ring ORDER BY id;
SELECT VALUE id FROM ring:1;
SELECT * FROM ring ORDER BY id;
SELECT VALUE members FROM ring:2;
