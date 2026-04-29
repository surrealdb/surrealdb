/**
[test]

[[test.results]]
value = "[{ id: counter:test, count: 1 }]"

[[test.results]]
value = "[{ id: counter:test, count: 2 }]"

[[test.results]]
value = "[{ id: counter:test, count: 3 }]"

[[test.results]]
value = "[{ id: counter:test, count: 4 }]"

*/

UPSERT counter:test SET count = IF count THEN count + 1 ELSE 1 END;
UPSERT counter:test SET count += 1;
UPSERT counter:test SET count = IF count THEN count + 1 ELSE 1 END;
UPSERT counter:test SET count += 1;
