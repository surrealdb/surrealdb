/**
[test]

[[test.results]]
value = "[{ id: test:a, name: 'A' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[NULL]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: { id: test:d, name: 'D' } }]]"

[[test.results]]
value = "[{ id: test:e, name: 'E' }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[{ name: 'G' }]"

[[test.results]]
value = "[{ alias: 'H', the_id: test:h }]"


*/

INSERT INTO test [{id: test:a, name: 'A'}];
INSERT INTO test [{id: test:b, name: 'B'}] RETURN NONE;
INSERT INTO test [{id: test:c, name: 'C'}] RETURN NULL;
INSERT INTO test [{id: test:d, name: 'D'}] RETURN DIFF;
INSERT INTO test [{id: test:e, name: 'E'}] RETURN AFTER;
INSERT INTO test [{id: test:f, name: 'F'}] RETURN BEFORE;
INSERT INTO test [{id: test:g, name: 'G'}] RETURN name;
INSERT INTO test [{id: test:h, name: 'H'}] RETURN name AS alias, id AS the_id;
