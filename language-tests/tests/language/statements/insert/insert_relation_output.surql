/**
[test]

[[test.results]]
value = "[{ id: edge:a, in: test:a, name: 'A', out: test:a }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[NULL]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: { id: edge:d, in: test:a, name: 'D', out: test:a } }]]"

[[test.results]]
value = "[{ id: edge:e, in: test:a, name: 'E', out: test:a }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[{ name: 'G' }]"

[[test.results]]
value = "[{ alias: 'H', the_id: edge:h }]"

*/

INSERT RELATION INTO edge [{in: test:a, id: edge:a, out: test:a, name: 'A'}];
INSERT RELATION INTO edge [{in: test:a, id: edge:b, out: test:a, name: 'B'}] RETURN NONE;
INSERT RELATION INTO edge [{in: test:a, id: edge:c, out: test:a, name: 'C'}] RETURN NULL;
INSERT RELATION INTO edge [{in: test:a, id: edge:d, out: test:a, name: 'D'}] RETURN DIFF;
INSERT RELATION INTO edge [{in: test:a, id: edge:e, out: test:a, name: 'E'}] RETURN AFTER;
INSERT RELATION INTO edge [{in: test:a, id: edge:f, out: test:a, name: 'F'}] RETURN BEFORE;
INSERT RELATION INTO edge [{in: test:a, id: edge:g, out: test:a, name: 'G'}] RETURN name;
INSERT RELATION INTO edge [{in: test:a, id: edge:h, out: test:a, name: 'H'}] RETURN name AS alias, id AS the_id;
