/**
[test]
reason = "Setup for 3.03.02_record_scalar_content.surql: the 3.3.2 binary writes a record whose stored content is the scalar 5, by a PATCH that replaces the root, next to an ordinary record under an index. The 4.0 build refuses to create such a record, so only this pair reaches a store that holds one."
run = false

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "[{ a: 2, id: t:2 }]"

[[test.results]]
value = "[5]"

*/
DEFINE TABLE t SCHEMALESS;
DEFINE INDEX t_a ON t FIELDS a;
CREATE t:1 SET a = 1;
CREATE t:2 SET a = 2;
UPDATE t:2 PATCH [{op: 'replace', path: '', value: 5}];
