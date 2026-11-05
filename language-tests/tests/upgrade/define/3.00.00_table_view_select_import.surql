/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: src:1, name: 'a', score: 10 }]"

[[test.results]]
value = "[{ id: src:2, name: 'b', score: 20 }]"

[[test.results]]
value = "[{ id: src:3, name: 'c', score: 30 }]"

*/
DEFINE TABLE src SCHEMALESS;
DEFINE TABLE high_scores AS
    SELECT id, name, score FROM src WHERE score >= 20;
CREATE src:1 SET name = 'a', score = 10;
CREATE src:2 SET name = 'b', score = 20;
CREATE src:3 SET name = 'c', score = 30;
