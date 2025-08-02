/**
[test]
#wip = true # Made wip because the value PR broke it but I don't have time to figure out how to fix this

[[test.results]]
value = "[{ id: b:1, t: 'Hello World' }]"

[[test.results]]
value = "[{ id: b:2, t: 'Foo Bar' }]"

[[test.results]]
value = "[{ id: b:3, t: 'Bar Hello' }]"

[[test.results]]
value = "None"

[[test.results]]
value = "None"

[[test.results]]
value = "[{ id: b:1 }]"

[[test.results]]
value = "[{ id: b:2 }]"

[[test.results]]
value = "[{ id: b:1 }]"

[[test.results]]
value = "[{ id: b:2 }]"

[[test.results]]
value = "[{ id: b:1 }, { id: b:3 }]"

[[test.results]]
value = "[{ id: b:2 }, { id: b:3 }]"

[[test.results]]
value = "[{ id: b:1 }]"

[[test.results]]
value = "[{ id: b:2 }]"

[[test.results]]
value = "[{ id: b:1 }]"

[[test.results]]
value = "[{ id: b:2 }]"

[[test.results]]
value = "[{ id: b:1 }, { id: b:3 }]"

[[test.results]]
value = "[{ id: b:2 }, { id: b:3 }]"

*/

CREATE b:1 SET t = 'Hello World';
CREATE b:2 SET t = 'Foo Bar';
CREATE b:3 SET t = 'Bar Hello';
DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX i ON b FIELDS t FULLTEXT ANALYZER simple BM25;
SELECT id FROM b WHERE t @@ 'hello world';
SELECT id FROM b WHERE t @@ 'bar foo';
SELECT id FROM b WHERE t @AND@ 'hello world';
SELECT id FROM b WHERE t @AND@ 'bar foo';
SELECT id FROM b WHERE t @OR@ 'hello world';
SELECT id FROM b WHERE t @OR@ 'bar foo';
SELECT id FROM b WHERE t @1@ 'hello world';
SELECT id FROM b WHERE t @1@ 'bar foo';
SELECT id FROM b WHERE t @1,AND@ 'hello world';
SELECT id FROM b WHERE t @1,AND@ 'bar foo';
SELECT id FROM b WHERE t @1,OR@ 'hello world';
SELECT id FROM b WHERE t @1,OR@ 'bar foo';
