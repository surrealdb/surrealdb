/**

[test]
issue = 5581
reason = "Check that SELECT COUNT() with index uses the right iteration operation"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx_indexedField', operator: '=', value: 'cake' }, table: 'test' }, operation: 'Iterate Index Count' }, { detail: { idioms: { count: ['count'] }, type: 'Group' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ count: 10 }]"

[[test.results]]
value = "[{ count: 10 }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx_indexedField', operator: '=', value: 'cake' }, table: 'test' }, operation: 'Iterate Index' }, { detail: { idioms: { count: ['count'] }, type: 'Group' }, operation: 'Collector' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

*/

DEFINE INDEX idx_indexedField ON test FIELDS indexedField;
CREATE |test:10| CONTENT { indexedField: 'cake', unindexedField: true} RETURN NONE;

SELECT COUNT() FROM test WHERE indexedField = 'cake' GROUP ALL EXPLAIN;
SELECT COUNT() FROM test WITH NOINDEX WHERE indexedField = 'cake' GROUP ALL;
SELECT COUNT() FROM test WHERE indexedField = 'cake' GROUP ALL;

SELECT COUNT() FROM test WHERE indexedField = 'cake' AND unindexedField = false GROUP ALL EXPLAIN;
SELECT COUNT() FROM test WITH NOINDEX WHERE indexedField = 'cake' AND unindexedField = false GROUP ALL;
SELECT COUNT() FROM test WHERE indexedField = 'cake' AND unindexedField = false GROUP ALL;
