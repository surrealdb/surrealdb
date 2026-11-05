/**
[env]

[test]
issue = 5581
reason = "Check that SELECT COUNT() with index uses the right iteration operation"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *] {rows: 1}
    IndexCountScan [ctx: Db] [source: test, condition: indexedField = 'cake'] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "[{ count: 10 }]"

[[test.results]]
value = "[{ count: 10 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *] {rows: 1}
    Aggregate [ctx: Db] [mode: GROUP ALL] {rows: 1}
        Filter [ctx: Db] [predicate: unindexedField = false] {rows: 1}
            IndexScan [ctx: Db] [table: test, index: idx_indexedField, access: = 'cake', direction: Forward] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0 }]"

*/

-- Index keys are counted only for a column declared to hold no array.
DEFINE FIELD indexedField ON test TYPE string;
DEFINE INDEX idx_indexedField ON test FIELDS indexedField;
CREATE |test:10| CONTENT { indexedField: 'cake', unindexedField: true} RETURN NONE;

EXPLAIN ANALYZE SELECT COUNT() FROM test WHERE indexedField = 'cake' GROUP ALL;
SELECT COUNT() FROM test WITH NOINDEX WHERE indexedField = 'cake' GROUP ALL;
SELECT COUNT() FROM test WHERE indexedField = 'cake' GROUP ALL;

EXPLAIN ANALYZE SELECT COUNT() FROM test WHERE indexedField = 'cake' AND unindexedField = false GROUP ALL;
SELECT COUNT() FROM test WITH NOINDEX WHERE indexedField = 'cake' AND unindexedField = false GROUP ALL;
SELECT COUNT() FROM test WHERE indexedField = 'cake' AND unindexedField = false GROUP ALL;
