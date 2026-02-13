/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ details: '', error: 'Found a future which would be stored in the database', kind: 'incompatible future', location: { column: 30, kind: 'error', label: 'Incompatible future', length: 14, line: 1, source: 'DEFINE FIELD f ON t3 DEFAULT <future> ( 1 )', truncation: 'none' }, origin: ['ns', 'test', 'db', 'test', 'table', 't3', 'field', 'f'], severity: 'will_break' }]"

*/

# Can be migrated without issue;
DEFINE FIELD f ON t1 VALUE <future> { 1 };
DEFINE FIELD f ON t2 VALUE ((<future> { 1 }));
DEFINE FIELD f ON t3 DEFAULT <future> { 1 };

migration::diagnose()
