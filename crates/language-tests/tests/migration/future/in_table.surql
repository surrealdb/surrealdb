/**
[test]

[[test.results]]
value = "[{ a: 1, id: foo:1}]"

[[test.results]]
value = "[{ error: 'Found a future which would be stored in the database', kind: 'incompatible future', location: { column: 4, kind: 'error', label: 'Incompatible future', length: 13, line: 1, source: '{a:<future> {1;}', truncation: 'none' }, origin: '/ns/test/db/test/table/foo/record/1', severity: 'will_break' }]"

*/

CREATE foo:1 SET a = <future> { 1 };

migration::diagnose()
