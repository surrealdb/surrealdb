/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ details: '', error: 'Found a future which would be stored in the database', kind: 'incompatible future', location: { column: 44, kind: 'error', label: 'Incompatible future', length: 23, line: 1, source: 'DEFINE FUNCTION fn::t(){CREATE foo SET a = <future> {time::now();}', truncation: 'none' }, origin: '/ns/test/db/test/fn/t', severity: 'will_break' }]"

*/

DEFINE FUNCTION fn::t() {
	CREATE foo SET a = <future> { time::now() };
};

migration::diagnose()
