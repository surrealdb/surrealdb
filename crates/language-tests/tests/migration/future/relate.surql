/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ error: 'Found a future which would be stored in the database', kind: 'incompatible future', location: { column: 52, kind: 'error', label: 'Incompatible future', length: 23, line: 1, source: ': SET a = <future> {time::now();}', truncation: 'start' }, origin: '/ns/test/db/test/fn/t', severity: 'will_break' }]"

*/

DEFINE FUNCTION fn::t() {
	RELATE a:1->b->c:1 SET a = <future> { time::now() };
};

migration::diagnose()
