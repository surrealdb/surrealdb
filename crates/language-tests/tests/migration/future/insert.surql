/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ error: 'Found a future which would be stored in the database', kind: 'incompatible future', location: { column: 42, kind: 'error', label: 'Incompatible future', length: 23, line: 1, source: 'DEFINE PARAM  fn::t(){INSERT INTO foo {f:<future> {time::now();}', truncation: 'none' }, origin: '/ns/test/db/test/fn/t', severity: 'will_break' }]"

*/

DEFINE FUNCTION fn::t() {
	INSERT INTO foo {
		f: <future>{ time::now() }
	}
};

migration::diagnose()
