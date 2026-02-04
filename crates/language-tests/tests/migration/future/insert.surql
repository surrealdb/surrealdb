/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ details: '', error: 'Found a future which would be stored in the database', kind: 'incompatible future', location: { column: 44, kind: 'error', label: 'Incompatible future', length: 24, line: 1, source: 'DEFINE FUNCTION fn::t(){INSERT INTO foo {f:<future> ( time::now() )', truncation: 'none' }, origin: ['ns', 'test', 'db', 'test'], severity: 'will_break' }, { details: '', error: 'Found a future which would be stored in the database', kind: 'incompatible future', location: { column: 44, kind: 'error', label: 'Incompatible future', length: 24, line: 1, source: 'DEFINE FUNCTION fn::t(){INSERT INTO foo {f:<future> ( time::now() )', truncation: 'none' }, origin: ['ns', 'test', 'db', 'test', 'function', 't'], severity: 'will_break' }]"

*/

DEFINE FUNCTION fn::t() {
	INSERT INTO foo {
		f: <future>{ time::now() }
	}
};

migration::diagnose()
