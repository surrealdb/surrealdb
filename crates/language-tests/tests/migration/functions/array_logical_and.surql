/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ details: '', error: 'Found use of function array::logical_and, this function now has different behavior', kind: 'function logical_and', location: { column: 27, kind: 'error', length: 35, line: 1, source: 'DEFINE FUNCTION fn::foo(){array::logical_and([true], [false])', truncation: 'none' }, origin: '/ns/test/db/test/fn/foo', severity: 'unlikely_break' }, { details: '', error: 'Found use of function array::logical_and, this function now has different behavior', kind: 'function logical_and', location: { column: 71, kind: 'error', length: 35, line: 1, source: 'COMPUTED {array::logical_and([true], [false])', truncation: 'start' }, origin: '/ns/test/db/testtb/bar/field/foo/value', severity: 'unlikely_break' }]"

*/

{
	DEFINE FUNCTION fn::foo(){
		array::logical_and([true],[false])
	};
	DEFINE FIELD foo ON bar VALUE <future> { array::logical_and([true],[false]) };
};

migration::diagnose();
