/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ details: '', error: 'Found use of function array::logical_or, this function now has different behavior', kind: 'function logical_or', location: { column: 27, kind: 'error', length: 34, line: 1, source: 'DEFINE FUNCTION fn::foo(){array::logical_or([true], [false])', truncation: 'none' }, origin: ['ns', 'test', 'db', 'test'], severity: 'unlikely_break' }, { details: '', error: 'Found use of function array::logical_or, this function now has different behavior', kind: 'function logical_or', location: { column: 27, kind: 'error', length: 34, line: 1, source: 'DEFINE FUNCTION fn::foo(){array::logical_or([true], [false])', truncation: 'none' }, origin: ['ns', 'test', 'db', 'test', 'function', 'foo'], severity: 'unlikely_break' }, { details: '', error: 'Found use of function array::logical_or, this function now has different behavior', kind: 'function logical_or', location: { column: 71, kind: 'error', length: 34, line: 1, source: 'COMPUTED {array::logical_or([true], [false])', truncation: 'start' }, origin: ['ns', 'test', 'db', 'test'], severity: 'unlikely_break' }]"

*/

{
	DEFINE FUNCTION fn::foo(){
		array::logical_or([true],[false])
	};
	DEFINE FIELD foo ON bar VALUE <future> { array::logical_or([true],[false]) };
};

migration::diagnose();
