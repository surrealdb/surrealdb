/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ details: '', error: 'Found use of function array::range, this function now has different behavior', kind: 'function logical_or', location: { column: 27, kind: 'error', length: 18, line: 1, source: 'DEFINE FUNCTION fn::foo(){array::range(1, 2)', truncation: 'none' }, origin: ['ns', 'test', 'db', 'test'], severity: 'can_break' }, { details: '', error: 'Found use of function array::range, this function now has different behavior', kind: 'function logical_or', location: { column: 27, kind: 'error', length: 18, line: 1, source: 'DEFINE FUNCTION fn::foo(){array::range(1, 2)', truncation: 'none' }, origin: ['ns', 'test', 'db', 'test', 'function', 'foo'], severity: 'can_break' }, { details: '', error: 'Found use of function array::range, this function now has different behavior', kind: 'function logical_or', location: { column: 35, kind: 'error', length: 18, line: 1, source: 'DEFINE FIELD foo ON bar COMPUTED {array::range(1, 2)', truncation: 'none' }, origin: ['ns', 'test', 'db', 'test', 'table', 'bar', 'field', 'foo'], severity: 'can_break' }]"

*/

{
	DEFINE FUNCTION fn::foo(){
		array::range(1,2)
	};
	DEFINE FIELD foo ON bar VALUE <future> { array::range(1,2) };
};

migration::diagnose();
