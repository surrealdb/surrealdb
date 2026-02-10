/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ details: '', error: 'Found use of function math::max, this function now returns `-Infinity` when called with an empty array', kind: 'function logical_or', location: { column: 27, kind: 'error', length: 13, line: 1, source: 'DEFINE FUNCTION fn::foo(){math::max([])', truncation: 'none' }, origin: ['ns', 'test', 'db', 'test'], severity: 'unlikely_break' }, { details: '', error: 'Found use of function math::max, this function now returns `-Infinity` when called with an empty array', kind: 'function logical_or', location: { column: 27, kind: 'error', length: 13, line: 1, source: 'DEFINE FUNCTION fn::foo(){math::max([])', truncation: 'none' }, origin: ['ns', 'test', 'db', 'test', 'function', 'foo'], severity: 'unlikely_break' }, { details: '', error: 'Found use of function math::max, this function now returns `-Infinity` when called with an empty array', kind: 'function logical_or', location: { column: 35, kind: 'error', length: 13, line: 1, source: 'DEFINE FIELD foo ON bar COMPUTED {math::max([])', truncation: 'none' }, origin: ['ns', 'test', 'db', 'test', 'table', 'bar', 'field', 'foo'], severity: 'unlikely_break' }]"

*/

{
	DEFINE FUNCTION fn::foo(){
		math::max([])
	};
	DEFINE FIELD foo ON bar VALUE <future> { math::max([]) };
};

migration::diagnose();
