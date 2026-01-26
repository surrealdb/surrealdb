/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ details: '', error: 'Found use of function math::min, this function now return `Infinity` when called with an empty array', kind: 'function logical_or', location: { column: 27, kind: 'error', length: 13, line: 1, source: 'DEFINE FUNCTION fn::foo(){math::min([])', truncation: 'none' }, origin: ['ns', 'test', 'db', 'test', 'function', 'foo'], severity: 'unlikely_break' }, { details: '', error: 'Found use of function math::min, this function now return `Infinity` when called with an empty array', kind: 'function logical_or', location: { column: 27, kind: 'error', length: 13, line: 1, source: 'DEFINE FUNCTION fn::foo(){math::min([])', truncation: 'none' }, origin: ['ns', 'test', 'db', 'test', 'function', 'foo', 'function', 'foo'], severity: 'unlikely_break' }, { details: '', error: 'Found use of function math::min, this function now return `Infinity` when called with an empty array', kind: 'function logical_or', location: { column: 71, kind: 'error', length: 13, line: 1, source: 'COMPUTED {math::min([])', truncation: 'start' }, origin: ['ns', 'test', 'db', 'test', 'table', 'bar', 'field', 'foo', 'value'], severity: 'unlikely_break' }]"

*/

{
	DEFINE FUNCTION fn::foo(){
		math::min([]);
	};
	DEFINE FIELD foo ON bar VALUE <future> { math::min([]) };
};

migration::diagnose();
