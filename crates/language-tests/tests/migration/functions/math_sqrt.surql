/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ details: '', error: 'Found use of function math::sqrt, this function now returns `NaN` when called with a negative number', kind: 'function logical_or', location: { column: 27, kind: 'error', length: 14, line: 1, source: 'DEFINE FUNCTION fn::foo(){math::sqrt(-1)', truncation: 'none' }, origin: ['ns', 'test', 'db', 'test'], severity: 'unlikely_break' }, { details: '', error: 'Found use of function math::sqrt, this function now returns `NaN` when called with a negative number', kind: 'function logical_or', location: { column: 27, kind: 'error', length: 14, line: 1, source: 'DEFINE FUNCTION fn::foo(){math::sqrt(-1)', truncation: 'none' }, origin: ['ns', 'test', 'db', 'test', 'function', 'foo'], severity: 'unlikely_break' }, { details: '', error: 'Found use of function math::sqrt, this function now returns `NaN` when called with a negative number', kind: 'function logical_or', location: { column: 71, kind: 'error', length: 14, line: 1, source: 'COMPUTED {math::sqrt(-1)', truncation: 'start' }, origin: ['ns', 'test', 'db', 'test'], severity: 'unlikely_break' }]"

*/

{
	DEFINE FUNCTION fn::foo(){
		math::sqrt(-1);
	};
	DEFINE FIELD foo ON bar VALUE <future> { math::sqrt(-1) };
};

migration::diagnose();
