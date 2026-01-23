/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ details: '', error: 'Found usage of all idiom `.*` behavior of which has changed when used on an array or object', kind: 'all idiom', location: { column: 35, kind: 'error', length: 2, line: 1, source: 'DEFINE FUNCTION fn::foo(){($param).*', truncation: 'none' }, origin: ['ns', 'test', 'db', 'test', 'function', 'foo'], severity: 'can_break' }, { details: '', error: 'Found usage of all idiom `.*` behavior of which has changed when used on an array or object', kind: 'all idiom', location: { column: 35, kind: 'error', length: 2, line: 1, source: 'DEFINE FUNCTION fn::foo(){($param).*', truncation: 'none' }, origin: ['ns', 'test', 'db', 'test', 'function', 'foo', 'function', 'foo'], severity: 'can_break' }]"

*/
{
	DEFINE FUNCTION fn::foo() {
		$param.*;
	};
	NONE;
};

migration::diagnose()
