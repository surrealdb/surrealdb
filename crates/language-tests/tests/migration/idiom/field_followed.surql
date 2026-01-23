/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ details: '', error: 'Found usage of an idiom with a field followed by another idiom part', kind: 'field idiom followed', location: { column: 35, kind: 'error', length: 8, line: 1, source: 'DEFINE FUNCTION fn::foo(){($param).name[1]', truncation: 'none' }, origin: ['ns', 'test', 'db', 'test', 'function', 'foo'], severity: 'can_break' }, { details: '', error: 'Found usage of an idiom with a field followed by another idiom part', kind: 'field idiom followed', location: { column: 35, kind: 'error', length: 8, line: 1, source: 'DEFINE FUNCTION fn::foo(){($param).name[1]', truncation: 'none' }, origin: ['ns', 'test', 'db', 'test', 'function', 'foo', 'function', 'foo'], severity: 'can_break' }]"

*/

{
	DEFINE FUNCTION fn::foo() {
		$param.name[1];
	};
	NONE;
};

migration::diagnose()
