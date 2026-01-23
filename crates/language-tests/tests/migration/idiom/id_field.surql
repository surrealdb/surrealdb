/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ details: '', error: 'Found usage of an `.id` idiom, this field had special behavior for record-ids in 2.0 which has been removed in 3.0', kind: 'id field', location: { column: 35, kind: 'error', length: 3, line: 1, source: 'DEFINE FUNCTION fn::foo(){($param).id', truncation: 'none' }, origin: ['ns', 'test', 'db', 'test', 'function', 'foo'], severity: 'can_break' }, { details: '', error: 'Found usage of an `.id` idiom, this field had special behavior for record-ids in 2.0 which has been removed in 3.0', kind: 'id field', location: { column: 35, kind: 'error', length: 3, line: 1, source: 'DEFINE FUNCTION fn::foo(){($param).id', truncation: 'none' }, origin: ['ns', 'test', 'db', 'test', 'function', 'foo', 'function', 'foo'], severity: 'can_break' }]"

*/
{
	DEFINE FUNCTION fn::foo() {
		$param.id.foo;
	};
	NONE;
};

migration::diagnose()
