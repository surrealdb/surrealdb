/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ details: '', error: 'Found usage of special `$this` parameter, the semantics of which have changed', kind: 'this parent self parameter', location: { column: 34, kind: 'error', label: 'The value of this parameter might change in 3.0', length: 5, line: 1, source: 'DEFINE FUNCTION fn::FOO(){SELECT $this', truncation: 'none' }, origin: '/ns/test/db/test/fn/FOO', severity: 'might_break' }]"

*/

DEFINE FUNCTION fn::FOO (){
	SELECT $this FROM bar;
};
migration::diagnose()
