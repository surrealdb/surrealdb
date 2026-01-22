/**
[test]

[[test.results]]
value = "NULL"

[[test.results]]
value = "[{ details: '', error: 'Found future which can be stored in the datastore', kind: 'stored closure', location: { column: 44, kind: 'error', length: 10, line: 1, source: 'DEFINE FUNCTION fn::foo(){CREATE f SET a = ||{(1+1);}', truncation: 'none' }, origin: '/ns/test/db/test/fn/foo', severity: 'will_break' }]"

*/

{
	DEFINE FUNCTION fn::foo(){
		CREATE f SET a = || { 1 + 1 };
	};
	DEFINE FUNCTION fn::bar(){
		CREATE f SET a = (|| { 1 + 1 }) + 1;
	};
	NULL;
};

migration::diagnose();
