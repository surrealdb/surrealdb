/**
[env]

[env.capabilities]
insecure-storable-closures = true

[test]

[[test.results]]
value = "NULL"

[[test.results]]
value = "[{ details: '', error: 'Found future which can be stored in the datastore', kind: 'stored closure', location: { column: 4, kind: 'error', length: 10, line: 1, source: '{a:||{(1+1);}', truncation: 'none' }, origin: ['ns', 'test', 'db', 'test', 'table', 'f', 'record', 1], severity: 'will_break' }]"

*/
{
	CREATE f:1 SET a = || { 1 + 1 };
	NULL;
};

migration::diagnose();
