/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ in: 'DEFINE FIELD in ON test TYPE record<a> PERMISSIONS FULL', out: 'DEFINE FIELD out ON test TYPE record<b> PERMISSIONS FULL' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ in: 'DEFINE FIELD in ON test TYPE record<a> PERMISSIONS FULL', out: 'DEFINE FIELD out ON test TYPE record<b> PERMISSIONS FULL' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ arr: 'DEFINE FIELD arr ON test TYPE array<number> PERMISSIONS FULL', "arr[*]": 'DEFINE FIELD arr[*] ON test TYPE number PERMISSIONS FULL', in: 'DEFINE FIELD in ON test TYPE record<a> PERMISSIONS FULL', out: 'DEFINE FIELD out ON test TYPE record<b> PERMISSIONS FULL' }'''

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ arr: 'DEFINE FIELD arr ON test TYPE array<number> PERMISSIONS FULL', "arr[*]": 'DEFINE FIELD arr[*] ON test TYPE number PERMISSIONS FULL', in: 'DEFINE FIELD in ON test TYPE record<a> PERMISSIONS FULL', out: 'DEFINE FIELD out ON test TYPE record<b> PERMISSIONS FULL' }'''


*/
OPTION IMPORT;
DEFINE TABLE test TYPE RELATION IN a OUT b;

(INFO FOR TB test).fields;

DEFINE FIELD in ON test TYPE record<a>;
DEFINE FIELD out ON test TYPE record<b>;

(INFO FOR TB test).fields;

DEFINE FIELD arr ON test TYPE array<number>;

(INFO FOR TB test).fields;

DEFINE FIELD arr.* ON test TYPE number;

(INFO FOR TB test).fields;
