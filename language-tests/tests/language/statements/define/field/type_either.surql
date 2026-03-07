/**
[test]
reason = "Test DEFINE FIELD with either types and option types"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ events: {  }, fields: { arr: 'DEFINE FIELD arr ON typed_test TYPE array<int> PERMISSIONS FULL', "arr.*": 'DEFINE FIELD arr.* ON typed_test TYPE int PERMISSIONS FULL', nested: 'DEFINE FIELD nested ON typed_test TYPE none | record<typed_test> PERMISSIONS FULL', opt: 'DEFINE FIELD opt ON typed_test TYPE none | string PERMISSIONS FULL', st: 'DEFINE FIELD st ON typed_test TYPE set<string> PERMISSIONS FULL', "st.*": 'DEFINE FIELD st.* ON typed_test TYPE string PERMISSIONS FULL', val: 'DEFINE FIELD val ON typed_test TYPE string | int PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"""

[[test.results]]
error = "Couldn't coerce value for field `st` of `typed_test:1`: Expected `set` but found `['a', 'b', 'a']`"

[[test.results]]
error = "Couldn't coerce value for field `st` of `typed_test:2`: Expected `set` but found `['c']`"

*/

DEFINE TABLE typed_test;
DEFINE FIELD val ON typed_test TYPE string | int;
DEFINE FIELD opt ON typed_test TYPE option<string>;
DEFINE FIELD nested ON typed_test TYPE option<record<typed_test>>;
DEFINE FIELD arr ON typed_test TYPE array<int>;
DEFINE FIELD st ON typed_test TYPE set<string>;
INFO FOR TABLE typed_test;
CREATE typed_test:1 SET val = 42, opt = NONE, nested = NONE, arr = [1, 2, 3], st = ['a', 'b', 'a'];
CREATE typed_test:2 SET val = 'hello', opt = 'world', nested = typed_test:1, arr = [4, 5], st = ['c'];
