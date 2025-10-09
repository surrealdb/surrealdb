/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: { foo: 'DEFINE FUNCTION fn::foo() { RETURN (SELECT * FROM foo WHERE true) OR false } PERMISSIONS FULL' }, models: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"

[[test.results]]
value = "[{ id: foo:v3cq5e4gkqdjz9xe4lrb }]"

[[test.results]]
value = "[{ id: foo:v3cq5e4gkqdjz9xe4lrb }]"

*/

DEFINE FUNCTION fn::foo(){
	RETURN (SELECT * FROM foo WHERE true) || false
};
INFO FOR DB;
CREATE foo;
fn::foo();
