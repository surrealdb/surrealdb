/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: { test: 'DEFINE FUNCTION fn::test($first: string, $last: string) { RETURN $first + $last } PERMISSIONS FULL' }, models: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"

[[test.results]]
value = "'ab'"

*/

DEFINE FUNCTION fn::test($first: string, $last: string) {
    RETURN $first + $last;
};
INFO FOR DB;
fn::test("a","b");
