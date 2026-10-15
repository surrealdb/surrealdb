/**
[test]
reason = "A write whose table an embedded script removes while it runs writes every row it was given, and the table is gone after it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }, { id: t:3 }]"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }, { id: t:3 }]"

[[test.results]]
error = "The table 't' does not exist"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, extensions: {  }, functions: { x: 'DEFINE FUNCTION fn::x() { RETURN function() { return surrealdb.query("REMOVE TABLE IF EXISTS t") } } PERMISSIONS FULL' }, models: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""
*/
DEFINE FUNCTION fn::x() { RETURN function() { return surrealdb.query("REMOVE TABLE IF EXISTS t") } };
CREATE t:1, t:2, t:3;
UPDATE t SET a = fn::x();
SELECT * FROM t;
INFO FOR DB;
