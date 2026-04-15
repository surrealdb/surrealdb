/**
[test]
reason = "Test encoding::json::decode with various JSON inputs"

[[test.results]]
value = "'hello'"

[[test.results]]
value = "''"

[[test.results]]
value = "42"

[[test.results]]
value = "-7"

[[test.results]]
value = "3.14f"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "NULL"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[]"

[[test.results]]
value = "{ name: 'test', value: 42 }"

[[test.results]]
value = "{  }"

[[test.results]]
value = "{ user: { name: 'Tobie', tags: ['admin', 'user'] } }"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }]"

[[test.results]]
value = "{ count: 5, roundtrip: true }"

[[test.results]]
error = "Incorrect arguments for function encoding::json::decode(). Invalid JSON"

*/

encoding::json::decode('"hello"');
encoding::json::decode('""');
encoding::json::decode('42');
encoding::json::decode('-7');
encoding::json::decode('3.14');
encoding::json::decode('true');
encoding::json::decode('false');
encoding::json::decode('null');
encoding::json::decode('[1,2,3]');
encoding::json::decode('[]');
encoding::json::decode('{"name":"test","value":42}');
encoding::json::decode('{}');
encoding::json::decode('{"user":{"name":"Tobie","tags":["admin","user"]}}');
encoding::json::decode('[{"id":1},{"id":2}]');
encoding::json::decode(encoding::json::encode({ roundtrip: true, count: 5 }));
encoding::json::decode('hello"');
