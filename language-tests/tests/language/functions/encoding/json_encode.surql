/**
[test]
reason = "Test encoding::json::encode with various value types"

[[test.results]]
value = """'"hello"'"""

[[test.results]]
value = """'""'"""

[[test.results]]
value = "'42'"

[[test.results]]
value = "'-7'"

[[test.results]]
value = "'3.14'"

[[test.results]]
value = "'true'"

[[test.results]]
value = "'false'"

[[test.results]]
value = "'null'"

[[test.results]]
value = "'null'"

[[test.results]]
value = "'[1,2,3]'"

[[test.results]]
value = "'[]'"

[[test.results]]
value = """'{"name":"test","value":42}'"""

[[test.results]]
value = "'{}'"

[[test.results]]
value = """'{"user":{"name":"Tobie","tags":["admin","user"]}}'"""

[[test.results]]
value = """'[{"id":1},{"id":2}]'"""

*/

encoding::json::encode("hello");
encoding::json::encode("");
encoding::json::encode(42);
encoding::json::encode(-7);
encoding::json::encode(3.14);
encoding::json::encode(true);
encoding::json::encode(false);
encoding::json::encode(NULL);
encoding::json::encode(NONE);
encoding::json::encode([1, 2, 3]);
encoding::json::encode([]);
encoding::json::encode({ name: "test", value: 42 });
encoding::json::encode({});
encoding::json::encode({ user: { name: "Tobie", tags: ["admin", "user"] } });
encoding::json::encode([{ id: 1 }, { id: 2 }]);
