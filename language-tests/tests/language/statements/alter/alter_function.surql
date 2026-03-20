/**
[test]
reason = "Test ALTER FUNCTION with all variations"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: { greet: "DEFINE FUNCTION fn::greet($name: string) -> string { RETURN 'hello ' + $name } PERMISSIONS FULL" }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: { greet: "DEFINE FUNCTION fn::greet($name: string) -> string { RETURN 'hi ' + $name } COMMENT 'updated' PERMISSIONS NONE" }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: { greet: "DEFINE FUNCTION fn::greet($name: string) -> string { RETURN 'hi ' + $name } PERMISSIONS NONE" }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "The function 'fn::nonexistent' does not exist"

*/

-- Setup
DEFINE FUNCTION fn::greet($name: string) -> string { RETURN "hello " + $name; };
INFO FOR DB;

-- Alter: change body, comment, and permissions
ALTER FUNCTION fn::greet($name: string) -> string { RETURN "hi " + $name; } COMMENT "updated" PERMISSIONS NONE;
INFO FOR DB;

-- Alter: drop comment
ALTER FUNCTION fn::greet DROP COMMENT;
INFO FOR DB;

-- IF EXISTS on non-existent function
ALTER FUNCTION IF EXISTS fn::nonexistent COMMENT "nope";

-- Non-existent function errors
ALTER FUNCTION fn::nonexistent COMMENT "nope";
