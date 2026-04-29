/**
[test]
reason = "Test ALTER BUCKET with all variations"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: { mybucket: "DEFINE BUCKET mybucket BACKEND 'memory' PERMISSIONS FULL" }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: { mybucket: "DEFINE BUCKET mybucket READONLY BACKEND 'memory' PERMISSIONS NONE COMMENT 'updated bucket'" }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: { mybucket: "DEFINE BUCKET mybucket BACKEND 'memory' PERMISSIONS FULL" }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "The bucket 'nonexistent' does not exist"

[env.capabilities]
allow-experimental = ["files"]

*/

-- Setup
DEFINE BUCKET mybucket BACKEND 'memory';
INFO FOR DB;

-- Alter: set readonly, comment, and permissions
ALTER BUCKET mybucket READONLY COMMENT "updated bucket" PERMISSIONS NONE;
INFO FOR DB;

-- Alter: drop readonly, comment, and change permissions
ALTER BUCKET mybucket DROP READONLY DROP COMMENT PERMISSIONS FULL;
INFO FOR DB;

-- IF EXISTS on non-existent bucket
ALTER BUCKET IF EXISTS nonexistent COMMENT "nope";

-- Non-existent bucket errors
ALTER BUCKET nonexistent COMMENT "nope";
