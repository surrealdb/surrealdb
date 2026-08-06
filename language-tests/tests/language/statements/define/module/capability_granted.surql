/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = "Granting the `surrealism` experimental capability satisfies the DEFINE MODULE gate, which is purely a runtime check: the statement parses and writes its definition to the database catalog. `files` is granted alongside it because the file executable form is a file literal, which carries its own experimental gate. Defining a module records catalog metadata only and needs no Surrealism runtime, so it succeeds in a build that omits the `surrealism` cargo feature -- as this one does; invoking the module is what requires the runtime."

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: { "mod::test": 'DEFINE MODULE mod::test AS f"bucket:/module.wasm" PERMISSIONS FULL' }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""
*/

DEFINE MODULE mod::test AS f"bucket:/module.wasm";
INFO FOR DB;
