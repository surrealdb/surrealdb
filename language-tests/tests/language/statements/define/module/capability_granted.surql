/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = "Granting the `surrealism` experimental capability satisfies the DEFINE MODULE gate, which is purely a runtime check: the statement parses and writes its definition to the database catalog. `files` is granted alongside it because the file executable form is a file literal, which carries its own experimental gate. Defining a module records catalog metadata only and needs no Surrealism runtime, so it succeeds in a build that omits the `surrealism` cargo feature -- as this one does; invoking the module is what requires the runtime. Both executable forms carry the trailing UNSIGNED clause, which is mandatory while no module source is signature-verified, and both render it back through INFO FOR DB. The silo form takes no `mod::` name: it is keyed by the package coordinates themselves."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: { "mod::test": 'DEFINE MODULE mod::test AS f"bucket:/module.wasm" UNSIGNED PERMISSIONS FULL', "silo::acme::pkg::<1.0.0>": 'DEFINE MODULE silo::acme::pkg::<1.0.0> UNSIGNED PERMISSIONS FULL' }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""
*/

-- 0: the file executable form, named through `mod::`
DEFINE MODULE mod::test AS f"bucket:/module.wasm" UNSIGNED;

-- 1: the silo executable form, keyed by its package coordinates
DEFINE MODULE silo::acme::pkg::<1.0.0> UNSIGNED;

-- 2: both render back with the UNSIGNED clause they were defined with
INFO FOR DB;
