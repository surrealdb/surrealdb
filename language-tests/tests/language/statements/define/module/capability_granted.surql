/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = "Granting the `surrealism` experimental capability satisfies the DEFINE MODULE gate, which is purely a runtime check: the statement parses and writes its definition to the database catalog. `files` is granted alongside it because the file executable form is a file literal, which carries its own experimental gate. Defining a module records catalog metadata only and needs no Surrealism runtime, so it succeeds in a build that omits the `surrealism` cargo feature -- as this one does; invoking the module is what requires the runtime. A named module takes `FROM` before its executable, since what follows is the module's source rather than an alias for it. Both executable forms carry the trailing UNSIGNED clause, mandatory while no module source is signature-verified, and the silo form takes no `mod::` name at all: it is keyed by the package coordinates themselves."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: { "mod::test": 'DEFINE MODULE mod::test FROM f"bucket:/module.wasm" UNSIGNED PERMISSIONS FULL', "silo::acme::pkg::<1.0.0>": 'DEFINE MODULE silo::acme::pkg::<1.0.0> UNSIGNED PERMISSIONS FULL' }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""
*/

-- 0: the file executable form, named through `mod::` and sourced with FROM
DEFINE MODULE mod::test FROM f"bucket:/module.wasm" UNSIGNED;

-- 1: the silo executable form, keyed by its package coordinates, which takes
-- no name and so no FROM
DEFINE MODULE silo::acme::pkg::<1.0.0> UNSIGNED;

-- 2: both render back canonically
INFO FOR DB;
