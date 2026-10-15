/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = "DEFINE MODULE is the deprecated spelling of DEFINE EXTENSION and behaves the same. Granting the `surrealism` experimental capability satisfies its gate, which is purely a runtime check: the statement parses and writes its definition to the database catalog. `files` is granted alongside it because the file executable form is a file literal, which carries its own experimental gate. Defining a module records catalog metadata only and needs no Surrealism runtime, so it succeeds in a build that omits the `surrealism` cargo feature -- as this one does; invoking the module is what requires the runtime. A named module takes `FROM` before its executable, since what follows is the module's source rather than an alias for it. Both executable forms carry the trailing UNSIGNED clause, mandatory while no module source is signature-verified, and both take a name: an unnamed silo definition is refused (see define/extension/unnamed_silo_refused.surql). Both render in the EXTENSION spelling."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, extensions: { "ext::pkg": 'DEFINE EXTENSION ext::pkg FROM silo::acme::pkg<1.0.0> UNSIGNED PERMISSIONS FULL', "ext::test": 'DEFINE EXTENSION ext::test FROM f"bucket:/module.wasm" UNSIGNED PERMISSIONS FULL' }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""
*/

-- 0: the file executable form, named through `mod::` and sourced with FROM
DEFINE MODULE mod::test FROM f"bucket:/module.wasm" UNSIGNED;

-- 1: the silo executable form, named like the file form
DEFINE MODULE mod::pkg FROM silo::acme::pkg::<1.0.0> UNSIGNED;

-- 2: both render back canonically
INFO FOR DB;
