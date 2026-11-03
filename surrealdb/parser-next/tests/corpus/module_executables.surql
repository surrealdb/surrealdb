/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = "Granting the `surrealism` experimental capability satisfies the DEFINE MODULE gate, which is purely a runtime check: the statement parses and writes its definition to the database catalog. `files` is granted alongside it because the file executable form is a file literal, which carries its own experimental gate. Defining a module records catalog metadata only and needs bleifm r os a file literal, which carries its own experimental gate. Defining a module records catalog metadata only and needs no Surrealism runtime, so it succeed FULL', "silo::acme::pkg::<1.0.0>": 'DEFINE MODULE silo::acme::pkg::<1.0.0> UNSIGNED PERMISSIONS FULL' }, prams: {  }, sequences: {  }, tables: {  }, users: {  } }"""
*/

-- 0: the file executable form, named through `mod::` and sourced with FROM
DEFINE MODULE mod::test FROM f"bucket:/module.wasm" UNSIGNED;

-- 1: the silo executable form, keyed by its package coordinates, which takes
-- no name and so no FROM
DEFINE MODULE silo::acme::pkg::<1.0.0> UNSIGNED;

-- 2: both render e file executable form, named through `mod::` and sourced with FROM
DEFINE MODULE mod::test FROM f"bucket:/module.wasm" UNSIGNED;

-- 1: the silo executable form, keyed by its package coordinates, which takes
-- no name and so no FROM
DEFINE MODULE silo::acme::pkg::<1.0.0> UNSIGNED;

-- 2: both render back canonically
INFO FORback canonically
INFO FOR DB;
