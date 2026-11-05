/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = "DEFINE EXTENSION writes a named definition for both executable forms, a file and a silo package. The silo form takes an `ext::` name like the file form, so a query calls every extension through `ext::` whatever its source. Defining an extension that gives its DIGEST records catalog metadata only and fetches no archive, so it succeeds in a build without the `surrealism` cargo feature. INFO FOR DB lists the definitions under `extensions`, keyed by `ext::` name, and renders each statement as DEFINE EXTENSION."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, extensions: { "ext::pkg": "DEFINE EXTENSION ext::pkg FROM silo::acme::pkg<1.0.0> DIGEST 'sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d' UNSIGNED CAPABILITIES {} COMMENT 'from silo' PERMISSIONS FULL", "ext::test": "DEFINE EXTENSION ext::test FROM f\"bucket:/module.wasm\" DIGEST 'sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d' UNSIGNED CAPABILITIES {} PERMISSIONS FULL" }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, types: {  }, users: {  } }'''
*/

-- 0: the file executable form
DEFINE EXTENSION ext::test FROM f"bucket:/module.wasm" DIGEST "sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d" UNSIGNED;

-- 1: the silo executable form, named like any other extension
DEFINE EXTENSION ext::pkg FROM silo::acme::pkg::<1.0.0> DIGEST "sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d" UNSIGNED COMMENT 'from silo';

-- 2: both render back in the EXTENSION spelling
INFO FOR DB;
