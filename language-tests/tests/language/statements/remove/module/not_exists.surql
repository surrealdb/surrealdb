/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = "With the `surrealism` experimental capability granted, REMOVE MODULE parses and executes against the database catalog. Removing an absent module errors and IF EXISTS turns the same not-found into NONE. Neither outcome depends on the `surrealism` cargo feature: the not-found path returns before the feature-gated module-cache eviction. The silo name form is covered separately in not_exists_silo_name.surql."

[[test.results]]
error = "The module 'mod::test' does not exist"

[[test.results]]
value = "NONE"

*/

-- 0: no such module, so the removal errors
REMOVE MODULE mod::test;

-- 1: IF EXISTS swallows the not-found and yields NONE
REMOVE MODULE IF EXISTS mod::test;
