/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = "`ext::name::fn()` and its deprecated alias `mod::name::fn()` call the same definition, so both report the same outcome under the same `ext::` name. A call renders with the prefix it was written in, so a stored body keeps the key its projections were written to produce."

[[test.results]]
value = "NONE"

[[test.results]]
error = "Extension function 'ext::test::run' requires the 'surrealism' feature to be enabled"

[[test.results]]
error = "Extension function 'ext::test::run' requires the 'surrealism' feature to be enabled"

[[test.results]]
error = "Extension function 'ext::missing::run' requires the 'surrealism' feature to be enabled"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ wrap: 'DEFINE FUNCTION fn::wrap() { RETURN mod::test::run(1) } PERMISSIONS FULL' }"
*/

-- 0
DEFINE EXTENSION ext::test FROM f"bucket:/module.wasm" DIGEST "sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d" UNSIGNED;

-- 1
RETURN ext::test::run();

-- 2
RETURN mod::test::run();

-- 3: an absent extension
RETURN ext::missing::run();

-- 4: the call renders as written
DEFINE FUNCTION fn::wrap() { RETURN mod::test::run(1); };

-- 5
(INFO FOR DB).functions;
