/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = "ALTER EXTENSION changes a definition addressed by its `ext::` name, and the deprecated ALTER MODULE and `mod::` spellings address the same definition."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ "ext::test": "DEFINE EXTENSION ext::test FROM f\"bucket:/module.wasm\" UNSIGNED COMMENT 'first' PERMISSIONS FULL" }'''

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ "ext::test": "DEFINE EXTENSION ext::test FROM f\"bucket:/module.wasm\" UNSIGNED COMMENT 'second' PERMISSIONS FULL" }'''

[[test.results]]
error = "The extension 'ext::missing' does not exist"

[[test.results]]
value = "NONE"
*/

-- 0
DEFINE EXTENSION ext::test FROM f"bucket:/module.wasm" UNSIGNED;

-- 1: the new spelling
ALTER EXTENSION ext::test COMMENT 'first';

-- 2
(INFO FOR DB).extensions;

-- 3: the deprecated spelling reaches the same definition
ALTER MODULE mod::test COMMENT 'second';

-- 4
(INFO FOR DB).extensions;

-- 5: an absent extension
ALTER EXTENSION ext::missing COMMENT 'x';

-- 6
ALTER EXTENSION IF EXISTS ext::missing COMMENT 'x';
