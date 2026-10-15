/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = "REMOVE EXTENSION removes a definition by its `ext::` name. The deprecated REMOVE MODULE and `mod::` spellings remove the same definition, and an absent extension errors unless IF EXISTS is given."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{  }"

[[test.results]]
error = "The extension 'ext::a' does not exist"

[[test.results]]
value = "NONE"
*/

-- 0
DEFINE EXTENSION ext::a FROM f"bucket:/a.wasm" UNSIGNED;

-- 1
DEFINE EXTENSION ext::b FROM f"bucket:/b.wasm" UNSIGNED;

-- 2: the new spelling
REMOVE EXTENSION ext::a;

-- 3: the deprecated spelling removes a definition made with the new one
REMOVE MODULE mod::b;

-- 4
(INFO FOR DB).extensions;

-- 5
REMOVE EXTENSION ext::a;

-- 6
REMOVE EXTENSION IF EXISTS ext::a;
