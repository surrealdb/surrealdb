/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Test REMOVE CONFIG API removes an existing API config"

# 0: Define an API config
[[test.results]]
value = "NONE"

# 1: Verify it exists
[[test.results]]
value = "[{ api: { permissions: true } }]"

# 2: Remove the config
[[test.results]]
value = "NONE"

# 3: Verify it is gone
[[test.results]]
value = "[]"
*/

-- 0: Define an API config
DEFINE CONFIG API;

-- 1: Verify it exists
(INFO FOR DB STRUCTURE).configs;

-- 2: Remove the config
REMOVE CONFIG API;

-- 3: Verify it is gone
(INFO FOR DB STRUCTURE).configs;
