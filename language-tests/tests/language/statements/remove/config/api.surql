/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Test REMOVE CONFIG API removes an existing API config"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

*/

-- 0: Define an API config
DEFINE CONFIG API;

-- 1: Remove the config
REMOVE CONFIG API;

-- 2: Verify it is gone
(INFO FOR DB STRUCTURE).configs;
