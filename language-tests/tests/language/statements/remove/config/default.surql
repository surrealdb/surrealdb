/**
[env]
auth = { level = "owner" }

[test]
reason = "Test REMOVE CONFIG DEFAULT removes an existing default config, and REMOVE CONFIG IF EXISTS DEFAULT handles the non-existent case"

# 0: IF EXISTS returns NONE when no DEFAULT config exists
[[test.results]]
value = "NONE"

# 1: Without IF EXISTS, errors when not found
[[test.results]]
error = "The config for default does not exist"

# 2: Define a DEFAULT config
[[test.results]]
value = "NONE"

# 3: Verify it exists
[[test.results]]
value = "{ database: 'mydb', namespace: 'myns' }"

# 4: Remove the config
[[test.results]]
value = "NONE"

# 5: Verify it is gone
[[test.results]]
value = "{}"
*/

-- 0: IF EXISTS returns NONE when no DEFAULT config exists
REMOVE CONFIG IF EXISTS DEFAULT;

-- 1: Without IF EXISTS, errors
REMOVE CONFIG DEFAULT;

-- 2: Define a DEFAULT config
DEFINE CONFIG DEFAULT NAMESPACE myns DATABASE mydb;

-- 3: Verify it exists
(INFO FOR ROOT).defaults;

-- 4: Remove the config
REMOVE CONFIG DEFAULT;

-- 5: Verify it is gone
(INFO FOR ROOT).defaults;
