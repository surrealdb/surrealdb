/**

[test]

[env.capabilities]
allow-experimental = ["files"]

# 0: Info before any buckets
[[test.results]]
value = "{  }"

# 1: Setup buckets
[[test.results]]
value = '"OK"'

# 2: Info shows multiple buckets
[[test.results]]
value = "['bucket1', 'bucket2']"

# 3: Info shows bucket details
[[test.results]]
value = "\"DEFINE BUCKET bucket1 BACKEND 'memory' PERMISSIONS FULL COMMENT 'First bucket'\""

# 4: Remove bucket
[[test.results]]
value = '"OK"'

# 5: Info after bucket removal
[[test.results]]
value = "['bucket2']"

*/

// 0: Info before any buckets
(INFO FOR DATABASE).buckets;

// 1: Setup buckets
{
	DEFINE BUCKET bucket1 BACKEND "memory" COMMENT "First bucket";
	DEFINE BUCKET bucket2 BACKEND "memory" READONLY COMMENT "Second bucket";
	RETURN "OK";
};

// 2: Info shows multiple buckets
(INFO FOR DATABASE).buckets.keys();

// 3: Info shows bucket details
(INFO FOR DATABASE).buckets.bucket1;

// 4: Remove bucket
{
	REMOVE BUCKET bucket1;
	RETURN "OK";
};

// 5: Info after bucket removal
(INFO FOR DATABASE).buckets.keys();
