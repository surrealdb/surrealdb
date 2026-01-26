/**

[test]

[env.capabilities]
allow-experimental = ["files"]

# 0: Setup
[[test.results]]
value = '"OK"'

# 1: List empty bucket
[[test.results]]
value = "[]"

# 2: List with limit 0
[[test.results]]
value = "[]"

# 3: Setup with files
[[test.results]]
value = '"OK"'

# 4: List with very large limit
[[test.results]]
value = "3"

# 5: List with non-existent start key
[[test.results]]
value = "[]"

# 6: List with prefix that matches nothing
[[test.results]]
value = "[]"

# 7: List with both start and prefix
[[test.results]]
value = '[f"test:/b.txt"]'

*/

// 0: Setup
{
	DEFINE BUCKET test BACKEND "memory";
	RETURN "OK";
};

// 1: List empty bucket
file::list("test");

// 2: List with limit 0
file::list("test", { limit: 0 });

// 3: Setup with files
{
	file::put(f"test:/a.txt", "a");
	file::put(f"test:/b.txt", "b");
	file::put(f"test:/c.txt", "c");
	RETURN "OK";
};

// 4: List with very large limit
file::list("test", { limit: 999999 }).len();

// 5: List with non-existent start key
file::list("test", { start: "/zzz_nonexistent" });

// 6: List with prefix that matches nothing
file::list("test", { prefix: "/nonexistent/" });

// 7: List with both start and prefix
file::list("test", { start: "/a", prefix: "/b" }).file;
