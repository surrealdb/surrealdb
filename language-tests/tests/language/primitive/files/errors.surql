/**

[test]

[env.capabilities]
allow-experimental = ["files"]

# 0: Put on non-existent bucket
[[test.results]]
error = "The bucket 'nonexistent' does not exist"

# 0b: Get on non-existent bucket  
[[test.results]]
error = "The bucket 'nonexistent' does not exist"

# 1: Setup
[[test.results]]
value = '"OK"'

# 2: Copy nonexistent file
[[test.results]]
error = "Operation for bucket `test` failed: Source key does not exist: /b.txt"

# 3: Rename to same location
[[test.results]]
error = "Operation for bucket `test` failed: Source key does not exist: /b.txt"

# 4: Delete non-existent file (should succeed - idempotent)
[[test.results]]
value = "NONE"

# 5: Get non-existent file returns NONE
[[test.results]]
value = "NONE"

# 6: Rename_if_not_exists on nonexistent file should error
[[test.results]]
error = "Operation for bucket `test` failed: Source key does not exist: /nonexistent.txt"

# 7: Copy_if_not_exists on nonexistent file should succeed (idempotent)
[[test.results]]
value = "NONE"

*/

// 0: Put on non-existent bucket fails
file::put(f"nonexistent:/file.txt", "data");

// 0b: Get on non-existent bucket fails
file::get(f"nonexistent:/file.txt");

// 1: Setup
{
	DEFINE BUCKET test BACKEND "memory";
	file::put(f"test:/a.txt", "data");
	RETURN "OK";
};

// 2: Copy nonexistent file
file::copy(f"test:/b.txt", "c.txt");

// 3: Rename nonexistent file
file::rename(f"test:/b.txt", "c.txt");

// 4: Delete non-existent file (should succeed - idempotent)
file::delete(f"test:/nonexistent.txt");

// 5: Get non-existent file returns NONE
file::get(f"test:/nonexistent.txt");

// 6: Rename_if_not_exists on nonexistent file should error
file::rename_if_not_exists(f"test:/nonexistent.txt", "target.txt");

// 7: Copy_if_not_exists on nonexistent file should succeed (idempotent)
file::copy_if_not_exists(f"test:/nonexistent.txt", "target.txt");
