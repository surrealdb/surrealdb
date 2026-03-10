/**

[test]

[env.capabilities]
allow-experimental = ["files"]

# 0: Setup
[[test.results]]
value = '"OK"'

# 1: Define readonly bucket succeeds
[[test.results]]
value = '"OK"'

# 2: Put on readonly bucket fails
[[test.results]]
error = "Write operation is not supported, as bucket `readonly_bucket` is in read-only mode"

# 3: Put if not exists on readonly bucket fails
[[test.results]]
error = "Write operation is not supported, as bucket `readonly_bucket` is in read-only mode"

# 4: Delete on readonly bucket fails
[[test.results]]
error = "Write operation is not supported, as bucket `readonly_bucket` is in read-only mode"

# 5: Copy on readonly bucket fails
[[test.results]]
error = "Write operation is not supported, as bucket `readonly_bucket` is in read-only mode"

# 6: Rename on readonly bucket fails
[[test.results]]
error = "Write operation is not supported, as bucket `readonly_bucket` is in read-only mode"

*/

// 0: Setup
{
	DEFINE BUCKET readonly_bucket BACKEND "memory" READONLY;
	RETURN "OK";
};

// 1: Define readonly bucket succeeds
"OK";

// 2: Put on readonly bucket fails
file::put(f"readonly_bucket:/file.txt", "data");

// 3: Put if not exists on readonly bucket fails
file::put_if_not_exists(f"readonly_bucket:/file.txt", "data");

// 4: Delete on readonly bucket fails
file::delete(f"readonly_bucket:/file.txt");

// 5: Copy on readonly bucket fails
file::copy(f"readonly_bucket:/file.txt", "other.txt");

// 6: Rename on readonly bucket fails
file::rename(f"readonly_bucket:/file.txt", "other.txt");
