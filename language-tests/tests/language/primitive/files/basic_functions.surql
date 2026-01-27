/**

[test]

[env.capabilities]
allow-experimental = ["files"]


# 0: Define bucket
[[test.results]]
value = "NONE"

# 1: Get file
[[test.results]]
value = "NONE"
# 2: Head file
[[test.results]]
value = "NONE"
# 3: Check if file exists
[[test.results]]
value = "false"

# 4: Put file if not exists
[[test.results]]
value = "NONE"
# 5: Get file should be new value
[[test.results]]
value = '"abc"'

# 6: Put file if not exists
[[test.results]]
value = "NONE"
# 7: Get file should be old value
[[test.results]]
value = '"abc"'

# 8: Put file
[[test.results]]
value = "NONE"
# 9: Get file should be new value
[[test.results]]
value = '"def"'

# 10: Check if file exists
[[test.results]]
value = "true"

# 11: Copy file if not exists
[[test.results]]
value = "NONE"
# 12: Get file should be new value
[[test.results]]
value = '"def"'

# 13: Put file
[[test.results]]
# 14: Copy file if not exists
value = "NONE"
# 15: Get file should be NONE
[[test.results]]
value = "NONE"
# 16: Copy file
[[test.results]]
value = '"def"'

# 17: Get file should be new value
[[test.results]]
value = "NONE"
# 18: Rename file if not exists
[[test.results]]
value = '"abc"'

# 19: Get file should be new value
[[test.results]]
value = "NONE"
# 20: Rename file if not exists
[[test.results]]
value = "NONE"
# 21: Rename file
[[test.results]]
value = "NONE"
# 22: Rename file
[[test.results]]
value = '"abc"'
# 23: Rename file
[[test.results]]
value = "NONE"
# 24: Delete file
[[test.results]]
value = "NONE"

# 25: Get file should not exist
[[test.results]]
value = "NONE"

# 25: List files
[[test.results]]
value = '[]'

*/

// 0: Define bucket
DEFINE BUCKET test BACKEND "memory";

// 1: Get file
file::get(f"test:/a.txt").?.to_string();
// 2: Head file
file::head(f"test:/a.txt");
// 3: Check if file exists
file::exists(f"test:/a.txt");

// 4: Put file if not exists
file::put_if_not_exists(f"test:/a.txt", "abc");
// 5: Get file
file::get(f"test:/a.txt").?.to_string();

// 6: Put file if not exists
file::put_if_not_exists(f"test:/a.txt", "def");
// 7: Get file should be old value
file::get(f"test:/a.txt").?.to_string();

// 8: Put file
file::put(f"test:/a.txt", "def");
// 9: Get file should be new value
file::get(f"test:/a.txt").?.to_string();

// 10: Check if file exists
file::exists(f"test:/a.txt");

// 11: Copy file if not exists
file::copy_if_not_exists(f"test:/a.txt", "b.txt");
// 12: Get file should be new value
file::get(f"test:/b.txt").?.to_string();

// 13: Put file
file::put(f"test:/b.txt", "abc");
// 14: Copy file if not exists
file::copy_if_not_exists(f"test:/b.txt", "a.txt");
// 15: Get file should be NONE
file::get(f"test:/a.txt").?.to_string();
// 16: Copy file
file::copy(f"test:/b.txt", "a.txt");
// 17: Get file should be new value
file::get(f"test:/a.txt").?.to_string();

// 18: Rename file if not exists
file::rename_if_not_exists(f"test:/a.txt", "c.txt");
// 19: Get file should be new value
file::get(f"test:/a.txt").?.to_string();
// 20: Rename file if not exists
file::rename_if_not_exists(f"test:/b.txt", "c.txt");
// 21: Get file should be new value
file::get(f"test:/b.txt").?.to_string();
// 22: Rename file
file::rename(f"test:/b.txt", "c.txt");
// 23: Get file should not exist
file::get(f"test:/b.txt").?.to_string();

// 24: Delete file
file::delete(f"test:/c.txt");

// 25: List files
file::list("test");
