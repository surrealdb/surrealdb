/**

[test]

[env.capabilities]
allow-experimental = ["files"]

# 0: Setup
[[test.results]]
value = '"OK"'

# 1: Empty file content
[[test.results]]
value = "NONE"

# 2: Get empty file
[[test.results]]
value = 'b""'

# 3: Empty file size
[[test.results]]
value = "0"

# 4: Long path
[[test.results]]
value = "NONE"

# 5: Get long path
[[test.results]]
value = '"content"'

# 6: Special characters in path
[[test.results]]
value = "NONE"

# 7: Check special chars file exists
[[test.results]]
value = "true"

# 8: Root level file
[[test.results]]
value = "NONE"

# 9: Get root level file
[[test.results]]
value = '"root"'

# 10: Path with numbers
[[test.results]]
value = "NONE"

# 11: Check numbers path exists
[[test.results]]
value = "true"

*/

// 0: Setup
{
	DEFINE BUCKET test BACKEND "memory";
	RETURN "OK";
};

// 1: Empty file content
file::put(f"test:/empty.txt", "");

// 2: Get empty file
file::get(f"test:/empty.txt");

// 3: Empty file size
file::head(f"test:/empty.txt").size;

// 4: Long path
file::put(f"test:/very/deeply/nested/directory/structure/file.txt", "content");

// 5: Get long path
file::get(f"test:/very/deeply/nested/directory/structure/file.txt").to_string();

// 6: Special characters in path
file::put(f"test:/path-with_special.chars.txt", "test");

// 7: Check special chars file exists
file::exists(f"test:/path-with_special.chars.txt");

// 8: Root level file
file::put(f"test:/root.txt", "root");

// 9: Get root level file
file::get(f"test:/root.txt").to_string();

// 10: Path with numbers
file::put(f"test:/file123/test456.txt", "nums");

// 11: Check numbers path exists
file::exists(f"test:/file123/test456.txt");
