/**

[test]

[env.capabilities]
allow-experimental = ["files"]

# 0: Setup
[[test.results]]
value = '"OK"'

# 1: Head should return metadata object
[[test.results]]
value = "NONE"

# 2: Metadata should have size field
[[test.results]]
value = "5"

# 3: Metadata should have updated field
[[test.results]]
value = "true"

# 4: Metadata should have file field
[[test.results]]
value = 'f"test:/file.txt"'

# 5: Head on non-existent file returns NONE
[[test.results]]
value = "NONE"

*/

// 0: Setup
{
	DEFINE BUCKET test BACKEND "memory";
	file::put(f"test:/file.txt", "hello");
	RETURN "OK";
};

// 1: Head should return metadata object
LET $meta = file::head(f"test:/file.txt");

// 2: Metadata should have size field
$meta.size;

// 3: Metadata should have updated field
type::is_datetime($meta.updated);

// 4: Metadata should have file field
$meta.file;

// 5: Head on non-existent file returns NONE
file::head(f"test:/missing.txt");
