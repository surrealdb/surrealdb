/**

[test]

[env.capabilities]
allow-experimental = ["files"]

# 0: Setup
[[test.results]]
value = '"OK"'

# 1: Put and get bytes directly
[[test.results]]
value = "NONE"

# 2: Get bytes
[[test.results]]
value = 'b"123fff"'

# 3: Put array
[[test.results]]
value = "NONE"

# 4: Get array as bytes
[[test.results]]
value = 'b"010203"'

# 5: Get returns bytes type
[[test.results]]
value = "true"


*/

// 0: Setup
{
	DEFINE BUCKET test BACKEND "memory";
	RETURN "OK";
};

// 1: Put and get bytes directly
file::put(f"test:/data.bin", b"123fff");

// 2: Get bytes
file::get(f"test:/data.bin");

// 3: Put array
file::put(f"test:/array.dat", [1, 2, 3]);

// 4: Get array as bytes
file::get(f"test:/array.dat");

// 5: Get returns bytes type
type::is_bytes(file::get(f"test:/data.bin"));
