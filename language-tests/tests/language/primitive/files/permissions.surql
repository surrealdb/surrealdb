/**

[test]

[env.capabilities]
allow-experimental = ["files"]

# 0: Setup NONE permissions
[[test.results]]
value = '"OK"'

# 1: Operation on NONE permission bucket
[[test.results]]
value = "NONE"

# 2: Setup FULL permissions
[[test.results]]
value = '"OK"'

# 3: Put on FULL permission bucket
[[test.results]]
value = "NONE"

# 4: Check file exists on FULL permission bucket
[[test.results]]
value = "true"

*/

// 0: Setup NONE permissions
{
	DEFINE BUCKET restricted BACKEND "memory" PERMISSIONS NONE;
	RETURN "OK";
};

// 1: Operation on NONE permission bucket
file::put(f"restricted:/file.txt", "data");

// 2: Setup FULL permissions
{
	REMOVE BUCKET restricted;
	DEFINE BUCKET restricted BACKEND "memory" PERMISSIONS FULL;
	RETURN "OK";
};

// 3: Put on FULL permission bucket
file::put(f"restricted:/file.txt", "data");

// 4: Check file exists on FULL permission bucket
file::exists(f"restricted:/file.txt");
