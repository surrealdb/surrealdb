/**

[test]

[env.capabilities]
allow-experimental = ["files"]

# 0: Setup
[[test.results]]
value = "'OK'"

# 1: Get dst file -- NONE
[[test.results]]
value = "NONE"

# 2: Copy source file to dst
[[test.results]]
value = "NONE"

# 3: Get dst file -- NONE
[[test.results]]
value = "NONE"

# 4: Copy dst -> dst
[[test.results]]
value = 'b"616263"'

# 5: Get dst file -- "abc"
[[test.results]]
value = "NONE"

# 6: Copy dst -> dst with only path
[[test.results]]
value = "NONE"

# 7: Get dst file -- "abc"
[[test.results]]
value = 'b"616263"'

*/

// 0: Setup
{
    DEFINE BUCKET src BACKEND "memory";
    DEFINE BUCKET dst BACKEND "memory";
    RETURN "OK";
};

// 1: Create source file
file::put(f"src:/a.txt", "abc");
// 2: Get dst file -- NONE
file::get(f"dst:/a.txt");
// 3: Copy source file to dst
file::copy(f"src:/a.txt", f"dst:/a.txt");
// 4: Get dst file -- "abc"
file::get(f"dst:/a.txt");
// 5: Copy dst -> dst
file::copy(f"dst:/a.txt", f"dst:/b.txt");
// 6: Copy dst -> dst with only path
file::copy(f"dst:/b.txt", "c.txt");
// 7: Get dst file -- "abc"
file::get(f"dst:/c.txt");