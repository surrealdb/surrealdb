/**

[test]

[env.capabilities]
allow-experimental = ["files"]

[[test.results]]
value = 'f"test:/a.txt"'

[[test.results]]
value = "'test'"

[[test.results]]
value = "'/b.txt'"

*/

f"test:/a.txt";
f"test:/b.txt".bucket();
f"test:/b.txt".key();
