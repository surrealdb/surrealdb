/**

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '[{ file: f"test:/a.txt", size: 3 }, { file: f"test:/b.txt", size: 3 }, { file: f"test:/c.txt", size: 3 }, { file: f"test:/folder1/a.txt", size: 3 }, { file: f"test:/folder1/b.txt", size: 3 }, { file: f"test:/folder1/c.txt", size: 3 }, { file: f"test:/folder2/a.txt", size: 3 }, { file: f"test:/folder2/b.txt", size: 3 }, { file: f"test:/folder2/c.txt", size: 3 }]'
[[test.results]]
value = '[{ file: f"test:/a.txt", size: 3 }, { file: f"test:/b.txt", size: 3 }, { file: f"test:/c.txt", size: 3 }]'
[[test.results]]
value = '[{ file: f"test:/folder1/a.txt", size: 3 }, { file: f"test:/folder1/b.txt", size: 3 }, { file: f"test:/folder1/c.txt", size: 3 }]'
[[test.results]]
value = '[{ file: f"test:/folder1/a.txt", size: 3 }, { file: f"test:/folder1/b.txt", size: 3 }, { file: f"test:/folder1/c.txt", size: 3 }, { file: f"test:/folder2/a.txt", size: 3 }, { file: f"test:/folder2/b.txt", size: 3 }, { file: f"test:/folder2/c.txt", size: 3 }]'
[[test.results]]
value = '[{ file: f"test:/folder1/a.txt", size: 3 }, { file: f"test:/folder1/b.txt", size: 3 }, { file: f"test:/folder1/c.txt", size: 3 }]'

[env.capabilities]
allow-experimental = ["files"]

*/

DEFINE BUCKET test BACKEND "memory";

{
	f"test:/a.txt".put("abc");
	f"test:/b.txt".put("abc");
	f"test:/c.txt".put("abc");
	f"test:/folder1/a.txt".put("abc");
	f"test:/folder1/b.txt".put("abc");
	f"test:/folder1/c.txt".put("abc");
	f"test:/folder2/a.txt".put("abc");
	f"test:/folder2/b.txt".put("abc");
	f"test:/folder2/c.txt".put("abc");
};

file::list("test").map(|$v| $v.{ file, size });
file::list("test", { limit: 3 }).map(|$v| $v.{ file, size });
file::list("test", { limit: 3, start: "/folder1" }).map(|$v| $v.{ file, size });
file::list("test", { start: "/folder1" }).map(|$v| $v.{ file, size });
file::list("test", { prefix: "/folder1" }).map(|$v| $v.{ file, size });
