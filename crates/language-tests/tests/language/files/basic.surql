/**

[test]

[[test.results]]
value = 'f"test:/a.txt"'

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"
[[test.results]]
value = "false"

[[test.results]]
value = "NONE"
[[test.results]]
value = '"abc"'

[[test.results]]
value = "NONE"
[[test.results]]
value = '"abc"'

[[test.results]]
value = "NONE"
[[test.results]]
value = '"def"'

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"
[[test.results]]
value = '"def"'

[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"
[[test.results]]
value = '"def"'
[[test.results]]
value = "NONE"
[[test.results]]
value = '"abc"'

[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"
[[test.results]]
value = '"abc"'
[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"

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

[[test.results]]
value = "'test'"
[[test.results]]
value = "'/b.txt'"

[env.capabilities]
allow-experimental = ["files"]

*/

f"test:/a.txt";

DEFINE BUCKET test BACKEND "memory";

f"test:/a.txt".get()?.to_string();
f"test:/a.txt".head();
f"test:/a.txt".exists();

f"test:/a.txt".put_if_not_exists("abc");
f"test:/a.txt".get()?.to_string();

f"test:/a.txt".put_if_not_exists("def");
f"test:/a.txt".get()?.to_string();

f"test:/a.txt".put("def");
f"test:/a.txt".get()?.to_string();

f"test:/a.txt".exists();

f"test:/a.txt".copy_if_not_exists("b.txt");
f"test:/b.txt".get()?.to_string();

f"test:/b.txt".put("abc");
f"test:/b.txt".copy_if_not_exists("a.txt");
f"test:/a.txt".get()?.to_string();
f"test:/b.txt".copy("a.txt");
f"test:/a.txt".get()?.to_string();

f"test:/a.txt".rename_if_not_exists("c.txt");
f"test:/a.txt".get()?.to_string();
f"test:/b.txt".rename_if_not_exists("c.txt");
f"test:/b.txt".get()?.to_string();
f"test:/b.txt".rename("c.txt");
f"test:/b.txt".get()?.to_string();

f"test:/c.txt".delete();

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

f"test:/b.txt".bucket();
f"test:/b.txt".key();
