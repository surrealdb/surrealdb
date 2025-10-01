/**

[test]

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

[env.capabilities]
allow-experimental = ["files"]

*/

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
