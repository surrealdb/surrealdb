/**
[test]
reason = "Test recursion depth limits and error handling"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found 0 for bound but expected at least 1."

[[test.results]]
error = "Exceeded the idiom recursion limit of 256."

[[test.results]]
value = "a:257"

[[test.results]]
error = "Found 257 for bound but expected 256 at most."

[[test.results]]
error = "Tried to use a `@` repeat recurse symbol in a position where it is not supported"

[env]
timeout = 10000
*/

FOR $i IN 1..=300 {
	UPSERT type::record('a', $i) SET link = type::record('a', $i + 1);
};

a:1.{0..}.link;
a:1.{1..}.link;
a:1.{..256}.link;
a:1.{..257}.link;

a:1.@;
