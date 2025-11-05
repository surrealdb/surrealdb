/**
[test]

[[test.results]]
value = "'a'"

[[test.results]]
error = "Could not cast into `'b'` using input `'a'`"

[[test.results]]
error = "Could not cast into `'b'` using input `123`"

[[test.results]]
value = "123"

[[test.results]]
value = "123"

[[test.results]]
error = "Could not cast into `123` using input `'a'`"

[[test.results]]
value = "[123, 'abc']"

[[test.results]]
value = "[123, 'abc']"

[[test.results]]
error = "Could not cast into `[123, 'abd']` using input `[123, 'abc']`"

[[test.results]]
value = "{ a: 1d }"

[[test.results]]
error = "Could not cast into `{ a: 2d }` using input `{ a: 1d }`"

[[test.results]]
value = "true"

[[test.results]]
error = "Could not cast into `true` using input `false`"

*/
<"a" | "b"> "a";
<"b"> "a";
<"b"> 123;
<123 | 456> 123;
<123 | "b"> 123;
<123> "a";
<[number, "abc"]> [123, "abc"];
<[123, "abc"]> [123, "abc"];
<[123, "abd"]> [123, "abc"];
<{ a: 1d | 2d }> { a: 1d };
<{ a: 2d }> { a: 1d };
<true> true;
<true> false;
