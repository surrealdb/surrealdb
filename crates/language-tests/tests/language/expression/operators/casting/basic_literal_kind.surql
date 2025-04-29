/**
[test]

[[test.results]]
value = "'a'"

[[test.results]]
error = "Expected `'b'` but found a `'a'`"

[[test.results]]
error = "Expected `'b'` but found a `123`"

[[test.results]]
value = "123"

[[test.results]]
value = "123"

[[test.results]]
error = "Expected `123` but found a `'a'`"

[[test.results]]
value = "[123, 'abc']"

[[test.results]]
value = "[123, 'abc']"

[[test.results]]
error = "Expected `[123, 'abd']` but found a `[123, 'abc']`"

[[test.results]]
value = "{ a: 1d }"

[[test.results]]
error = "Expected `{ a: 2d }` but found a `{ a: 1d }`"

[[test.results]]
value = "true"

[[test.results]]
error = "Expected `true` but found a `false`"

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
