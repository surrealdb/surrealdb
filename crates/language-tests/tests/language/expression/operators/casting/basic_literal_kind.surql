/**
[test]

[[test.results]]
value = "'a'"

[[test.results]]
error = "Expected a 'b' but cannot convert 'a' into a 'b'"

[[test.results]]
error = "Expected a 'b' but cannot convert 123 into a 'b'"

[[test.results]]
value = "123"

[[test.results]]
value = "123"

[[test.results]]
error = "Expected a 123 but cannot convert 'a' into a 123"

[[test.results]]
value = "[123, 'abc']"

[[test.results]]
value = "[123, 'abc']"

[[test.results]]
error = "Expected a [123, 'abd'] but cannot convert [123, 'abc'] into a [123, 'abd']"

[[test.results]]
value = "{ a: 1d }"

[[test.results]]
error = "Expected a { a: 2d } but cannot convert { a: 1d } into a { a: 2d }"

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
