/**

[env.capabilities]
allow-experimental = ["files"]

[test]

[[test.results]]
value = 'f"foo:/bar"'

[[test.results]]
value = 'f"foo.baz:/bar"'

[[test.results]]
value = 'f"2:/bar"'

[[test.results]]
value = 'f"foo:/"'
*/

f"foo:/bar";
f"foo.baz:/bar";
f"\u{32}:/bar";
f"foo:/";
