/**
[env.capabilities]
allow-experimental = []

[test]
reason = "An `ext::` function call is rejected at parse time when Surrealism is not enabled, with the caret on the `ext` keyword."

[test.results]
parsing-error = """
Surrealism extensions are not enabled
  --> [19:8]
   |
19 | RETURN ext::nonexistent();
   |        ^^^ Use of `ext::` requires Surrealism extensions
"""

*/

RETURN ext::nonexistent();
