/**
[env.capabilities]
allow-experimental = []

[test]
reason = "REMOVE EXTENSION is rejected at parse time when Surrealism is not enabled, with the caret on the EXTENSION keyword."

[test.results]
parsing-error = """
Surrealism extensions are not enabled
  --> [19:8]
   |
19 | REMOVE EXTENSION ext::test;
   |        ^^^^^^^^^ Use of `REMOVE EXTENSION` requires Surrealism extensions
"""

*/

REMOVE EXTENSION ext::test;
