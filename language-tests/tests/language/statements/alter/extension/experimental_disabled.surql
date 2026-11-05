/**
[env.capabilities]
allow-experimental = []

[test]
reason = "ALTER EXTENSION is rejected at parse time when Surrealism is not enabled, with the caret on the EXTENSION keyword."

[test.results]
parsing-error = """
Surrealism extensions are not enabled
  --> [19:7]
   |
19 | ALTER EXTENSION ext::test COMMENT 'updated';
   |       ^^^^^^^^^ Use of `ALTER EXTENSION` requires Surrealism extensions
"""

*/

ALTER EXTENSION ext::test COMMENT 'updated';
