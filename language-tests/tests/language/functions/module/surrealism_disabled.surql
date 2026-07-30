/**
[env.capabilities]
allow-experimental = []

[test]
reason = "A `mod::` function call is rejected at parse time when Surrealism modules are not enabled, with the caret on the `mod` keyword."

[test.results]
parsing-error = """
Surrealism modules are not enabled
  --> [19:8]
   |
19 | RETURN mod::nonexistent();
   |        ^^^ Use of `mod::` requires Surrealism modules
"""
*/

-- Try to call a module function (requires experimental capability)
RETURN mod::nonexistent();
