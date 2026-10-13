/**
[env.capabilities]
allow-experimental = []

[test]
reason = "An analyzer FUNCTION reference to a `mod::` module function is rejected at parse time when Surrealism modules are not enabled, with the caret on the `mod` keyword."

[test.results]
parsing-error = """
Surrealism modules are not enabled
  --> [18:30]
   |
18 | DEFINE ANALYZER ana FUNCTION mod::demo::alter TOKENIZERS class;
   |                              ^^^ Use of `mod::` requires Surrealism modules
"""
*/

DEFINE ANALYZER ana FUNCTION mod::demo::alter TOKENIZERS class;
