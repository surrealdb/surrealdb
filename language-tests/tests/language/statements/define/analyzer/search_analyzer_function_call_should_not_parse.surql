/**
[env.capabilities]
allow-experimental = []

[test]

[test.results]
parsing-error = """
Analyzer FUNCTION expects a function name, not a function call
  --> [17:66]
   |
17 | ...e_function();
   |              ^ Remove `()` from the analyzer FUNCTION clause (for example: `fn::some` or `ext::demo::some`)
"""
*/

DEFINE ANALYZER tries_to_call_function FUNCTION fn::some_function();