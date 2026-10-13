/**
[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = "With the `surrealism` experimental capability granted, REMOVE MODULE reaches its own grammar and reports a missing module name; the capability gate would otherwise report first, so this diagnostic is only observable once the capability is enabled."

[test.results]
parsing-error = """
Unexpected token `;`, expected a module name
  --> [18:14]
   |
18 | REMOVE MODULE;
   |              ^
"""
*/

REMOVE MODULE;
