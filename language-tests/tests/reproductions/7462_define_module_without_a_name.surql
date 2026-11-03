/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = "DEFINE MODULE with an executable and no name is a parse error, not an internal 'unreachable logic' error at execution."
issue = 7462

[test.results]
parsing-error = """
Unexpected token `a file`, expected an extension name
  --> [23:15]
   |
23 | DEFINE MODULE f"bucket:/x.surli";
   |               ^^^^^^^^^^^^^^^^^^
"""
*/
DEFINE MODULE f"bucket:/x.surli";
