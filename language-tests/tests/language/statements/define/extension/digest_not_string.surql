/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = """DIGEST takes a string literal, not an expression."""

[test.results]
parsing-error = """
Unexpected token `a parameter`, expected a strand
  --> [24:65]
   |
24 | ...i" DIGEST $digest UNSIGNED;
   |              ^^^^^^^
"""

*/

DEFINE EXTENSION ext::color FROM f"modules:/color.surli" DIGEST $digest UNSIGNED;
