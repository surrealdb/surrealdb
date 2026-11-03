/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = """A name bound twice is refused rather than resolved by order."""

[test.results]
parsing-error = """
Duplicate field `a` in `BINDINGS`
  --> [23:86]
   |
23 | ...S { a: 1, a: 2 };
   |              ^
"""
*/

DEFINE EXTENSION ext::mailer FROM f"modules:/mailer.surli" UNSIGNED BINDINGS { a: 1, a: 2 };
