/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = """BINDINGS takes an object literal."""

[test.results]
parsing-error = """
Expected an object for `BINDINGS`
  --> [23:78]
   |
23 | ... BINDINGS ["api_key"];
   |              ^^^^^^^^^^^
"""
*/

DEFINE EXTENSION ext::mailer FROM f"modules:/mailer.surli" UNSIGNED BINDINGS ["api_key"];
