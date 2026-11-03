/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = """BINDINGS is one object; a second clause is refused rather than merged or replacing the first."""

[test.results]
parsing-error = """
Conflicting clause on this extension definition
  --> [27:81]
   |
27 | ...NDINGS {} BINDINGS { a: 1 };
   |              ^^^^^^^^ An extension definition takes one `BINDINGS` object
  --> [27:69]
   |
27 | ... UNSIGNED BINDINGS {} BINDINGS { a: 1 };
   |              ^^^^^^^^ First given here
"""
*/

DEFINE EXTENSION ext::mailer FROM f"modules:/mailer.surli" UNSIGNED BINDINGS {} BINDINGS { a: 1 };
