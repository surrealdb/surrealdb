/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = """A definition names its signer with SIGNER or waives verification with UNSIGNED, never both."""

[test.results]
parsing-error = """
Conflicting clause on this extension definition
  --> [28:93]
   |
28 | ...9 AAAA" } UNSIGNED;
   |              ^^^^^^^^ Remove one of `SIGNER` and `UNSIGNED`
  --> [28:58]
   |
28 | ...or.surli" SIGNER { key: "ssh-ed25519 AAAA" } UNSIGNED;
   |              ^^^^^^ First given here
"""

*/

DEFINE EXTENSION ext::color FROM f"modules:/color.surli" SIGNER { key: "ssh-ed25519 AAAA" } UNSIGNED;
