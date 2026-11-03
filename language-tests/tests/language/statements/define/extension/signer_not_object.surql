/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = """SIGNER takes an object literal; an expression would make the trust decision depend on evaluation."""

[test.results]
parsing-error = """
Expected an object for `SIGNER`
  --> [24:65]
   |
24 | ...i" SIGNER $signer;
   |              ^^^^^^^
"""

*/

DEFINE EXTENSION ext::color FROM f"modules:/color.surli" SIGNER $signer;
