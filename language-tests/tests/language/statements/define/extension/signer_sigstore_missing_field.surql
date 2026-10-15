/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = """A Sigstore SIGNER needs its anchor (`ca`, `log`) and its identity rule (`issuer`, `identity`): a certificate authority vouches for many identities, so an anchor without a rule would accept any of them."""

[test.results]
parsing-error = """
Missing field `identity` in `SIGNER { sigstore }`
  --> [24:77]
   |
24 | ...sigstore: { ca: ["Zm9vYmFy"], log: "bG9na2V5", issuer: "https://token.acti...
   |              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
"""

*/

DEFINE EXTENSION ext::color FROM f"modules:/color.surli" SIGNER { sigstore: { ca: ["Zm9vYmFy"], log: "bG9na2V5", issuer: "https://token.actions.githubusercontent.com" } };
