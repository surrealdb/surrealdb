/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = """A Sigstore SIGNER needs at least one certificate authority certificate to chain to."""

[test.results]
parsing-error = """
Invalid `ca`
  --> [25:83]
   |
25 | ...re: { ca: [], log: "bG9na2V5", tsa: ["dHNhY2VydA=="], issuer: "i", identit...
   |              ^^ Expected at least one certificate authority certificate
"""


*/

DEFINE EXTENSION ext::color FROM f"modules:/color.surli" SIGNER { sigstore: { ca: [], log: "bG9na2V5", tsa: ["dHNhY2VydA=="], issuer: "i", identity: "x" } };
