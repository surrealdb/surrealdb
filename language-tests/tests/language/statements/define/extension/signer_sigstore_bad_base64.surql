/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = """Certificates and keys in a Sigstore SIGNER are standard base64 of their DER encoding."""

[test.results]
parsing-error = """
Expected standard base64 for `ca`
  --> [25:84]
   |
25 | ...e: { ca: ["not base64!"], log: "bG9na2V5", tsa: ["dHNhY2VydA=="], issuer: ...
   |              ^^^^^^^^^^^^^
"""


*/

DEFINE EXTENSION ext::color FROM f"modules:/color.surli" SIGNER { sigstore: { ca: ["not base64!"], log: "bG9na2V5", tsa: ["dHNhY2VydA=="], issuer: "i", identity: "x" } };
