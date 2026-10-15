/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = """A Sigstore SIGNER needs at least one timestamp authority certificate: Rekor v2 records no time, so the signing time the short-lived certificate is checked against comes only from an RFC 3161 timestamp."""

[test.results]
parsing-error = """
Invalid `tsa`
  --> [25:119]
   |
25 | ...V5", tsa: [], issuer: "i", identity: "x" } };
   |              ^^ Expected at least one timestamp authority certificate: Rekor v2 records no time, so the signing time comes from an RFC 3161 timestamp
"""


*/

DEFINE EXTENSION ext::color FROM f"modules:/color.surli" SIGNER { sigstore: { ca: ["Zm9vYmFy"], log: "bG9na2V5", tsa: [], issuer: "i", identity: "x" } };
