/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = """Repository and owner IDs are the numeric IDs the certificate carries, written as strings of decimal digits; a name would be reusable."""

[test.results]
parsing-error = """
Expected a string of decimal digits for `owner_id`
  --> [25:175]
   |
25 | ...owner_id: "acme" } };
   |              ^^^^^^
"""


*/

DEFINE EXTENSION ext::color FROM f"modules:/color.surli" SIGNER { sigstore: { ca: ["Zm9vYmFy"], log: "bG9na2V5", tsa: ["dHNhY2VydA=="], issuer: "i", identity: "x", owner_id: "acme" } };
