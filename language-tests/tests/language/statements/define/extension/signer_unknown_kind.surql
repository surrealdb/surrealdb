/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = """SIGNER holds exactly one of `sigstore` and `key`."""

[test.results]
parsing-error = """
Unknown field `x509` in `SIGNER`
  --> [24:67]
   |
24 | ... SIGNER { x509: "abc" };
   |              ^^^^ Expected exactly one of `sigstore` and `key`
"""

*/

DEFINE EXTENSION ext::color FROM f"modules:/color.surli" SIGNER { x509: "abc" };
