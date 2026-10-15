/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = """SIGNER holds exactly one of `sigstore` and `key`: an archive carries one signature, so a definition names one anchor."""

[test.results]
parsing-error = """
Invalid `SIGNER`
  --> [24:65]
   |
24 | ...i" SIGNER { key: "ssh-ed25519 AAAA", sigstore: {} };
   |              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Expected exactly one of `sigstore` and `key`
"""

*/

DEFINE EXTENSION ext::color FROM f"modules:/color.surli" SIGNER { key: "ssh-ed25519 AAAA", sigstore: {} };
