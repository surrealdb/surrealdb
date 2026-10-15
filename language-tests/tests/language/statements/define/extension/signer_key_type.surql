/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = """A key SIGNER accepts `ssh-ed25519` and `ecdsa-sha2-nistp256` public keys."""

[test.results]
parsing-error = """
Invalid `SIGNER` key
  --> [24:72]
   |
24 | ...ER { key: "ssh-rsa AAAAB3NzaC1yc2E release@acme" };
   |              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ The key type must be `ssh-ed25519` or `ecdsa-sha2-nistp256`
"""

*/

DEFINE EXTENSION ext::color FROM f"modules:/color.surli" SIGNER { key: "ssh-rsa AAAAB3NzaC1yc2E release@acme" };
