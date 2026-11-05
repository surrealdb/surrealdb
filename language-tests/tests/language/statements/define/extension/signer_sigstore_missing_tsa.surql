/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = """`tsa` is required in a Sigstore SIGNER, like `ca` and `log`: without a timestamp authority no signing time can be trusted."""

[test.results]
parsing-error = """
Missing field `tsa` in `SIGNER { sigstore }`
  --> [25:77]
   |
25 | ...sigstore: { ca: ["Zm9vYmFy"], log: "bG9na2V5", issuer: "i", identity: "x" } };
   |              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
"""


*/

DEFINE EXTENSION ext::color FROM f"modules:/color.surli" SIGNER { sigstore: { ca: ["Zm9vYmFy"], log: "bG9na2V5", issuer: "i", identity: "x" } };
