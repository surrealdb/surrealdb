/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = """Unknown fields in a Sigstore SIGNER are refused, so a misspelt identity rule cannot be silently ignored."""

[test.results]
parsing-error = """
Unknown field `repo_id` in `SIGNER { sigstore }`
  --> [25:165]
   |
25 | ...ity: "x", repo_id: "1" } };
   |              ^^^^^^^ Expected one of `ca`, `log`, `tsa`, `issuer`, `identity`, `repository_id`, `owner_id`
"""


*/

DEFINE EXTENSION ext::color FROM f"modules:/color.surli" SIGNER { sigstore: { ca: ["Zm9vYmFy"], log: "bG9na2V5", tsa: ["dHNhY2VydA=="], issuer: "i", identity: "x", repo_id: "1" } };
