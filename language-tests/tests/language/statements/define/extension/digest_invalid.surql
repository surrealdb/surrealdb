/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = """DIGEST is `sha256:` followed by 64 lowercase hexadecimal digits."""

[test.results]
parsing-error = """
Invalid module digest
  --> [24:65]
   |
24 | ...i" DIGEST "sha256:5D5D" UNSIGNED;
   |              ^^^^^^^^^^^^^ Expected `sha256:` followed by 64 lowercase hexadecimal digits
"""

*/

DEFINE EXTENSION ext::color FROM f"modules:/color.surli" DIGEST "sha256:5D5D" UNSIGNED;
