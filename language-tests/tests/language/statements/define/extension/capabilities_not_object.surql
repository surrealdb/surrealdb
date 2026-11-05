/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = """CAPABILITIES takes an object literal."""

[test.results]
parsing-error = """
Expected an object for `CAPABILITIES`
  --> [24:80]
   |
24 | ...ABILITIES ["net"];
   |              ^^^^^^^
"""

*/

DEFINE EXTENSION ext::color FROM f"modules:/color.surli" UNSIGNED CAPABILITIES ["net"];
