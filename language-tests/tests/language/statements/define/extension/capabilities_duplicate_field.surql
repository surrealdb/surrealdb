/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = """A CAPABILITIES field given twice is refused rather than resolved by order."""

[test.results]
parsing-error = """
Duplicate field `write` in `CAPABILITIES`
  --> [24:96]
   |
24 | ...e: false, write: true };
   |              ^^^^^
"""

*/

DEFINE EXTENSION ext::color FROM f"modules:/color.surli" UNSIGNED CAPABILITIES { write: false, write: true };
