/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = """CAPABILITIES is one object; a second clause is refused rather than merged or replacing the first."""

[test.results]
parsing-error = """
Conflicting clause on this extension definition
  --> [28:83]
   |
28 | ...LITIES {} CAPABILITIES { write: true };
   |              ^^^^^^^^^^^^ An extension definition takes one `CAPABILITIES` object
  --> [28:67]
   |
28 | ... UNSIGNED CAPABILITIES {} CAPABILITIES { write: true };
   |              ^^^^^^^^^^^^ First given here
"""

*/

DEFINE EXTENSION ext::color FROM f"modules:/color.surli" UNSIGNED CAPABILITIES {} CAPABILITIES { write: true };
