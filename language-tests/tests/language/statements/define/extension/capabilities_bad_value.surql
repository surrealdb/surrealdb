/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = """Each CAPABILITIES field takes literals of one shape; `memory` is a positive number of bytes."""

[test.results]
parsing-error = """
Invalid `memory`
  --> [24:90]
   |
24 | ...{ memory: -1 };
   |              ^^ Expected a positive integer number of bytes
"""

*/

DEFINE EXTENSION ext::color FROM f"modules:/color.surli" UNSIGNED CAPABILITIES { memory: -1 };
