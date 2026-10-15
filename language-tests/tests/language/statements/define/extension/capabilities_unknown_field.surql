/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = """Unknown CAPABILITIES fields are refused, so a misspelt or long-form name such as `allow_net` cannot be silently ignored."""

[test.results]
parsing-error = """
Unknown field `allow_net` in `CAPABILITIES`
  --> [24:82]
   |
24 | ...ILITIES { allow_net: ["api.color.io:443"] };
   |              ^^^^^^^^^ Expected one of `net`, `functions`, `queries`, `scripting`, `write`, `memory`, `timeout`, `strict_timeout`
"""

*/

DEFINE EXTENSION ext::color FROM f"modules:/color.surli" UNSIGNED CAPABILITIES { allow_net: ["api.color.io:443"] };
