/**
[env.capabilities]
allow-experimental = []

[test]
reason = "DEFINE EXTENSION is rejected at parse time when the `surrealism` experimental capability is absent, with the caret on the EXTENSION keyword."

[test.results]
parsing-error = """
Surrealism extensions are not enabled
  --> [19:8]
   |
19 | DEFINE EXTENSION ext::test FROM f"module.wasm" UNSIGNED;
   |        ^^^^^^^^^ Use of `DEFINE EXTENSION` requires Surrealism extensions
"""

*/

DEFINE EXTENSION ext::test FROM f"module.wasm" UNSIGNED;
