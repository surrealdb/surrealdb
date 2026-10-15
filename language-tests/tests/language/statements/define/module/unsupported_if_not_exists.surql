/**
[env.capabilities]
allow-experimental = []

[test]
reason = "The DEFINE MODULE gate fires before IF NOT EXISTS is consumed, so the clause does not soften the rejection and the caret stays on the MODULE keyword rather than moving to IF."

[test.results]
parsing-error = """
Surrealism extensions are not enabled
  --> [18:8]
   |
18 | DEFINE MODULE IF NOT EXISTS mod::test AS f"module.wasm";
   |        ^^^^^^ Use of `DEFINE MODULE` requires Surrealism extensions
"""
*/

DEFINE MODULE IF NOT EXISTS mod::test AS f"module.wasm";
