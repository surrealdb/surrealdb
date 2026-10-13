/**
[env.capabilities]
allow-experimental = []

[test]
reason = "The DEFINE MODULE gate fires before OVERWRITE is consumed, so the clause does not change the rejection and the caret stays on the MODULE keyword rather than moving to OVERWRITE."

[test.results]
parsing-error = """
Surrealism modules are not enabled
  --> [18:8]
   |
18 | DEFINE MODULE OVERWRITE mod::test AS f"module.wasm";
   |        ^^^^^^ Use of `DEFINE MODULE` requires Surrealism modules
"""
*/

DEFINE MODULE OVERWRITE mod::test AS f"module.wasm";
