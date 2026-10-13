/**
[env.capabilities]
allow-experimental = []

[test]
reason = "DEFINE MODULE is rejected at parse time when the `surrealism` experimental capability is absent, with the caret on the MODULE keyword. The gate is purely a runtime check on that capability and does not depend on the `surrealism` cargo feature; capability_granted.surql covers the granted case."

[test.results]
parsing-error = """
Surrealism modules are not enabled
  --> [18:8]
   |
18 | DEFINE MODULE mod::test AS f"module.wasm";
   |        ^^^^^^ Use of `DEFINE MODULE` requires Surrealism modules
"""
*/

DEFINE MODULE mod::test AS f"module.wasm";
