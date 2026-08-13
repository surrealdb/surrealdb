/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = "No module source is signature-verified yet, so DEFINE MODULE requires the trailing UNSIGNED clause and rejects a definition that omits it. Requiring the opt-out now rather than once verification exists means a definition written today keeps its meaning when the keyword starts gating a real check."

[test.results]
parsing-error = """
Expected the `UNSIGNED` keyword on this module definition
  --> [23:28]
   |
23 | DEFINE MODULE mod::test AS f"bucket:/module.wasm";
   |                            ^^^^^^^^^^^^^^^^^^^^^^ Surrealism modules are not signature-verified yet, so a module must be defined as `UNSIGNED`
"""
*/

DEFINE MODULE mod::test AS f"bucket:/module.wasm";
