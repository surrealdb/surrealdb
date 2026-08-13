/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = "The UNSIGNED requirement is not specific to the file executable form. Silo packages are not signed yet either, so a silo executable is rejected the same way."

[test.results]
parsing-error = """
Expected the `UNSIGNED` keyword on this module definition
  --> [23:38]
   |
23 | DEFINE MODULE silo::acme::pkg::<1.0.0>;
   |                                      ^ Surrealism modules are not signature-verified yet, so a module must be defined as `UNSIGNED`
"""
*/

DEFINE MODULE silo::acme::pkg::<1.0.0>;
