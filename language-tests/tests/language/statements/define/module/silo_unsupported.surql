/**
[env.capabilities]
allow-experimental = []

[test]
reason = "The DEFINE MODULE gate fires before the module executable is parsed, so the silo executable form `organisation::package<major.minor.patch>` is unreachable while the `surrealism` experimental capability is ungranted and the caret stays on the MODULE keyword."

[test.results]
parsing-error = """
Surrealism modules are not enabled
  --> [18:8]
   |
18 | DEFINE MODULE silo::acme::pkg::<1.0.0>;
   |        ^^^^^^ Use of `DEFINE MODULE` requires Surrealism modules
"""
*/

DEFINE MODULE silo::acme::pkg::<1.0.0>;
