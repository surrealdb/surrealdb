/**
[env.capabilities]
allow-experimental = []

[test]
reason = "DEFINE MODULE is rejected at parse time when Surrealism modules are not enabled, with the caret on the MODULE keyword. In a build compiled without the `surrealism` cargo feature the rejection is unconditional; with that feature compiled in, the same diagnostic is reported whenever the `surrealism` experimental capability is absent."

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
