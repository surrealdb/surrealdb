/**
[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = "In a build compiled without the `surrealism` cargo feature the DEFINE MODULE gate never consults the experimental capability: granting `surrealism` yields a rejection byte-identical to the capability-off case, so a user has no in-language way to reach the statement. With that feature compiled in, the granted capability satisfies the gate and the statement parses, so no parsing error is reported at all."

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
