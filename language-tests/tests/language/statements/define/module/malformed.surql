/**
[env.capabilities]
allow-experimental = []

[test]
reason = "A DEFINE MODULE with no module executable reports the unavailability diagnostic rather than a syntax error, because the gate precedes all syntax validation: malformed and well-formed DEFINE MODULE are indistinguishable while it fires. In a build compiled without the `surrealism` cargo feature the gate is unconditional; with that feature compiled in it consults the `surrealism` experimental capability, which this test leaves ungranted, so the same diagnostic is reported. Only a feature-on build with the capability granted reaches the statement's own grammar and reports a missing module executable."

[test.results]
parsing-error = """
Surrealism modules are not enabled
  --> [18:8]
   |
18 | DEFINE MODULE;
   |        ^^^^^^ Use of `DEFINE MODULE` requires Surrealism modules
"""
*/

DEFINE MODULE;
