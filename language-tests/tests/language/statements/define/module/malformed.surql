/**
[env.capabilities]
allow-experimental = []

[test]
reason = "A DEFINE MODULE with no module executable reports the unavailability diagnostic rather than a syntax error, because the gate precedes all syntax validation: malformed and well-formed DEFINE MODULE are indistinguishable while it fires. The gate consults the `surrealism` experimental capability, which this test leaves ungranted. Only with the capability granted does parsing reach the statement's own grammar and report a missing module executable."

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
