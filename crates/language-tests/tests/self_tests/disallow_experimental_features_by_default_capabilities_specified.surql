/**
[test]
reason = "Test for bug where experimental features would all be enabled when the capabilities table was specified"

[test.results]
parsing-error = """
Experimental capability `record_references` is not enabled
  --> [20:26]
   |
20 | DEFINE FIELD c ON b TYPE references<a>;
   |                          ^^^^^^^^^^ Use of `REFERENCES` keyword is still experimental
"""

[env.capabilities]
scripting = true

*/

DEFINE TABLE a;
DEFINE FIELD c ON b TYPE references<a>;
