/**
[test]

[test.results]
parsing-error = """
Experimental capability `record_references` is not enabled
  --> [16:26]
   |
16 | DEFINE FIELD c ON b TYPE references<a>;
   |                          ^^^^^^^^^^ Use of `REFERENCES` keyword is still experimental
"""

*/

DEFINE TABLE a;
DEFINE FIELD c ON b TYPE references<a>;
