/**
[test]
reason = "A SELECT is split across workers by the planner from what it reads, never by a clause: PARALLEL is not part of the statement's syntax, so the statement is a parse error rather than a request the planner ignores."
issue = 5171

[test.results]
parsing-error = """
Unexpected token `PARALLEL`, expected Eof
  --> [16:106]
   |
16 | ... person:1 PARALLEL;
   |              ^^^^^^^^
"""
*/

SELECT VALUE array::sort(array::distinct(->order->product<-order<-person->order->product)) FROM person:1 PARALLEL;
