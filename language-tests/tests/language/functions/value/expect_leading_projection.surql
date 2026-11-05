/**
[test]
reason = "A `value::` path call cannot open a SELECT projection list. The parser reserves the leading word `value` there for the SELECT VALUE clause, so the following `::` has nowhere to attach and the whole statement fails to parse. The restriction is positional and applies to every `value::` function, and equally to a plain field named `value`; parenthesising the call or moving it past another projection entry parses"

[test.results]
parsing-error = """
Unexpected token `::`, expected an expression
  --> [15:13]
   |
15 | SELECT value::expect(age, |$a| $a > 5) AS age FROM person;
   |             ^^
"""
*/

SELECT value::expect(age, |$a| $a > 5) AS age FROM person;
