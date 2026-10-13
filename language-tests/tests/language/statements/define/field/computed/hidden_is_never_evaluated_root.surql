/**
[env]
imports = ["language/statements/define/field/computed/hidden_is_never_evaluated_import.surql"]

[test]
reason = """
	The root principal sees every field, so a read derives every computed
	field, as it did before a hidden field went underived for a checked caller
	(register #2). `bad` is derived like the others and its body, which writes
	through `fn::r`, is refused by the COMPUTED frame.
"""

[[test.results]]
error = "A COMPUTED clause cannot contain a statement that modifies data"
*/

SELECT * FROM doc;
